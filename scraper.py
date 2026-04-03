"""
Web scraper for Idealista Madrid real estate listings.

Hybrid fetching strategy (cost optimization):
1. First tries DIRECT request via curl_cffi (free, impersonates Chrome TLS)
2. Falls back to Bright Data Web Unlocker API only when direct fails

Optimizations applied:
- No redundant district-level scraping (barrios cover all territory)
- Early exit when barrio was already fully processed today
- Smart pagination using historical page counts per barrio
- Intelligent HTTP retry (only retries on transient errors, not 404/502)
- Integrated retry mode for failed barrios (--retry flag)
- Configurable description fetching (disabled by default)
- Per-phase request tracking (venta / alquiler / retry)
- Rental scraping frequency control (RENTAL_SCRAPE_INTERVAL_DAYS)
- Per-barrio "already scraped today" guard to avoid double-runs
- Request budget cap (BRIGHTDATA_REQUEST_BUDGET) to prevent runaway costs
- HYBRID mode: direct+fallback saves 70-90% of proxy costs
"""

import os
import re
import sys
import time
import json
import random
from typing import Dict, List, Optional
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import urllib3

# curl_cffi: browser-grade TLS fingerprinting (free, no proxy needed)
try:
    from curl_cffi import requests as cffi_requests
    HAS_CURL_CFFI = True
except ImportError:
    HAS_CURL_CFFI = False
    print("⚠ curl_cffi not installed — falling back to BrightData for all requests")
    print("  Install with: pip install curl_cffi")

# Disable SSL warnings when using Bright Data proxy
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from database import (
    init_database,
    get_active_listing_ids,
    insert_listing,
    update_listing,
    mark_as_sold,
    mark_stale_as_sold,
    migrate_create_scraping_log_table,
    migrate_create_rental_prices_table,
    log_scraping_execution,
    upsert_rental_snapshot,
)


# Load environment variables
load_dotenv()

BRIGHTDATA_USER = os.getenv('BRIGHTDATA_USER')
BRIGHTDATA_PASS = os.getenv('BRIGHTDATA_PASS')
BRIGHTDATA_HOST = os.getenv('BRIGHTDATA_HOST', 'brd.superproxy.io:33335')

BASE_URL = "https://www.idealista.com"

# ============================================================================
# OPTIMIZATION CONFIGURATION
# ============================================================================

# Enable/disable fetching individual property descriptions (1 extra request per listing!)
FETCH_DESCRIPTIONS = os.getenv('FETCH_DESCRIPTIONS', 'false').lower() == 'true'

# File to persist page count history per barrio (for smart pagination)
PAGE_HISTORY_FILE = "barrio_page_history.json"

# Margin to add to historical max pages (safety buffer).
# Reduced from 2 to 1: saves 139 requests/day. The extra page is almost always 404.
PAGE_HISTORY_MARGIN = int(os.getenv('PAGE_HISTORY_MARGIN', '1'))

# Maximum pages to scrape per barrio (hard limit)
MAX_PAGES_PER_BARRIO = int(os.getenv('MAX_PAGES_PER_BARRIO', '30'))

# Early exit: if this % of listings on page 1 were already seen today, skip remaining pages
# Reduced from 90% to 80%: saves ~30 requests/day on multi-page barrios
EARLY_EXIT_THRESHOLD = float(os.getenv('EARLY_EXIT_THRESHOLD', '0.80'))

# ---------------------------------------------------------------------------
# LOW-ACTIVITY BARRIO FREQUENCY
# ---------------------------------------------------------------------------
# Barrios with max_pages <= this threshold are "low-activity".
# They are scraped every LOW_ACTIVITY_INTERVAL days instead of daily.
# Saves ~35 requests/day on average (71 barrios with 1 page).
# Does NOT affect price history: existing listings stay in DB.
# Only risk: a new listing published & sold within the skip window is missed.
LOW_ACTIVITY_MAX_PAGES = int(os.getenv('LOW_ACTIVITY_MAX_PAGES', '1'))
LOW_ACTIVITY_INTERVAL_DAYS = int(os.getenv('LOW_ACTIVITY_INTERVAL_DAYS', '2'))

# File tracking when each low-activity barrio was last scraped
LOW_ACTIVITY_TRACKER_FILE = "low_activity_last_scraped.json"

# ---------------------------------------------------------------------------
# COST CONTROL
# ---------------------------------------------------------------------------

# Maximum BrightData requests allowed per run (0 = unlimited).
# If exceeded mid-run, scraping stops gracefully and logs a warning.
# Set via env var to protect against runaway costs.
BRIGHTDATA_REQUEST_BUDGET = int(os.getenv('BRIGHTDATA_REQUEST_BUDGET', '0'))

# ---------------------------------------------------------------------------
# FETCH MODE
# ---------------------------------------------------------------------------
# 'hybrid'  : try direct (curl_cffi) first, fallback to BrightData on failure
# 'direct'  : only direct requests (no BrightData at all — risky but free)
# 'proxy'   : all requests via BrightData (most reliable, ~$0.004/req)
#
# Default changed to 'proxy' (2026-03-30): Idealista's anti-bot is now blocking
# direct requests frequently, causing incomplete scrapes on ~30% of days.
# Consistent data quality is worth the extra cost (~$2-3/day).
FETCH_MODE = os.getenv('FETCH_MODE', 'proxy').lower()

# Seconds to wait between direct requests (politeness delay to avoid bans)
DIRECT_REQUEST_DELAY = float(os.getenv('DIRECT_REQUEST_DELAY', '2.0'))

# After this many consecutive direct failures, switch to proxy-only for rest of run
DIRECT_FAIL_THRESHOLD = int(os.getenv('DIRECT_FAIL_THRESHOLD', '10'))

# How often to re-scrape rental prices (days). Rental data changes slowly;
# daily scraping wastes ~139 requests per run. Default: every 7 days.
RENTAL_SCRAPE_INTERVAL_DAYS = int(os.getenv('RENTAL_SCRAPE_INTERVAL_DAYS', '7'))

# File that records the last date rental scraping ran successfully.
RENTAL_LAST_SCRAPED_FILE = "rental_last_scraped.txt"


def load_page_history() -> Dict:
    """Load historical page counts per barrio from JSON file."""
    try:
        if Path(PAGE_HISTORY_FILE).exists():
            with open(PAGE_HISTORY_FILE, 'r') as f:
                return json.load(f)
    except (json.JSONDecodeError, IOError):
        pass
    return {}


def save_page_history(history: Dict) -> None:
    """Save historical page counts per barrio to JSON file."""
    try:
        with open(PAGE_HISTORY_FILE, 'w') as f:
            json.dump(history, f, indent=2, ensure_ascii=False)
    except IOError as e:
        print(f"  ⚠ Could not save page history: {e}")


def get_max_pages_for_barrio(history: Dict, barrio_key: str) -> int:
    """
    Get the smart page limit for a barrio based on history.
    Returns historical max + margin, or MAX_PAGES_PER_BARRIO if no history.
    """
    if barrio_key in history:
        historical_max = history[barrio_key].get('max_pages', MAX_PAGES_PER_BARRIO)
        return min(historical_max + PAGE_HISTORY_MARGIN, MAX_PAGES_PER_BARRIO)
    return MAX_PAGES_PER_BARRIO


# ---------------------------------------------------------------------------
# LOW-ACTIVITY BARRIO FREQUENCY CONTROL
# ---------------------------------------------------------------------------

def _load_low_activity_tracker() -> Dict:
    """Load last-scraped dates for low-activity barrios."""
    try:
        if Path(LOW_ACTIVITY_TRACKER_FILE).exists():
            with open(LOW_ACTIVITY_TRACKER_FILE, 'r') as f:
                return json.load(f)
    except (json.JSONDecodeError, IOError):
        pass
    return {}


def _save_low_activity_tracker(tracker: Dict) -> None:
    """Persist last-scraped dates for low-activity barrios."""
    try:
        with open(LOW_ACTIVITY_TRACKER_FILE, 'w') as f:
            json.dump(tracker, f, indent=2, ensure_ascii=False)
    except IOError as e:
        print(f"  ⚠ Could not save low-activity tracker: {e}")


def should_skip_low_activity(barrio_key: str, page_history: Dict,
                              tracker: Dict) -> bool:
    """
    Check if a low-activity barrio can be skipped today.

    A barrio is "low-activity" if its historical max_pages <= LOW_ACTIVITY_MAX_PAGES.
    These barrios are scraped every LOW_ACTIVITY_INTERVAL_DAYS instead of daily.

    Returns True if the barrio should be SKIPPED (was scraped recently enough).
    """
    if LOW_ACTIVITY_INTERVAL_DAYS <= 1:
        return False  # Feature disabled (scrape daily)

    # Check if this barrio qualifies as low-activity
    info = page_history.get(barrio_key, {})
    max_pages = info.get('max_pages', MAX_PAGES_PER_BARRIO)
    if max_pages > LOW_ACTIVITY_MAX_PAGES:
        return False  # Active barrio — always scrape

    # Check when it was last scraped
    last_scraped = tracker.get(barrio_key)
    if not last_scraped:
        return False  # Never tracked — scrape now

    try:
        last_date = datetime.strptime(last_scraped, "%Y-%m-%d").date()
        days_since = (datetime.utcnow().date() - last_date).days
        return days_since < LOW_ACTIVITY_INTERVAL_DAYS
    except (ValueError, TypeError):
        return False  # Bad date — scrape to be safe


def update_page_history(history: Dict, barrio_key: str, pages_found: int) -> None:
    """Update historical page count and last-scraped date for a barrio."""
    today = datetime.now().strftime("%Y-%m-%d")
    if barrio_key not in history:
        history[barrio_key] = {'max_pages': pages_found, 'last_updated': today, 'last_scraped': today}
    else:
        history[barrio_key]['max_pages'] = max(
            history[barrio_key].get('max_pages', 0),
            pages_found
        )
        history[barrio_key]['last_updated'] = today
        history[barrio_key]['last_scraped'] = today


def was_barrio_scraped_today(history: Dict, barrio_key: str) -> bool:
    """Return True if this barrio was already fully scraped today."""
    today = datetime.now().strftime("%Y-%m-%d")
    return history.get(barrio_key, {}).get('last_scraped') == today

# Madrid's barrios organized by district (139 total - Centro fixed with compound names)
# Format: (Distrito, Barrio, URL_path)
BARRIO_URLS = [
    # Arganzuela (6 barrios)
    ("Arganzuela", "Acacias", "/venta-viviendas/madrid/arganzuela/acacias/"),
    ("Arganzuela", "Chopera", "/venta-viviendas/madrid/arganzuela/chopera/"),
    ("Arganzuela", "Delicias", "/venta-viviendas/madrid/arganzuela/delicias/"),
    ("Arganzuela", "Imperial", "/venta-viviendas/madrid/arganzuela/imperial/"),
    ("Arganzuela", "Legazpi", "/venta-viviendas/madrid/arganzuela/legazpi/"),
    ("Arganzuela", "Palos de la Frontera", "/venta-viviendas/madrid/arganzuela/palos-de-la-frontera/"),
    
    # Barajas (5 barrios)
    ("Barajas", "Aeropuerto", "/venta-viviendas/madrid/barajas/aeropuerto/"),
    ("Barajas", "Alameda de Osuna", "/venta-viviendas/madrid/barajas/alameda-de-osuna/"),
    ("Barajas", "Campo de las Naciones", "/venta-viviendas/madrid/barajas/campo-de-las-naciones-corralejos/"),
    ("Barajas", "Casco Histórico de Barajas", "/venta-viviendas/madrid/barajas/casco-historico-de-barajas/"),
    ("Barajas", "Timón", "/venta-viviendas/madrid/barajas/timon/"),
    
    # Carabanchel (8 barrios)
    ("Carabanchel", "Abrantes", "/venta-viviendas/madrid/carabanchel/abrantes/"),
    ("Carabanchel", "Buena Vista", "/venta-viviendas/madrid/carabanchel/buena-vista/"),
    ("Carabanchel", "Comillas", "/venta-viviendas/madrid/carabanchel/comillas/"),
    ("Carabanchel", "Opañel", "/venta-viviendas/madrid/carabanchel/opanel/"),
    ("Carabanchel", "PAU de Carabanchel", "/venta-viviendas/madrid/carabanchel/pau-de-carabanchel/"),
    ("Carabanchel", "Puerta Bonita", "/venta-viviendas/madrid/carabanchel/puerta-bonita/"),
    ("Carabanchel", "San Isidro", "/venta-viviendas/madrid/carabanchel/san-isidro/"),
    ("Carabanchel", "Vista Alegre", "/venta-viviendas/madrid/carabanchel/vista-alegre/"),
    
    # Centro (6 barrios)
    ("Centro", "Chueca-Justicia", "/venta-viviendas/madrid/centro/chueca-justicia/"),
    ("Centro", "Huertas-Cortes", "/venta-viviendas/madrid/centro/huertas-cortes/"),
    ("Centro", "Lavapiés-Embajadores", "/venta-viviendas/madrid/centro/lavapies-embajadores/"),
    ("Centro", "Malasaña-Universidad", "/venta-viviendas/madrid/centro/malasana-universidad/"),
    ("Centro", "Palacio", "/venta-viviendas/madrid/centro/palacio/"),
    ("Centro", "Sol", "/venta-viviendas/madrid/centro/sol/"),
    
    # Chamartín (6 barrios)
    ("Chamartín", "Bernabéu-Hispanoamérica", "/venta-viviendas/madrid/chamartin/bernabeu-hispanoamerica/"),
    ("Chamartín", "Castilla", "/venta-viviendas/madrid/chamartin/castilla/"),
    ("Chamartín", "Ciudad Jardín", "/venta-viviendas/madrid/chamartin/ciudad-jardin/"),
    ("Chamartín", "El Viso", "/venta-viviendas/madrid/chamartin/el-viso/"),
    ("Chamartín", "Nueva España", "/venta-viviendas/madrid/chamartin/nueva-espana/"),
    ("Chamartín", "Prosperidad", "/venta-viviendas/madrid/chamartin/prosperidad/"),
    
    # Chamberí (6 barrios)
    ("Chamberí", "Almagro", "/venta-viviendas/madrid/chamberi/almagro/"),
    ("Chamberí", "Arapiles", "/venta-viviendas/madrid/chamberi/arapiles/"),
    ("Chamberí", "Gaztambide", "/venta-viviendas/madrid/chamberi/gaztambide/"),
    ("Chamberí", "Nuevos Ministerios-Ríos Rosas", "/venta-viviendas/madrid/chamberi/nuevos-ministerios-rios-rosas/"),
    ("Chamberí", "Trafalgar", "/venta-viviendas/madrid/chamberi/trafalgar/"),
    ("Chamberí", "Vallehermoso", "/venta-viviendas/madrid/chamberi/vallehermoso/"),
    
    # Ciudad Lineal (9 barrios)
    ("Ciudad Lineal", "Atalaya", "/venta-viviendas/madrid/ciudad-lineal/atalaya/"),
    ("Ciudad Lineal", "Colina", "/venta-viviendas/madrid/ciudad-lineal/colina/"),
    ("Ciudad Lineal", "Concepción", "/venta-viviendas/madrid/ciudad-lineal/concepcion/"),
    ("Ciudad Lineal", "Costillares", "/venta-viviendas/madrid/ciudad-lineal/costillares/"),
    ("Ciudad Lineal", "Pueblo Nuevo", "/venta-viviendas/madrid/ciudad-lineal/pueblo-nuevo/"),
    ("Ciudad Lineal", "Quintana", "/venta-viviendas/madrid/ciudad-lineal/quintana/"),
    ("Ciudad Lineal", "San Juan Bautista", "/venta-viviendas/madrid/ciudad-lineal/san-juan-bautista/"),
    ("Ciudad Lineal", "San Pascual", "/venta-viviendas/madrid/ciudad-lineal/san-pascual/"),
    ("Ciudad Lineal", "Ventas", "/venta-viviendas/madrid/ciudad-lineal/ventas/"),
    
    # Fuencarral-El Pardo (10 barrios)
    ("Fuencarral-El Pardo", "Arroyo del Fresno", "/venta-viviendas/madrid/fuencarral/arroyo-del-fresno/"),
    ("Fuencarral-El Pardo", "El Pardo", "/venta-viviendas/madrid/fuencarral/el-pardo/"),
    ("Fuencarral-El Pardo", "Fuentelarreina", "/venta-viviendas/madrid/fuencarral/fuentelarreina/"),
    ("Fuencarral-El Pardo", "La Paz", "/venta-viviendas/madrid/fuencarral/la-paz/"),
    ("Fuencarral-El Pardo", "Las Tablas", "/venta-viviendas/madrid/fuencarral/las-tablas/"),
    ("Fuencarral-El Pardo", "Mirasierra", "/venta-viviendas/madrid/fuencarral/mirasierra/"),
    ("Fuencarral-El Pardo", "Montecarmelo", "/venta-viviendas/madrid/fuencarral/montecarmelo/"),
    ("Fuencarral-El Pardo", "Peñagrande", "/venta-viviendas/madrid/fuencarral/penagrande/"),
    ("Fuencarral-El Pardo", "Pilar", "/venta-viviendas/madrid/fuencarral/pilar/"),
    ("Fuencarral-El Pardo", "Tres Olivos-Valverde", "/venta-viviendas/madrid/fuencarral/tres-olivos-valverde/"),
    
    # Hortaleza (8 barrios)
    ("Hortaleza", "Apóstol Santiago", "/venta-viviendas/madrid/hortaleza/apostol-santiago/"),
    ("Hortaleza", "Canillas", "/venta-viviendas/madrid/hortaleza/canillas/"),
    ("Hortaleza", "Conde Orgaz-Piovera", "/venta-viviendas/madrid/hortaleza/conde-orgaz-piovera/"),
    ("Hortaleza", "Palomas", "/venta-viviendas/madrid/hortaleza/palomas/"),
    ("Hortaleza", "Pinar del Rey", "/venta-viviendas/madrid/hortaleza/pinar-del-rey/"),
    ("Hortaleza", "Sanchinarro", "/venta-viviendas/madrid/hortaleza/sanchinarro/"),
    ("Hortaleza", "Valdebebas-Valdefuentes", "/venta-viviendas/madrid/hortaleza/valdebebas-valdefuentes/"),
    ("Hortaleza", "Virgen del Cortijo-Manoteras", "/venta-viviendas/madrid/hortaleza/virgen-del-cortijo-manoteras/"),
    
    # Latina (7 barrios)
    ("Latina", "Águilas", "/venta-viviendas/madrid/latina/aguilas/"),
    ("Latina", "Aluche", "/venta-viviendas/madrid/latina/aluche/"),
    ("Latina", "Campamento", "/venta-viviendas/madrid/latina/campamento/"),
    ("Latina", "Cuatro Vientos", "/venta-viviendas/madrid/latina/cuatro-vientos/"),
    ("Latina", "Los Cármenes", "/venta-viviendas/madrid/latina/los-carmenes/"),
    ("Latina", "Lucero", "/venta-viviendas/madrid/latina/lucero/"),
    ("Latina", "Puerta del Ángel", "/venta-viviendas/madrid/latina/puerta-del-angel/"),
    
    # Moncloa-Aravaca (7 barrios)
    ("Moncloa-Aravaca", "Aravaca", "/venta-viviendas/madrid/moncloa/aravaca/"),
    ("Moncloa-Aravaca", "Argüelles", "/venta-viviendas/madrid/moncloa/arguelles/"),
    ("Moncloa-Aravaca", "Casa de Campo", "/venta-viviendas/madrid/moncloa/casa-de-campo/"),
    ("Moncloa-Aravaca", "Ciudad Universitaria", "/venta-viviendas/madrid/moncloa/ciudad-universitaria/"),
    ("Moncloa-Aravaca", "El Plantío", "/venta-viviendas/madrid/moncloa/el-plantio/"),
    ("Moncloa-Aravaca", "Valdemarín", "/venta-viviendas/madrid/moncloa/valdemarin/"),
    ("Moncloa-Aravaca", "Valdezarza", "/venta-viviendas/madrid/moncloa/valdezarza/"),
    
    # Moratalaz (6 barrios)
    ("Moratalaz", "Fontarrón", "/venta-viviendas/madrid/moratalaz/fontarron/"),
    ("Moratalaz", "Horcajo", "/venta-viviendas/madrid/moratalaz/horcajo/"),
    ("Moratalaz", "Marroquina", "/venta-viviendas/madrid/moratalaz/marroquina/"),
    ("Moratalaz", "Media Legua", "/venta-viviendas/madrid/moratalaz/media-legua/"),
    ("Moratalaz", "Pavones", "/venta-viviendas/madrid/moratalaz/pavones/"),
    ("Moratalaz", "Vinateros", "/venta-viviendas/madrid/moratalaz/vinateros/"),
    
    # Puente de Vallecas (6 barrios)
    ("Puente de Vallecas", "Entrevías", "/venta-viviendas/madrid/puente-de-vallecas/entrevias/"),
    ("Puente de Vallecas", "Numancia", "/venta-viviendas/madrid/puente-de-vallecas/numancia/"),
    ("Puente de Vallecas", "Palomeras Bajas", "/venta-viviendas/madrid/puente-de-vallecas/palomeras-bajas/"),
    ("Puente de Vallecas", "Palomeras Sureste", "/venta-viviendas/madrid/puente-de-vallecas/palomeras-sureste/"),
    ("Puente de Vallecas", "Portazgo", "/venta-viviendas/madrid/puente-de-vallecas/portazgo/"),
    ("Puente de Vallecas", "San Diego", "/venta-viviendas/madrid/puente-de-vallecas/san-diego/"),
    
    # Retiro (6 barrios)
    ("Retiro", "Adelfas", "/venta-viviendas/madrid/retiro/adelfas/"),
    ("Retiro", "Estrella", "/venta-viviendas/madrid/retiro/estrella/"),
    ("Retiro", "Ibiza", "/venta-viviendas/madrid/retiro/ibiza/"),
    ("Retiro", "Jerónimos", "/venta-viviendas/madrid/retiro/jeronimos/"),
    ("Retiro", "Niño Jesús", "/venta-viviendas/madrid/retiro/nino-jesus/"),
    ("Retiro", "Pacífico", "/venta-viviendas/madrid/retiro/pacifico/"),
    
    # Salamanca (6 barrios)
    ("Salamanca", "Castellana", "/venta-viviendas/madrid/barrio-de-salamanca/castellana/"),
    ("Salamanca", "Fuente del Berro", "/venta-viviendas/madrid/barrio-de-salamanca/fuente-del-berro/"),
    ("Salamanca", "Goya", "/venta-viviendas/madrid/barrio-de-salamanca/goya/"),
    ("Salamanca", "Guindalera", "/venta-viviendas/madrid/barrio-de-salamanca/guindalera/"),
    ("Salamanca", "Lista", "/venta-viviendas/madrid/barrio-de-salamanca/lista/"),
    ("Salamanca", "Recoletos", "/venta-viviendas/madrid/barrio-de-salamanca/recoletos/"),
    
    # San Blas-Canillejas (8 barrios)
    ("San Blas-Canillejas", "Amposta", "/venta-viviendas/madrid/san-blas/amposta/"),
    ("San Blas-Canillejas", "Arcos", "/venta-viviendas/madrid/san-blas/arcos/"),
    ("San Blas-Canillejas", "Canillejas", "/venta-viviendas/madrid/san-blas/canillejas/"),
    ("San Blas-Canillejas", "Hellín", "/venta-viviendas/madrid/san-blas/hellin/"),
    ("San Blas-Canillejas", "Rejas", "/venta-viviendas/madrid/san-blas/rejas/"),
    ("San Blas-Canillejas", "Rosas", "/venta-viviendas/madrid/san-blas/rosas/"),
    ("San Blas-Canillejas", "Salvador", "/venta-viviendas/madrid/san-blas/salvador/"),
    ("San Blas-Canillejas", "Simancas", "/venta-viviendas/madrid/san-blas/simancas/"),
    
    # Tetuán (6 barrios)
    ("Tetuán", "Bellas Vistas", "/venta-viviendas/madrid/tetuan/bellas-vistas/"),
    ("Tetuán", "Berruguete", "/venta-viviendas/madrid/tetuan/berruguete/"),
    ("Tetuán", "Cuatro Caminos", "/venta-viviendas/madrid/tetuan/cuatro-caminos/"),
    ("Tetuán", "Cuzco-Castillejos", "/venta-viviendas/madrid/tetuan/cuzco-castillejos/"),
    ("Tetuán", "Valdeacederas", "/venta-viviendas/madrid/tetuan/valdeacederas/"),
    ("Tetuán", "Ventilla-Almenara", "/venta-viviendas/madrid/tetuan/ventilla-almenara/"),
    
    # Usera (7 barrios)
    ("Usera", "12 de Octubre-Orcasur", "/venta-viviendas/madrid/usera/12-de-octubre-orcasur/"),
    ("Usera", "Almendrales", "/venta-viviendas/madrid/usera/almendrales/"),
    ("Usera", "Moscardó", "/venta-viviendas/madrid/usera/moscardo/"),
    ("Usera", "Orcasitas", "/venta-viviendas/madrid/usera/orcasitas/"),
    ("Usera", "Pradolongo", "/venta-viviendas/madrid/usera/pradolongo/"),
    ("Usera", "San Fermín", "/venta-viviendas/madrid/usera/san-fermin/"),
    ("Usera", "Zofío", "/venta-viviendas/madrid/usera/zofio/"),
    
    # Vicálvaro (7 barrios)
    ("Vicálvaro", "Ambroz", "/venta-viviendas/madrid/vicalvaro/ambroz/"),
    ("Vicálvaro", "Casco Histórico de Vicálvaro", "/venta-viviendas/madrid/vicalvaro/casco-historico-de-vicalvaro/"),
    ("Vicálvaro", "El Cañaveral", "/venta-viviendas/madrid/vicalvaro/el-canaveral/"),
    ("Vicálvaro", "Los Ahijones", "/venta-viviendas/madrid/vicalvaro/los-ahijones/"),
    ("Vicálvaro", "Los Berrocales", "/venta-viviendas/madrid/vicalvaro/los-berrocales/"),
    ("Vicálvaro", "Los Cerros", "/venta-viviendas/madrid/vicalvaro/los-cerros/"),
    ("Vicálvaro", "Valdebernardo-Valderrivas", "/venta-viviendas/madrid/vicalvaro/valdebernardo-valderrivas/"),
    
    # Villa de Vallecas (4 barrios)
    ("Villa de Vallecas", "Casco Histórico de Vallecas", "/venta-viviendas/madrid/villa-de-vallecas/casco-historico-de-vallecas/"),
    ("Villa de Vallecas", "Ensanche de Vallecas-La Gavia", "/venta-viviendas/madrid/villa-de-vallecas/ensanche-de-vallecas-la-gavia/"),
    ("Villa de Vallecas", "Santa Eugenia", "/venta-viviendas/madrid/villa-de-vallecas/santa-eugenia/"),
    ("Villa de Vallecas", "Valdecarros", "/venta-viviendas/madrid/villa-de-vallecas/valdecarros/"),
    
    # Villaverde (5 barrios)
    ("Villaverde", "Butarque", "/venta-viviendas/madrid/villaverde/butarque/"),
    ("Villaverde", "Los Ángeles", "/venta-viviendas/madrid/villaverde/los-angeles/"),
    ("Villaverde", "Los Rosales", "/venta-viviendas/madrid/villaverde/los-rosales/"),
    ("Villaverde", "San Cristóbal", "/venta-viviendas/madrid/villaverde/san-cristobal/"),
    ("Villaverde", "Villaverde Alto", "/venta-viviendas/madrid/villaverde/villaverde-alto/"),
]


def get_proxy_config() -> Optional[Dict]:
    """
    Configure Bright Data proxy settings.
    
    Returns:
        Proxy configuration dict or None if credentials missing
    """
    if not all([BRIGHTDATA_USER, BRIGHTDATA_PASS, BRIGHTDATA_HOST]):
        print("⚠ Warning: Bright Data credentials not configured")
        print("  Set BRIGHTDATA_USER, BRIGHTDATA_PASS, BRIGHTDATA_HOST in .env")
        return None
    
    from urllib.parse import quote
    user_enc = quote(BRIGHTDATA_USER, safe='')
    pass_enc = quote(BRIGHTDATA_PASS, safe='')
    proxy_url = f'http://{user_enc}:{pass_enc}@{BRIGHTDATA_HOST}'
    return {
        'http': proxy_url,
        'https': proxy_url
    }

# Global request counter for Bright Data usage tracking
request_counter = {'successful': 0, 'failed': 0, 'total': 0}

# Direct (free) request counters
direct_counter = {'successful': 0, 'failed': 0, 'total': 0}

# Consecutive direct failures (triggers auto-switch to proxy-only)
_consecutive_direct_fails = 0

# Per-phase request counters: venta (sale scraping), rental, retry
phase_counters: Dict[str, int] = {'venta': 0, 'rental': 0, 'retry': 0}

# Current scraping phase (updated before each section runs)
_current_phase: str = 'venta'

# Global tracking for retryable errors (502, 404)
retry_errors = []  # List of (distrito, barrio, url_path, error_code) tuples


def _set_phase(phase: str) -> None:
    """Switch the current scraping phase for request attribution."""
    global _current_phase
    _current_phase = phase


def _budget_exceeded() -> bool:
    """Return True if the configured request budget has been hit."""
    if BRIGHTDATA_REQUEST_BUDGET <= 0:
        return False
    return request_counter['total'] >= BRIGHTDATA_REQUEST_BUDGET

def get_brightdata_cost_estimate():
    """
    Calculate estimated Bright Data cost and show hybrid savings.
    Bright Data Web Unlocker pricing: ~$3-5 per 1000 requests (varies by plan)
    Using conservative estimate of $4 per 1000 requests.
    """
    proxy_requests = request_counter['total']
    direct_requests = direct_counter['total']
    total_all = proxy_requests + direct_requests
    cost_per_1k = 4.0  # USD per 1000 requests
    actual_cost = (proxy_requests / 1000) * cost_per_1k
    would_have_cost = (total_all / 1000) * cost_per_1k
    savings = would_have_cost - actual_cost

    return {
        'total_requests': total_all,
        'direct_requests': direct_requests,
        'direct_successful': direct_counter['successful'],
        'proxy_requests': proxy_requests,
        'proxy_successful': request_counter['successful'],
        'failed_requests': request_counter['failed'] + direct_counter['failed'],
        'estimated_cost_usd': round(actual_cost, 2),
        'savings_usd': round(savings, 2),
        'savings_pct': round((savings / would_have_cost * 100) if would_have_cost > 0 else 0, 1),
        'cost_per_request': round(cost_per_1k / 1000, 4),
        'fetch_mode': FETCH_MODE,
    }


def _get_random_headers() -> Dict[str, str]:
    """Return realistic browser headers with randomized User-Agent."""
    user_agents = [
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:134.0) Gecko/20100101 Firefox/134.0',
    ]
    return {
        'User-Agent': random.choice(user_agents),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
    }


def _fetch_direct(url: str) -> tuple:
    """
    Fetch via curl_cffi impersonating a real Chrome browser (free, no proxy).

    curl_cffi mimics the TLS fingerprint of a real browser, which is the
    primary signal anti-bot systems use to block requests.

    Returns:
        Tuple of (HTML content or None, status_code)
    """
    global _consecutive_direct_fails

    if not HAS_CURL_CFFI:
        return None, 0

    try:
        direct_counter['total'] += 1
        # impersonate= tells curl_cffi which browser TLS fingerprint to use
        response = cffi_requests.get(
            url,
            headers=_get_random_headers(),
            impersonate="chrome131",
            timeout=30,
        )

        if response.status_code == 200:
            # Check for CAPTCHA/challenge pages
            text = response.text
            if _is_challenge_page(text):
                direct_counter['failed'] += 1
                _consecutive_direct_fails += 1
                return None, 403  # Treat as blocked
            direct_counter['successful'] += 1
            _consecutive_direct_fails = 0
            return text, 200
        else:
            direct_counter['failed'] += 1
            _consecutive_direct_fails += 1
            return None, response.status_code

    except Exception as e:
        direct_counter['failed'] += 1
        _consecutive_direct_fails += 1
        print(f"  ⚠ Direct request error: {e}")
        return None, 0


def _is_challenge_page(html: str) -> bool:
    """Detect if the response is a CAPTCHA or anti-bot challenge page."""
    if len(html) < 2000:
        return True  # Real Idealista pages are much larger
    challenge_signals = [
        'captcha', 'challenge', 'cf-browser-verification',
        'ray-id', 'just a moment', 'checking your browser',
        'access denied', 'blocked',
    ]
    html_lower = html[:5000].lower()
    return any(signal in html_lower for signal in challenge_signals)


def _fetch_via_proxy(
    url: str,
    proxies: Optional[Dict],
    retries: int = 3,
    silent_404: bool = False,
) -> tuple:
    """
    Fetch via Bright Data proxy (original method, costs ~$0.004/request).

    Only retries on transient errors (timeouts, connection errors).
    Returns immediately on definitive errors (404, 502) to save API calls.
    """
    headers = _get_random_headers()
    DEFINITIVE_ERRORS = {404, 403, 410, 502}

    for attempt in range(retries):
        try:
            request_counter['total'] += 1
            phase_counters[_current_phase] = phase_counters.get(_current_phase, 0) + 1

            response = requests.get(
                url,
                proxies=proxies,
                headers=headers,
                timeout=60,
                verify=False,
            )

            if response.status_code == 200:
                request_counter['successful'] += 1
                return response.text, 200
            elif response.status_code in DEFINITIVE_ERRORS:
                request_counter['failed'] += 1
                if response.status_code == 404:
                    if not silent_404:
                        with open('404_errors.log', 'a') as f:
                            f.write(f"{url}\n")
                        print(f"  ⚠ HTTP 404 Not Found - logged (no retry)")
                else:
                    print(f"  ⚠ HTTP {response.status_code} - definitive error (no retry)")
                return None, response.status_code
            else:
                request_counter['failed'] += 1
                print(f"  ⚠ HTTP {response.status_code} for {url} (attempt {attempt + 1}/{retries})")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return None, response.status_code

        except requests.exceptions.RequestException as e:
            request_counter['failed'] += 1
            print(f"  ⚠ Proxy request error (attempt {attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)

    return None, 0


def fetch_page(
    url: str,
    proxies: Optional[Dict] = None,
    retries: int = 3,
    silent_404: bool = False,
) -> tuple:
    """
    Fetch HTML content with hybrid strategy (direct → proxy fallback).

    Strategy depends on FETCH_MODE:
    - 'hybrid':  try direct first (free), fallback to proxy on failure
    - 'direct':  only direct requests (free but risky)
    - 'proxy':   only Bright Data proxy (original behavior)

    Auto-switches to proxy-only if DIRECT_FAIL_THRESHOLD consecutive
    direct failures are detected (likely IP ban).

    Args:
        url:         Target URL
        proxies:     Proxy configuration
        retries:     Number of retry attempts (only for proxy/transient errors)
        silent_404:  If True, suppress 404 log file writes.

    Returns:
        Tuple of (HTML content or None, status_code)
    """
    mode = FETCH_MODE

    # Auto-degrade to proxy if too many consecutive direct failures
    if mode == 'hybrid' and _consecutive_direct_fails >= DIRECT_FAIL_THRESHOLD:
        if _consecutive_direct_fails == DIRECT_FAIL_THRESHOLD:
            print(f"  ⚠ {DIRECT_FAIL_THRESHOLD} consecutive direct failures — auto-switching to proxy-only")
        mode = 'proxy'

    # ----- DIRECT attempt (free) -----
    if mode in ('hybrid', 'direct') and HAS_CURL_CFFI:
        # Politeness delay to reduce chance of IP ban
        time.sleep(DIRECT_REQUEST_DELAY + random.uniform(0, 1.0))

        html, status = _fetch_direct(url)
        if status == 200:
            return html, 200

        if mode == 'direct':
            # Direct-only: no fallback, return whatever we got
            if status == 404 and not silent_404:
                with open('404_errors.log', 'a') as f:
                    f.write(f"{url}\n")
                print(f"  ⚠ HTTP 404 Not Found (direct) - logged")
            return html, status

        # hybrid mode: direct failed, try proxy
        print(f"  ↻ Direct blocked (HTTP {status}) — falling back to BrightData proxy")

    # ----- PROXY attempt (paid) -----
    if proxies is None:
        proxies = get_proxy_config()
    if proxies is None:
        print("  ✗ No proxy configured and direct request failed")
        return None, 0

    return _fetch_via_proxy(url, proxies, retries=retries, silent_404=silent_404)


def extract_number(text: str) -> Optional[int]:
    """Extract first number from text string."""
    match = re.search(r'\d+', text.replace('.', '').replace(',', ''))
    return int(match.group()) if match else None


def extract_float(text: str) -> Optional[float]:
    """Extract float number from text string."""
    match = re.search(r'[\d,]+', text)
    if match:
        return float(match.group().replace(',', '.'))
    return None


def fetch_property_description(url: str, proxies: Optional[Dict] = None) -> Optional[str]:
    """
    Fetch property description from individual property page.

    WARNING: This makes 1 additional Bright Data request per listing!
    Only called when FETCH_DESCRIPTIONS=true in environment.

    Args:
        url: Property detail page URL
        proxies: Proxy configuration

    Returns:
        Property description text or None if not found/disabled
    """
    if not FETCH_DESCRIPTIONS:
        return None

    try:
        html, status_code = fetch_page(url, proxies, retries=2)
        if not html or status_code != 200:
            return None
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # Try multiple selectors for description
        # Idealista uses different classes for descriptions
        description_selectors = [
            ('div', {'class': 'comment'}),
            ('div', {'class': 'adCommentsLanguage'}),
            ('div', {'id': 'details'}),
            ('span', {'class': 'adDescription'}),
        ]
        
        for tag, attrs in description_selectors:
            desc_elem = soup.find(tag, attrs)
            if desc_elem:
                description = desc_elem.get_text(strip=True)
                if description and len(description) > 20:  # Valid description
                    return description
        
        return None
        
    except Exception as e:
        print(f"  ⚠ Error fetching description from {url}: {e}")
        return None


def parse_listing(article: BeautifulSoup, distrito: str, barrio: str) -> Optional[Dict]:
    """
    Parse a single listing article element.
    
    Args:
        article: BeautifulSoup article element
        distrito: District name
        barrio: Barrio (neighborhood) name
        
    Returns:
        Dictionary with listing data or None if parsing fails
    """
    try:
        # Extract listing ID (primary key)
        listing_id = article.get('data-element-id')
        if not listing_id:
            return None
        
        # Extract title and URL
        link_elem = article.find('a', class_='item-link')
        if not link_elem:
            return None
        
        title = link_elem.get_text(strip=True)
        url = BASE_URL + link_elem.get('href', '')
        
        # Extract price
        price_elem = article.find('span', class_='item-price')
        price = None
        if price_elem:
            price_text = price_elem.get_text(strip=True)
            price = extract_number(price_text)
        
        # Extract details (rooms, size, floor, orientation)
        rooms = None
        size_sqm = None
        floor = None
        orientation = None
        
        detail_spans = article.find_all('span', class_='item-detail')
        for span in detail_spans:
            text = span.get_text(strip=True)
            
            if 'hab' in text.lower():
                rooms = extract_number(text)
            elif 'm²' in text or 'm2' in text:
                size_sqm = extract_float(text)
            elif any(word in text.lower() for word in ['planta', 'bajo', 'ático', 'piso']):
                floor = text
            elif 'interior' in text.lower():
                orientation = 'Interior'
            elif 'exterior' in text.lower():
                orientation = 'Exterior'
        
        
        # Determine seller type
        seller_type = 'Particular'
        if article.find('span', class_='logo-branding') or article.find('picture', class_='logo-branding'):
            seller_type = 'Agencia'
        
        # Check if new development
        is_new_development = bool(article.find('span', class_='item-new-construction'))
        
        # Extract partial description (truncated text from listing card)
        description = None
        description_selectors = [
            ('div', {'class': 'item-description'}),
            ('p', {'class': 'item-description'}),
            ('div', {'class': 'description'}),
            ('span', {'class': 'item-description'}),
            ('div', {'class': 'item-detail-char'}),
        ]
        
        for tag, attrs in description_selectors:
            desc_elem = article.find(tag, attrs)
            if desc_elem:
                description = desc_elem.get_text(strip=True)
                if description and len(description) > 10:
                    # Clean up and limit length
                    description = description.strip()
                    if len(description) > 500:
                        description = description[:500] + '...'
                    break
        
        return {
            'listing_id': listing_id,
            'title': title,
            'url': url,
            'price': price,
            'distrito': distrito,
            'barrio': barrio,
            'rooms': rooms,
            'size_sqm': size_sqm,
            'floor': floor,
            'orientation': orientation,
            'seller_type': seller_type,
            'is_new_development': is_new_development,
            'description': description
        }
        
    except Exception as e:
        print(f"  ⚠ Error parsing listing: {e}")
        return None


def scrape_barrio(
    distrito: str, barrio: str, url_path: str,
    proxies: Optional[Dict], seen_ids: set,
    page_history: Optional[Dict] = None
) -> tuple[int, int, int, int]:
    """
    Scrape all pages for a single barrio with optimizations:
    - Smart pagination: uses historical page counts to limit requests
    - Early exit: skips remaining pages if all listings on page 1 already seen today
    - Tracks 502 errors globally for later retry

    Args:
        distrito: District name
        barrio: Barrio (neighborhood) name
        url_path: URL path for barrio
        proxies: Proxy configuration
        seen_ids: Set to track seen listing IDs (modified in place)
        page_history: Dict with historical page counts per barrio (optional)

    Returns:
        Tuple of (total_listings, new_listings, updated_listings, idealista_total)
        idealista_total: total count announced by Idealista in <h1> header
    """
    listings_count = 0
    total_new = 0
    total_updated = 0
    idealista_total = 0       # Total announced by Idealista in <h1>
    page = 1
    today = datetime.now().strftime("%Y-%m-%d")

    # Guard: skip barrio if already fully scraped today (e.g. double-run in CI)
    barrio_key = f"{distrito}|{barrio}"
    if page_history is not None and was_barrio_scraped_today(page_history, barrio_key):
        print(f"\n⏭️  {distrito} - {barrio}: ya scrapeado hoy — omitiendo")
        return 0, 0, 0, 0

    # Budget guard: stop before making more requests if budget is set and hit
    if _budget_exceeded():
        print(f"\n🛑 PRESUPUESTO ALCANZADO ({request_counter['total']:,} req) — omitiendo {barrio}")
        return 0, 0, 0, 0

    print(f"\n📍 Scraping {distrito} - {barrio}...")

    # Smart pagination: get max pages from history
    if page_history is not None:
        max_pages = get_max_pages_for_barrio(page_history, barrio_key)
        if max_pages < MAX_PAGES_PER_BARRIO:
            print(f"  📊 Smart pagination: limit {max_pages} páginas (histórico)")
    else:
        max_pages = MAX_PAGES_PER_BARRIO

    actual_pages = 0

    while page <= max_pages:
        # Build URL with pagination
        if page == 1:
            url = BASE_URL + url_path
        else:
            url = BASE_URL + url_path + f"pagina-{page}.htm"

        print(f"  Page {page}...", end=' ')

        html, status_code = fetch_page(url, proxies)

        if status_code in (404, 502):
            error_msg = "404 Not Found" if status_code == 404 else "502 Bad Gateway"
            print(f"❌ {error_msg} - stopping barrio (will retry later)")
            # Add to global retry list (only once per barrio)
            entry = (distrito, barrio, url_path, status_code)
            if entry not in retry_errors:
                retry_errors.append(entry)
            return listings_count, total_new, total_updated, idealista_total

        if not html:
            print(f"❌ Failed to fetch (Status: {status_code}) - stopping barrio")
            break

        soup = BeautifulSoup(html, 'html.parser')

        # On page 1, extract the total count Idealista announces in <h1>
        # e.g. "1.234 pisos en venta en Sol, Centro" → 1234
        if page == 1:
            h1 = soup.find('h1', id='h1-container')
            if not h1:
                h1 = soup.find('h1')
            if h1:
                h1_text = h1.get_text(strip=True)
                # Match number at start, with optional thousands separators (dots)
                m = re.match(r'([\d.]+)', h1_text)
                if m:
                    try:
                        idealista_total = int(m.group(1).replace('.', ''))
                    except ValueError:
                        pass

        # Find all listing articles
        articles = soup.find_all('article', class_='item')

        if not articles:
            if soup.find('div', class_='no-results'):
                print(f"✓ No listings found for this area")
            else:
                print(f"✓ No more listings (end of pagination)")
            break

        actual_pages = page
        print(f"Found {len(articles)} listings", end=' ')

        # Parse each listing
        new_count = 0
        updated_count = 0
        already_seen_today_count = 0

        for article in articles:
            listing_data = parse_listing(article, distrito, barrio)

            if listing_data and listing_data['listing_id']:
                listing_id = listing_data['listing_id']

                if listing_id in seen_ids:
                    update_listing(listing_id, listing_data)
                    seen_ids.remove(listing_id)
                    updated_count += 1
                    # Track if this listing was already updated today
                    # (seen_ids only contains IDs NOT yet seen in this run)
                else:
                    # Check: is this a truly new listing, or was it already processed
                    # earlier in this same run? (i.e., not in seen_ids because it was
                    # already removed by a previous barrio or page)
                    insert_listing(listing_data)
                    new_count += 1

                listings_count += 1

        total_new += new_count
        total_updated += updated_count
        print(f"({new_count} new, {updated_count} updated)")

        # EARLY EXIT: Only skip remaining pages if we're certain this barrio
        # has just 1 page of results (i.e., Idealista says total <= page size).
        # Previously, this checked "update_ratio >= 80% and new_count == 0" on
        # page 1 to detect double-runs. However, that killed pagination for ALL
        # barrios where page 1 listings were already in the DB from yesterday,
        # freezing coverage at ~30 listings per barrio (29% total coverage).
        # The was_barrio_scraped_today() guard already handles double-runs.
        if page == 1 and len(articles) > 0:
            if idealista_total > 0 and idealista_total <= len(articles):
                print(f"  ⚡ Early exit: Idealista reports {idealista_total} total ≤ {len(articles)} on page 1 — single page barrio")
                break

        # Check for next page
        next_button = soup.find('a', class_='icon-arrow-right-after')
        if not next_button:
            print(f"  ✓ Reached last page (Total pages: {page})")
            break

        if page == max_pages:
            print(f"  ⚠️ Reached {max_pages} page limit for this barrio")

        page += 1
        time.sleep(1)  # Rate limiting

    # Update page history with actual pages found
    if page_history is not None and actual_pages > 0:
        update_page_history(page_history, barrio_key, actual_pages)

    if idealista_total > 0:
        print(f"  🏁 Finished {distrito} - {barrio}: {listings_count}/{idealista_total} listings ({total_new} new, {total_updated} updated)")
    else:
        print(f"  🏁 Finished {distrito} - {barrio}: {listings_count} listings ({total_new} new, {total_updated} updated)")
    return listings_count, total_new, total_updated, idealista_total


def retry_failed_barrios(
    proxies: Optional[Dict], active_ids: set,
    page_history: Optional[Dict] = None, auto: bool = False
) -> tuple[int, int, int]:
    """
    Retry scraping barrios that had 404 or 502 errors.
    Prompts user interactively and allows recursive retries.

    Args:
        proxies: Proxy configuration
        active_ids: Set of active listing IDs (modified in place)
        page_history: Dict with historical page counts per barrio
        auto: If True, skip interactive prompt and retry automatically

    Returns:
        Tuple of (total_listings, new_listings, updated_listings)
    """
    if not retry_errors:
        return 0, 0, 0

    print("\n" + "=" * 60)
    print("🔄 RETRY FAILED BARRIOS (404/502)")
    print("=" * 60)
    print(f"\nFound {len(retry_errors)} barrios with errors:")

    for i, (distrito, barrio, url_path, error_code) in enumerate(retry_errors, 1):
        print(f"  {i}. {distrito} - {barrio} (HTTP {error_code})")

    if not auto:
        if sys.stdin.isatty():
            response = input("\n¿Quieres reintentar estos barrios? (y/n): ")
            if response.lower() != 'y':
                print("Skipping retries.")
                return 0, 0, 0
        else:
            print("\nNon-interactive mode — retrying automatically.")

    print("\n🔄 Retrying failed barrios...")

    total_listings = 0
    total_new = 0
    total_updated = 0

    barrios_to_retry = retry_errors.copy()
    retry_errors.clear()

    for distrito, barrio, url_path, error_code in barrios_to_retry:
        count, new_count, updated_count, _idealista = scrape_barrio(
            distrito, barrio, url_path, proxies, active_ids, page_history
        )
        total_listings += count
        total_new += new_count
        total_updated += updated_count
        time.sleep(2)  # Extra delay for retries

    print(f"\n✓ Retry complete. Processed {total_listings} listings ({total_new} new, {total_updated} updated).")

    if retry_errors:
        print(f"\n⚠️  Still have {len(retry_errors)} barrios with errors.")
        retry_count, retry_new, retry_updated = retry_failed_barrios(
            proxies, active_ids, page_history, auto
        )
        return total_listings + retry_count, total_new + retry_new, total_updated + retry_updated

    return total_listings, total_new, total_updated


def get_failed_barrios_from_log() -> List[tuple]:
    """
    Get barrios that failed in the last scraping execution from the database.
    Used by --retry mode to automatically determine which barrios to retry.

    Returns:
        List of (distrito, barrio, url_path) tuples for barrios that need retry
    """
    from database import get_connection
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            # Get today's date
            today = datetime.now().strftime("%Y-%m-%d")

            # Find barrios that have active listings but were NOT updated today
            # This indicates the scraper failed to reach them
            cursor.execute("""
                SELECT DISTINCT distrito, barrio
                FROM listings
                WHERE status = 'active'
                AND last_seen_date < ?
                AND barrio NOT LIKE '%(General)%'
                GROUP BY distrito, barrio
                HAVING COUNT(*) >= 3
            """, (today,))

            missing_barrios = set()
            for row in cursor.fetchall():
                missing_barrios.add((row[0], row[1]))

        # Match against BARRIO_URLS to get the URL paths
        result = []
        for distrito, barrio, url_path in BARRIO_URLS:
            if (distrito, barrio) in missing_barrios:
                result.append((distrito, barrio, url_path))

        return result

    except Exception as e:
        print(f"⚠ Error getting failed barrios from log: {e}")
        return []


def run_scraper(retry_only: bool = False):
    """
    Main scraper orchestration function.

    Optimizations vs original:
    - REMOVED: District-level scraping (saves ~30% of requests)
    - ADDED: Smart pagination using historical page counts
    - ADDED: Early exit when barrio already processed today
    - ADDED: --retry mode to only scrape previously failed barrios

    Args:
        retry_only: If True, only scrape barrios that failed in last execution
    """
    start_time = datetime.now()

    mode_label = "RETRY MODE" if retry_only else "FULL SCRAPE"
    print("=" * 60)
    print(f"🏠 Madrid Real Estate Tracker - Scraper ({mode_label})")
    print("=" * 60)
    print(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    if FETCH_DESCRIPTIONS:
        print("⚠ Description fetching ENABLED (extra API cost)")
    print()

    # Initialize database
    init_database()
    migrate_create_scraping_log_table()

    # Configure proxy
    proxies = get_proxy_config()
    if not proxies:
        print("\n⚠ Running without proxy (may get blocked by Idealista)")
        if sys.stdin.isatty():
            response = input("Continue anyway? (y/n): ")
            if response.lower() != 'y':
                print("Aborted.")
                return
        else:
            print("  Non-interactive mode — continuing without proxy.")

    # Load active listing IDs (for sold detection)
    print("\n📊 Loading active listings from database...")
    active_ids = get_active_listing_ids()
    print(f"  Found {len(active_ids)} active listings")

    # Load smart pagination history
    page_history = load_page_history()
    if page_history:
        print(f"  📊 Loaded page history for {len(page_history)} barrios")

    total_listings = 0
    total_new = 0
    total_updated = 0

    # Coverage tracking: per-barrio data for quality report
    # {barrio_key: {"distrito": str, "barrio": str, "scraped": int, "idealista": int}}
    coverage_data = {}

    # Determine which barrios to scrape
    low_activity_tracker = _load_low_activity_tracker()
    skipped_low_activity = 0

    if retry_only:
        # RETRY MODE: only scrape barrios that failed previously
        barrios_to_scrape = get_failed_barrios_from_log()
        if not barrios_to_scrape:
            print("\n✅ No failed barrios found — all barrios are up to date!")
            return
        print(f"\n🔄 Retrying {len(barrios_to_scrape)} failed barrios...")
    else:
        # FULL MODE: scrape all barrios (NO district-level scraping — saves ~30% requests)
        barrios_to_scrape = BARRIO_URLS
        print(f"\n🏘️ Scraping {len(barrios_to_scrape)} barrios...")
        if LOW_ACTIVITY_INTERVAL_DAYS > 1:
            print(f"  💡 Low-activity barrios (≤{LOW_ACTIVITY_MAX_PAGES} pág) → every {LOW_ACTIVITY_INTERVAL_DAYS} days")

    # Scrape barrios (venta phase)
    _set_phase('venta')
    for entry in barrios_to_scrape:
        distrito, barrio, url_path = entry[0], entry[1], entry[2]
        barrio_key = f"{distrito}|{barrio}"

        # Skip low-activity barrios that were scraped recently (cost optimization)
        if not retry_only and should_skip_low_activity(barrio_key, page_history, low_activity_tracker):
            skipped_low_activity += 1
            continue

        count, new_count, updated_count, idealista_count = scrape_barrio(
            distrito, barrio, url_path, proxies, active_ids, page_history
        )
        total_listings += count
        total_new += new_count
        total_updated += updated_count

        # Track coverage per barrio for quality report (always record attempted barrios)
        coverage_data[barrio_key] = {
            "distrito": distrito,
            "barrio": barrio,
            "scraped": count,
            "idealista": idealista_count,
        }

        # Mark this barrio as scraped today in the low-activity tracker
        low_activity_tracker[barrio_key] = datetime.utcnow().strftime("%Y-%m-%d")

        time.sleep(1)  # Rate limiting between barrios

    # Save low-activity tracker
    _save_low_activity_tracker(low_activity_tracker)
    if skipped_low_activity > 0:
        print(f"\n  💡 Skipped {skipped_low_activity} low-activity barrios (scraped recently)")

    # Save updated page history
    save_page_history(page_history)
    print(f"\n  💾 Saved page history for {len(page_history)} barrios")

    # Retry failed barrios (retry phase)
    _set_phase('retry')
    retry_count, retry_new, retry_updated = retry_failed_barrios(
        proxies, active_ids, page_history
    )
    total_listings += retry_count
    total_new += retry_new
    total_updated += retry_updated

    # Mark stale listings as sold using two-tier approach:
    # Tier 1: 7 days + barrio coverage confirmed → sold
    # Tier 2: 21 days hard cutoff (no barrio coverage needed) → sold
    if not retry_only:
        print(f"\n🔍 Checking for sold/removed properties...")
        print(f"  Tier 1: properties not seen in 7+ days (barrio scraped recently)")
        print(f"  Tier 2: properties not seen in 21+ days (hard cutoff)")

        sold_count = mark_stale_as_sold(days_threshold=7)
        print(f"  ✓ Marked {sold_count} listings as sold/removed")

        if active_ids:
            print(f"  ℹ️  {len(active_ids)} properties not seen in this scrape")
            print(f"  ℹ️  These will be marked as sold if not seen within 7-21 days")

    # Bright Data usage report
    cost_data = get_brightdata_cost_estimate()
    end_time = datetime.now()

    # Log execution to database
    log_scraping_execution(
        start_time=start_time,
        end_time=end_time,
        properties_processed=total_listings,
        new_listings=total_new,
        updated_listings=total_updated,
        total_requests=cost_data['total_requests'],
        cost_estimate_usd=cost_data['estimated_cost_usd'],
        status='success' if not retry_errors else 'partial_errors'
    )

    # Summary
    print("\n" + "=" * 60)
    print(f"✅ Scraping Complete ({mode_label})")
    print("=" * 60)
    print(f"Total listings processed: {total_listings}")
    print(f"  ✓ New: {total_new}")
    print(f"  ✓ Updated: {total_updated}")
    if retry_errors:
        print(f"⚠️  {len(retry_errors)} barrios still have errors (not retried):")
        for d, b, u, code in retry_errors:
            print(f"     - {d} - {b} (HTTP {code})")
        print(f"   Run: python scraper.py --retry")
    print(f"Completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # -------------------------------------------------------------------------
    # 💰 COST REPORT  (venta + retry phases, before rental)
    # -------------------------------------------------------------------------
    total_req    = cost_data['total_requests']
    direct_req   = cost_data['direct_requests']
    direct_ok    = cost_data['direct_successful']
    proxy_req    = cost_data['proxy_requests']
    proxy_ok     = cost_data['proxy_successful']
    fail_req     = cost_data['failed_requests']
    cost_usd     = cost_data['estimated_cost_usd']
    savings_usd  = cost_data['savings_usd']
    savings_pct  = cost_data['savings_pct']
    fetch_mode   = cost_data['fetch_mode']
    duration     = (end_time - start_time).total_seconds()

    # Budget warning if cap was reached
    if BRIGHTDATA_REQUEST_BUDGET > 0 and proxy_req >= BRIGHTDATA_REQUEST_BUDGET:
        print(f"\n⚠️  PRESUPUESTO ALCANZADO: {proxy_req:,} / {BRIGHTDATA_REQUEST_BUDGET:,} requests de proxy")
        print(f"   Aumenta BRIGHTDATA_REQUEST_BUDGET en .env o en GitHub Secrets si necesitas más.")

    print("\n")
    print("╔" + "═" * 62 + "╗")
    title = f"  💰  RESUMEN DE COSTE — modo {fetch_mode.upper()}"
    print("║" + title.center(62) + "║")
    print("╠" + "═" * 62 + "╣")

    if total_req > 0:
        # Direct (free) vs Proxy (paid) breakdown
        if direct_req > 0:
            d_rate = direct_ok / direct_req * 100 if direct_req > 0 else 0
            print(f"║  {'Requests directas (gratis):':<30} {direct_req:>8,}                   ║")
            print(f"║  {'  ✓ Exitosas:':<30} {direct_ok:>8,}  ({d_rate:4.1f}%)         ║")
        if proxy_req > 0:
            p_rate = proxy_ok / proxy_req * 100 if proxy_req > 0 else 0
            print(f"║  {'Requests proxy (BrightData):':<30} {proxy_req:>8,}                   ║")
            print(f"║  {'  ✓ Exitosas:':<30} {proxy_ok:>8,}  ({p_rate:4.1f}%)         ║")
        print(f"║  {'Requests totales:':<30} {total_req:>8,}                   ║")
        print(f"║  {'  ✗ Fallidas (total):':<30} {fail_req:>8,}                   ║")
        print("╠" + "─" * 62 + "╣")

        # Per-phase breakdown
        venta_req  = phase_counters.get('venta', 0)
        retry_req  = phase_counters.get('retry', 0)
        print(f"║  {'Por fase — venta (anuncios):':<30} {venta_req:>8,}                   ║")
        print(f"║  {'Por fase — retries:':<30} {retry_req:>8,}                   ║")
        print(f"║  {'Por fase — alquiler:':<30} {'(ver abajo)':>12}               ║")
        print("╠" + "─" * 62 + "╣")

        # Cost
        cost_per_req     = cost_usd / proxy_req if proxy_req > 0 else 0
        cost_per_listing = cost_usd / total_listings if total_listings > 0 else 0
        print(f"║  {'Coste real (solo proxy):':<30} {'$' + f'{cost_usd:.4f}':>10} USD         ║")
        if savings_usd > 0:
            print(f"║  {'Ahorro vs proxy-only:':<30} {'$' + f'{savings_usd:.4f}':>10} USD ({savings_pct:.0f}%)  ║")
        if total_listings > 0:
            print(f"║  {'Coste por anuncio:':<30} {'$' + f'{cost_per_listing:.5f}':>10} USD         ║")
        print("╠" + "─" * 62 + "╣")

        # Timing
        mins, secs = divmod(int(duration), 60)
        req_per_min = total_req / (duration / 60) if duration > 0 else 0
        print(f"║  {'Duración total:':<30} {f'{mins}m {secs}s':>10}                  ║")
        print(f"║  {'Velocidad:':<30} {f'{req_per_min:.1f} req/min':>14}              ║")
    else:
        print("║" + "  Sin requests — todos los barrios ya estaban al día".center(62) + "║")

    print("╚" + "═" * 62 + "╝")

    # -------------------------------------------------------------------------
    # 🏘️  RENTAL PRICE SCRAPING — frequency-controlled
    # -------------------------------------------------------------------------
    _set_phase('rental')
    _run_rental_if_due(proxies)

    # -------------------------------------------------------------------------
    # 🔍  NLP ANALYSIS — scan new descriptions for seller signals
    # -------------------------------------------------------------------------
    try:
        from nlp_analyzer import run_nlp_batch
        nlp_stats = run_nlp_batch(force_reanalyze=False)
        print(
            f"🔍 NLP: {nlp_stats['processed']:,} nuevas descripciones analizadas, "
            f"{nlp_stats['with_signals']:,} con señales"
        )
    except Exception as exc:
        print(f"⚠️  NLP analysis skipped: {exc}")

    # -------------------------------------------------------------------------
    # ☁️  AUTO-UPLOAD TO GOOGLE DRIVE
    # -------------------------------------------------------------------------
    _auto_upload_to_drive()

    # -------------------------------------------------------------------------
    # 📬  DAILY EMAIL REPORT
    # -------------------------------------------------------------------------
    _send_email_report()

    # -------------------------------------------------------------------------
    # 📊  SCRAPING QUALITY REPORT EMAIL
    # -------------------------------------------------------------------------
    _send_scraping_quality_email(coverage_data, total_listings, total_new, total_updated,
                                  len(retry_errors), cost_data, start_time, end_time)


# ============================================================================
# RENTAL SCRAPING
# ============================================================================

def _parse_rental_prices(html: str) -> List[float]:
    """
    Extract monthly asking rents (€/mes) from a rental listing page.
    Returns a list of valid rent values found on the page.
    """
    prices: List[float] = []
    try:
        soup = BeautifulSoup(html, 'html.parser')
        for article in soup.find_all('article', class_='item'):
            price_elem = article.find('span', class_='item-price')
            if not price_elem:
                continue
            price_text = price_elem.get_text(strip=True)
            # Rental prices on Idealista are shown as "X.XXX €/mes"
            value = extract_number(price_text)
            if value and 100 <= value <= 20_000:   # sanity range for monthly rent
                prices.append(float(value))
    except Exception as exc:
        print(f"  ⚠ Error parsing rental prices: {exc}")
    return prices


def _rental_last_scraped_date() -> Optional[str]:
    """Return the date string of the last successful rental scraping, or None."""
    try:
        p = Path(RENTAL_LAST_SCRAPED_FILE)
        if p.exists():
            return p.read_text(strip=True) if hasattr(p.read_text(), 'strip') else p.read_text().strip()
    except IOError:
        pass
    return None


def _rental_save_scraped_date() -> None:
    """Record today as the last successful rental scraping date."""
    try:
        Path(RENTAL_LAST_SCRAPED_FILE).write_text(datetime.now().strftime("%Y-%m-%d"))
    except IOError as e:
        print(f"  ⚠ Could not save rental last-scraped date: {e}")


def _rental_is_due() -> bool:
    """Return True if it's time to re-scrape rental prices."""
    if RENTAL_SCRAPE_INTERVAL_DAYS <= 0:
        return True  # 0 = always run
    last = _rental_last_scraped_date()
    if last is None:
        return True  # Never ran before
    from datetime import date
    try:
        last_date = date.fromisoformat(last)
        delta = (date.today() - last_date).days
        return delta >= RENTAL_SCRAPE_INTERVAL_DAYS
    except ValueError:
        return True  # Bad date format → re-run to be safe


def _run_rental_if_due(proxies: Optional[Dict] = None) -> None:
    """
    Run rental scraping only when the configured interval has elapsed.
    Prints a clear skip message when not due, so CI logs are easy to read.
    """
    if not _rental_is_due():
        last = _rental_last_scraped_date()
        from datetime import date
        days_since = (date.today() - date.fromisoformat(last)).days
        days_left  = RENTAL_SCRAPE_INTERVAL_DAYS - days_since
        rental_req_saved = len(BARRIO_URLS)
        rental_cost_saved = rental_req_saved * (4.0 / 1000)
        print("\n")
        print("╔" + "═" * 62 + "╗")
        print("║" + "  🏘️   ALQUILER — SCRAPING OMITIDO (intervalo)".center(62) + "║")
        print("╠" + "═" * 62 + "╣")
        print(f"║  {'Último scrape:':<30} {last:<30} ║")
        print(f"║  {'Días desde el último:':<30} {days_since:<30} ║")
        print(f"║  {'Próximo scrape en:':<30} {f'{days_left} días':30} ║")
        print(f"║  {'Requests ahorradas hoy:':<30} {f'~{rental_req_saved} req (≈${rental_cost_saved:.4f})':30} ║")
        print(f"║  {'Configura con:':<30} {'RENTAL_SCRAPE_INTERVAL_DAYS':30} ║")
        print("╚" + "═" * 62 + "╝")
        return

    stored = run_rental_scraping(proxies)

    # Print per-rental-phase cost
    rental_req  = phase_counters.get('rental', 0)
    rental_cost = rental_req * (4.0 / 1000)
    print(f"\n  💰 Alquiler: {rental_req:,} requests usadas ≈ ${rental_cost:.4f} USD")

    if stored > 0:
        _rental_save_scraped_date()


def run_rental_scraping(proxies: Optional[Dict] = None) -> int:
    """
    Scrape page-1 of rental listings for every barrio in BARRIO_URLS,
    compute the median monthly rent, and store it in the rental_prices table.

    Only page 1 is requested per barrio (~184 extra Bright Data calls).
    Skips barrios where fewer than 3 rental prices are found.

    Args:
        proxies: Proxy configuration (same as used for sale scraping)

    Returns:
        Number of barrio snapshots successfully stored
    """
    import statistics

    print("\n")
    print("╔" + "═" * 58 + "╗")
    print("║" + "  🏘️   SCRAPING PRECIOS DE ALQUILER (pág. 1/barrio)".center(58) + "║")
    print("╚" + "═" * 58 + "╝")

    # Ensure rental table exists (migration safe to run multiple times)
    migrate_create_rental_prices_table()

    today      = datetime.now().strftime("%Y-%m-%d")
    stored     = 0
    skipped    = 0
    errors     = 0
    total      = len(BARRIO_URLS)

    for idx, (distrito, barrio, sale_url_path) in enumerate(BARRIO_URLS, 1):
        # Progress every 20 barrios
        if idx == 1 or idx % 20 == 0 or idx == total:
            print(f"  [{idx}/{total}] {distrito} - {barrio}...")

        # Convert sale URL to rental URL
        rental_url_path = sale_url_path.replace(
            "/venta-viviendas/", "/alquiler-viviendas/", 1
        )
        url = BASE_URL + rental_url_path

        # silent_404=True: 404 means no rental listings in this barrio — expected,
        # no need to log to 404_errors.log or print a warning
        html, status_code = fetch_page(url, proxies, silent_404=True)

        if status_code == 404:
            skipped += 1
            continue
        if not html or status_code != 200:
            print(f"  ⚠ {distrito}/{barrio}: HTTP {status_code} — omitiendo")
            errors += 1
            continue

        prices = _parse_rental_prices(html)

        if len(prices) < 3:
            # Not enough data points for a meaningful median
            skipped += 1
            continue

        median_rent = statistics.median(prices)
        ok = upsert_rental_snapshot(
            distrito=distrito,
            barrio=barrio,
            median_rent=median_rent,
            listing_count=len(prices),
            date=today,
        )
        if ok:
            stored += 1

        # Brief sleep to avoid hammering the proxy
        time.sleep(0.3)

    print(f"\n  ✅ Alquiler: {stored} barrios guardados, "
          f"{skipped} sin datos (sin anuncios), {errors} errores")
    return stored


def _auto_upload_to_drive():
    """
    Upload real_estate.db to Google Drive automatically after scraping.
    Skipped silently if credentials.json / token.json are not present,
    so the scraper keeps working without the upload configured.
    """
    from pathlib import Path
    credentials_file = Path(__file__).parent / "credentials.json"
    token_file       = Path(__file__).parent / "token.json"

    if not credentials_file.exists() and not token_file.exists():
        print(
            "\n💡 Upload automático no configurado. Para activarlo:\n"
            "   python upload_to_drive.py\n"
            "   (Solo necesitas hacerlo una vez)\n"
        )
        return

    print("\n")
    print("╔" + "═" * 58 + "╗")
    print("║" + "  ☁️   SUBIENDO BASE DE DATOS A GOOGLE DRIVE".center(58) + "║")
    print("╚" + "═" * 58 + "╝")

    try:
        from upload_to_drive import upload_db, get_credentials
        get_credentials()
        db_path = Path(__file__).parent / "real_estate.db"
        success = upload_db(db_path)
        if success:
            print("✅ Dashboard actualizado con los datos del scraping de hoy.\n")
        else:
            print("⚠️  El upload falló. Sube real_estate.db manualmente a Drive.\n")
    except Exception as exc:
        print(f"⚠️  Error en upload automático: {exc}")
        print("   Sube real_estate.db manualmente a Drive.\n")


def _send_email_report():
    """
    Send daily HTML market report via Gmail.
    Skipped silently if GMAIL_APP_PASSWORD is not set in .env.
    """
    try:
        from email_report import send_daily_report
        print("\n")
        print("╔" + "═" * 58 + "╗")
        print("║" + "  📬  ENVIANDO INFORME DIARIO POR EMAIL".center(58) + "║")
        print("╚" + "═" * 58 + "╝")
        send_daily_report()
    except Exception as exc:
        print(f"⚠️  Error enviando informe por email: {exc}")


def _send_scraping_quality_email(coverage_data: dict, total_scraped: int,
                                  total_new: int, total_updated: int,
                                  errors_count: int, cost_data: dict,
                                  start_time, end_time):
    """
    Send a scraping quality report email with per-district coverage data.
    Shows: detected by Idealista vs actually scraped, per barrio and district.
    """
    try:
        from email_report import send_report
        print("\n")
        print("╔" + "═" * 58 + "╗")
        print("║" + "  📊  ENVIANDO INFORME DE CALIDAD DE SCRAPING".center(58) + "║")
        print("╚" + "═" * 58 + "╝")

        if not coverage_data:
            print("  ⚠ No hay datos de cobertura — omitiendo email de calidad")
            return

        # Aggregate by district
        district_agg = {}
        for bk, info in coverage_data.items():
            d = info["distrito"]
            if d not in district_agg:
                district_agg[d] = {"idealista": 0, "scraped": 0, "barrios": []}
            district_agg[d]["idealista"] += info["idealista"]
            district_agg[d]["scraped"] += info["scraped"]
            district_agg[d]["barrios"].append(info)

        total_idealista = sum(v["idealista"] for v in district_agg.values())
        total_scraped_sum = sum(v["scraped"] for v in district_agg.values())
        coverage_pct = (total_scraped_sum / total_idealista * 100) if total_idealista > 0 else 0

        duration = (end_time - start_time).total_seconds()
        mins, secs = divmod(int(duration), 60)

        # Determine overall health
        if coverage_pct >= 90:
            health_icon = "🟢"
            health_text = "Excelente"
        elif coverage_pct >= 70:
            health_icon = "🟡"
            health_text = "Aceptable"
        else:
            health_icon = "🔴"
            health_text = "Bajo — revisar"

        # Build HTML email
        html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"></head>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif; background: #f5f5f5; margin: 0; padding: 20px;">
<div style="max-width: 700px; margin: 0 auto; background: #fff; border-radius: 12px; overflow: hidden; box-shadow: 0 2px 8px rgba(0,0,0,0.1);">

  <!-- Header -->
  <div style="background: linear-gradient(135deg, #1a1a2e, #16213e); color: #fff; padding: 24px 30px;">
    <h1 style="margin: 0; font-size: 22px;">{health_icon} Informe de calidad — Scraping diario</h1>
    <p style="margin: 8px 0 0; opacity: 0.8; font-size: 14px;">{start_time.strftime('%d/%m/%Y %H:%M')} — Duración: {mins}m {secs}s</p>
  </div>

  <!-- KPIs -->
  <div style="padding: 20px 30px;">
    <table style="width: 100%; border-collapse: collapse; margin-bottom: 20px;">
      <tr>
        <td style="text-align: center; padding: 12px; background: #f0f4ff; border-radius: 8px; width: 25%;">
          <div style="font-size: 24px; font-weight: 700; color: #1a1a2e;">{total_idealista:,}</div>
          <div style="font-size: 12px; color: #666; margin-top: 4px;">Idealista anuncia</div>
        </td>
        <td style="width: 8px;"></td>
        <td style="text-align: center; padding: 12px; background: #f0fff4; border-radius: 8px; width: 25%;">
          <div style="font-size: 24px; font-weight: 700; color: #16a34a;">{total_scraped_sum:,}</div>
          <div style="font-size: 12px; color: #666; margin-top: 4px;">Scrapeados</div>
        </td>
        <td style="width: 8px;"></td>
        <td style="text-align: center; padding: 12px; background: {'#f0fff4' if coverage_pct >= 90 else '#fffbeb' if coverage_pct >= 70 else '#fef2f2'}; border-radius: 8px; width: 25%;">
          <div style="font-size: 24px; font-weight: 700; color: {'#16a34a' if coverage_pct >= 90 else '#d97706' if coverage_pct >= 70 else '#dc2626'};">{coverage_pct:.1f}%</div>
          <div style="font-size: 12px; color: #666; margin-top: 4px;">Cobertura</div>
        </td>
        <td style="width: 8px;"></td>
        <td style="text-align: center; padding: 12px; background: #faf5ff; border-radius: 8px; width: 25%;">
          <div style="font-size: 24px; font-weight: 700; color: #7c3aed;">{health_text}</div>
          <div style="font-size: 12px; color: #666; margin-top: 4px;">Estado</div>
        </td>
      </tr>
    </table>

    <!-- Secondary KPIs -->
    <table style="width: 100%; border-collapse: collapse; margin-bottom: 24px;">
      <tr>
        <td style="padding: 8px 12px; font-size: 13px; color: #666;">Nuevos hoy:</td>
        <td style="padding: 8px 12px; font-size: 13px; font-weight: 600;">{total_new:,}</td>
        <td style="padding: 8px 12px; font-size: 13px; color: #666;">Actualizados:</td>
        <td style="padding: 8px 12px; font-size: 13px; font-weight: 600;">{total_updated:,}</td>
        <td style="padding: 8px 12px; font-size: 13px; color: #666;">Errores:</td>
        <td style="padding: 8px 12px; font-size: 13px; font-weight: 600; color: {'#dc2626' if errors_count > 0 else '#16a34a'};">{errors_count}</td>
      </tr>
      <tr>
        <td style="padding: 8px 12px; font-size: 13px; color: #666;">Coste proxy:</td>
        <td style="padding: 8px 12px; font-size: 13px; font-weight: 600;">${cost_data.get('estimated_cost_usd', 0):.4f}</td>
        <td style="padding: 8px 12px; font-size: 13px; color: #666;">Requests:</td>
        <td style="padding: 8px 12px; font-size: 13px; font-weight: 600;">{cost_data.get('total_requests', 0):,}</td>
        <td style="padding: 8px 12px; font-size: 13px; color: #666;">Discrepancia:</td>
        <td style="padding: 8px 12px; font-size: 13px; font-weight: 600; color: {'#dc2626' if (total_idealista - total_scraped_sum) > 500 else '#d97706'};">{total_idealista - total_scraped_sum:,} pisos</td>
      </tr>
    </table>

    <!-- District table -->
    <h2 style="font-size: 16px; margin: 0 0 12px; color: #1a1a2e;">Cobertura por distrito</h2>
    <table style="width: 100%; border-collapse: collapse; font-size: 13px;">
      <thead>
        <tr style="background: #f8fafc;">
          <th style="text-align: left; padding: 10px 8px; border-bottom: 2px solid #e2e8f0; color: #475569;">Distrito</th>
          <th style="text-align: right; padding: 10px 8px; border-bottom: 2px solid #e2e8f0; color: #475569;">Idealista</th>
          <th style="text-align: right; padding: 10px 8px; border-bottom: 2px solid #e2e8f0; color: #475569;">Scrapeados</th>
          <th style="text-align: right; padding: 10px 8px; border-bottom: 2px solid #e2e8f0; color: #475569;">Cobertura</th>
          <th style="text-align: right; padding: 10px 8px; border-bottom: 2px solid #e2e8f0; color: #475569;">Discrepancia</th>
        </tr>
      </thead>
      <tbody>"""

        # Sort districts by name
        for distrito in sorted(district_agg.keys()):
            d = district_agg[distrito]
            d_pct = (d["scraped"] / d["idealista"] * 100) if d["idealista"] > 0 else 0
            d_diff = d["idealista"] - d["scraped"]

            if d_pct >= 90:
                pct_color = "#16a34a"
                row_bg = "#f0fff4"
            elif d_pct >= 70:
                pct_color = "#d97706"
                row_bg = "#fffbeb"
            else:
                pct_color = "#dc2626"
                row_bg = "#fef2f2"

            html += f"""
        <tr style="background: {row_bg};">
          <td style="padding: 8px; border-bottom: 1px solid #f1f5f9; font-weight: 600;">{distrito}</td>
          <td style="padding: 8px; border-bottom: 1px solid #f1f5f9; text-align: right;">{d['idealista']:,}</td>
          <td style="padding: 8px; border-bottom: 1px solid #f1f5f9; text-align: right;">{d['scraped']:,}</td>
          <td style="padding: 8px; border-bottom: 1px solid #f1f5f9; text-align: right; font-weight: 700; color: {pct_color};">{d_pct:.0f}%</td>
          <td style="padding: 8px; border-bottom: 1px solid #f1f5f9; text-align: right; color: {'#dc2626' if d_diff > 50 else '#666'};">{d_diff:+,}</td>
        </tr>"""

        # Total row
        html += f"""
        <tr style="background: #f1f5f9; font-weight: 700;">
          <td style="padding: 10px 8px; border-top: 2px solid #cbd5e1;">TOTAL MADRID</td>
          <td style="padding: 10px 8px; border-top: 2px solid #cbd5e1; text-align: right;">{total_idealista:,}</td>
          <td style="padding: 10px 8px; border-top: 2px solid #cbd5e1; text-align: right;">{total_scraped_sum:,}</td>
          <td style="padding: 10px 8px; border-top: 2px solid #cbd5e1; text-align: right; color: {'#16a34a' if coverage_pct >= 90 else '#d97706' if coverage_pct >= 70 else '#dc2626'};">{coverage_pct:.1f}%</td>
          <td style="padding: 10px 8px; border-top: 2px solid #cbd5e1; text-align: right;">{total_idealista - total_scraped_sum:+,}</td>
        </tr>
      </tbody>
    </table>"""

        # Barrio detail for districts with low coverage
        low_districts = {d: v for d, v in district_agg.items()
                        if v["idealista"] > 0 and (v["scraped"] / v["idealista"] * 100) < 80}
        if low_districts:
            html += """
    <h2 style="font-size: 16px; margin: 24px 0 12px; color: #dc2626;">Detalle barrios con baja cobertura (&lt;80%)</h2>
    <table style="width: 100%; border-collapse: collapse; font-size: 12px;">
      <thead>
        <tr style="background: #fef2f2;">
          <th style="text-align: left; padding: 8px; border-bottom: 1px solid #fecaca;">Distrito</th>
          <th style="text-align: left; padding: 8px; border-bottom: 1px solid #fecaca;">Barrio</th>
          <th style="text-align: right; padding: 8px; border-bottom: 1px solid #fecaca;">Idealista</th>
          <th style="text-align: right; padding: 8px; border-bottom: 1px solid #fecaca;">Scrapeados</th>
        </tr>
      </thead>
      <tbody>"""
            for distrito in sorted(low_districts.keys()):
                for b in sorted(low_districts[distrito]["barrios"], key=lambda x: x["barrio"]):
                    b_pct = (b["scraped"] / b["idealista"] * 100) if b["idealista"] > 0 else 0
                    html += f"""
        <tr>
          <td style="padding: 6px 8px; border-bottom: 1px solid #f1f5f9; color: #666;">{b['distrito']}</td>
          <td style="padding: 6px 8px; border-bottom: 1px solid #f1f5f9;">{b['barrio']}</td>
          <td style="padding: 6px 8px; border-bottom: 1px solid #f1f5f9; text-align: right;">{b['idealista']:,}</td>
          <td style="padding: 6px 8px; border-bottom: 1px solid #f1f5f9; text-align: right;">{b['scraped']:,}</td>
        </tr>"""
            html += """
      </tbody>
    </table>"""

        html += """
  </div>

  <!-- Footer -->
  <div style="background: #f8fafc; padding: 16px 30px; text-align: center; font-size: 12px; color: #94a3b8;">
    Informe generado automáticamente por el scraper de inmobiliario
  </div>

</div>
</body></html>"""

        subject = f"{health_icon} Scraping {start_time.strftime('%d/%m')}: {coverage_pct:.0f}% cobertura — {total_scraped_sum:,}/{total_idealista:,} pisos"
        send_report(html, subject)

    except Exception as exc:
        print(f"⚠️  Error enviando informe de calidad: {exc}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # Support --retry flag to only scrape previously failed barrios
    retry_mode = '--retry' in sys.argv
    run_scraper(retry_only=retry_mode)
