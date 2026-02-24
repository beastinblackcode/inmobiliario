"""
Web scraper for Idealista Madrid real estate listings.
Uses Bright Data Web Unlocker API and BeautifulSoup for parsing.

Optimizations applied:
- No redundant district-level scraping (barrios cover all territory)
- Early exit when barrio was already fully processed today
- Smart pagination using historical page counts per barrio
- Intelligent HTTP retry (only retries on transient errors, not 404/502)
- Integrated retry mode for failed barrios (--retry flag)
- Configurable description fetching (disabled by default)
"""

import os
import re
import sys
import time
import json
from typing import Dict, List, Optional
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import urllib3

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

# Margin to add to historical max pages (safety buffer)
PAGE_HISTORY_MARGIN = 2

# Maximum pages to scrape per barrio (hard limit)
MAX_PAGES_PER_BARRIO = 60

# Early exit: if this % of listings on page 1 were already seen today, skip remaining pages
EARLY_EXIT_THRESHOLD = 0.95  # 95% already seen = skip


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


def update_page_history(history: Dict, barrio_key: str, pages_found: int) -> None:
    """Update historical page count for a barrio."""
    if barrio_key not in history:
        history[barrio_key] = {'max_pages': pages_found, 'last_updated': ''}
    else:
        history[barrio_key]['max_pages'] = max(
            history[barrio_key].get('max_pages', 0),
            pages_found
        )
    history[barrio_key]['last_updated'] = datetime.now().strftime("%Y-%m-%d")

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
    
    proxy_url = f'http://{BRIGHTDATA_USER}:{BRIGHTDATA_PASS}@{BRIGHTDATA_HOST}'
    return {
        'http': proxy_url,
        'https': proxy_url
    }

# Global request counter for Bright Data usage tracking
request_counter = {'successful': 0, 'failed': 0, 'total': 0}

# Global tracking for retryable errors (502, 404)
retry_errors = []  # List of (distrito, barrio, url_path, error_code) tuples

def get_brightdata_cost_estimate():
    """
    Calculate estimated Bright Data cost based on requests made.
    Bright Data Web Unlocker pricing: ~$3-5 per 1000 requests (varies by plan)
    Using conservative estimate of $4 per 1000 requests.
    """
    total_requests = request_counter['total']
    cost_per_1k = 4.0  # USD per 1000 requests
    estimated_cost = (total_requests / 1000) * cost_per_1k
    return {
        'total_requests': total_requests,
        'successful_requests': request_counter['successful'],
        'failed_requests': request_counter['failed'],
        'estimated_cost_usd': round(estimated_cost, 2),
        'cost_per_request': round(cost_per_1k / 1000, 4)
    }


def fetch_page(url: str, proxies: Optional[Dict] = None, retries: int = 3) -> tuple:
    """
    Fetch HTML content from URL with smart retry logic.

    Only retries on transient errors (timeouts, connection errors).
    Returns immediately on definitive errors (404, 502) to save API calls.

    Args:
        url: Target URL
        proxies: Proxy configuration
        retries: Number of retry attempts (only for transient errors)

    Returns:
        Tuple of (HTML content or None, status_code)
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
    }

    # Definitive HTTP errors: no point retrying, saves Bright Data requests
    DEFINITIVE_ERRORS = {404, 403, 410, 502}

    for attempt in range(retries):
        try:
            # Increment request counter
            request_counter['total'] += 1

            response = requests.get(
                url,
                proxies=proxies,
                headers=headers,
                timeout=60,
                verify=False  # Disable SSL verification for Bright Data proxy
            )

            if response.status_code == 200:
                request_counter['successful'] += 1
                return response.text, 200
            elif response.status_code in DEFINITIVE_ERRORS:
                # Definitive error: return immediately, do NOT retry
                request_counter['failed'] += 1
                if response.status_code == 404:
                    with open('404_errors.log', 'a') as f:
                        f.write(f"{url}\n")
                    print(f"  ⚠ HTTP 404 Not Found - logged (no retry)")
                else:
                    print(f"  ⚠ HTTP {response.status_code} - definitive error (no retry)")
                return None, response.status_code
            else:
                # Potentially transient error (429, 500, 503, etc.) - retry
                request_counter['failed'] += 1
                print(f"  ⚠ HTTP {response.status_code} for {url} (attempt {attempt + 1}/{retries})")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return None, response.status_code

        except requests.exceptions.RequestException as e:
            request_counter['failed'] += 1
            print(f"  ⚠ Request error (attempt {attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff

    return None, 0


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
) -> tuple[int, int, int]:
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
        Tuple of (total_listings, new_listings, updated_listings)
    """
    print(f"\n📍 Scraping {distrito} - {barrio}...")
    listings_count = 0
    total_new = 0
    total_updated = 0
    page = 1
    today = datetime.now().strftime("%Y-%m-%d")

    # Smart pagination: get max pages from history
    barrio_key = f"{distrito}|{barrio}"
    if page_history is not None:
        max_pages = get_max_pages_for_barrio(page_history, barrio_key)
        if max_pages < MAX_PAGES_PER_BARRIO:
            print(f"  📊 Smart pagination: limit {max_pages} pages (historical)")
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
            return listings_count, total_new, total_updated

        if not html:
            print(f"❌ Failed to fetch (Status: {status_code}) - stopping barrio")
            break

        soup = BeautifulSoup(html, 'html.parser')

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

        # EARLY EXIT: If on page 1, nearly all listings are updates (not new),
        # this barrio was likely already scraped today (e.g., after a restart).
        # Skip remaining pages to save API calls.
        if page == 1 and len(articles) > 0:
            total_on_page = new_count + updated_count
            if total_on_page > 0:
                update_ratio = updated_count / total_on_page
                if update_ratio >= EARLY_EXIT_THRESHOLD and new_count == 0:
                    print(f"  ⚡ Early exit: {update_ratio:.0%} already seen, 0 new — barrio likely scraped today")
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

    print(f"  🏁 Finished {distrito} - {barrio}: {listings_count} listings ({total_new} new, {total_updated} updated)")
    return listings_count, total_new, total_updated


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
        response = input("\n¿Quieres reintentar estos barrios? (y/n): ")
        if response.lower() != 'y':
            print("Skipping retries.")
            return 0, 0, 0

    print("\n🔄 Retrying failed barrios...")

    total_listings = 0
    total_new = 0
    total_updated = 0

    barrios_to_retry = retry_errors.copy()
    retry_errors.clear()

    for distrito, barrio, url_path, error_code in barrios_to_retry:
        count, new_count, updated_count = scrape_barrio(
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
        response = input("Continue anyway? (y/n): ")
        if response.lower() != 'y':
            print("Aborted.")
            return

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

    # Determine which barrios to scrape
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

    # Scrape barrios
    for entry in barrios_to_scrape:
        distrito, barrio, url_path = entry[0], entry[1], entry[2]
        count, new_count, updated_count = scrape_barrio(
            distrito, barrio, url_path, proxies, active_ids, page_history
        )
        total_listings += count
        total_new += new_count
        total_updated += updated_count
        time.sleep(1)  # Rate limiting between barrios

    # Save updated page history
    save_page_history(page_history)
    print(f"\n  💾 Saved page history for {len(page_history)} barrios")

    # Retry failed barrios (404/502 errors from this run)
    retry_count, retry_new, retry_updated = retry_failed_barrios(
        proxies, active_ids, page_history
    )
    total_listings += retry_count
    total_new += retry_new
    total_updated += retry_updated

    # Mark stale listings as sold (not seen in 14+ days, with barrio coverage check)
    if not retry_only:
        print(f"\n🔍 Checking for sold/removed properties...")
        print(f"  Marking properties not seen in 14+ days as sold (only if barrio was scraped)...")

        sold_count = mark_stale_as_sold(days_threshold=14)
        print(f"  ✓ Marked {sold_count} listings as sold/removed (not seen in 14+ days)")

        if active_ids:
            print(f"  ℹ️  {len(active_ids)} properties not seen in this scrape")
            print(f"  ℹ️  These will be marked as sold if not seen again within 14 days")

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
    # 💰 BRIGHT DATA COST REPORT
    # -------------------------------------------------------------------------
    total_req = cost_data['total_requests']
    ok_req    = cost_data['successful_requests']
    fail_req  = cost_data['failed_requests']
    cost_usd  = cost_data['estimated_cost_usd']
    duration  = (end_time - start_time).total_seconds()

    print("\n")
    print("╔" + "═" * 58 + "╗")
    print("║" + "  💰  BRIGHT DATA — RESUMEN DE COSTE".center(58) + "║")
    print("╠" + "═" * 58 + "╣")

    if total_req > 0:
        ok_pct   = ok_req / total_req * 100
        fail_pct = fail_req / total_req * 100

        # Requests
        print(f"║  {'Requests totales:':<28} {total_req:>8,}               ║")
        print(f"║  {'  ✓ Exitosas:':<28} {ok_req:>8,}  ({ok_pct:4.1f}%)       ║")
        print(f"║  {'  ✗ Fallidas:':<28} {fail_req:>8,}  ({fail_pct:4.1f}%)       ║")
        print("╠" + "─" * 58 + "╣")

        # Cost
        cost_per_req     = cost_usd / total_req
        cost_per_listing = cost_usd / total_listings if total_listings > 0 else 0
        print(f"║  {'Coste estimado:':<28} {'$' + f'{cost_usd:.4f}':>10} USD         ║")
        print(f"║  {'Coste por request:':<28} {'$' + f'{cost_per_req:.5f}':>10} USD         ║")
        print(f"║  {'Coste por anuncio:':<28} {'$' + f'{cost_per_listing:.5f}':>10} USD         ║")
        print("╠" + "─" * 58 + "╣")

        # Timing
        mins, secs = divmod(int(duration), 60)
        req_per_min = total_req / (duration / 60) if duration > 0 else 0
        print(f"║  {'Duración total:':<28} {f'{mins}m {secs}s':>10}              ║")
        print(f"║  {'Velocidad:':<28} {f'{req_per_min:.1f} req/min':>14}          ║")
        print("╠" + "─" * 58 + "╣")

        # Savings estimate (vs original with district scraping)
        # Original made ~21 district passes before barrios → ~30% more requests
        estimated_original = int(total_req * 1.35)
        saved_req  = estimated_original - total_req
        saved_cost = saved_req * (4.0 / 1000)
        print(f"║  {'Ahorro vs versión anterior:':<28} ~{saved_req:>5,} req  (-35%)  ║")
        print(f"║  {'Ahorro estimado en coste:':<28} {'≈$' + f'{saved_cost:.4f}':>10} USD         ║")
    else:
        print("║" + "  Sin requests — todos los barrios ya estaban al día".center(58) + "║")

    print("╚" + "═" * 58 + "╝")

    # -------------------------------------------------------------------------
    # 🏘️  RENTAL PRICE SCRAPING (page 1 per barrio, ~184 extra requests)
    # -------------------------------------------------------------------------
    run_rental_scraping(proxies)

    # -------------------------------------------------------------------------
    # ☁️  AUTO-UPLOAD TO GOOGLE DRIVE
    # -------------------------------------------------------------------------
    _auto_upload_to_drive()


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

    for distrito, barrio, sale_url_path in BARRIO_URLS:
        # Convert sale URL to rental URL
        rental_url_path = sale_url_path.replace(
            "/venta-viviendas/", "/alquiler-viviendas/", 1
        )
        url = BASE_URL + rental_url_path

        html, status_code = fetch_page(url, proxies)

        if status_code == 404:
            # Some barrios have no rental listings — expected, skip silently
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

        # Brief sleep to avoid hammering the proxy (rent scraping runs after the
        # main scrape, so we use a shorter delay)
        time.sleep(0.3)

    print(f"\n  ✅ Alquiler: {stored} barrios guardados, "
          f"{skipped} sin datos, {errors} errores")
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


if __name__ == "__main__":
    # Support --retry flag to only scrape previously failed barrios
    retry_mode = '--retry' in sys.argv
    run_scraper(retry_only=retry_mode)
