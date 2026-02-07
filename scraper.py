"""
Web scraper for Idealista Madrid real estate listings.
Uses Bright Data Web Unlocker API and BeautifulSoup for parsing.
"""

import os
import re
import time
from typing import Dict, List, Optional
from datetime import datetime

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
    mark_as_sold
)


# Load environment variables
load_dotenv()

BRIGHTDATA_USER = os.getenv('BRIGHTDATA_USER')
BRIGHTDATA_PASS = os.getenv('BRIGHTDATA_PASS')
BRIGHTDATA_HOST = os.getenv('BRIGHTDATA_HOST', 'brd.superproxy.io:33335')

BASE_URL = "https://www.idealista.com"

# Madrid's barrios organized by district (184 total)
# Format: (Distrito, Barrio, URL_path)
BARRIO_URLS = [
    # Arganzuela
    ("Arganzuela", "Acacias", "/venta-viviendas/madrid/arganzuela/acacias/"),
    ("Arganzuela", "Chopera", "/venta-viviendas/madrid/arganzuela/chopera/"),
    ("Arganzuela", "Delicias", "/venta-viviendas/madrid/arganzuela/delicias/"),
    ("Arganzuela", "Imperial", "/venta-viviendas/madrid/arganzuela/imperial/"),
    ("Arganzuela", "Legazpi", "/venta-viviendas/madrid/arganzuela/legazpi/"),
    ("Arganzuela", "Palos de la Frontera", "/venta-viviendas/madrid/arganzuela/palos-de-la-frontera/"),
    
    # Barajas
    ("Barajas", "Aeropuerto", "/venta-viviendas/madrid/barajas/aeropuerto/"),
    ("Barajas", "Alameda de Osuna", "/venta-viviendas/madrid/barajas/alameda-de-osuna/"),
    ("Barajas", "Campo de las Naciones-Corralejos", "/venta-viviendas/madrid/barajas/campo-de-las-naciones-corralejos/"),
    ("Barajas", "Casco Histórico de Barajas", "/venta-viviendas/madrid/barajas/casco-historico-de-barajas/"),
    ("Barajas", "Timón", "/venta-viviendas/madrid/barajas/timon/"),
    
    # Salamanca
    ("Salamanca", "Castellana", "/venta-viviendas/madrid/barrio-de-salamanca/castellana/"),
    ("Salamanca", "Fuente del Berro", "/venta-viviendas/madrid/barrio-de-salamanca/fuente-del-berro/"),
    ("Salamanca", "Goya", "/venta-viviendas/madrid/barrio-de-salamanca/goya/"),
    ("Salamanca", "Guindalera", "/venta-viviendas/madrid/barrio-de-salamanca/guindalera/"),
    ("Salamanca", "Lista", "/venta-viviendas/madrid/barrio-de-salamanca/lista/"),
    ("Salamanca", "Recoletos", "/venta-viviendas/madrid/barrio-de-salamanca/recoletos/"),
    
    # Carabanchel
    ("Carabanchel", "Abrantes", "/venta-viviendas/madrid/carabanchel/abrantes/"),
    ("Carabanchel", "Buena Vista", "/venta-viviendas/madrid/carabanchel/buena-vista/"),
    ("Carabanchel", "Comillas", "/venta-viviendas/madrid/carabanchel/comillas/"),
    ("Carabanchel", "Opañel", "/venta-viviendas/madrid/carabanchel/opanel/"),
    ("Carabanchel", "PAU de Carabanchel", "/venta-viviendas/madrid/carabanchel/pau-de-carabanchel/"),
    ("Carabanchel", "Puerta Bonita", "/venta-viviendas/madrid/carabanchel/puerta-bonita/"),
    ("Carabanchel", "San Isidro", "/venta-viviendas/madrid/carabanchel/san-isidro/"),
    ("Carabanchel", "Vista Alegre", "/venta-viviendas/madrid/carabanchel/vista-alegre/"),
    
    # Centro
    ("Centro", "Chueca-Justicia", "/venta-viviendas/madrid/centro/chueca-justicia/"),
    ("Centro", "Huertas-Cortes", "/venta-viviendas/madrid/centro/huertas-cortes/"),
    ("Centro", "Lavapiés-Embajadores", "/venta-viviendas/madrid/centro/lavapies-embajadores/"),
    ("Centro", "Malasaña-Universidad", "/venta-viviendas/madrid/centro/malasana-universidad/"),
    ("Centro", "Palacio", "/venta-viviendas/madrid/centro/palacio/"),
    ("Centro", "Sol", "/venta-viviendas/madrid/centro/sol/"),
    
    # Chamartín
    ("Chamartín", "Bernabéu-Hispanoamérica", "/venta-viviendas/madrid/chamartin/bernabeu-hispanoamerica/"),
    ("Chamartín", "Castilla", "/venta-viviendas/madrid/chamartin/castilla/"),
    ("Chamartín", "Ciudad Jardín", "/venta-viviendas/madrid/chamartin/ciudad-jardin/"),
    ("Chamartín", "El Viso", "/venta-viviendas/madrid/chamartin/el-viso/"),
    ("Chamartín", "Nueva España", "/venta-viviendas/madrid/chamartin/nueva-espana/"),
    ("Chamartín", "Prosperidad", "/venta-viviendas/madrid/chamartin/prosperidad/"),
    
    # Chamberí
    ("Chamberí", "Almagro", "/venta-viviendas/madrid/chamberi/almagro/"),
    ("Chamberí", "Arapiles", "/venta-viviendas/madrid/chamberi/arapiles/"),
    ("Chamberí", "Gaztambide", "/venta-viviendas/madrid/chamberi/gaztambide/"),
    ("Chamberí", "Nuevos Ministerios-Ríos Rosas", "/venta-viviendas/madrid/chamberi/nuevos-ministerios-rios-rosas/"),
    ("Chamberí", "Trafalgar", "/venta-viviendas/madrid/chamberi/trafalgar/"),
    ("Chamberí", "Vallehermoso", "/venta-viviendas/madrid/chamberi/vallehermoso/"),
    
    # Ciudad Lineal
    ("Ciudad Lineal", "Atalaya", "/venta-viviendas/madrid/ciudad-lineal/atalaya/"),
    ("Ciudad Lineal", "Colina", "/venta-viviendas/madrid/ciudad-lineal/colina/"),
    ("Ciudad Lineal", "Concepción", "/venta-viviendas/madrid/ciudad-lineal/concepcion/"),
    ("Ciudad Lineal", "Costillares", "/venta-viviendas/madrid/ciudad-lineal/costillares/"),
    ("Ciudad Lineal", "Pueblo Nuevo", "/venta-viviendas/madrid/ciudad-lineal/pueblo-nuevo/"),
    ("Ciudad Lineal", "Quintana", "/venta-viviendas/madrid/ciudad-lineal/quintana/"),
    ("Ciudad Lineal", "San Juan Bautista", "/venta-viviendas/madrid/ciudad-lineal/san-juan-bautista/"),
    ("Ciudad Lineal", "San Pascual", "/venta-viviendas/madrid/ciudad-lineal/san-pascual/"),
    ("Ciudad Lineal", "Ventas", "/venta-viviendas/madrid/ciudad-lineal/ventas/"),
    
    # Fuencarral-El Pardo
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
    
    # Hortaleza
    ("Hortaleza", "Apóstol Santiago", "/venta-viviendas/madrid/hortaleza/apostol-santiago/"),
    ("Hortaleza", "Canillas", "/venta-viviendas/madrid/hortaleza/canillas/"),
    ("Hortaleza", "Conde Orgaz-Piovera", "/venta-viviendas/madrid/hortaleza/conde-orgaz-piovera/"),
    ("Hortaleza", "Palomas", "/venta-viviendas/madrid/hortaleza/palomas/"),
    ("Hortaleza", "Pinar del Rey", "/venta-viviendas/madrid/hortaleza/pinar-del-rey/"),
    ("Hortaleza", "Sanchinarro", "/venta-viviendas/madrid/hortaleza/sanchinarro/"),
    ("Hortaleza", "Valdebebas-Valdefuentes", "/venta-viviendas/madrid/hortaleza/valdebebas-valdefuentes/"),
    ("Hortaleza", "Virgen del Cortijo-Manoteras", "/venta-viviendas/madrid/hortaleza/virgen-del-cortijo-manoteras/"),
    
    # Latina
    ("Latina", "Águilas", "/venta-viviendas/madrid/latina/aguilas/"),
    ("Latina", "Aluche", "/venta-viviendas/madrid/latina/aluche/"),
    ("Latina", "Campamento", "/venta-viviendas/madrid/latina/campamento/"),
    ("Latina", "Cuatro Vientos", "/venta-viviendas/madrid/latina/cuatro-vientos/"),
    ("Latina", "Los Cármenes", "/venta-viviendas/madrid/latina/los-carmenes/"),
    ("Latina", "Lucero", "/venta-viviendas/madrid/latina/lucero/"),
    ("Latina", "Puerta del Ángel", "/venta-viviendas/madrid/latina/puerta-del-angel/"),
    
    # Moncloa-Aravaca
    ("Moncloa-Aravaca", "Aravaca", "/venta-viviendas/madrid/moncloa/aravaca/"),
    ("Moncloa-Aravaca", "Argüelles", "/venta-viviendas/madrid/moncloa/arguelles/"),
    ("Moncloa-Aravaca", "Casa de Campo", "/venta-viviendas/madrid/moncloa/casa-de-campo/"),
    ("Moncloa-Aravaca", "Ciudad Universitaria", "/venta-viviendas/madrid/moncloa/ciudad-universitaria/"),
    ("Moncloa-Aravaca", "El Plantío", "/venta-viviendas/madrid/moncloa/el-plantio/"),
    ("Moncloa-Aravaca", "Valdemarín", "/venta-viviendas/madrid/moncloa/valdemarin/"),
    ("Moncloa-Aravaca", "Valdezarza", "/venta-viviendas/madrid/moncloa/valdezarza/"),
    
    # Moratalaz
    ("Moratalaz", "Fontarrón", "/venta-viviendas/madrid/moratalaz/fontarron/"),
    ("Moratalaz", "Horcajo", "/venta-viviendas/madrid/moratalaz/horcajo/"),
    ("Moratalaz", "Marroquina", "/venta-viviendas/madrid/moratalaz/marroquina/"),
    ("Moratalaz", "Media Legua", "/venta-viviendas/madrid/moratalaz/media-legua/"),
    ("Moratalaz", "Pavones", "/venta-viviendas/madrid/moratalaz/pavones/"),
    ("Moratalaz", "Vinateros", "/venta-viviendas/madrid/moratalaz/vinateros/"),
    
    # Puente de Vallecas
    ("Puente de Vallecas", "Entrevías", "/venta-viviendas/madrid/puente-de-vallecas/entrevias/"),
    ("Puente de Vallecas", "Numancia", "/venta-viviendas/madrid/puente-de-vallecas/numancia/"),
    ("Puente de Vallecas", "Palomeras Bajas", "/venta-viviendas/madrid/puente-de-vallecas/palomeras-bajas/"),
    ("Puente de Vallecas", "Palomeras Sureste", "/venta-viviendas/madrid/puente-de-vallecas/palomeras-sureste/"),
    ("Puente de Vallecas", "Portazgo", "/venta-viviendas/madrid/puente-de-vallecas/portazgo/"),
    ("Puente de Vallecas", "San Diego", "/venta-viviendas/madrid/puente-de-vallecas/san-diego/"),
    
    # Retiro
    ("Retiro", "Adelfas", "/venta-viviendas/madrid/retiro/adelfas/"),
    ("Retiro", "Estrella", "/venta-viviendas/madrid/retiro/estrella/"),
    ("Retiro", "Ibiza", "/venta-viviendas/madrid/retiro/ibiza/"),
    ("Retiro", "Jerónimos", "/venta-viviendas/madrid/retiro/jeronimos/"),
    ("Retiro", "Niño Jesús", "/venta-viviendas/madrid/retiro/nino-jesus/"),
    ("Retiro", "Pacífico", "/venta-viviendas/madrid/retiro/pacifico/"),
    
    # San Blas-Canillejas
    ("San Blas-Canillejas", "Amposta", "/venta-viviendas/madrid/san-blas/amposta/"),
    ("San Blas-Canillejas", "Arcos", "/venta-viviendas/madrid/san-blas/arcos/"),
    ("San Blas-Canillejas", "Canillejas", "/venta-viviendas/madrid/san-blas/canillejas/"),
    ("San Blas-Canillejas", "Hellín", "/venta-viviendas/madrid/san-blas/hellin/"),
    ("San Blas-Canillejas", "Rejas", "/venta-viviendas/madrid/san-blas/rejas/"),
    ("San Blas-Canillejas", "Rosas", "/venta-viviendas/madrid/san-blas/rosas/"),
    ("San Blas-Canillejas", "Salvador", "/venta-viviendas/madrid/san-blas/salvador/"),
    ("San Blas-Canillejas", "Simancas", "/venta-viviendas/madrid/san-blas/simancas/"),
    
    # Tetuán
    ("Tetuán", "Bellas Vistas", "/venta-viviendas/madrid/tetuan/bellas-vistas/"),
    ("Tetuán", "Berruguete", "/venta-viviendas/madrid/tetuan/berruguete/"),
    ("Tetuán", "Cuatro Caminos", "/venta-viviendas/madrid/tetuan/cuatro-caminos/"),
    ("Tetuán", "Cuzco-Castillejos", "/venta-viviendas/madrid/tetuan/cuzco-castillejos/"),
    ("Tetuán", "Valdeacederas", "/venta-viviendas/madrid/tetuan/valdeacederas/"),
    ("Tetuán", "Ventilla-Almenara", "/venta-viviendas/madrid/tetuan/ventilla-almenara/"),
    
    # Usera
    ("Usera", "12 de Octubre-Orcasur", "/venta-viviendas/madrid/usera/12-de-octubre-orcasur/"),
    ("Usera", "Almendrales", "/venta-viviendas/madrid/usera/almendrales/"),
    ("Usera", "Moscardó", "/venta-viviendas/madrid/usera/moscardo/"),
    ("Usera", "Orcasitas", "/venta-viviendas/madrid/usera/orcasitas/"),
    ("Usera", "Pradolongo", "/venta-viviendas/madrid/usera/pradolongo/"),
    ("Usera", "San Fermín", "/venta-viviendas/madrid/usera/san-fermin/"),
    ("Usera", "Zofío", "/venta-viviendas/madrid/usera/zofio/"),
    
    # Vicálvaro
    ("Vicálvaro", "Ambroz", "/venta-viviendas/madrid/vicalvaro/ambroz/"),
    ("Vicálvaro", "Casco Histórico de Vicálvaro", "/venta-viviendas/madrid/vicalvaro/casco-historico-de-vicalvaro/"),
    ("Vicálvaro", "El Cañaveral", "/venta-viviendas/madrid/vicalvaro/el-canaveral/"),
    ("Vicálvaro", "Los Ahijones", "/venta-viviendas/madrid/vicalvaro/los-ahijones/"),
    ("Vicálvaro", "Los Berrocales", "/venta-viviendas/madrid/vicalvaro/los-berrocales/"),
    ("Vicálvaro", "Los Cerros", "/venta-viviendas/madrid/vicalvaro/los-cerros/"),
    ("Vicálvaro", "Valdebernardo-Valderrivas", "/venta-viviendas/madrid/vicalvaro/valdebernardo-valderrivas/"),
    
    # Villa de Vallecas
    ("Villa de Vallecas", "Casco Histórico de Vallecas", "/venta-viviendas/madrid/villa-de-vallecas/casco-historico-de-vallecas/"),
    ("Villa de Vallecas", "Ensanche de Vallecas-La Gavia", "/venta-viviendas/madrid/villa-de-vallecas/ensanche-de-vallecas-la-gavia/"),
    ("Villa de Vallecas", "Santa Eugenia", "/venta-viviendas/madrid/villa-de-vallecas/santa-eugenia/"),
    ("Villa de Vallecas", "Valdecarros", "/venta-viviendas/madrid/villa-de-vallecas/valdecarros/"),
    
    # Villaverde
    ("Villaverde", "Butarque", "/venta-viviendas/madrid/villaverde/butarque/"),
    ("Villaverde", "Los Ángeles", "/venta-viviendas/madrid/villaverde/los-angeles/"),
    ("Villaverde", "Los Rosales", "/venta-viviendas/madrid/villaverde/los-rosales/"),
    ("Villaverde", "San Cristóbal", "/venta-viviendas/madrid/villaverde/san-cristobal/"),
    ("Villaverde", "Villaverde Alto", "/venta-viviendas/madrid/villaverde/villaverde-alto/"),
    
    # ============================================================================
    # ADDITIONAL BARRIOS - Added to increase coverage
    # ============================================================================
    
    # Additional Arganzuela barrios
    ("Arganzuela", "Atocha", "/venta-viviendas/madrid/arganzuela/atocha/"),
    
    # Additional Fuencarral-El Pardo barrios
    ("Fuencarral-El Pardo", "Barrio del Pilar", "/venta-viviendas/madrid/fuencarral/barrio-del-pilar/"),
    ("Fuencarral-El Pardo", "El Goloso", "/venta-viviendas/madrid/fuencarral/el-goloso/"),
    ("Fuencarral-El Pardo", "Valverde", "/venta-viviendas/madrid/fuencarral/valverde/"),
    
    # Additional Latina barrios
    ("Latina", "Batán", "/venta-viviendas/madrid/latina/batan/"),
    
    # Additional Moncloa-Aravaca barrios
    ("Moncloa-Aravaca", "Dehesa de la Villa", "/venta-viviendas/madrid/moncloa/dehesa-de-la-villa/"),
    
    # Additional Moratalaz barrios
    ("Moratalaz", "Arroyo del Olivar", "/venta-viviendas/madrid/moratalaz/arroyo-del-olivar/"),
    
    # Additional Puente de Vallecas barrios
    ("Puente de Vallecas", "Doña Carlota", "/venta-viviendas/madrid/puente-de-vallecas/dona-carlota/"),
    ("Puente de Vallecas", "Pozo del Tío Raimundo", "/venta-viviendas/madrid/puente-de-vallecas/pozo-del-tio-raimundo/"),
    
    # Additional San Blas-Canillejas barrios
    ("San Blas-Canillejas", "Barrio del Aeropuerto", "/venta-viviendas/madrid/san-blas/barrio-del-aeropuerto/"),
    ("San Blas-Canillejas", "Casco Histórico de Canillejas", "/venta-viviendas/madrid/san-blas/casco-historico-de-canillejas/"),
    ("San Blas-Canillejas", "Colonia Jardín", "/venta-viviendas/madrid/san-blas/colonia-jardin/"),
    
    # Additional Hortaleza barrios
    ("Hortaleza", "Pinar de Chamartín", "/venta-viviendas/madrid/hortaleza/pinar-de-chamartin/"),
    
    # Additional Chamartín barrios
    ("Chamartín", "Pinar de Chamartín", "/venta-viviendas/madrid/chamartin/pinar-de-chamartin/"),
    ("Chamartín", "Costillares", "/venta-viviendas/madrid/chamartin/costillares/"),
    
    # Additional Retiro barrios
    ("Retiro", "Atocha", "/venta-viviendas/madrid/retiro/atocha/"),
    
    # Additional Salamanca barrios
    ("Salamanca", "Concepción", "/venta-viviendas/madrid/barrio-de-salamanca/concepcion/"),
    
    # Additional Tetuán barrios
    ("Tetuán", "Almenara", "/venta-viviendas/madrid/tetuan/almenara/"),
    
    # Additional Usera barrios
    ("Usera", "Poblado Dirigido de Orcasitas", "/venta-viviendas/madrid/usera/poblado-dirigido-de-orcasitas/"),
    
    # Additional Villaverde barrios
    ("Villaverde", "Marconi", "/venta-viviendas/madrid/villaverde/marconi/"),
    ("Villaverde", "San Andrés", "/venta-viviendas/madrid/villaverde/san-andres/"),
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


def fetch_page(url: str, proxies: Optional[Dict] = None, retries: int = 3) -> Optional[str]:
    """
    Fetch HTML content from URL with retry logic.
    
    Args:
        url: Target URL
        proxies: Proxy configuration
        retries: Number of retry attempts
        
    Returns:
        HTML content or None if failed
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
    }
    
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
                return response.text
            else:
                request_counter['failed'] += 1
                print(f"  ⚠ HTTP {response.status_code} for {url}")
                
        except requests.exceptions.RequestException as e:
            request_counter['failed'] += 1
            print(f"  ⚠ Request error (attempt {attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
    
    return None


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
    
    Args:
        url: Property detail page URL
        proxies: Proxy configuration
        
    Returns:
        Property description text or None if not found
    """
    try:
        html = fetch_page(url, proxies, retries=2)
        if not html:
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


def scrape_barrio(distrito: str, barrio: str, url_path: str, proxies: Optional[Dict], seen_ids: set) -> int:
    """
    Scrape all pages for a single barrio.
    
    Args:
        distrito: District name
        barrio: Barrio (neighborhood) name
        url_path: URL path for barrio
        proxies: Proxy configuration
        seen_ids: Set to track seen listing IDs (modified in place)
        
    Returns:
        Number of listings processed
    """
    print(f"\n📍 Scraping {distrito} - {barrio}...")
    listings_count = 0
    page = 1
    
    while page <= 60:  # Idealista limit
        # Build URL with pagination
        if page == 1:
            url = BASE_URL + url_path
        else:
            url = BASE_URL + url_path + f"pagina-{page}.htm"
        
        print(f"  Page {page}...", end=' ')
        
        html = fetch_page(url, proxies)
        if not html:
            print("❌ Failed to fetch")
            break
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # Find all listing articles
        articles = soup.find_all('article', class_='item')
        
        if not articles:
            print(f"✓ No more listings (end of pagination)")
            break
        
        print(f"Found {len(articles)} listings")
        
        # Parse each listing
        for article in articles:
            listing_data = parse_listing(article, distrito, barrio)
            
            if listing_data and listing_data['listing_id']:
                listing_id = listing_data['listing_id']
                
                # Check if listing exists in database
                if listing_id in seen_ids:
                    # Update existing listing
                    update_listing(listing_id, listing_data)
                    seen_ids.remove(listing_id)  # Remove from "not seen" set
                else:
                    # Insert new listing (without description to avoid 502 errors)
                    insert_listing(listing_data)
                
                listings_count += 1
        
        # Check for next page
        next_button = soup.find('a', class_='icon-arrow-right-after')
        if not next_button:
            print(f"  ✓ Reached last page")
            break
        
        page += 1
        time.sleep(1)  # Rate limiting
    
    return listings_count


def run_scraper():
    """
    Main scraper orchestration function.
    Iterates through all districts and updates database.
    """
    print("=" * 60)
    print("🏠 Madrid Real Estate Tracker - Scraper")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Initialize database
    init_database()
    
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
    
    # Scrape all barrios
    total_listings = 0
    
    for distrito, barrio, url_path in BARRIO_URLS:
        count = scrape_barrio(distrito, barrio, url_path, proxies, active_ids)
        total_listings += count
        time.sleep(1)  # Rate limiting between barrios
    
    # Mark unseen listings as sold
    print(f"\n🔍 Checking for sold/removed properties...")
    print(f"  {len(active_ids)} listings not seen in this scrape")
    
    if active_ids:
        sold_count = mark_as_sold(active_ids)
        print(f"  ✓ Marked {sold_count} listings as sold/removed")
    
    # Summary
    print("\n" + "=" * 60)
    print("✅ Scraping Complete")
    print("=" * 60)
    print(f"Total listings processed: {total_listings}")
    print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # Bright Data usage report
    cost_data = get_brightdata_cost_estimate()
    print("\n💰 Bright Data Usage Report")
    print("=" * 60)
    print(f"Total requests: {cost_data['total_requests']:,}")
    print(f"  ✓ Successful: {cost_data['successful_requests']:,} ({cost_data['successful_requests']/cost_data['total_requests']*100:.1f}%)")
    print(f"  ✗ Failed: {cost_data['failed_requests']:,} ({cost_data['failed_requests']/cost_data['total_requests']*100:.1f}%)")
    print(f"Estimated cost: ${cost_data['estimated_cost_usd']:.2f} USD")
    print(f"Cost per request: ${cost_data['cost_per_request']:.4f} USD")
    print("=" * 60)


if __name__ == "__main__":
    run_scraper()
