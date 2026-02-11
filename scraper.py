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
    mark_as_sold,
    mark_stale_as_sold
)


# Load environment variables
load_dotenv()

BRIGHTDATA_USER = os.getenv('BRIGHTDATA_USER')
BRIGHTDATA_PASS = os.getenv('BRIGHTDATA_PASS')
BRIGHTDATA_HOST = os.getenv('BRIGHTDATA_HOST', 'brd.superproxy.io:33335')

BASE_URL = "https://www.idealista.com"

# Madrid's barrios organized by district (144 total - verified from Idealista)
# Format: (Distrito, Barrio, URL_path)
BARRIO_URLS = [
    # Arganzuela (6 barrios)
    ("Arganzuela", "Acacias", "/venta-viviendas/madrid/arganzuela/acacias/"),
    ("Arganzuela", "Chopera", "/venta-viviendas/madrid/arganzuela/chopera/"),
    ("Arganzuela", "Delicias", "/venta-viviendas/madrid/arganzuela/delicias/"),
    ("Arganzuela", "Imperial", "/venta-viviendas/madrid/arganzuela/imperial/"),
    ("Arganzuela", "Legazpi", "/venta-viviendas/madrid/arganzuela/legazpi/"),
    ("Arganzuela", "Palos de Moguer", "/venta-viviendas/madrid/arganzuela/palos-de-moguer/"),
    
    # Barajas (5 barrios)
    ("Barajas", "Aeropuerto", "/venta-viviendas/madrid/barajas/aeropuerto/"),
    ("Barajas", "Alameda de Osuna", "/venta-viviendas/madrid/barajas/alameda-de-osuna/"),
    ("Barajas", "Casco Histórico de Barajas", "/venta-viviendas/madrid/barajas/casco-historico-de-barajas/"),
    ("Barajas", "Corralejos", "/venta-viviendas/madrid/barajas/corralejos/"),
    ("Barajas", "Timón", "/venta-viviendas/madrid/barajas/timon/"),
    
    # Carabanchel (8 barrios)
    ("Carabanchel", "Abrantes", "/venta-viviendas/madrid/carabanchel/abrantes/"),
    ("Carabanchel", "Buenavista", "/venta-viviendas/madrid/carabanchel/buenavista/"),
    ("Carabanchel", "Comillas", "/venta-viviendas/madrid/carabanchel/comillas/"),
    ("Carabanchel", "Opañel", "/venta-viviendas/madrid/carabanchel/opanel/"),
    ("Carabanchel", "Puerta Bonita", "/venta-viviendas/madrid/carabanchel/puerta-bonita/"),
    ("Carabanchel", "San Isidro", "/venta-viviendas/madrid/carabanchel/san-isidro/"),
    ("Carabanchel", "Vista Alegre", "/venta-viviendas/madrid/carabanchel/vista-alegre/"),
    ("Carabanchel", "Vistalegre-La Chimenea", "/venta-viviendas/madrid/carabanchel/vistalegre-la-chimenea/"),
    
    # Centro (6 barrios)
    ("Centro", "Cortes", "/venta-viviendas/madrid/centro/cortes/"),
    ("Centro", "Embajadores", "/venta-viviendas/madrid/centro/embajadores/"),
    ("Centro", "Justicia", "/venta-viviendas/madrid/centro/justicia/"),
    ("Centro", "Palacio", "/venta-viviendas/madrid/centro/palacio/"),
    ("Centro", "Sol", "/venta-viviendas/madrid/centro/sol/"),
    ("Centro", "Universidad", "/venta-viviendas/madrid/centro/universidad/"),
    
    # Chamartín (6 barrios)
    ("Chamartín", "Castilla", "/venta-viviendas/madrid/chamartin/castilla/"),
    ("Chamartín", "Ciudad Jardín", "/venta-viviendas/madrid/chamartin/ciudad-jardin/"),
    ("Chamartín", "El Viso", "/venta-viviendas/madrid/chamartin/el-viso/"),
    ("Chamartín", "Hispanoamérica", "/venta-viviendas/madrid/chamartin/hispanoamerica/"),
    ("Chamartín", "Nueva España", "/venta-viviendas/madrid/chamartin/nueva-espana/"),
    ("Chamartín", "Prosperidad", "/venta-viviendas/madrid/chamartin/prosperidad/"),
    
    # Chamberí (6 barrios)
    ("Chamberí", "Almagro", "/venta-viviendas/madrid/chamberi/almagro/"),
    ("Chamberí", "Arapiles", "/venta-viviendas/madrid/chamberi/arapiles/"),
    ("Chamberí", "Gaztambide", "/venta-viviendas/madrid/chamberi/gaztambide/"),
    ("Chamberí", "Ríos Rosas", "/venta-viviendas/madrid/chamberi/rios-rosas/"),
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
    
    # Fuencarral-El Pardo (9 barrios)
    ("Fuencarral-El Pardo", "Barrio del Pilar", "/venta-viviendas/madrid/fuencarral/barrio-del-pilar/"),
    ("Fuencarral-El Pardo", "El Goloso", "/venta-viviendas/madrid/fuencarral/el-goloso/"),
    ("Fuencarral-El Pardo", "El Pardo", "/venta-viviendas/madrid/fuencarral/el-pardo/"),
    ("Fuencarral-El Pardo", "Fuentelarreina", "/venta-viviendas/madrid/fuencarral/fuentelarreina/"),
    ("Fuencarral-El Pardo", "La Paz", "/venta-viviendas/madrid/fuencarral/la-paz/"),
    ("Fuencarral-El Pardo", "Mirasierra", "/venta-viviendas/madrid/fuencarral/mirasierra/"),
    ("Fuencarral-El Pardo", "Peñagrande", "/venta-viviendas/madrid/fuencarral/penagrande/"),
    ("Fuencarral-El Pardo", "Tres Olivos-Valverde", "/venta-viviendas/madrid/fuencarral/tres-olivos-valverde/"),
    ("Fuencarral-El Pardo", "Valverde", "/venta-viviendas/madrid/fuencarral/valverde/"),
    
    # Hortaleza (9 barrios)
    ("Hortaleza", "Apóstol Santiago", "/venta-viviendas/madrid/hortaleza/apostol-santiago/"),
    ("Hortaleza", "Canillas", "/venta-viviendas/madrid/hortaleza/canillas/"),
    ("Hortaleza", "Palomas", "/venta-viviendas/madrid/hortaleza/palomas/"),
    ("Hortaleza", "Pinar de Chamartín", "/venta-viviendas/madrid/hortaleza/pinar-de-chamartin/"),
    ("Hortaleza", "Pinar del Rey", "/venta-viviendas/madrid/hortaleza/pinar-del-rey/"),
    ("Hortaleza", "Piovera", "/venta-viviendas/madrid/hortaleza/piovera/"),
    ("Hortaleza", "Sanchinarro", "/venta-viviendas/madrid/hortaleza/sanchinarro/"),
    ("Hortaleza", "Valdebebas-Valdefuentes", "/venta-viviendas/madrid/hortaleza/valdebebas-valdefuentes/"),
    ("Hortaleza", "Virgen del Cortijo-Manoteras", "/venta-viviendas/madrid/hortaleza/virgen-del-cortijo-manoteras/"),
    
    # Latina (8 barrios)
    ("Latina", "Águilas", "/venta-viviendas/madrid/latina/aguilas/"),
    ("Latina", "Aluche", "/venta-viviendas/madrid/latina/aluche/"),
    ("Latina", "Batán", "/venta-viviendas/madrid/latina/batan/"),
    ("Latina", "Campamento", "/venta-viviendas/madrid/latina/campamento/"),
    ("Latina", "Cuatro Vientos", "/venta-viviendas/madrid/latina/cuatro-vientos/"),
    ("Latina", "Los Cármenes", "/venta-viviendas/madrid/latina/los-carmenes/"),
    ("Latina", "Lucero", "/venta-viviendas/madrid/latina/lucero/"),
    ("Latina", "Puerta del Ángel", "/venta-viviendas/madrid/latina/puerta-del-angel/"),
    
    # Moncloa-Aravaca (8 barrios)
    ("Moncloa-Aravaca", "Aravaca", "/venta-viviendas/madrid/moncloa/aravaca/"),
    ("Moncloa-Aravaca", "Argüelles", "/venta-viviendas/madrid/moncloa/arguelles/"),
    ("Moncloa-Aravaca", "Casa de Campo", "/venta-viviendas/madrid/moncloa/casa-de-campo/"),
    ("Moncloa-Aravaca", "Ciudad Universitaria", "/venta-viviendas/madrid/moncloa/ciudad-universitaria/"),
    ("Moncloa-Aravaca", "Dehesa de la Villa", "/venta-viviendas/madrid/moncloa/dehesa-de-la-villa/"),
    ("Moncloa-Aravaca", "El Plantío", "/venta-viviendas/madrid/moncloa/el-plantio/"),
    ("Moncloa-Aravaca", "Valdemarín", "/venta-viviendas/madrid/moncloa/valdemarin/"),
    ("Moncloa-Aravaca", "Valdezarza", "/venta-viviendas/madrid/moncloa/valdezarza/"),
    
    # Moratalaz (7 barrios)
    ("Moratalaz", "Arroyo del Olivar", "/venta-viviendas/madrid/moratalaz/arroyo-del-olivar/"),
    ("Moratalaz", "Fontarrón", "/venta-viviendas/madrid/moratalaz/fontarron/"),
    ("Moratalaz", "Horcajo", "/venta-viviendas/madrid/moratalaz/horcajo/"),
    ("Moratalaz", "Marroquina", "/venta-viviendas/madrid/moratalaz/marroquina/"),
    ("Moratalaz", "Media Legua", "/venta-viviendas/madrid/moratalaz/media-legua/"),
    ("Moratalaz", "Pavones", "/venta-viviendas/madrid/moratalaz/pavones/"),
    ("Moratalaz", "Vinateros", "/venta-viviendas/madrid/moratalaz/vinateros/"),
    
    # Puente de Vallecas (8 barrios)
    ("Puente de Vallecas", "Doña Carlota", "/venta-viviendas/madrid/puente-de-vallecas/dona-carlota/"),
    ("Puente de Vallecas", "Entrevías", "/venta-viviendas/madrid/puente-de-vallecas/entrevias/"),
    ("Puente de Vallecas", "Numancia", "/venta-viviendas/madrid/puente-de-vallecas/numancia/"),
    ("Puente de Vallecas", "Palomeras Bajas", "/venta-viviendas/madrid/puente-de-vallecas/palomeras-bajas/"),
    ("Puente de Vallecas", "Palomeras Sureste", "/venta-viviendas/madrid/puente-de-vallecas/palomeras-sureste/"),
    ("Puente de Vallecas", "Portazgo", "/venta-viviendas/madrid/puente-de-vallecas/portazgo/"),
    ("Puente de Vallecas", "Pozo del Tío Raimundo", "/venta-viviendas/madrid/puente-de-vallecas/pozo-del-tio-raimundo/"),
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

# Global tracking for 502 errors
errors_502 = []  # List of (distrito, barrio, url_path) tuples

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
    Fetch HTML content from URL with retry logic.
    
    Args:
        url: Target URL
        proxies: Proxy configuration
        retries: Number of retry attempts
        
    Returns:
        Tuple of (HTML content or None, status_code)
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
                return response.text, 200
            elif response.status_code == 404:
                request_counter['failed'] += 1
                # Log 404 errors to file for later removal
                with open('404_errors.log', 'a') as f:
                    f.write(f"{url}\n")
                print(f"  ⚠ HTTP 404 Not Found - logged to 404_errors.log")
                return None, 404
            elif response.status_code == 502:
                request_counter['failed'] += 1
                print(f"  ⚠ HTTP 502 Bad Gateway")
                return None, 502
            else:
                request_counter['failed'] += 1
                print(f"  ⚠ HTTP {response.status_code} for {url}")
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
    
    Args:
        url: Property detail page URL
        proxies: Proxy configuration
        
    Returns:
        Property description text or None if not found
    """
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


def scrape_barrio(distrito: str, barrio: str, url_path: str, proxies: Optional[Dict], seen_ids: set) -> int:
    """
    Scrape all pages for a single barrio.
    Tracks 502 errors globally for later retry.
    
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
        
        html, status_code = fetch_page(url, proxies)
        
        if status_code == 502:
            print("❌ 502 Bad Gateway - will retry later")
            # Add to global errors list (only once per barrio)
            if (distrito, barrio, url_path) not in errors_502:
                errors_502.append((distrito, barrio, url_path))
            return listings_count  # Stop scraping this barrio
        
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


def retry_502_errors(proxies: Optional[Dict], active_ids: set) -> int:
    """
    Retry scraping barrios that had 502 errors.
    Prompts user interactively and allows recursive retries.
    
    Args:
        proxies: Proxy configuration
        active_ids: Set of active listing IDs (modified in place)
        
    Returns:
        Number of listings processed in retry attempts
    """
    if not errors_502:
        return 0
    
    print("\n" + "=" * 60)
    print("🔄 RETRY 502 ERRORS")
    print("=" * 60)
    print(f"\nFound {len(errors_502)} barrios with 502 errors:")
    
    for i, (distrito, barrio, url_path) in enumerate(errors_502, 1):
        print(f"  {i}. {distrito} - {barrio}")
    
    response = input("\n¿Quieres reintentar estos barrios? (y/n): ")
    
    if response.lower() != 'y':
        print("Skipping retries.")
        return 0
    
    print("\n🔄 Retrying barrios with 502 errors...")
    
    total_listings = 0
    # Create a copy of errors to retry
    barrios_to_retry = errors_502.copy()
    # Clear the global list
    errors_502.clear()
    
    for distrito, barrio, url_path in barrios_to_retry:
        count = scrape_barrio(distrito, barrio, url_path, proxies, active_ids)
        total_listings += count
        time.sleep(1)
    
    print(f"\n✓ Retry complete. Processed {total_listings} listings.")
    
    # Recursive retry if there are still errors
    if errors_502:
        print(f"\n⚠️  Still have {len(errors_502)} barrios with 502 errors.")
        return total_listings + retry_502_errors(proxies, active_ids)
    
    return total_listings


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
    
    # Retry 502 errors
    retry_count = retry_502_errors(proxies, active_ids)
    total_listings += retry_count
    
    # Mark stale listings as sold (not seen in 7+ days)
    print(f"\n🔍 Checking for sold/removed properties...")
    print(f"  Marking properties not seen in 7+ days as sold...")
    
    sold_count = mark_stale_as_sold(days_threshold=7)
    print(f"  ✓ Marked {sold_count} listings as sold/removed (not seen in 7+ days)")
    
    # Log properties not seen in this scrape (but not marking as sold yet)
    if active_ids:
        print(f"  ℹ️  {len(active_ids)} properties not seen in this scrape")
        print(f"  ℹ️  These will be marked as sold if not seen again within 7 days")

    
    # Summary
    print("\n" + "=" * 60)
    print("✅ Scraping Complete")
    print("=" * 60)
    print(f"Total listings processed: {total_listings}")
    if errors_502:
        print(f"⚠️  {len(errors_502)} barrios still have 502 errors (not retried)")
        print(f"   Run the scraper again to retry these barrios.")
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
