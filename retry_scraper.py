"""
Retry scraper for barrios that failed with 502 errors.
This script only scrapes the barrios that were not updated today.
"""

import os
import re
import time
from typing import Dict, List, Optional, Set
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import urllib3

# Disable SSL warnings
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

# Barrios that failed to scrape (missing from today's update)
RETRY_BARRIOS = [
    # Fuencarral-El Pardo
    ("Fuencarral-El Pardo", "El Pardo", "/venta-viviendas/madrid/fuencarral/el-pardo/"),
    ("Fuencarral-El Pardo", "Las Tablas", "/venta-viviendas/madrid/fuencarral/las-tablas/"),
    
    # Hortaleza
    ("Hortaleza", "Palomas", "/venta-viviendas/madrid/hortaleza/palomas/"),
    
    # Latina
    ("Latina", "Campamento", "/venta-viviendas/madrid/latina/campamento/"),
    ("Latina", "Cuatro Vientos", "/venta-viviendas/madrid/latina/cuatro-vientos/"),
    
    # Moncloa-Aravaca
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
]


# Import helper functions from main scraper
def get_proxy_config() -> Optional[Dict]:
    """Configure Bright Data proxy settings."""
    if not all([BRIGHTDATA_USER, BRIGHTDATA_PASS, BRIGHTDATA_HOST]):
        print("⚠ Warning: Bright Data credentials not configured")
        return None
    
    proxy_url = f'http://{BRIGHTDATA_USER}:{BRIGHTDATA_PASS}@{BRIGHTDATA_HOST}'
    return {
        'http': proxy_url,
        'https': proxy_url
    }


request_counter = {'successful': 0, 'failed': 0, 'total': 0}


def fetch_page(url: str, proxies: Optional[Dict] = None, retries: int = 3) -> Optional[str]:
    """Fetch HTML content from URL with retry logic."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
    }
    
    for attempt in range(retries):
        try:
            request_counter['total'] += 1
            
            response = requests.get(
                url,
                proxies=proxies,
                headers=headers,
                timeout=60,
                verify=False
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
                time.sleep(2 ** attempt)
    
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


def parse_listing(article: BeautifulSoup, distrito: str, barrio: str) -> Optional[Dict]:
    """Parse a single listing article element."""
    try:
        listing_id = article.get('data-element-id')
        if not listing_id:
            return None
        
        link_elem = article.find('a', class_='item-link')
        if not link_elem:
            return None
        
        title = link_elem.get_text(strip=True)
        url = BASE_URL + link_elem.get('href', '')
        
        price_elem = article.find('span', class_='item-price')
        price = None
        if price_elem:
            price_text = price_elem.get_text(strip=True)
            price = extract_number(price_text)
        
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
        
        seller_type = 'Particular'
        if article.find('span', class_='logo-branding') or article.find('picture', class_='logo-branding'):
            seller_type = 'Agencia'
        
        is_new_development = bool(article.find('span', class_='item-new-construction'))
        
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
            'is_new_development': is_new_development
        }
        
    except Exception as e:
        print(f"  ⚠ Error parsing listing: {e}")
        return None


def scrape_barrio(distrito: str, barrio: str, url_path: str, proxies: Optional[Dict], seen_ids: set) -> int:
    """Scrape all pages for a single barrio."""
    print(f"\n📍 Scraping {distrito} - {barrio}...")
    listings_count = 0
    page = 1
    
    while page <= 60:
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
        articles = soup.find_all('article', class_='item')
        
        if not articles:
            print(f"✓ No more listings")
            break
        
        print(f"Found {len(articles)} listings")
        
        for article in articles:
            listing_data = parse_listing(article, distrito, barrio)
            
            if listing_data and listing_data['listing_id']:
                listing_id = listing_data['listing_id']
                
                if listing_id in seen_ids:
                    update_listing(listing_id, listing_data)
                    seen_ids.remove(listing_id)
                else:
                    insert_listing(listing_data)
                
                listings_count += 1
        
        next_button = soup.find('a', class_='icon-arrow-right-after')
        if not next_button:
            print(f"  ✓ Reached last page")
            break
        
        page += 1
        time.sleep(1)
    
    return listings_count


def run_retry_scraper():
    """Run scraper for failed barrios only."""
    print("=" * 60)
    print("🔄 Madrid Real Estate Tracker - Retry Scraper")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    print(f"Retrying {len(RETRY_BARRIOS)} barrios that failed with 502 errors\n")
    
    init_database()
    
    proxies = get_proxy_config()
    if not proxies:
        print("\n⚠ Running without proxy (may get blocked)")
        response = input("Continue anyway? (y/n): ")
        if response.lower() != 'y':
            print("Aborted.")
            return
    
    print("\n📊 Loading active listings from database...")
    active_ids = get_active_listing_ids()
    print(f"  Found {len(active_ids)} active listings")
    
    total_listings = 0
    successful_barrios = 0
    failed_barrios = []
    
    for distrito, barrio, url_path in RETRY_BARRIOS:
        try:
            count = scrape_barrio(distrito, barrio, url_path, proxies, active_ids)
            total_listings += count
            successful_barrios += 1
            time.sleep(2)  # Extra delay between barrios to avoid rate limiting
        except Exception as e:
            print(f"  ❌ Error scraping {distrito} - {barrio}: {e}")
            failed_barrios.append((distrito, barrio))
    
    print("\n" + "=" * 60)
    print("✅ Retry Scraping Complete")
    print("=" * 60)
    print(f"Successful barrios: {successful_barrios}/{len(RETRY_BARRIOS)}")
    print(f"Total listings processed: {total_listings}")
    
    if failed_barrios:
        print(f"\n⚠️ Failed barrios ({len(failed_barrios)}):")
        for distrito, barrio in failed_barrios:
            print(f"  - {distrito} - {barrio}")
    
    print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)


if __name__ == "__main__":
    run_retry_scraper()
