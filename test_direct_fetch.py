"""
Test script: validate direct fetching (curl_cffi) against Idealista.

Compares direct (free) vs BrightData proxy results on a sample of barrios.
Run from your Mac:
    pip install curl_cffi
    python test_direct_fetch.py

Results show success rate, timing, and estimated monthly savings.
"""

import time
import random
import json
from datetime import datetime

try:
    from curl_cffi import requests as cffi_requests
    print("✓ curl_cffi loaded")
except ImportError:
    print("✗ curl_cffi not installed. Run: pip install curl_cffi")
    exit(1)

import requests
import urllib3
from dotenv import load_dotenv
import os
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

# ----- Config -----

BRIGHTDATA_USER = os.getenv('BRIGHTDATA_USER')
BRIGHTDATA_PASS = os.getenv('BRIGHTDATA_PASS')
BRIGHTDATA_HOST = os.getenv('BRIGHTDATA_HOST', 'brd.superproxy.io:33335')

BASE_URL = "https://www.idealista.com"

# Sample barrios: mix of large, medium, small neighborhoods
TEST_BARRIOS = [
    ("Centro", "Embajadores", "/venta-viviendas/madrid/centro/embajadores/"),
    ("Salamanca", "Recoletos", "/venta-viviendas/madrid/salamanca/recoletos/"),
    ("Latina", "Lucero", "/venta-viviendas/madrid/latina/lucero/"),
    ("Carabanchel", "Vista Alegre", "/venta-viviendas/madrid/carabanchel/vista-alegre/"),
    ("Vallecas", "Numancia", "/venta-viviendas/madrid/puente-de-vallecas/numancia/"),
    ("Tetuán", "Bellas Vistas", "/venta-viviendas/madrid/tetuan/bellas-vistas/"),
    ("Chamberí", "Trafalgar", "/venta-viviendas/madrid/chamberi/trafalgar/"),
    ("Hortaleza", "Pinar del Rey", "/venta-viviendas/madrid/hortaleza/pinar-del-rey/"),
]

USER_AGENTS = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15',
]


def get_headers():
    return {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
    }


def is_challenge_page(html: str) -> bool:
    """Detect CAPTCHA or anti-bot challenge."""
    if len(html) < 2000:
        return True
    signals = ['captcha', 'challenge', 'cf-browser-verification',
               'ray-id', 'just a moment', 'checking your browser',
               'access denied', 'blocked']
    html_lower = html[:5000].lower()
    return any(s in html_lower for s in signals)


def count_listings(html: str) -> int:
    """Count property listings found on page."""
    soup = BeautifulSoup(html, 'html.parser')
    articles = soup.find_all('article', class_='item')
    return len(articles)


def test_direct(url: str) -> dict:
    """Test direct fetch via curl_cffi."""
    start = time.time()
    try:
        resp = cffi_requests.get(
            url,
            headers=get_headers(),
            impersonate="chrome131",
            timeout=30,
        )
        elapsed = time.time() - start

        if resp.status_code == 200 and not is_challenge_page(resp.text):
            listings = count_listings(resp.text)
            return {'ok': True, 'status': 200, 'time': elapsed,
                    'listings': listings, 'size': len(resp.text)}
        else:
            return {'ok': False, 'status': resp.status_code, 'time': elapsed,
                    'challenge': is_challenge_page(resp.text) if resp.status_code == 200 else False}
    except Exception as e:
        return {'ok': False, 'status': 0, 'time': time.time() - start, 'error': str(e)}


def test_proxy(url: str) -> dict:
    """Test fetch via BrightData proxy."""
    if not all([BRIGHTDATA_USER, BRIGHTDATA_PASS]):
        return {'ok': False, 'status': 0, 'error': 'No BrightData credentials'}

    proxy_url = f'http://{BRIGHTDATA_USER}:{BRIGHTDATA_PASS}@{BRIGHTDATA_HOST}'
    proxies = {'http': proxy_url, 'https': proxy_url}

    start = time.time()
    try:
        resp = requests.get(url, proxies=proxies, headers=get_headers(),
                           timeout=60, verify=False)
        elapsed = time.time() - start

        if resp.status_code == 200:
            listings = count_listings(resp.text)
            return {'ok': True, 'status': 200, 'time': elapsed,
                    'listings': listings, 'size': len(resp.text)}
        else:
            return {'ok': False, 'status': resp.status_code, 'time': elapsed}
    except Exception as e:
        return {'ok': False, 'status': 0, 'time': time.time() - start, 'error': str(e)}


def main():
    print("=" * 70)
    print("  IDEALISTA FETCH TEST: Direct (curl_cffi) vs BrightData Proxy")
    print("=" * 70)
    print(f"  Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  Barrios a testear: {len(TEST_BARRIOS)}")
    print(f"  BrightData configurado: {'Sí' if BRIGHTDATA_USER else 'No'}")
    print("=" * 70)

    results = []

    for distrito, barrio, path in TEST_BARRIOS:
        url = BASE_URL + path
        print(f"\n▸ {distrito} / {barrio}")
        print(f"  URL: {url}")

        # Test direct
        print("  [DIRECT] Fetching...", end=" ", flush=True)
        direct = test_direct(url)
        if direct['ok']:
            print(f"✓ {direct['status']} — {direct['listings']} listings, "
                  f"{direct['size']//1024}KB, {direct['time']:.1f}s")
        else:
            reason = direct.get('error', f"HTTP {direct['status']}")
            if direct.get('challenge'):
                reason = "CAPTCHA/challenge detected"
            print(f"✗ {reason} ({direct['time']:.1f}s)")

        # Test proxy (only if configured)
        if BRIGHTDATA_USER:
            print("  [PROXY]  Fetching...", end=" ", flush=True)
            proxy = test_proxy(url)
            if proxy['ok']:
                print(f"✓ {proxy['status']} — {proxy['listings']} listings, "
                      f"{proxy['size']//1024}KB, {proxy['time']:.1f}s")
            else:
                reason = proxy.get('error', f"HTTP {proxy['status']}")
                print(f"✗ {reason} ({proxy['time']:.1f}s)")
        else:
            proxy = {'ok': False, 'status': 0, 'error': 'Not configured'}

        results.append({
            'barrio': f"{distrito}/{barrio}",
            'direct': direct,
            'proxy': proxy,
        })

        # Politeness delay between barrios
        delay = 2.0 + random.uniform(0, 1.5)
        print(f"  ⏳ Waiting {delay:.1f}s...")
        time.sleep(delay)

    # ----- Summary -----
    print("\n" + "=" * 70)
    print("  RESULTS SUMMARY")
    print("=" * 70)

    direct_ok = sum(1 for r in results if r['direct']['ok'])
    proxy_ok = sum(1 for r in results if r['proxy']['ok'])
    total = len(results)

    print(f"\n  Direct (curl_cffi):  {direct_ok}/{total} successful "
          f"({direct_ok/total*100:.0f}%)")
    if BRIGHTDATA_USER:
        print(f"  Proxy (BrightData):  {proxy_ok}/{total} successful "
              f"({proxy_ok/total*100:.0f}%)")

    # Listing comparison
    matching = 0
    for r in results:
        if r['direct']['ok'] and r['proxy']['ok']:
            d_count = r['direct'].get('listings', 0)
            p_count = r['proxy'].get('listings', 0)
            if d_count == p_count:
                matching += 1
            else:
                print(f"  ⚠ {r['barrio']}: direct={d_count} vs proxy={p_count} listings")

    # Cost projection
    daily_requests = 330  # approx venta requests/day
    cost_per_req = 0.004  # $4/1000

    direct_rate = direct_ok / total if total > 0 else 0
    proxy_needed = daily_requests * (1 - direct_rate)

    daily_hybrid = proxy_needed * cost_per_req
    daily_proxy_only = daily_requests * cost_per_req
    monthly_hybrid = daily_hybrid * 30
    monthly_proxy_only = daily_proxy_only * 30

    print(f"\n  --- Monthly Cost Projection (30 days) ---")
    print(f"  Proxy-only (current):  ${monthly_proxy_only:.0f}/mes")
    print(f"  Hybrid (estimated):    ${monthly_hybrid:.0f}/mes")
    print(f"  Estimated savings:     ${monthly_proxy_only - monthly_hybrid:.0f}/mes "
          f"({(1 - monthly_hybrid/monthly_proxy_only)*100:.0f}%)")

    if direct_rate >= 0.8:
        print(f"\n  ✓ RECOMMENDATION: Hybrid mode looks viable! "
              f"Direct success rate {direct_rate*100:.0f}% is above 80% threshold.")
        print(f"    Set FETCH_MODE=hybrid in .env to activate.")
    elif direct_rate >= 0.5:
        print(f"\n  ~ RECOMMENDATION: Moderate success rate ({direct_rate*100:.0f}%). "
              f"Hybrid mode will save some costs but needs monitoring.")
    else:
        print(f"\n  ✗ RECOMMENDATION: Direct fetching not viable ({direct_rate*100:.0f}%). "
              f"Idealista is blocking direct requests aggressively.")
        print(f"    Keep using BrightData proxy (FETCH_MODE=proxy).")

    # Save results to JSON for reference
    report = {
        'date': datetime.now().isoformat(),
        'direct_success_rate': direct_rate,
        'results': [{
            'barrio': r['barrio'],
            'direct_ok': r['direct']['ok'],
            'proxy_ok': r['proxy']['ok'],
            'direct_listings': r['direct'].get('listings'),
            'proxy_listings': r['proxy'].get('listings'),
        } for r in results],
        'cost_projection': {
            'monthly_proxy_only': round(monthly_proxy_only, 2),
            'monthly_hybrid': round(monthly_hybrid, 2),
            'savings_pct': round((1 - monthly_hybrid/monthly_proxy_only)*100, 1) if monthly_proxy_only > 0 else 0,
        }
    }
    with open('test_direct_fetch_report.json', 'w') as f:
        json.dump(report, f, indent=2)
    print(f"\n  Report saved: test_direct_fetch_report.json")


if __name__ == '__main__':
    main()
