"""
Test script: validate ScraperAPI against Idealista.

ScraperAPI works as a proxy — minimal code change vs BrightData.
Run from your Mac:
    python test_scraperapi_fetch.py

Free tier: 5,000 requests to test.
"""

import time
import random
import json
from datetime import datetime

import requests
import urllib3
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import os

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

# ----- Config -----

SCRAPERAPI_KEY = os.getenv('SCRAPERAPI_KEY', '41335ee35697033a5a4fa383675af4e7')

# BrightData for comparison
BRIGHTDATA_USER = os.getenv('BRIGHTDATA_USER')
BRIGHTDATA_PASS = os.getenv('BRIGHTDATA_PASS')
BRIGHTDATA_HOST = os.getenv('BRIGHTDATA_HOST', 'brd.superproxy.io:33335')

BASE_URL = "https://www.idealista.com"

# Sample barrios
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


def is_challenge_page(html: str) -> bool:
    """Detect CAPTCHA or anti-bot challenge."""
    if len(html) < 2000:
        return True
    signals = ['captcha', 'challenge', 'cf-browser-verification',
               'ray-id', 'just a moment', 'checking your browser',
               'access denied', 'blocked', 'are you a human']
    html_lower = html[:5000].lower()
    return any(s in html_lower for s in signals)


def count_listings(html: str) -> int:
    """Count property listings found on page."""
    soup = BeautifulSoup(html, 'html.parser')
    articles = soup.find_all('article', class_='item')
    return len(articles)


def test_scraperapi_proxy_mode(url: str) -> dict:
    """
    ScraperAPI via proxy mode (drop-in replacement for BrightData).
    Just change the proxy URL — rest of code stays the same.
    """
    proxy_url = f'http://scraperapi:{SCRAPERAPI_KEY}@proxy-server.scraperapi.com:8001'
    proxies = {'http': proxy_url, 'https': proxy_url}
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
    }

    start = time.time()
    try:
        resp = requests.get(url, proxies=proxies, headers=headers,
                           timeout=120, verify=False)
        elapsed = time.time() - start

        if resp.status_code == 200 and not is_challenge_page(resp.text):
            listings = count_listings(resp.text)
            return {'ok': True, 'status': 200, 'time': elapsed,
                    'listings': listings, 'size': len(resp.text)}
        elif resp.status_code == 200:
            return {'ok': False, 'status': 200, 'time': elapsed,
                    'challenge': True, 'size': len(resp.text)}
        else:
            return {'ok': False, 'status': resp.status_code, 'time': elapsed}
    except Exception as e:
        return {'ok': False, 'status': 0, 'time': time.time() - start, 'error': str(e)}


def test_scraperapi_api_mode(url: str) -> dict:
    """
    ScraperAPI via API mode (sends URL as parameter).
    Supports render=true for JavaScript rendering.
    """
    api_url = "http://api.scraperapi.com"
    params = {
        'api_key': SCRAPERAPI_KEY,
        'url': url,
        'render': 'true',          # JS rendering (uses 5 credits instead of 1)
        'country_code': 'es',       # Spanish IP (important for Idealista geo-check)
    }

    start = time.time()
    try:
        resp = requests.get(api_url, params=params, timeout=120)
        elapsed = time.time() - start

        if resp.status_code == 200 and not is_challenge_page(resp.text):
            listings = count_listings(resp.text)
            return {'ok': True, 'status': 200, 'time': elapsed,
                    'listings': listings, 'size': len(resp.text)}
        elif resp.status_code == 200:
            return {'ok': False, 'status': 200, 'time': elapsed,
                    'challenge': True, 'size': len(resp.text)}
        else:
            return {'ok': False, 'status': resp.status_code, 'time': elapsed,
                    'body': resp.text[:200]}
    except Exception as e:
        return {'ok': False, 'status': 0, 'time': time.time() - start, 'error': str(e)}


def test_brightdata(url: str) -> dict:
    """BrightData proxy for comparison."""
    if not all([BRIGHTDATA_USER, BRIGHTDATA_PASS]):
        return {'ok': False, 'status': 0, 'error': 'No BrightData credentials'}

    proxy_url = f'http://{BRIGHTDATA_USER}:{BRIGHTDATA_PASS}@{BRIGHTDATA_HOST}'
    proxies = {'http': proxy_url, 'https': proxy_url}
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
    }

    start = time.time()
    try:
        resp = requests.get(url, proxies=proxies, headers=headers,
                           timeout=60, verify=False)
        elapsed = time.time() - start
        if resp.status_code == 200 and not is_challenge_page(resp.text):
            listings = count_listings(resp.text)
            return {'ok': True, 'status': 200, 'time': elapsed,
                    'listings': listings, 'size': len(resp.text)}
        elif resp.status_code == 200:
            return {'ok': False, 'status': 200, 'time': elapsed, 'challenge': True}
        else:
            return {'ok': False, 'status': resp.status_code, 'time': elapsed}
    except Exception as e:
        return {'ok': False, 'status': 0, 'time': time.time() - start, 'error': str(e)}


def main():
    print("=" * 70)
    print("  IDEALISTA FETCH TEST: ScraperAPI vs BrightData")
    print("=" * 70)
    print(f"  Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  Barrios: {len(TEST_BARRIOS)}")
    print(f"  ScraperAPI key: {SCRAPERAPI_KEY[:8]}...")
    print(f"  BrightData: {'Sí' if BRIGHTDATA_USER else 'No'}")
    print("=" * 70)

    results = []

    for distrito, barrio, path in TEST_BARRIOS:
        url = BASE_URL + path
        print(f"\n▸ {distrito} / {barrio}")

        # --- ScraperAPI: Proxy mode ---
        print("  [SAPI proxy]  ", end="", flush=True)
        sapi_proxy = test_scraperapi_proxy_mode(url)
        if sapi_proxy['ok']:
            print(f"✓ {sapi_proxy['listings']} listings, "
                  f"{sapi_proxy['size']//1024}KB, {sapi_proxy['time']:.1f}s")
        else:
            reason = sapi_proxy.get('error', f"HTTP {sapi_proxy['status']}")
            if sapi_proxy.get('challenge'):
                reason = "CAPTCHA/challenge"
            print(f"✗ {reason} ({sapi_proxy['time']:.1f}s)")

        time.sleep(2)

        # --- ScraperAPI: API mode with render ---
        print("  [SAPI render] ", end="", flush=True)
        sapi_api = test_scraperapi_api_mode(url)
        if sapi_api['ok']:
            print(f"✓ {sapi_api['listings']} listings, "
                  f"{sapi_api['size']//1024}KB, {sapi_api['time']:.1f}s")
        else:
            reason = sapi_api.get('error', f"HTTP {sapi_api['status']}")
            if sapi_api.get('challenge'):
                reason = "CAPTCHA/challenge"
            print(f"✗ {reason} ({sapi_api['time']:.1f}s)")

        time.sleep(2)

        # --- BrightData ---
        if BRIGHTDATA_USER:
            print("  [BrightData]  ", end="", flush=True)
            bd = test_brightdata(url)
            if bd['ok']:
                print(f"✓ {bd['listings']} listings, "
                      f"{bd['size']//1024}KB, {bd['time']:.1f}s")
            else:
                reason = bd.get('error', f"HTTP {bd['status']}")
                if bd.get('challenge'):
                    reason = "CAPTCHA/challenge"
                print(f"✗ {reason} ({bd['time']:.1f}s)")
        else:
            bd = {'ok': False, 'status': 0, 'error': 'Not configured'}

        results.append({
            'barrio': f"{distrito}/{barrio}",
            'scraperapi_proxy': sapi_proxy,
            'scraperapi_render': sapi_api,
            'brightdata': bd,
        })

        time.sleep(2)

    # ----- Summary -----
    print("\n" + "=" * 70)
    print("  RESULTS SUMMARY")
    print("=" * 70)

    total = len(results)
    sp_ok = sum(1 for r in results if r['scraperapi_proxy']['ok'])
    sr_ok = sum(1 for r in results if r['scraperapi_render']['ok'])
    bd_ok = sum(1 for r in results if r['brightdata']['ok'])

    print(f"\n  ScraperAPI (proxy):   {sp_ok}/{total} ({sp_ok/total*100:.0f}%)")
    print(f"  ScraperAPI (render):  {sr_ok}/{total} ({sr_ok/total*100:.0f}%)")
    print(f"  BrightData:           {bd_ok}/{total} ({bd_ok/total*100:.0f}%)")

    # Best ScraperAPI method
    best_ok = max(sp_ok, sr_ok)
    best_name = "proxy" if sp_ok >= sr_ok else "render"
    best_rate = best_ok / total

    # Cost comparison
    daily_req = 330
    # ScraperAPI: $49/month for 100K req; render mode uses 5 credits each
    sapi_credits_per_req = 5 if best_name == "render" else 1
    sapi_monthly_credits = daily_req * 30 * sapi_credits_per_req
    # $49 for 100K credits, $0.00049 per credit
    sapi_cost = max(49.0, sapi_monthly_credits * 0.00049) if sapi_monthly_credits > 100000 else 49.0

    # BrightData
    bd_cost = daily_req * 30 * 0.004

    print(f"\n  --- Monthly Cost Comparison ---")
    print(f"  BrightData (current):        ${bd_cost:.0f}/mes")
    print(f"  ScraperAPI ({best_name}, ${49}/plan): ${sapi_cost:.0f}/mes")

    if best_rate >= 0.8:
        if sapi_cost < bd_cost:
            print(f"\n  ✓ ScraperAPI ({best_name}) works AND is cheaper!")
            print(f"    Savings: ${bd_cost - sapi_cost:.0f}/mes")
        else:
            print(f"\n  ~ ScraperAPI works but is NOT cheaper than BrightData.")
    else:
        print(f"\n  ✗ ScraperAPI success rate too low ({best_rate*100:.0f}%).")

    # Listing comparison
    print(f"\n  --- Detail ---")
    for r in results:
        sp = r['scraperapi_proxy']
        sr = r['scraperapi_render']
        bd = r['brightdata']
        print(f"  {r['barrio']:<30}  "
              f"Proxy:{'✓' if sp['ok'] else '✗'} {str(sp.get('listings','-')):>3}  "
              f"Render:{'✓' if sr['ok'] else '✗'} {str(sr.get('listings','-')):>3}  "
              f"BD:{'✓' if bd['ok'] else '✗'} {str(bd.get('listings','-')):>3}")

    # Save report
    report = {
        'date': datetime.now().isoformat(),
        'scraperapi_proxy_rate': sp_ok / total,
        'scraperapi_render_rate': sr_ok / total,
        'brightdata_rate': bd_ok / total,
        'results': [{
            'barrio': r['barrio'],
            'sp_ok': r['scraperapi_proxy']['ok'],
            'sr_ok': r['scraperapi_render']['ok'],
            'bd_ok': r['brightdata']['ok'],
            'sp_listings': r['scraperapi_proxy'].get('listings'),
            'sr_listings': r['scraperapi_render'].get('listings'),
            'bd_listings': r['brightdata'].get('listings'),
        } for r in results],
    }
    with open('test_scraperapi_report.json', 'w') as f:
        json.dump(report, f, indent=2)
    print(f"\n  Report saved: test_scraperapi_report.json")


if __name__ == '__main__':
    main()
