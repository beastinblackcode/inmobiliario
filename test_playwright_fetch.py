"""
Test script: validate Playwright + stealth fetching against Idealista.

Uses a real headless Chromium browser with anti-detection patches.
Run from your Mac:
    pip install playwright playwright-stealth
    playwright install chromium
    python test_playwright_fetch.py

Results show success rate, timing, and estimated monthly savings.
"""

import asyncio
import time
import random
import json
from datetime import datetime

try:
    from playwright.async_api import async_playwright
    print("✓ playwright loaded")
except ImportError:
    print("✗ playwright not installed. Run:")
    print("  pip install playwright playwright-stealth")
    print("  playwright install chromium")
    exit(1)

try:
    from playwright_stealth import stealth_async
    HAS_STEALTH = True
    print("✓ playwright-stealth loaded")
except ImportError:
    HAS_STEALTH = False
    print("⚠ playwright-stealth not installed (will try without it)")
    print("  pip install playwright-stealth")

from bs4 import BeautifulSoup
import requests
import urllib3
from dotenv import load_dotenv
import os

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

# ----- Config -----

BRIGHTDATA_USER = os.getenv('BRIGHTDATA_USER')
BRIGHTDATA_PASS = os.getenv('BRIGHTDATA_PASS')
BRIGHTDATA_HOST = os.getenv('BRIGHTDATA_HOST', 'brd.superproxy.io:33335')

BASE_URL = "https://www.idealista.com"

# Sample barrios: mix of sizes
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


def test_proxy(url: str) -> dict:
    """Test fetch via BrightData proxy (for comparison)."""
    if not all([BRIGHTDATA_USER, BRIGHTDATA_PASS]):
        return {'ok': False, 'status': 0, 'error': 'No BrightData credentials'}

    proxy_url = f'http://{BRIGHTDATA_USER}:{BRIGHTDATA_PASS}@{BRIGHTDATA_HOST}'
    proxies = {'http': proxy_url, 'https': proxy_url}
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
    }

    start = time.time()
    try:
        resp = requests.get(url, proxies=proxies, headers=headers,
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


async def test_playwright(barrios: list) -> list:
    """
    Test all barrios using a single Playwright browser session.

    Reuses one browser context for all requests (like a real user browsing).
    """
    results = []

    async with async_playwright() as p:
        # Launch with realistic settings
        browser = await p.chromium.launch(
            headless=True,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-features=IsolateOrigins,site-per-process',
                '--no-sandbox',
            ]
        )

        # Create context with realistic viewport, locale, timezone
        context = await browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            locale='es-ES',
            timezone_id='Europe/Madrid',
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            java_script_enabled=True,
            ignore_https_errors=True,
            extra_http_headers={
                'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
                'DNT': '1',
            },
        )

        # Apply stealth patches if available
        page = await context.new_page()
        if HAS_STEALTH:
            await stealth_async(page)

        # Remove webdriver flag manually as extra precaution
        await page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            // Override permissions query
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                    Promise.resolve({ state: Notification.permission }) :
                    originalQuery(parameters)
            );
            // Chrome runtime mock
            window.chrome = { runtime: {} };
            // Languages
            Object.defineProperty(navigator, 'languages', {get: () => ['es-ES', 'es', 'en']});
            // Platform
            Object.defineProperty(navigator, 'platform', {get: () => 'MacIntel'});
            // Hardware concurrency
            Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 8});
        """)

        # First, visit the homepage to get cookies (like a real user)
        print("\n  [PLAYWRIGHT] Visiting homepage first (get cookies)...")
        try:
            await page.goto("https://www.idealista.com", wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(2000 + random.randint(0, 2000))

            # Accept cookies if banner appears
            try:
                accept_btn = page.locator('#didomi-notice-agree-button')
                if await accept_btn.is_visible(timeout=3000):
                    await accept_btn.click()
                    print("  [PLAYWRIGHT] Cookies accepted")
                    await page.wait_for_timeout(1000)
            except Exception:
                pass  # No cookie banner, that's fine

        except Exception as e:
            print(f"  [PLAYWRIGHT] Homepage visit failed: {e}")

        # Now scrape each barrio
        for distrito, barrio, path in barrios:
            url = BASE_URL + path
            print(f"\n▸ {distrito} / {barrio}")
            print(f"  URL: {url}")

            # Playwright fetch
            print("  [PLAYWRIGHT] Fetching...", end=" ", flush=True)
            start = time.time()

            try:
                response = await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                # Wait a bit for dynamic content
                await page.wait_for_timeout(1500 + random.randint(0, 1500))

                elapsed = time.time() - start
                status = response.status if response else 0
                html = await page.content()

                if status == 200 and not is_challenge_page(html):
                    listings = count_listings(html)
                    print(f"✓ {status} — {listings} listings, "
                          f"{len(html)//1024}KB, {elapsed:.1f}s")
                    pw_result = {'ok': True, 'status': 200, 'time': elapsed,
                                'listings': listings, 'size': len(html)}
                else:
                    challenge = is_challenge_page(html) if status == 200 else False
                    reason = "CAPTCHA/challenge" if challenge else f"HTTP {status}"
                    print(f"✗ {reason} ({elapsed:.1f}s)")
                    pw_result = {'ok': False, 'status': status, 'time': elapsed,
                                'challenge': challenge}

                    # Save failed HTML for debugging
                    if challenge:
                        debug_file = f"debug_playwright_{barrio.lower().replace(' ', '_')}.html"
                        with open(debug_file, 'w') as f:
                            f.write(html)
                        print(f"  [DEBUG] HTML saved to {debug_file}")

            except Exception as e:
                elapsed = time.time() - start
                print(f"✗ Error: {e} ({elapsed:.1f}s)")
                pw_result = {'ok': False, 'status': 0, 'time': elapsed, 'error': str(e)}

            # Proxy comparison (optional)
            if BRIGHTDATA_USER:
                print("  [PROXY]      Fetching...", end=" ", flush=True)
                proxy_result = test_proxy(url)
                if proxy_result['ok']:
                    print(f"✓ {proxy_result['status']} — {proxy_result['listings']} listings, "
                          f"{proxy_result['size']//1024}KB, {proxy_result['time']:.1f}s")
                else:
                    reason = proxy_result.get('error', f"HTTP {proxy_result['status']}")
                    print(f"✗ {reason} ({proxy_result['time']:.1f}s)")
            else:
                proxy_result = {'ok': False, 'status': 0, 'error': 'Not configured'}

            results.append({
                'barrio': f"{distrito}/{barrio}",
                'playwright': pw_result,
                'proxy': proxy_result,
            })

            # Human-like delay between pages
            delay = 3.0 + random.uniform(0, 3.0)
            print(f"  ⏳ Waiting {delay:.1f}s...")
            await page.wait_for_timeout(int(delay * 1000))

        await browser.close()

    return results


def print_summary(results: list):
    """Print comparison summary and cost projection."""
    print("\n" + "=" * 70)
    print("  RESULTS SUMMARY")
    print("=" * 70)

    pw_ok = sum(1 for r in results if r['playwright']['ok'])
    proxy_ok = sum(1 for r in results if r['proxy']['ok'])
    total = len(results)

    print(f"\n  Playwright (headless):  {pw_ok}/{total} successful "
          f"({pw_ok/total*100:.0f}%)")
    if BRIGHTDATA_USER:
        print(f"  Proxy (BrightData):     {proxy_ok}/{total} successful "
              f"({proxy_ok/total*100:.0f}%)")

    # Listing comparison
    print(f"\n  --- Listing Count Comparison ---")
    for r in results:
        pw = r['playwright']
        px = r['proxy']
        pw_count = pw.get('listings', '-')
        px_count = px.get('listings', '-')
        pw_icon = "✓" if pw['ok'] else "✗"
        px_icon = "✓" if px['ok'] else "✗"
        match = "=" if pw.get('listings') == px.get('listings') and pw['ok'] and px['ok'] else "≠"
        print(f"  {r['barrio']:<35} PW:{pw_icon} {str(pw_count):>3}  |  "
              f"Proxy:{px_icon} {str(px_count):>3}  {match}")

    # Timing comparison
    if pw_ok > 0:
        pw_times = [r['playwright']['time'] for r in results if r['playwright']['ok']]
        proxy_times = [r['proxy']['time'] for r in results if r['proxy']['ok']]
        print(f"\n  --- Average Fetch Time ---")
        print(f"  Playwright:  {sum(pw_times)/len(pw_times):.1f}s")
        if proxy_times:
            print(f"  Proxy:       {sum(proxy_times)/len(proxy_times):.1f}s")

    # Cost projection
    daily_requests = 330
    cost_per_req = 0.004

    pw_rate = pw_ok / total if total > 0 else 0
    proxy_needed = daily_requests * (1 - pw_rate)

    daily_hybrid = proxy_needed * cost_per_req
    daily_proxy_only = daily_requests * cost_per_req
    monthly_hybrid = daily_hybrid * 30
    monthly_proxy_only = daily_proxy_only * 30

    print(f"\n  --- Monthly Cost Projection (30 days) ---")
    print(f"  Proxy-only (current):   ${monthly_proxy_only:.0f}/mes")
    print(f"  Hybrid (estimated):     ${monthly_hybrid:.0f}/mes")
    print(f"  Estimated savings:      ${monthly_proxy_only - monthly_hybrid:.0f}/mes "
          f"({(1 - monthly_hybrid/monthly_proxy_only)*100:.0f}%)")

    if pw_rate >= 0.8:
        print(f"\n  ✓ RECOMMENDATION: Playwright hybrid mode looks viable!")
        print(f"    Direct success rate {pw_rate*100:.0f}% is above 80% threshold.")
        print(f"    Set FETCH_MODE=playwright in .env to activate.")
    elif pw_rate >= 0.5:
        print(f"\n  ~ RECOMMENDATION: Moderate success ({pw_rate*100:.0f}%). "
              f"Worth using in hybrid mode for partial savings.")
    else:
        print(f"\n  ✗ RECOMMENDATION: Playwright not viable ({pw_rate*100:.0f}%).")
        print(f"    Idealista detects headless browsers too.")

    # Save report
    report = {
        'date': datetime.now().isoformat(),
        'playwright_success_rate': pw_rate,
        'stealth_enabled': HAS_STEALTH,
        'results': [{
            'barrio': r['barrio'],
            'playwright_ok': r['playwright']['ok'],
            'proxy_ok': r['proxy']['ok'],
            'playwright_listings': r['playwright'].get('listings'),
            'proxy_listings': r['proxy'].get('listings'),
            'playwright_time': round(r['playwright'].get('time', 0), 2),
        } for r in results],
        'cost_projection': {
            'monthly_proxy_only': round(monthly_proxy_only, 2),
            'monthly_hybrid': round(monthly_hybrid, 2),
            'savings_pct': round((1 - monthly_hybrid/monthly_proxy_only)*100, 1) if monthly_proxy_only > 0 else 0,
        }
    }
    with open('test_playwright_fetch_report.json', 'w') as f:
        json.dump(report, f, indent=2)
    print(f"\n  Report saved: test_playwright_fetch_report.json")


def main():
    print("=" * 70)
    print("  IDEALISTA FETCH TEST: Playwright (stealth) vs BrightData Proxy")
    print("=" * 70)
    print(f"  Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  Barrios a testear: {len(TEST_BARRIOS)}")
    print(f"  Stealth patches: {'Sí' if HAS_STEALTH else 'No'}")
    print(f"  BrightData configurado: {'Sí' if BRIGHTDATA_USER else 'No'}")
    print("=" * 70)

    results = asyncio.run(test_playwright(TEST_BARRIOS))
    print_summary(results)


if __name__ == '__main__':
    main()
