"""
Benchmark de servicios de scraping contra Idealista.

Objetivo: decidir qué proveedor sustituye (o no) a BrightData Web Unlocker
para el scraping diario. Cada candidato se ejecuta N veces sobre una sola
URL (Moratalaz por defecto) y se reportan success rate, latencia, $/req
efectivo, bytes recibidos y nº de listings parseables.

Servicios cubiertos (orden de la tabla final):
    1. curl_cffi             (gratis, control)
    2. BrightData Unlocker   (incumbent)
    3. ZenRows Premium
    4. Oxylabs Web Scraper API
    5. ScrapingBee Stealth
    6. Apify Idealista actor

Cada candidato se salta limpio si su credencial no está en .env. Así puedes
ejecutar el bench varias veces incrementalmente según vayas registrándote
en cada servicio.

Run:
    python bench_scrapers.py
    python bench_scrapers.py --url <otra_url> --n 5

Output:
    stdout — tabla resumen
    bench_scrapers_report.json — resultados por candidato/intento
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Optional

import requests
import urllib3
from bs4 import BeautifulSoup
from dotenv import load_dotenv

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

# Reuse the production parser so we measure what actually lands in the DB,
# not a half-baked re-implementation. parse_listing has no global side
# effects, safe to import.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from scraper import parse_listing  # noqa: E402

DEFAULT_URL = "https://www.idealista.com/venta-viviendas/madrid/moratalaz/"


# ──────────────────────────────────────────────────────────────────────────
# Credit / cost table (USD per *successful* request; failed attempts are
# usually not charged on these providers, but the script reports the
# theoretical cost as if charged).
# ──────────────────────────────────────────────────────────────────────────
COST_PER_REQ = {
    'curl_cffi':         0.0,
    'brightdata':        0.003,    # $15/GB · ~200 KB/page
    'zenrows':           0.003,    # premium plan ~$3/1k
    'oxylabs':           0.00135,  # web scraper API entry tier
    'scrapingbee':       0.005,    # stealth proxies
    'apify':             0.0008,   # actor avg, depends on result count
}


# ──────────────────────────────────────────────────────────────────────────
# Quality helpers
# ──────────────────────────────────────────────────────────────────────────

def _is_challenge(html: str) -> bool:
    if not html or len(html) < 2000:
        return True
    snippet = html[:5000].lower()
    return any(s in snippet for s in (
        'captcha', 'challenge', 'cf-browser-verification',
        'ray-id', 'just a moment', 'access denied', 'blocked',
    ))


def _quality(html: str) -> tuple[int, int]:
    """Return (n_listings, n_with_orientation) for the page HTML."""
    soup = BeautifulSoup(html, 'html.parser')
    articles = soup.find_all('article', class_='item')
    n_orient = 0
    for art in articles:
        parsed = parse_listing(art, "Moratalaz", "Moratalaz")
        if parsed and parsed.get('orientation'):
            n_orient += 1
    return len(articles), n_orient


# ──────────────────────────────────────────────────────────────────────────
# Per-attempt result
# ──────────────────────────────────────────────────────────────────────────

@dataclass
class Attempt:
    ok: bool
    status: int
    latency_s: float
    bytes_received: int = 0
    n_listings: int = 0
    n_orientation: int = 0
    reason: str = ""


# ──────────────────────────────────────────────────────────────────────────
# Candidate runners
# Each returns an Attempt. If credentials are missing, raises RuntimeError
# (caller skips the candidate cleanly).
# ──────────────────────────────────────────────────────────────────────────

def run_curl_cffi(url: str) -> Attempt:
    try:
        from curl_cffi import requests as cffi_requests
    except ImportError:
        raise RuntimeError("curl_cffi not installed")
    headers = {
        'User-Agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                       'AppleWebKit/537.36 (KHTML, like Gecko) '
                       'Chrome/131.0.0.0 Safari/537.36'),
        'Accept-Language': 'es-ES,es;q=0.9',
    }
    start = time.time()
    try:
        r = cffi_requests.get(url, headers=headers, impersonate="chrome131", timeout=60)
        elapsed = time.time() - start
        if r.status_code != 200:
            return Attempt(False, r.status_code, elapsed, reason=f"HTTP {r.status_code}")
        if _is_challenge(r.text):
            return Attempt(False, 200, elapsed, len(r.text), reason="challenge")
        n, no = _quality(r.text)
        return Attempt(True, 200, elapsed, len(r.text), n, no)
    except Exception as e:
        return Attempt(False, 0, time.time() - start, reason=type(e).__name__)


def run_brightdata(url: str) -> Attempt:
    user = os.getenv('BRIGHTDATA_USER')
    pw   = os.getenv('BRIGHTDATA_PASS')
    host = os.getenv('BRIGHTDATA_HOST', 'brd.superproxy.io:33335')
    if not (user and pw):
        raise RuntimeError("BRIGHTDATA_USER / BRIGHTDATA_PASS missing")
    from urllib.parse import quote
    proxies = {
        'http':  f'http://{quote(user, safe="")}:{quote(pw, safe="")}@{host}',
        'https': f'http://{quote(user, safe="")}:{quote(pw, safe="")}@{host}',
    }
    headers = {
        'User-Agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                       'AppleWebKit/537.36 (KHTML, like Gecko) '
                       'Chrome/131.0.0.0 Safari/537.36'),
        'Accept-Language': 'es-ES,es;q=0.9',
    }
    start = time.time()
    try:
        r = requests.get(url, proxies=proxies, headers=headers, timeout=90, verify=False)
        elapsed = time.time() - start
        if r.status_code != 200:
            return Attempt(False, r.status_code, elapsed, reason=f"HTTP {r.status_code}")
        if _is_challenge(r.text):
            return Attempt(False, 200, elapsed, len(r.text), reason="challenge")
        n, no = _quality(r.text)
        return Attempt(True, 200, elapsed, len(r.text), n, no)
    except Exception as e:
        return Attempt(False, 0, time.time() - start, reason=type(e).__name__)


def run_zenrows(url: str) -> Attempt:
    key = os.getenv('ZENROWS_API_KEY')
    if not key:
        raise RuntimeError("ZENROWS_API_KEY missing")
    # Trial plan often rejects premium_proxy with HTTP 422; switch to
    # js_render which is cheaper and tends to be enabled on free tier.
    # Idealista in fact serves HTML server-side, but js_render adds
    # anti-bot evasion as a side-effect on ZenRows.
    params = {
        'apikey': key,
        'url': url,
        'js_render': 'true',
        'proxy_country': 'es',
    }
    start = time.time()
    try:
        r = requests.get("https://api.zenrows.com/v1/", params=params, timeout=120)
        elapsed = time.time() - start
        if r.status_code != 200:
            return Attempt(False, r.status_code, elapsed, reason=f"HTTP {r.status_code}")
        if _is_challenge(r.text):
            return Attempt(False, 200, elapsed, len(r.text), reason="challenge")
        n, no = _quality(r.text)
        return Attempt(True, 200, elapsed, len(r.text), n, no)
    except Exception as e:
        return Attempt(False, 0, time.time() - start, reason=type(e).__name__)


def run_oxylabs(url: str) -> Attempt:
    user = os.getenv('OXYLABS_USER')
    pw   = os.getenv('OXYLABS_PASS')
    if not (user and pw):
        raise RuntimeError("OXYLABS_USER / OXYLABS_PASS missing")
    # Drop `render: html` — Idealista's listing-page HTML is fully
    # server-rendered, and on the first bench 2/3 attempts came back
    # with `content: ""` despite status 200, consistent with Oxylabs'
    # internal headless browser running into the anti-bot screen mid
    # render. Asking only for raw HTML lets their cheaper, more stable
    # path do the fetch.
    payload = {
        'source': 'universal',
        'url': url,
        'geo_location': 'Spain',
    }
    start = time.time()
    try:
        r = requests.post(
            "https://realtime.oxylabs.io/v1/queries",
            auth=(user, pw), json=payload, timeout=120,
        )
        elapsed = time.time() - start
        if r.status_code != 200:
            return Attempt(False, r.status_code, elapsed, reason=f"HTTP {r.status_code}")
        data = r.json()
        html = data.get('results', [{}])[0].get('content', '') if isinstance(data, dict) else ''
        if not html:
            return Attempt(False, 200, elapsed, reason="empty payload")
        if _is_challenge(html):
            return Attempt(False, 200, elapsed, len(html), reason="challenge")
        n, no = _quality(html)
        return Attempt(True, 200, elapsed, len(html), n, no)
    except Exception as e:
        return Attempt(False, 0, time.time() - start, reason=type(e).__name__)


def run_scrapingbee(url: str) -> Attempt:
    key = os.getenv('SCRAPINGBEE_API_KEY')
    if not key:
        raise RuntimeError("SCRAPINGBEE_API_KEY missing")
    # Idealista on ScrapingBee returns HTTP 500 without render_js — their
    # stealth_proxy alone is not enough for the anti-bot challenge.
    # Adding render_js bumps credits cost dramatically but is the only
    # combination that has a chance of succeeding.
    params = {
        'api_key': key,
        'url': url,
        'stealth_proxy': 'true',
        'country_code': 'es',
        'render_js': 'true',
    }
    start = time.time()
    try:
        r = requests.get("https://app.scrapingbee.com/api/v1/", params=params, timeout=120)
        elapsed = time.time() - start
        if r.status_code != 200:
            return Attempt(False, r.status_code, elapsed, reason=f"HTTP {r.status_code}: {r.text[:80]}")
        if _is_challenge(r.text):
            return Attempt(False, 200, elapsed, len(r.text), reason="challenge")
        n, no = _quality(r.text)
        return Attempt(True, 200, elapsed, len(r.text), n, no)
    except Exception as e:
        return Attempt(False, 0, time.time() - start, reason=type(e).__name__)


def run_apify(url: str) -> Attempt:
    """
    Apify "Idealista scraper" actor. Different from the others: doesn't
    return raw HTML, returns parsed listings as items in a dataset. So we
    can't measure bytes the same way; we report 0 bytes and the listings
    parsed by the actor itself.
    """
    token = os.getenv('APIFY_TOKEN')
    if not token:
        raise RuntimeError("APIFY_TOKEN missing")
    # The Idealista actor is `igolaizola/idealista-scraper`. Trigger a
    # synchronous run-sync-get-dataset-items call so the bench actually
    # waits for results.
    actor_id = "igolaizola~idealista-scraper"
    api_url = f"https://api.apify.com/v2/acts/{actor_id}/run-sync-get-dataset-items"
    payload = {
        'startUrls': [{'url': url}],
        'maxItems': 30,           # one page of results
        'proxyConfiguration': {'useApifyProxy': True},
    }
    start = time.time()
    try:
        r = requests.post(api_url, params={'token': token}, json=payload, timeout=300)
        elapsed = time.time() - start
        if r.status_code != 200:
            return Attempt(False, r.status_code, elapsed, reason=f"HTTP {r.status_code}: {r.text[:80]}")
        items = r.json()
        if not isinstance(items, list):
            return Attempt(False, 200, elapsed, reason="unexpected payload")
        # Actor returns parsed JSON, not HTML. We treat n_orientation as
        # the number of items where the actor itself filled an orientation
        # field — if the actor doesn't expose it, this stays 0 (legitimate
        # quality signal: the actor doesn't capture it).
        n = len(items)
        n_or = sum(1 for it in items if isinstance(it, dict) and it.get('orientation'))
        # Pseudo-bytes: serialize JSON for size comparison.
        approx_bytes = len(json.dumps(items))
        return Attempt(True, 200, elapsed, approx_bytes, n, n_or)
    except Exception as e:
        return Attempt(False, 0, time.time() - start, reason=type(e).__name__)


# ──────────────────────────────────────────────────────────────────────────
# Bench driver
# ──────────────────────────────────────────────────────────────────────────

CANDIDATES: list[tuple[str, Callable[[str], Attempt]]] = [
    ('curl_cffi',     run_curl_cffi),
    ('brightdata',    run_brightdata),
    ('zenrows',       run_zenrows),
    ('oxylabs',       run_oxylabs),
    ('scrapingbee',   run_scrapingbee),
    ('apify',         run_apify),
]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--url', default=DEFAULT_URL)
    ap.add_argument('--n', type=int, default=3, help='attempts per candidate')
    ap.add_argument('--only', help='comma-separated subset (e.g. zenrows,oxylabs)')
    args = ap.parse_args()

    only = set(args.only.split(',')) if args.only else None

    print('=' * 78)
    print(f"  SCRAPER BENCH — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  URL: {args.url}")
    print(f"  Attempts per candidate: {args.n}")
    print('=' * 78)

    report: dict = {
        'url': args.url,
        'n_per_candidate': args.n,
        'date': datetime.now().isoformat(),
        'candidates': [],
    }

    for name, fn in CANDIDATES:
        if only and name not in only:
            continue
        print(f"\n▸ {name}")
        attempts: list[Attempt] = []
        skipped_reason = None
        for i in range(args.n):
            try:
                a = fn(args.url)
            except RuntimeError as e:
                skipped_reason = str(e)
                print(f"  ⏭  skipped: {e}")
                break
            attempts.append(a)
            mark = "✓" if a.ok else "✗"
            extra = (f"  {a.n_listings:>2} listings · {a.n_orientation:>2} orient · "
                     f"{a.bytes_received//1024:>4} KB" if a.ok else f"  ({a.reason})")
            print(f"  [{i+1}/{args.n}] {mark} status={a.status:>3}  {a.latency_s:5.1f}s{extra}")
            time.sleep(2)

        if skipped_reason:
            report['candidates'].append({'name': name, 'skipped': skipped_reason})
            continue

        ok_attempts = [a for a in attempts if a.ok]
        rate = len(ok_attempts) / len(attempts) if attempts else 0
        latencies = [a.latency_s for a in attempts]
        listings = [a.n_listings for a in ok_attempts] or [0]
        orient = [a.n_orientation for a in ok_attempts] or [0]
        bytes_avg = (sum(a.bytes_received for a in ok_attempts) // len(ok_attempts)) if ok_attempts else 0
        report['candidates'].append({
            'name': name,
            'success_rate': round(rate, 3),
            'p50_latency_s': round(statistics.median(latencies), 2),
            'avg_listings': round(statistics.mean(listings), 1),
            'avg_orientation_filled': round(statistics.mean(orient), 1),
            'avg_bytes': bytes_avg,
            'cost_per_req_usd': COST_PER_REQ[name],
            'attempts': [
                {
                    'ok': a.ok, 'status': a.status, 'latency_s': round(a.latency_s, 2),
                    'bytes': a.bytes_received, 'listings': a.n_listings,
                    'orientation': a.n_orientation, 'reason': a.reason,
                } for a in attempts
            ],
        })

    # ── Summary table ──────────────────────────────────────────────────────
    print('\n' + '=' * 78)
    print('  SUMMARY')
    print('=' * 78)
    print(f"  {'name':<14} {'ok':>6} {'p50_lat':>8} {'list':>5} {'orient':>7} "
          f"{'KB':>5} {'$/req':>9} {'$/mo*':>7}")
    print(f"  {'-'*14} {'-'*6} {'-'*8} {'-'*5} {'-'*7} {'-'*5} {'-'*9} {'-'*7}")
    REQ_PER_MONTH = 4_800
    for c in report['candidates']:
        if c.get('skipped'):
            print(f"  {c['name']:<14} skipped: {c['skipped']}")
            continue
        cost_mo = c['cost_per_req_usd'] * REQ_PER_MONTH
        print(f"  {c['name']:<14} {c['success_rate']*100:>5.0f}% "
              f"{c['p50_latency_s']:>7.1f}s "
              f"{c['avg_listings']:>5.1f} {c['avg_orientation_filled']:>7.1f} "
              f"{c['avg_bytes']//1024:>5} ${c['cost_per_req_usd']:>7.5f} "
              f"${cost_mo:>5.0f}")
    print(f"\n  *$/mo assuming {REQ_PER_MONTH:,} requests/month (lite-mode load)")

    with open('bench_scrapers_report.json', 'w') as f:
        json.dump(report, f, indent=2)
    print(f"\n  Report saved: bench_scrapers_report.json")


if __name__ == '__main__':
    main()
