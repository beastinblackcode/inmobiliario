"""
Unit tests for scraper lite-mode helpers — pure logic, no network.

Covers:
    resolve_scrape_mode
    _scrape_barrio_lite (with fetch_page monkey-patched)
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import patch

import pytest

import scraper

pytestmark = pytest.mark.unit


# ──────────────────────────────────────────────────────────────────────────
# resolve_scrape_mode
# ──────────────────────────────────────────────────────────────────────────

def test_resolve_scrape_mode_explicit_lite_returns_lite():
    assert scraper.resolve_scrape_mode('lite') == 'lite'


def test_resolve_scrape_mode_explicit_full_returns_full():
    assert scraper.resolve_scrape_mode('full') == 'full'


def test_resolve_scrape_mode_auto_stale_coverage_returns_full():
    # Last full sweep older than the interval → a fresh sweep is due.
    from datetime import date, timedelta
    stale = date.today() - timedelta(days=scraper.FULL_SWEEP_INTERVAL_DAYS + 1)
    with patch.object(scraper, 'get_last_full_sweep_date', return_value=stale):
        assert scraper.resolve_scrape_mode('auto') == 'full'


def test_resolve_scrape_mode_auto_fresh_coverage_returns_lite():
    from datetime import date, timedelta
    fresh = date.today() - timedelta(days=1)
    with patch.object(scraper, 'get_last_full_sweep_date', return_value=fresh):
        assert scraper.resolve_scrape_mode('auto') == 'lite'


def test_resolve_scrape_mode_auto_boundary_returns_full():
    # Exactly at the interval counts as due (>= comparison).
    from datetime import date, timedelta
    edge = date.today() - timedelta(days=scraper.FULL_SWEEP_INTERVAL_DAYS)
    with patch.object(scraper, 'get_last_full_sweep_date', return_value=edge):
        assert scraper.resolve_scrape_mode('auto') == 'full'


def test_resolve_scrape_mode_auto_no_coverage_returns_full():
    # Empty barrio_coverage (fresh DB) → sweep to establish coverage.
    with patch.object(scraper, 'get_last_full_sweep_date', return_value=None):
        assert scraper.resolve_scrape_mode('auto') == 'full'


# ──────────────────────────────────────────────────────────────────────────
# LITE_SORT_PARAM is the param we send to Idealista; lock it down so a typo
# in a refactor breaks the test instead of silently re-introducing default
# (relevance-based) sorting.
# ──────────────────────────────────────────────────────────────────────────

def test_lite_sort_param_uses_publication_date_desc():
    assert scraper.LITE_SORT_PARAM == "?ordenado-por=fecha-publicacion-desc"


# ──────────────────────────────────────────────────────────────────────────
# _scrape_barrio_lite — single page, sorted by newest
# ──────────────────────────────────────────────────────────────────────────

_LITE_FIXTURE_HTML = """
<html><body>
  <article class="item" data-element-id="999000001">
    <a class="item-link" href="/inmueble/999000001/">New listing top</a>
    <span class="item-price">450.000</span>
    <span class="item-detail">3 hab.</span>
    <span class="item-detail">90 m²</span>
  </article>
  <article class="item" data-element-id="999000002">
    <a class="item-link" href="/inmueble/999000002/">Already known</a>
    <span class="item-price">380.000</span>
    <span class="item-detail">2 hab.</span>
    <span class="item-detail">70 m²</span>
  </article>
  <article class="item" data-element-id="999000003">
    <a class="item-link" href="/inmueble/999000003/">Older known</a>
    <span class="item-price">300.000</span>
    <span class="item-detail">1 hab.</span>
    <span class="item-detail">55 m²</span>
  </article>
</body></html>
"""


def test_scrape_barrio_lite_hits_only_page_one_with_sort_param():
    """Verifies the lite path requests page 1 with the sort param appended."""
    captured = {}

    def fake_fetch(url, proxies):
        captured['url'] = url
        return _LITE_FIXTURE_HTML, 200

    with patch.object(scraper, 'fetch_page', side_effect=fake_fetch), \
         patch.object(scraper, 'insert_listing', return_value=True), \
         patch.object(scraper, 'update_listing', return_value=True):
        scraper._scrape_barrio_lite(
            "Centro", "Embajadores",
            "/venta-viviendas/madrid/centro/embajadores/",
            proxies=None,
            seen_ids=set(),
        )

    assert captured['url'].endswith(
        "/venta-viviendas/madrid/centro/embajadores/?ordenado-por=fecha-publicacion-desc"
    )


def test_scrape_barrio_lite_inserts_new_and_updates_known():
    """Listings already in seen_ids → update_listing; rest → insert_listing."""
    inserts: list[str] = []
    updates: list[str] = []

    def fake_insert(data):
        inserts.append(data['listing_id'])
        return True

    def fake_update(listing_id, data):
        updates.append(listing_id)
        return True

    # 002 and 003 are already known; 001 is brand-new.
    seen = {"999000002", "999000003"}

    with patch.object(scraper, 'fetch_page', return_value=(_LITE_FIXTURE_HTML, 200)), \
         patch.object(scraper, 'insert_listing', side_effect=fake_insert), \
         patch.object(scraper, 'update_listing', side_effect=fake_update):
        total, new, updated, idealista_total = scraper._scrape_barrio_lite(
            "Centro", "Embajadores",
            "/venta-viviendas/madrid/centro/embajadores/",
            proxies=None,
            seen_ids=seen,
        )

    assert inserts == ["999000001"]
    assert sorted(updates) == ["999000002", "999000003"]
    assert (total, new, updated, idealista_total) == (3, 1, 2, 0)


def test_scrape_barrio_lite_handles_404_gracefully():
    """A 404 doesn't crash and pushes the barrio onto retry_errors."""
    scraper.retry_errors.clear()
    with patch.object(scraper, 'fetch_page', return_value=(None, 404)):
        total, new, updated, _ = scraper._scrape_barrio_lite(
            "Latina", "Lucero",
            "/venta-viviendas/madrid/latina/lucero/",
            proxies=None,
            seen_ids=set(),
        )

    assert (total, new, updated) == (0, 0, 0)
    assert any(b == "Lucero" for (_, b, _, _) in scraper.retry_errors)
