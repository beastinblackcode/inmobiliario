"""
Unit tests for run_rental_scraping counters.

Regression guard for the silent-failure mode observed on 2026-05-23:
the pool of Postgres died mid-run, every upsert_rental_snapshot returned
False, and the summary showed `0 stored / 0 skipped / 4 errors` for a
91-barrio iteration. The new `upsert_failed` counter must now surface
that case in the summary line.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

import scraper

pytestmark = pytest.mark.unit


_RENTAL_FIXTURE_HTML = """
<html><body>
  <article class="item"><span class="item-price">1.200 €/mes</span></article>
  <article class="item"><span class="item-price">1.350 €/mes</span></article>
  <article class="item"><span class="item-price">1.500 €/mes</span></article>
  <article class="item"><span class="item-price">1.700 €/mes</span></article>
</body></html>
"""


def _fake_barrios(n: int = 3) -> list:
    """Return a tiny BARRIOS_TO_SCRAPE-shaped list."""
    return [
        ("Centro", f"Barrio{i}", f"/venta-viviendas/madrid/centro/barrio-{i}/")
        for i in range(n)
    ]


def test_upsert_failed_counter_increments_when_db_silently_fails(capsys):
    """Every upsert returns False → upsert_failed=N, stored=0, summary mentions it."""
    with patch.object(scraper, 'BARRIOS_TO_SCRAPE', _fake_barrios(3)), \
         patch.object(scraper, 'fetch_page', return_value=(_RENTAL_FIXTURE_HTML, 200)), \
         patch.object(scraper, 'migrate_create_rental_prices_table'), \
         patch.object(scraper, 'upsert_rental_snapshot', return_value=False):
        stored = scraper.run_rental_scraping(proxies=None)

    assert stored == 0
    summary = capsys.readouterr().out
    assert "3 fallos al guardar (DB)" in summary, summary
    assert "0 barrios guardados" in summary


def test_summary_omits_upsert_failed_clause_when_zero(capsys):
    """Happy path: upserts succeed, summary line is the legacy short form."""
    with patch.object(scraper, 'BARRIOS_TO_SCRAPE', _fake_barrios(2)), \
         patch.object(scraper, 'fetch_page', return_value=(_RENTAL_FIXTURE_HTML, 200)), \
         patch.object(scraper, 'migrate_create_rental_prices_table'), \
         patch.object(scraper, 'upsert_rental_snapshot', return_value=True):
        stored = scraper.run_rental_scraping(proxies=None)

    assert stored == 2
    summary = capsys.readouterr().out
    assert "fallos al guardar" not in summary
    assert "2 barrios guardados" in summary


def test_http_errors_counted_as_errors_http_not_db(capsys):
    """A 502 must increment `errors HTTP`, not `upsert_failed`."""
    with patch.object(scraper, 'BARRIOS_TO_SCRAPE', _fake_barrios(1)), \
         patch.object(scraper, 'fetch_page', return_value=(None, 502)), \
         patch.object(scraper, 'migrate_create_rental_prices_table'), \
         patch.object(scraper, 'upsert_rental_snapshot', return_value=True):
        stored = scraper.run_rental_scraping(proxies=None)

    assert stored == 0
    summary = capsys.readouterr().out
    assert "1 errores HTTP" in summary
    assert "fallos al guardar" not in summary
