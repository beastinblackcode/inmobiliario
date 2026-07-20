"""
Regression: mark_stale_as_sold Tier 1 must require *depth* coverage.

Post-mortem 2026-07-19. Tier 1 used to accept a barrio as "scraped
recently" when any single listing in it had a fresh last_seen_date. In
lite mode the scraper reads only page 1 (~30 listings per barrio), so
barrios whose deeper pages had gone unread for 42 days still passed the
check, and every deep-page listing was marked sold on schedule — ~5.6k
false positives, all reactivated by the next complete sweep.

Coverage now comes from ``barrio_coverage``, written only when a
full-mode sweep walks a barrio to the end of its pagination.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from database import mark_stale_as_sold, record_barrio_coverage, get_connection


def _d(days_ago: int) -> str:
    return (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")


def _insert(cursor, listing_id: str, distrito: str, barrio: str, last_seen: str):
    cursor.execute(
        """
        INSERT INTO listings
            (listing_id, title, url, price, distrito, barrio,
             first_seen_date, last_seen_date, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'active')
        """,
        (listing_id, f"Test {listing_id}", f"http://test/{listing_id}",
         300000, distrito, barrio, _d(120), last_seen),
    )


def _status(cursor, listing_id: str) -> str:
    cursor.execute("SELECT status FROM listings WHERE listing_id = ?", (listing_id,))
    return cursor.fetchone()[0]


@pytest.fixture
def barrio_setup(tmp_db):
    """One stale listing in a barrio that also has a fresh page-1 listing."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS barrio_coverage (
                distrito TEXT NOT NULL,
                barrio TEXT NOT NULL,
                last_deep_scrape_date TEXT NOT NULL,
                pages_scraped INTEGER,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (distrito, barrio)
            )
        """)
        # Buried on page 4, last seen 30 days ago.
        _insert(cursor, "DEEP01", "Centro", "Sol", _d(30))
        # Page-1 neighbour refreshed today by a lite run.
        _insert(cursor, "FRESH01", "Centro", "Sol", _d(0))
    yield


def test_lite_coverage_does_not_mark_deep_listing_sold(barrio_setup):
    """The bug: a fresh page-1 sibling must not vouch for the whole barrio."""
    mark_stale_as_sold(days_threshold=21)

    with get_connection() as conn:
        cursor = conn.cursor()
        assert _status(cursor, "DEEP01") == "active", (
            "Deep-page listing was marked sold on the strength of a page-1 "
            "sibling's last_seen_date — the pre-0009 false-sold bug."
        )


def test_full_sweep_coverage_does_mark_deep_listing_sold(barrio_setup):
    """With a real depth sweep recorded, Tier 1 should fire as intended."""
    record_barrio_coverage("Centro", "Sol", pages_scraped=5, scrape_date=_d(1))

    mark_stale_as_sold(days_threshold=21)

    with get_connection() as conn:
        cursor = conn.cursor()
        assert _status(cursor, "DEEP01") == "sold_removed"
        assert _status(cursor, "FRESH01") == "active"


def test_stale_coverage_does_not_vouch(barrio_setup):
    """Coverage older than the threshold must not count."""
    record_barrio_coverage("Centro", "Sol", pages_scraped=5, scrape_date=_d(40))

    mark_stale_as_sold(days_threshold=21)

    with get_connection() as conn:
        cursor = conn.cursor()
        assert _status(cursor, "DEEP01") == "active"


def test_hard_cutoff_still_catches_ghosts(tmp_db):
    """Tier 2 backstop fires past 60 days with no coverage at all."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS barrio_coverage (
                distrito TEXT NOT NULL, barrio TEXT NOT NULL,
                last_deep_scrape_date TEXT NOT NULL, pages_scraped INTEGER,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (distrito, barrio)
            )
        """)
        _insert(cursor, "GHOST01", "Latina", "Cármenes", _d(90))
        _insert(cursor, "RECENT01", "Latina", "Cármenes", _d(45))

    mark_stale_as_sold(days_threshold=21)

    with get_connection() as conn:
        cursor = conn.cursor()
        assert _status(cursor, "GHOST01") == "sold_removed"
        # 45 days is inside the 60-day backstop and has no coverage proof.
        assert _status(cursor, "RECENT01") == "active"
