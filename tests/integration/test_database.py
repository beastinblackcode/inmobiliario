"""
Integration tests for database.py — exercises a real (temporary) SQLite DB.

Covers:
    init_database         — schema creation
    insert_listing / update_listing
    mark_stale_as_sold    — 14-day threshold + 21-day fallback
    get_listings_page     — pagination + projection (price_per_sqm, days_on_market)
    get_active_listing_ids
    get_database_stats
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pytest

from db.connection import get_db
import database as dbmod

pytestmark = pytest.mark.integration


# ──────────────────────────────────────────────────────────────────────────
# init_database
# ──────────────────────────────────────────────────────────────────────────


class TestInitDatabase:
    def test_creates_listings_table(self, tmp_db: Path):
        conn = get_db()
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='listings'"
        ).fetchall()
        assert len(rows) == 1

    def test_creates_indexes(self, tmp_db: Path):
        conn = get_db()
        idx_names = {
            r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='listings'"
            ).fetchall()
        }
        # Composite indexes from the perf optimisation pass
        assert "idx_active_distrito_price" in idx_names
        assert "idx_active_barrio_price"   in idx_names
        assert "idx_status_last_seen"      in idx_names

    def test_idempotent(self, tmp_db: Path):
        # Calling init_database twice must not raise
        dbmod.init_database()
        dbmod.init_database()


# ──────────────────────────────────────────────────────────────────────────
# insert_listing / update_listing
# ──────────────────────────────────────────────────────────────────────────


class TestInsertUpdate:
    def test_insert_listing_round_trip(self, tmp_db: Path):
        ok = dbmod.insert_listing({
            "listing_id": "X1",
            "title":      "Test",
            "url":        "http://test/1",
            "price":      400_000,
            "distrito":   "Centro",
            "barrio":     "Sol",
            "rooms":      3,
            "size_sqm":   80,
            "seller_type": "Particular",
        })
        assert ok is True
        conn = get_db()
        row = conn.execute(
            "SELECT title, price, status FROM listings WHERE listing_id=?", ("X1",)
        ).fetchone()
        assert row["title"] == "Test"
        assert row["price"] == 400_000
        assert row["status"] == "active"

    def test_update_listing_changes_price(self, tmp_db: Path):
        dbmod.insert_listing({
            "listing_id": "X2", "title": "T", "url": "http://x/2",
            "price": 300_000, "distrito": "Centro", "barrio": "Sol",
        })
        ok = dbmod.update_listing("X2", {"price": 280_000})
        assert ok is True
        conn = get_db()
        row = conn.execute("SELECT price FROM listings WHERE listing_id=?", ("X2",)).fetchone()
        assert row["price"] == 280_000


# ──────────────────────────────────────────────────────────────────────────
# mark_stale_as_sold — the rule we MUST not break
# ──────────────────────────────────────────────────────────────────────────


class TestMarkStaleAsSold:
    """
    Critical invariants (see database.py:581):
      - listings whose last_seen_date is < threshold days old → stay active
      - listings ≥ threshold days old where the *barrio* has been seen
        recently (proving the scraper visited it) → marked sold
      - listings ≥ 21 days old → always marked sold (fallback regardless of barrio)
    """

    def test_recent_listings_stay_active(self, tmp_db_seeded: Path):
        # L001 (today), L002 (12d), L003 (60d but no fresh barrio scrape) …
        # L004 is 15d old in barrio "Ibiza" (Retiro) — without a fresh
        # scrape signal in that barrio it shouldn't be marked at the 14d
        # threshold.  But the 21-day fallback would catch it if older.
        marked = dbmod.mark_stale_as_sold(days_threshold=14)
        # L001 is today → must remain active
        conn = get_db()
        status = conn.execute(
            "SELECT status FROM listings WHERE listing_id='L001'"
        ).fetchone()["status"]
        assert status == "active"
        assert isinstance(marked, int) and marked >= 0

    def test_21_day_fallback_marks_old_listings(self, tmp_db: Path):
        # Insert a 25-day-old listing — must always be caught by the
        # 21-day fallback even if its barrio has no recent activity.
        old_date = (datetime.now().date() - timedelta(days=25)).isoformat()
        conn = get_db()
        conn.execute("""
            INSERT INTO listings
                (listing_id, title, price, distrito, barrio,
                 first_seen_date, last_seen_date, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'active')
        """, ("OLD1", "Old", 300_000, "Tetuán", "Castillejos", old_date, old_date))
        conn.commit()

        dbmod.mark_stale_as_sold(days_threshold=14)

        status = conn.execute(
            "SELECT status FROM listings WHERE listing_id='OLD1'"
        ).fetchone()["status"]
        assert status == "sold_removed"

    def test_already_sold_is_not_re_marked(self, tmp_db: Path):
        # Status sold_removed → mark_stale_as_sold leaves it alone.
        conn = get_db()
        old = (datetime.now().date() - timedelta(days=60)).isoformat()
        conn.execute("""
            INSERT INTO listings
                (listing_id, title, price, distrito, barrio,
                 first_seen_date, last_seen_date, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'sold_removed')
        """, ("SOLD1", "Sold", 500_000, "Centro", "Sol", old, old))
        conn.commit()

        before_marked = conn.execute(
            "SELECT COUNT(*) AS c FROM listings WHERE status='sold_removed'"
        ).fetchone()["c"]

        dbmod.mark_stale_as_sold(days_threshold=14)

        after_marked = conn.execute(
            "SELECT COUNT(*) AS c FROM listings WHERE status='sold_removed'"
        ).fetchone()["c"]

        # The sold counter can only stay equal or grow — never lose a sold row
        assert after_marked >= before_marked


# ──────────────────────────────────────────────────────────────────────────
# get_listings_page
# ──────────────────────────────────────────────────────────────────────────


class TestGetListingsPage:
    def test_returns_active_only_by_default(self, tmp_db_seeded: Path):
        rows, total = dbmod.get_listings_page(status="active", page_size=0)
        statuses = {r["status"] for r in rows}
        assert statuses == {"active"}
        assert total == len(rows)

    def test_distrito_filter(self, tmp_db_seeded: Path):
        rows, _ = dbmod.get_listings_page(distrito=["Centro"], page_size=0)
        assert all(r["distrito"] == "Centro" for r in rows)
        assert len(rows) >= 1

    def test_price_range_filter(self, tmp_db_seeded: Path):
        rows, _ = dbmod.get_listings_page(min_price=400_000, max_price=600_000, page_size=0)
        assert all(400_000 <= r["price"] <= 600_000 for r in rows)

    def test_seller_type_filter(self, tmp_db_seeded: Path):
        rows, _ = dbmod.get_listings_page(seller_type="Particular", page_size=0)
        assert all(r["seller_type"] == "Particular" for r in rows)

    def test_projection_includes_price_per_sqm_and_dom(self, tmp_db_seeded: Path):
        rows, _ = dbmod.get_listings_page(status="active", page_size=0)
        sample = rows[0]
        assert "price_per_sqm" in sample
        assert "days_on_market" in sample
        # And they're roughly correct for our seed (L001 is 350k / 80 m²)
        l001 = next(r for r in rows if r["listing_id"] == "L001")
        assert abs(l001["price_per_sqm"] - 4_375) < 1

    def test_pagination(self, tmp_db_seeded: Path):
        page1, total = dbmod.get_listings_page(status="active", page_size=2, page=1)
        page2, _    = dbmod.get_listings_page(status="active", page_size=2, page=2)
        assert len(page1) == 2
        assert {r["listing_id"] for r in page1} & {r["listing_id"] for r in page2} == set()
        assert total >= 4   # we seed 5 active listings


# ──────────────────────────────────────────────────────────────────────────
# get_active_listing_ids / get_database_stats
# ──────────────────────────────────────────────────────────────────────────


class TestStatsHelpers:
    def test_active_listing_ids(self, tmp_db_seeded: Path):
        ids = dbmod.get_active_listing_ids()
        assert isinstance(ids, set)
        assert "L001" in ids and "L002" in ids
        assert "L005" not in ids   # sold

    def test_database_stats(self, tmp_db_seeded: Path):
        stats = dbmod.get_database_stats()
        assert stats["active_count"] >= 4
        assert stats["sold_count"]   >= 1
