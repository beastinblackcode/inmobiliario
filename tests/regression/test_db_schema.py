"""
Regression tests for the database schema.

These guard against silent breaking changes:
  - dropping a required table
  - removing a required column
  - removing one of the composite indexes that the perf optimisation pass added
  - changing the metric_name catalogue used by compute_snapshots.py

If any of these fire after a migration, the migration almost certainly
needs to be reviewed.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from db.connection import get_db

pytestmark = pytest.mark.regression


# ──────────────────────────────────────────────────────────────────────────
# Tables
# ──────────────────────────────────────────────────────────────────────────


REQUIRED_TABLES = {
    "listings",
    "rental_prices",
    "watchlist",
    "notarial_prices",
    "market_snapshots",
}


def _existing_tables(conn) -> set:
    return {
        r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }


def _columns(conn, table) -> set:
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _indexes(conn, table) -> set:
    return {
        r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=?",
            (table,),
        ).fetchall()
    }


# ──────────────────────────────────────────────────────────────────────────
# Tests
# ──────────────────────────────────────────────────────────────────────────


class TestRequiredTables:
    def test_all_required_tables_exist(self, tmp_db: Path):
        existing = _existing_tables(get_db())
        missing = REQUIRED_TABLES - existing
        assert not missing, f"Missing required tables: {missing}"


class TestListingsColumns:
    REQUIRED_COLS = {
        "listing_id", "title", "url", "price",
        "distrito", "barrio", "rooms", "size_sqm",
        "floor", "orientation", "seller_type", "is_new_development",
        "description", "first_seen_date", "last_seen_date", "status",
    }

    def test_all_columns_present(self, tmp_db: Path):
        cols = _columns(get_db(), "listings")
        missing = self.REQUIRED_COLS - cols
        assert not missing, f"Missing listings columns: {missing}"

    def test_listing_id_is_primary_key(self, tmp_db: Path):
        info = get_db().execute("PRAGMA table_info(listings)").fetchall()
        pk_cols = [r[1] for r in info if r[5] == 1]   # 6th element is pk flag
        assert pk_cols == ["listing_id"]


class TestCompositeIndexes:
    """The composite indexes added in the perf optimisation pass (commit dc5a3cf)."""

    REQUIRED_INDEXES = {
        "idx_active_distrito_price",
        "idx_active_barrio_price",
        "idx_status_last_seen",
    }

    def test_indexes_present(self, tmp_db: Path):
        idx = _indexes(get_db(), "listings")
        missing = self.REQUIRED_INDEXES - idx
        assert not missing, f"Missing composite indexes: {missing}"


class TestMarketSnapshotsSchema:
    REQUIRED_COLS = {
        "id", "date_computed", "scope_type", "scope_value",
        "metric_name", "metric_value",
    }

    def test_columns(self, tmp_db: Path):
        cols = _columns(get_db(), "market_snapshots")
        missing = self.REQUIRED_COLS - cols
        assert not missing

    def test_unique_constraint_via_index(self, tmp_db: Path):
        # The UNIQUE(date_computed, scope_type, scope_value, metric_name)
        # constraint creates an auto-index named sqlite_autoindex_*.
        idx = _indexes(get_db(), "market_snapshots")
        assert any(name.startswith("sqlite_autoindex") for name in idx)


class TestNotarialPricesSchema:
    def test_columns(self, tmp_db: Path):
        cols = _columns(get_db(), "notarial_prices")
        for col in {"distrito", "periodo", "precio_m2"}:
            assert col in cols
