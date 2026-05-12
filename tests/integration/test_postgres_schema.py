"""Smoke tests for the Postgres backend introduced in Phase 3.

These run against a disposable Postgres container (testcontainers) and
exist to validate that:

  1. The Alembic ``0001`` revision applies cleanly against a stock
     ``postgres:16-alpine`` image.
  2. Every table the SQLite codebase expects ends up created.
  3. The type translations (TEXT date → DATE, INTEGER → BOOLEAN, etc.)
     are honoured — we sample one column per kind.
  4. The shared connection pool (``db.connection_pg``) can check out a
     connection and round-trip a simple query.

The tests are skipped automatically when Docker isn't available — see
``conftest.py:_docker_available``.
"""

from __future__ import annotations

import psycopg


# ─── tables we expect after upgrade head ────────────────────────────────
_EXPECTED_TABLES = {
    "alembic_version",      # Alembic itself
    "listings",
    "price_history",
    "rental_prices",
    "watchlist",
    "notarial_prices",
    "market_snapshots",
    "scraping_log",
    "custom_alerts",
    "listing_signals",
    "listing_amenities",
    "cgpj_lanzamientos",
    "property_fingerprints",      # added in 0002
    "listing_property_map",       # added in 0002
}


def test_all_tables_created(tmp_pg_db: str) -> None:
    """Every documented table must exist after ``alembic upgrade head``."""
    with psycopg.connect(tmp_pg_db) as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
        )
        actual = {r[0] for r in cur.fetchall()}

    missing = _EXPECTED_TABLES - actual
    extra   = actual - _EXPECTED_TABLES
    assert not missing, f"missing tables after upgrade: {sorted(missing)}"
    assert not extra,   f"unexpected extra tables: {sorted(extra)}"


def test_listings_date_columns_are_DATE(tmp_pg_db: str) -> None:
    """``first_seen_date`` and ``last_seen_date`` must be DATE, not TEXT."""
    with psycopg.connect(tmp_pg_db) as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT column_name, data_type
              FROM information_schema.columns
             WHERE table_name = 'listings'
               AND column_name IN ('first_seen_date', 'last_seen_date')
        """)
        types = dict(cur.fetchall())

    assert types == {
        "first_seen_date": "date",
        "last_seen_date":  "date",
    }


def test_listing_amenities_flags_are_BOOLEAN(tmp_pg_db: str) -> None:
    """The boolean flags must be ``boolean``, not ``integer`` (was 0/1 in SQLite)."""
    with psycopg.connect(tmp_pg_db) as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT column_name, data_type
              FROM information_schema.columns
             WHERE table_name = 'listing_amenities'
               AND column_name LIKE 'has_%'
        """)
        rows = cur.fetchall()

    assert rows, "no has_* columns found on listing_amenities"
    for col, dtype in rows:
        assert dtype == "boolean", f"{col} is {dtype}, expected boolean"


def test_price_history_unique_constraint(tmp_pg_db: str) -> None:
    """``price_history`` must reject two rows with the same (listing_id, date)."""
    with psycopg.connect(tmp_pg_db) as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO listings (listing_id, status) VALUES ('TEST1', 'active')"
        )
        cur.execute(
            "INSERT INTO price_history (listing_id, price, date_recorded) "
            "VALUES ('TEST1', 100000, '2026-05-09')"
        )
        try:
            cur.execute(
                "INSERT INTO price_history (listing_id, price, date_recorded) "
                "VALUES ('TEST1', 99000, '2026-05-09')"
            )
        except psycopg.errors.UniqueViolation:
            return
        raise AssertionError(
            "expected UniqueViolation on duplicate (listing_id, date_recorded)"
        )


def test_pool_round_trip(tmp_pg_db: str) -> None:
    """``db.connection_pg.get_pg_conn`` must hand out a live connection."""
    from db.connection_pg import get_pg_conn

    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 AS v")
            row = cur.fetchone()
    assert row == {"v": 1}
