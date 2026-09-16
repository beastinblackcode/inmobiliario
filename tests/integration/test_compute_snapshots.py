"""
Integration tests for compute_snapshots.py — populates the
market_snapshots table from a temporary DB and verifies the math.

Critical metrics to guard:
    active_count        — straight COUNT
    sold_count_30d/90d  — windowed, lag-shifted by 21 d
    absorption_rate     — sold_30d / active * 100
    months_of_supply    — active / (sold_90d / 3), capped at 36
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pytest

from db.connection import get_db
import compute_snapshots as cs

pytestmark = pytest.mark.integration


# ──────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────


def _insert(conn, lid, distrito, barrio, price, sqm, last_seen, status="active"):
    conn.execute("""
        INSERT INTO listings
            (listing_id, title, price, distrito, barrio, size_sqm,
             first_seen_date, last_seen_date, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (lid, f"L {lid}", price, distrito, barrio, sqm,
          last_seen, last_seen, status))


def _get_metric(conn, scope_type, scope_value, name):
    # ``scope_value IS ?`` is SQLite's NULL-safe equality.  Postgres
    # doesn't support that shape — we either need ``IS NOT DISTINCT FROM``
    # or branch on whether the value is NULL.  Branching is simpler and
    # works on both backends.
    if scope_value is None:
        row = conn.execute("""
            SELECT metric_value FROM market_snapshots
            WHERE scope_type=? AND scope_value IS NULL AND metric_name=?
            ORDER BY date_computed DESC LIMIT 1
        """, (scope_type, name)).fetchone()
    else:
        row = conn.execute("""
            SELECT metric_value FROM market_snapshots
            WHERE scope_type=? AND scope_value=? AND metric_name=?
            ORDER BY date_computed DESC LIMIT 1
        """, (scope_type, scope_value, name)).fetchone()
    return row[0] if row else None


# ──────────────────────────────────────────────────────────────────────────
# Tests
# ──────────────────────────────────────────────────────────────────────────


class TestComputeSnapshots:
    def test_writes_active_count_at_city_scope(self, tmp_db: Path):
        conn = get_db()
        today = datetime.now().date().isoformat()
        for i in range(5):
            _insert(conn, f"A{i}", "Centro", "Sol", 300_000, 80, today)
        conn.commit()

        cs.compute_all_snapshots(today)

        active = _get_metric(conn, "city", None, "active_count")
        assert active == 5

    def test_distrito_scope_isolates_count(self, tmp_db: Path):
        conn = get_db()
        today = datetime.now().date().isoformat()
        # 3 in Centro, 2 in Salamanca
        for i in range(3):
            _insert(conn, f"C{i}", "Centro", "Sol", 300_000, 80, today)
        for i in range(2):
            _insert(conn, f"S{i}", "Salamanca", "Goya", 800_000, 100, today)
        conn.commit()

        cs.compute_all_snapshots(today)

        assert _get_metric(conn, "distrito", "Centro",    "active_count") == 3
        assert _get_metric(conn, "distrito", "Salamanca", "active_count") == 2

    def test_absorption_rate_formula(self, tmp_db: Path):
        """
        absorption_rate = sold_30d / active_count × 100

        The sold window is lag-shifted by _MATURITY_LAG_DAYS, so it runs
        [today - LAG - 30, today - LAG). Offsets are expressed against the
        constant rather than written out: they were hard-coded for a 21-day
        lag, and silently stopped exercising the window when it moved.
        """
        from market_indicators import _MATURITY_LAG_DAYS as LAG

        conn = get_db()
        today_d = datetime.now().date()
        today = today_d.isoformat()

        # 10 active listings (today)
        for i in range(10):
            _insert(conn, f"A{i}", "Centro", "Sol", 300_000, 80, today)

        # 4 sold listings sitting inside the lag-shifted 30-day window
        for i in range(4):
            d = (today_d - timedelta(days=LAG + 2 + i)).isoformat()
            _insert(conn, f"S{i}", "Centro", "Sol", 300_000, 80, d, status="sold_removed")
        conn.commit()

        cs.compute_all_snapshots(today)

        absorption = _get_metric(conn, "city", None, "absorption_rate")
        # 4 / 10 * 100 = 40.0
        assert absorption == pytest.approx(40.0, abs=0.01)

    def test_months_of_supply_formula(self, tmp_db: Path):
        """
        months_of_supply = active / (sold_90d / 3)

        Window is [today - LAG - 90, today - LAG).
        With active=18 and sold_90d=6 → 18 / (6/3) = 18 / 2 = 9 months.
        """
        from market_indicators import _MATURITY_LAG_DAYS as LAG

        conn = get_db()
        today_d = datetime.now().date()
        today = today_d.isoformat()

        for i in range(18):
            _insert(conn, f"A{i}", "Centro", "Sol", 300_000, 80, today)

        for i in range(6):
            d = (today_d - timedelta(days=LAG + 2 + i * 5)).isoformat()
            _insert(conn, f"S{i}", "Centro", "Sol", 300_000, 80, d, status="sold_removed")
        conn.commit()

        cs.compute_all_snapshots(today)

        mos = _get_metric(conn, "city", None, "months_of_supply")
        assert mos == pytest.approx(9.0, abs=0.1)

    def test_months_of_supply_capped_at_36(self, tmp_db: Path):
        """When sold rate is microscopic, months_of_supply must cap at 36."""
        conn = get_db()
        today_d = datetime.now().date()
        today = today_d.isoformat()

        for i in range(1000):
            _insert(conn, f"A{i}", "Centro", "Sol", 300_000, 80, today)
        # Only 1 sold in the window → uncapped would be 3000 months
        d = (today_d - timedelta(days=30)).isoformat()
        _insert(conn, "SX", "Centro", "Sol", 300_000, 80, d, status="sold_removed")
        conn.commit()

        cs.compute_all_snapshots(today)

        mos = _get_metric(conn, "city", None, "months_of_supply")
        assert mos == pytest.approx(36.0, abs=0.01)

    def test_sold_count_7d_excludes_the_unclassified_zone(self, tmp_db: Path):
        """
        Regression guard for the sold_count (7-day) bug: the window is
        [today - LAG - 7, today - LAG), so it must sit entirely on the far
        side of the maturity cliff.

        A listing too recent to have been classified must not count. With
        LAG=21 the window straddled that cliff — measured on live data,
        cohorts 22 days old were 0 % classified and cohorts 29 days old were
        99.8 % — so the metric read structurally low, the same way it read a
        structural 0 back when the lag was 14.
        """
        from market_indicators import _MATURITY_LAG_DAYS as LAG

        conn = get_db()
        today_d = datetime.now().date()
        today = today_d.isoformat()

        for i in range(10):
            _insert(conn, f"A{i}", "Centro", "Sol", 300_000, 80, today)
        # Inside the 7-day window [today - LAG - 7, today - LAG):
        for i in range(3):
            d = (today_d - timedelta(days=LAG + 2 + i)).isoformat()
            _insert(conn, f"IN{i}", "Centro", "Sol", 300_000, 80, d, status="sold_removed")
        # Younger than the lag: not yet reliably classified, must be excluded.
        _insert(conn, "TOO_NEW", "Centro", "Sol", 300_000, 80,
                (today_d - timedelta(days=LAG - 3)).isoformat(), status="sold_removed")
        # Older than the window: belongs to an earlier bucket.
        _insert(conn, "TOO_OLD", "Centro", "Sol", 300_000, 80,
                (today_d - timedelta(days=LAG + 10)).isoformat(), status="sold_removed")
        conn.commit()

        cs.compute_all_snapshots(today)

        assert _get_metric(conn, "city", None, "sold_count") == 3

    def test_idempotent_for_distrito_and_barrio_scope(self, tmp_db: Path):
        """
        Running compute_all_snapshots twice must not duplicate rows for
        distrito/barrio scope (where scope_value is NOT NULL and the
        ON CONFLICT clause works correctly).

        NOTE: City-scope rows (scope_value=NULL) DO currently duplicate
        because SQLite's UNIQUE constraint treats NULL ≠ NULL.  This is
        a known bug in market_snapshots schema — tracked separately.
        """
        conn = get_db()
        today = datetime.now().date().isoformat()
        for i in range(3):
            _insert(conn, f"A{i}", "Centro", "Sol", 300_000, 80, today)
        conn.commit()

        cs.compute_all_snapshots(today)
        rows1 = conn.execute(
            "SELECT COUNT(*) FROM market_snapshots WHERE date_computed=? AND scope_value IS NOT NULL",
            (today,),
        ).fetchone()[0]

        cs.compute_all_snapshots(today)
        rows2 = conn.execute(
            "SELECT COUNT(*) FROM market_snapshots WHERE date_computed=? AND scope_value IS NOT NULL",
            (today,),
        ).fetchone()[0]

        assert rows1 == rows2
