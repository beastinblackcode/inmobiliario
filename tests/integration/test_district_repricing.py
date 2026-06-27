"""
Integration tests for market_indicators.get_district_repricing_breakdown —
the per-district "where are sellers actively cutting now" lens used by the
Oportunidades page.

Locks down:
  - net-over-window aggregation (a listing counts as cutting iff its net
    change in the window is negative),
  - active-only scope (sold listings excluded),
  - the min_active sample-size guard,
  - drops/ups event counts and median €/m².
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pytest

import market_indicators as mi

pytestmark = pytest.mark.integration


def _seed(listings, history):
    """listings: (id, distrito, price, size_sqm, status). history: (id, change, days_ago)."""
    from db.connection import get_db

    conn = get_db()
    today = datetime.now().date()
    conn.executemany(
        """
        INSERT INTO listings
            (listing_id, title, price, distrito, barrio, rooms, size_sqm,
             seller_type, first_seen_date, last_seen_date, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (lid, f"piso {lid}", price, distrito, "B", 2, size, "Particular",
             today.isoformat(), today.isoformat(), status)
            for (lid, distrito, price, size, status) in listings
        ],
    )
    conn.executemany(
        """
        INSERT INTO price_history (listing_id, price, date_recorded, change_amount, change_percent)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            (lid, 100000, (today - timedelta(days=days_ago)).isoformat(), change, 0.0)
            for (lid, change, days_ago) in history
        ],
    )
    conn.commit()


def test_breakdown_basic_aggregation(tmp_db: Path):
    # District D1: 3 active listings.
    #   A: one −5000 drop in window  → cutter
    #   B: +3000 then −1000 in window → net −... wait, net = +2000 → NOT a cutter, 1 drop + 1 up
    #   C: no changes                → not a cutter
    _seed(
        listings=[
            ("A", "D1", 300000, 100, "active"),
            ("B", "D1", 400000, 100, "active"),
            ("C", "D1", 200000, 50, "active"),
        ],
        history=[
            ("A", -5000, 10),
            ("B", 3000, 12), ("B", -1000, 8),
        ],
    )

    res = mi.get_district_repricing_breakdown(window_days=35, min_active=1)
    assert len(res) == 1
    d = res[0]
    assert d["distrito"] == "D1"
    assert d["active"] == 3
    # Only A has a negative net over the window.
    assert d["pct_cutting"] == pytest.approx(round(1 / 3 * 100, 1))
    assert d["avg_net_cut"] == -5000
    assert d["drops"] == 2   # A's drop + B's drop
    assert d["ups"] == 1     # B's increase
    # Median €/m² of [3000, 4000, 4000] = 4000
    assert d["median_sqm"] == 4000


def test_breakdown_excludes_sold(tmp_db: Path):
    """A sold listing with a cut must not appear in the active-only breakdown."""
    _seed(
        listings=[
            ("A", "D1", 300000, 100, "active"),
            ("S", "D1", 300000, 100, "sold_removed"),
        ],
        history=[("A", -1000, 5), ("S", -9000, 5)],
    )
    res = mi.get_district_repricing_breakdown(window_days=35, min_active=1)
    d = next(r for r in res if r["distrito"] == "D1")
    assert d["active"] == 1          # sold excluded
    assert d["drops"] == 1           # only A's event counted


def test_breakdown_window_excludes_old_changes(tmp_db: Path):
    """A drop older than the window must not count."""
    _seed(
        listings=[("A", "D1", 300000, 100, "active")],
        history=[("A", -5000, 99)],  # 99 days ago, outside a 35d window
    )
    res = mi.get_district_repricing_breakdown(window_days=35, min_active=1)
    d = res[0]
    assert d["pct_cutting"] == 0.0
    assert d["drops"] == 0


def test_breakdown_min_active_guard(tmp_db: Path):
    """Districts below min_active are dropped as thin samples."""
    _seed(
        listings=[("A", "D1", 300000, 100, "active")],
        history=[("A", -5000, 5)],
    )
    assert mi.get_district_repricing_breakdown(min_active=150) == []
