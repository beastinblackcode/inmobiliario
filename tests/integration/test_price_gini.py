"""
Integration tests for market_indicators.get_price_gini — exercises a real
(temporary) DB via the seeded fixture.

The Gini coefficient measures how unequally asking prices are spread:
    0 = every active flat priced the same, 1 = all value in one listing.

Locks down:
  - the exact value on the known seed (active prices only),
  - that sold listings are excluded,
  - graceful handling of empty / single-listing databases,
  - the mathematical bounds (0 ≤ G < 1) and the equal-prices edge case.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import market_indicators as mi

pytestmark = pytest.mark.integration


def test_gini_on_known_seed(tmp_db_seeded: Path):
    """
    Active prices in the seed are [350k, 500k, 600k, 700k, 900k] (L005 is
    sold_removed and must be excluded). For n=5:

        weighted = Σ (2i−6)·x = -1.4M -1.0M +0 +1.4M +3.6M = 2.6M
        G = 2.6M / (5 · 3.05M) = 0.170
    """
    res = mi.get_price_gini()

    assert res["error"] is None
    assert res["n"] == 5  # sold_removed L005 excluded
    assert res["current"] == pytest.approx(0.170, abs=0.001)
    assert res["mean_price"] == 610_000
    assert res["median_price"] == 600_000
    # 0.17 sits in the homogeneous band.
    assert res["trend"] == "down"


def test_gini_excludes_sold(tmp_db_seeded: Path):
    """The €800k sold listing must not inflate the count or the coefficient."""
    res = mi.get_price_gini()
    assert res["n"] == 5  # six rows seeded, one sold


def test_gini_empty_db(tmp_db: Path):
    """No active listings → graceful error, no crash."""
    res = mi.get_price_gini()
    assert res["current"] is None
    assert res["n"] == 0
    assert res["error"] is not None


def test_gini_bounds_and_units(tmp_db_seeded: Path):
    res = mi.get_price_gini()
    assert 0.0 <= res["current"] < 1.0
    assert res["unit"] == "índice"
