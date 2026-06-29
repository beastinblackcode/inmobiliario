"""
Unit test for the price-drops tab's evolution line-chart helper
(default_trend_districts) — picks the default districts to plot.
"""

from __future__ import annotations

import pytest

from tabs.price_drops_tab import default_trend_districts

pytestmark = pytest.mark.unit


def _rows():
    # Two weeks of data; listing counts decide the ranking.
    return [
        {"distrito": "Salamanca", "n_listings": 50, "avg_sqm": 9000, "week_start": "2026-06-01"},
        {"distrito": "Salamanca", "n_listings": 40, "avg_sqm": 9100, "week_start": "2026-06-08"},
        {"distrito": "Centro", "n_listings": 30, "avg_sqm": 8000, "week_start": "2026-06-01"},
        {"distrito": "Latina", "n_listings": 5, "avg_sqm": 4000, "week_start": "2026-06-01"},
        {"distrito": "Usera", "n_listings": 2, "avg_sqm": 3500, "week_start": "2026-06-01"},
    ]


def test_ranks_by_total_listings_descending():
    out = default_trend_districts(_rows(), n=3)
    assert out == ["Salamanca", "Centro", "Latina"]  # 90, 30, 5


def test_respects_n_limit():
    assert len(default_trend_districts(_rows(), n=2)) == 2


def test_empty_input():
    assert default_trend_districts([], n=6) == []


def test_ignores_rows_without_distrito_and_null_counts():
    rows = [
        {"distrito": None, "n_listings": 999},
        {"distrito": "Retiro", "n_listings": None},
        {"distrito": "Retiro", "n_listings": 7},
    ]
    # None distrito dropped; Retiro summed treating null as 0 → still selected.
    assert default_trend_districts(rows, n=5) == ["Retiro"]
