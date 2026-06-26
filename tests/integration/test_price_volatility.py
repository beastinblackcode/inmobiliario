"""
Integration tests for market_indicators.get_price_volatility — exercises a
real (temporary) DB, seeding the ``market_snapshots`` city-level
``median_price_sqm`` series the indicator reads.

Volatility = coefficient of variation (std ÷ mean × 100) of the daily median
€/m², in a 7-day window vs a 30-day baseline. A 7d clearly above the 30d
baseline flags turbulence (a leading signal of a trend change).

Locks down:
  - the exact CV on a hand-computable series,
  - the date-windowing (7d ⊂ 30d),
  - the turbulence ("up") trend rule,
  - graceful handling of a series with too few snapshots.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pytest

import market_indicators as mi

pytestmark = pytest.mark.integration


def _seed_city_sqm(points: list[tuple[int, float]]) -> None:
    """Insert city-level median_price_sqm snapshots.

    ``points`` = list of (days_ago, value). Written straight to
    market_snapshots (created by init_database in the tmp_db fixture).
    """
    from db.connection import get_db

    conn = get_db()
    today = datetime.now().date()
    conn.executemany(
        """
        INSERT INTO market_snapshots
            (date_computed, scope_type, scope_value, metric_name, metric_value)
        VALUES (?, 'city', NULL, 'median_price_sqm', ?)
        """,
        [
            ((today - timedelta(days=d)).isoformat(), v)
            for d, v in points
        ],
    )
    conn.commit()


def test_volatility_known_series(tmp_db: Path):
    """
    7d window [200, 100, 150] → mean 150, sample std 50.0 → CV 33.33%.
    30d window adds [150, 150, 150] → mean 150, std 31.62 → CV 21.08%.
    7d (33.33) > 30d (21.08) × 1.2 → turbulence ("up").
    """
    _seed_city_sqm([
        (0, 200), (1, 100), (2, 150),       # inside the 7d window
        (10, 150), (11, 150), (12, 150),    # 30d baseline only
    ])

    res = mi.get_price_volatility()

    assert res["error"] is None
    assert res["n_7d"] == 3
    assert res["n_30d"] == 6
    assert res["vol_7d"] == pytest.approx(33.33, abs=0.01)
    assert res["vol_30d"] == pytest.approx(21.08, abs=0.01)
    assert res["current"] == res["vol_7d"]
    assert res["std_7d_sqm"] == pytest.approx(50.0, abs=0.1)
    assert res["std_30d_sqm"] == pytest.approx(31.6, abs=0.1)
    assert res["change"] == pytest.approx(12.25, abs=0.05)
    assert res["trend"] == "up"  # turbulence building
    assert res["unit"] == "%"


def test_volatility_settling_is_down(tmp_db: Path):
    """Flat recent week (7d CV = 0) below a noisier baseline → 'down'."""
    _seed_city_sqm([
        (0, 150), (1, 150), (2, 150),       # 7d: perfectly flat → CV 0
        (10, 100), (11, 200), (12, 150),    # noisier older points
    ])

    res = mi.get_price_volatility()
    assert res["vol_7d"] == pytest.approx(0.0, abs=0.01)
    assert res["vol_30d"] > 0
    assert res["trend"] == "down"


def test_volatility_insufficient_series(tmp_db: Path):
    """Fewer than 2 usable snapshots → graceful error, no crash."""
    _seed_city_sqm([(0, 3000)])  # single point

    res = mi.get_price_volatility()
    assert res["current"] is None
    assert res["error"] is not None


def test_volatility_empty_db(tmp_db: Path):
    """No snapshots at all → graceful error."""
    res = mi.get_price_volatility()
    assert res["current"] is None
    assert res["error"] is not None
