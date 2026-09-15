"""
Unit tests for the price component of market_indicators.calculate_market_score.

The component used to bucket a *one-week* price move at ±2 % and ±5 %. Those
cut-offs were sized for the pre-2026-09 indicator, which re-sampled a
different set of districts every week and therefore swung ±15 %. On the
composition-controlled index a week moves ~0.2 %, so every bucket except the
two straddling zero became unreachable: the component was pinned to 45/60 and
its 20 % weight did nothing.

It now reads ``change_pct_horizon`` (a multi-week, block-to-block move),
normalised to a 4-week rate.

Locks down:
  - the full 15..90 range is reachable at realistic monthly rates,
  - the bucket edges,
  - normalisation by ``horizon_weeks``,
  - a payload without the field scores neutral rather than mis-scaling.
"""

from __future__ import annotations

import pytest

from market_indicators import calculate_market_score

pytestmark = pytest.mark.unit


def _prices_component(horizon_pct, horizon_weeks: int = 4) -> int:
    """The `prices` sub-score for a given price move.

    Everything else is left empty so it takes the neutral fallback and cannot
    affect what we are measuring.
    """
    price_trend = {}
    if horizon_pct is not None:
        price_trend = {
            "change_pct_horizon": horizon_pct,
            "horizon_weeks": horizon_weeks,
        }
    score = calculate_market_score(
        price_trend=price_trend, sales_speed={}, supply_demand={}, inventory={},
    )
    return score["components"]["prices"]


def test_full_range_is_reachable():
    """Realistic monthly rates must span the whole scale.

    Under the old weekly thresholds every one of these inputs scored 45 or 60.
    """
    monthly_to_score = {
        2.0: 90,    # ≈ +26 %/yr
        1.0: 75,    # ≈ +13 %/yr
        0.5: 60,
        0.0: 50,
        -0.5: 40,
        -1.0: 25,   # ≈ -13 %/yr
        -2.0: 15,
    }
    got = {pct: _prices_component(pct) for pct in monthly_to_score}

    assert got == monthly_to_score
    assert len(set(got.values())) == 7, "buckets collapsed"


def test_bucket_edges():
    """The boundaries themselves, since they are the whole point.

    Every cut-off is a strict ``>``, so a value sitting exactly on one falls
    into the lower-scoring bucket. The flat band is the exception: it is
    inclusive at both ends, so ±0.25 both read as flat.
    """
    assert _prices_component(1.51) == 90
    assert _prices_component(1.50) == 75
    assert _prices_component(0.76) == 75
    assert _prices_component(0.75) == 60
    assert _prices_component(0.26) == 60
    assert _prices_component(0.25) == 50
    assert _prices_component(-0.25) == 50
    assert _prices_component(-0.26) == 40
    assert _prices_component(-0.74) == 40
    assert _prices_component(-0.75) == 25
    assert _prices_component(-1.49) == 25
    assert _prices_component(-1.50) == 15


def test_a_flat_market_scores_neutral_not_bullish():
    """A market going nowhere must not read as rising.

    The old thresholds had no flat band — anything above 0.0 scored 60 — so
    noise in the last decimal decided whether the market looked like it was
    appreciating.
    """
    assert _prices_component(0.05) == 50
    assert _prices_component(-0.05) == 50
    assert _prices_component(0.0) == 50


def test_normalised_by_horizon():
    """A move is scored as a rate, so the same percentage over a shorter
    block counts for more."""
    # 0.75 % over 4 weeks is 0.75 %/month → the 60 bucket.
    assert _prices_component(0.75, horizon_weeks=4) == 60
    # The same 0.75 % over 3 weeks is 1.0 %/month → the 75 bucket.
    assert _prices_component(0.75, horizon_weeks=3) == 75


def test_missing_horizon_scores_neutral():
    """A payload from before the field existed must not be mis-scaled.

    Falling back to change_pct would read a ~4x smaller number on the same
    scale, making any market look flat.
    """
    assert _prices_component(None) == 50

    stale = {"change_pct": 2.5, "current": 480_000}
    score = calculate_market_score(
        price_trend=stale, sales_speed={}, supply_demand={}, inventory={},
    )
    assert score["components"]["prices"] == 50


def test_weights_sum_to_one():
    """A component's weight is only meaningful if the total is 1.0."""
    score = calculate_market_score(
        price_trend={}, sales_speed={}, supply_demand={}, inventory={},
    )
    assert sum(score["weights"].values()) == pytest.approx(1.0)
    assert set(score["weights"]) == set(score["components"])
