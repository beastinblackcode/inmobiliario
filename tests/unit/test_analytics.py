"""
Unit tests for analytics.py — pure scoring functions, no DB.

Covers:
    calculate_quality_score
    calculate_negotiability_score
    negotiability_label
    identify_bargains
"""

from __future__ import annotations

import pytest

from analytics import (
    calculate_quality_score,
    calculate_negotiability_score,
    negotiability_label,
)

pytestmark = pytest.mark.unit


# ──────────────────────────────────────────────────────────────────────────
# calculate_quality_score
# ──────────────────────────────────────────────────────────────────────────


class TestQualityScore:
    def test_score_is_clamped_between_0_and_100(
        self, sample_listings, sample_distrito_stats, sample_barrio_stats
    ):
        for row in sample_listings:
            score = calculate_quality_score(row, sample_distrito_stats, sample_barrio_stats)
            assert 0 <= score <= 100, f"score {score} out of range for {row['listing_id']}"

    def test_cheap_listing_scores_higher_than_expensive(
        self, sample_distrito_stats, sample_barrio_stats
    ):
        # Same distrito/barrio, identical except price/m²
        cheap = {
            "distrito": "Centro", "barrio": "Sol",
            "price_per_sqm": 3_500,  # much cheaper than barrio avg (5500)
            "days_on_market": 10, "num_drops": 0, "total_drop_pct": 0,
            "seller_type": "Agencia",
        }
        expensive = dict(cheap, price_per_sqm=7_500)  # well above

        s_cheap = calculate_quality_score(cheap, sample_distrito_stats, sample_barrio_stats)
        s_expensive = calculate_quality_score(expensive, sample_distrito_stats, sample_barrio_stats)
        assert s_cheap > s_expensive

    def test_more_drops_increase_score(self, sample_distrito_stats, sample_barrio_stats):
        base = {
            "distrito": "Salamanca", "barrio": "Goya",
            "price_per_sqm": 8_500, "days_on_market": 30,
            "seller_type": "Agencia", "total_drop_pct": -10,
        }
        s_no_drops    = calculate_quality_score({**base, "num_drops": 0}, sample_distrito_stats, sample_barrio_stats)
        s_three_drops = calculate_quality_score({**base, "num_drops": 3}, sample_distrito_stats, sample_barrio_stats)
        assert s_three_drops > s_no_drops

    def test_particular_seller_gets_bonus(self, sample_distrito_stats, sample_barrio_stats):
        base = {
            "distrito": "Centro", "barrio": "Sol",
            "price_per_sqm": 5_500, "days_on_market": 10,
            "num_drops": 0, "total_drop_pct": 0,
        }
        s_agency  = calculate_quality_score({**base, "seller_type": "Agencia"},    sample_distrito_stats, sample_barrio_stats)
        s_particular = calculate_quality_score({**base, "seller_type": "Particular"}, sample_distrito_stats, sample_barrio_stats)
        assert s_particular - s_agency == pytest.approx(10, abs=0.01)

    def test_falls_back_to_distrito_when_barrio_missing(
        self, sample_distrito_stats
    ):
        # No barrio_stats entry for "UnknownBarrio" — fallback to distrito
        row = {
            "distrito": "Centro", "barrio": "UnknownBarrio",
            "price_per_sqm": 3_000,  # very cheap vs Centro's 6000
            "days_on_market": 30, "num_drops": 1, "total_drop_pct": -5,
            "seller_type": "Particular",
        }
        score = calculate_quality_score(row, sample_distrito_stats, barrio_stats={})
        assert score > 30  # Should still receive cheap-vs-distrito bonus

    def test_handles_missing_price_per_sqm(self, sample_distrito_stats, sample_barrio_stats):
        row = {
            "distrito": "Centro", "barrio": "Sol",
            "price_per_sqm": None,
            "days_on_market": 0, "num_drops": 0, "total_drop_pct": 0,
            "seller_type": "Agencia",
        }
        # Should not raise; score returns the seller-type/dom contributions only (here, 0).
        score = calculate_quality_score(row, sample_distrito_stats, sample_barrio_stats)
        assert score == 0

    def test_notarial_bonus_applied_when_below_real_price(
        self, sample_distrito_stats, sample_barrio_stats
    ):
        notarial = {"Centro": 6_500}  # real escriturado €/m²
        row = {
            "distrito": "Centro", "barrio": "Sol",
            "price_per_sqm": 6_000,  # below the real notarial price
            "days_on_market": 0, "num_drops": 0, "total_drop_pct": 0,
            "seller_type": "Agencia",
        }
        no_bonus  = calculate_quality_score(row, sample_distrito_stats, sample_barrio_stats)
        with_bonus = calculate_quality_score(row, sample_distrito_stats, sample_barrio_stats, notarial)
        assert with_bonus > no_bonus


# ──────────────────────────────────────────────────────────────────────────
# calculate_negotiability_score
# ──────────────────────────────────────────────────────────────────────────


class TestNegotiabilityScore:
    def test_score_in_range(self, sample_listings, sample_distrito_stats):
        for row in sample_listings:
            s = calculate_negotiability_score(row, sample_distrito_stats)
            assert 0.0 <= s <= 100.0

    def test_old_listing_with_drops_gets_high_score(self, sample_distrito_stats):
        row = {
            "distrito": "Salamanca", "barrio": "Goya",
            "price_per_sqm": 9_500,  # 12% above distrito avg (8500)
            "days_on_market": 130,   # >120 days
            "num_drops": 3, "total_drop_pct": -16,
            "seller_type": "Particular",
        }
        s = calculate_negotiability_score(row, sample_distrito_stats)
        # All four components near max → should be high (>= 80)
        assert s >= 80

    def test_fresh_listing_low_score(self, sample_distrito_stats):
        row = {
            "distrito": "Salamanca", "barrio": "Goya",
            "price_per_sqm": 8_400,  # at distrito avg
            "days_on_market": 5,
            "num_drops": 0, "total_drop_pct": 0,
            "seller_type": "Agencia",
        }
        s = calculate_negotiability_score(row, sample_distrito_stats)
        # Only seller_score (4 for Agencia) + maybe gap_score (0 if at avg)
        assert s < 20

    def test_seller_type_unknown_gets_neutral(self, sample_distrito_stats):
        # price_per_sqm clearly BELOW distrito avg (6000) → no gap_score
        row = {
            "distrito": "Centro", "barrio": "Sol",
            "price_per_sqm": 4_500,
            "days_on_market": 5, "num_drops": 0, "total_drop_pct": 0,
            "seller_type": "",
        }
        s = calculate_negotiability_score(row, sample_distrito_stats)
        # Unknown seller_type → 8 points; days_score 0; drops 0; gap 0.
        assert s == 8.0


# ──────────────────────────────────────────────────────────────────────────
# negotiability_label
# ──────────────────────────────────────────────────────────────────────────


class TestNegotiabilityLabel:
    @pytest.mark.parametrize("score,expected_label", [
        (95, "Margen alto"),
        (70, "Margen alto"),
        (60, "Margen normal"),
        (45, "Margen normal"),
        (30, "Vendedor firme"),
        (20, "Vendedor firme"),
        (10, "Sin margen"),
        (0,  "Sin margen"),
    ])
    def test_thresholds(self, score, expected_label):
        emoji, label = negotiability_label(score)
        assert label == expected_label
        assert emoji  # non-empty


# Note: identify_bargains() depends on rank_opportunities() which hits the
# DB for price_history → integration test, not unit. See tests/integration/.
