"""
Unit tests for ``offer_engine.suggest_offer``.

The function is a pure aggregator over factor helpers — no DB, no UI.
Tests check both the individual factor behaviours (signs, caps,
neutrality on missing data) and the end-to-end aggregation.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from offer_engine import (
    CAP_DAYS_ON_MARKET,
    CAP_DROPS,
    CAP_NLP_SIGNALS,
    CAP_OVERPRICED,
    CAP_REPUBLICATIONS,
    CAP_SELLER_TYPE,
    MAX_DISCOUNT_PCT,
    OfferSuggestion,
    _factor_days_on_market,
    _factor_nlp_signals,
    _factor_overpriced,
    _factor_price_drops,
    _factor_republications,
    _factor_seller_type,
    suggest_offer,
)


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────


def _listing(**overrides) -> dict:
    base = {
        "price":          400_000,
        "days_on_market":      10,
        "num_drops":            0,
        "total_drop_pct":     0.0,
        "seller_type": "Agencia",
    }
    base.update(overrides)
    return base


# ──────────────────────────────────────────────────────────────────────
# Individual factor tests
# ──────────────────────────────────────────────────────────────────────


class TestDaysOnMarketFactor:
    def test_fresh_listing_yields_no_factor(self):
        assert _factor_days_on_market(_listing(days_on_market=15)) is None

    def test_30d_threshold_is_neutral(self):
        assert _factor_days_on_market(_listing(days_on_market=30)) is None

    def test_moderate_dom_produces_small_discount(self):
        f = _factor_days_on_market(_listing(days_on_market=60))
        assert f is not None
        assert -1.0 < f.discount_pct < 0

    def test_long_dom_capped(self):
        f = _factor_days_on_market(_listing(days_on_market=10 * 365))
        assert f is not None
        assert f.discount_pct == pytest.approx(-CAP_DAYS_ON_MARKET, abs=0.01)


class TestPriceDropsFactor:
    def test_no_drops_yields_no_factor(self):
        assert _factor_price_drops(_listing()) is None

    def test_single_drop_produces_small_discount(self):
        f = _factor_price_drops(_listing(num_drops=1, total_drop_pct=2.0))
        assert f is not None
        assert -3 < f.discount_pct < 0

    def test_many_drops_capped(self):
        f = _factor_price_drops(_listing(num_drops=20, total_drop_pct=50.0))
        assert f is not None
        assert f.discount_pct == pytest.approx(-CAP_DROPS, abs=0.01)


class TestSellerTypeFactor:
    def test_agencia_no_factor(self):
        assert _factor_seller_type(_listing(seller_type="Agencia")) is None

    def test_particular_full_discount(self):
        f = _factor_seller_type(_listing(seller_type="Particular"))
        assert f is not None
        assert f.discount_pct == pytest.approx(-CAP_SELLER_TYPE, abs=0.01)


class TestRepublicationsFactor:
    def test_no_history_no_factor(self):
        assert _factor_republications(None) is None

    def test_zero_reps_no_factor(self):
        ph = SimpleNamespace(republication_count=0)
        assert _factor_republications(ph) is None

    def test_one_rep_small_discount(self):
        ph = SimpleNamespace(republication_count=1)
        f = _factor_republications(ph)
        assert f is not None
        assert -2 < f.discount_pct < 0

    def test_many_reps_capped(self):
        ph = SimpleNamespace(republication_count=10)
        f = _factor_republications(ph)
        assert f is not None
        assert f.discount_pct == pytest.approx(-CAP_REPUBLICATIONS, abs=0.01)


class TestNlpSignalsFactor:
    def test_no_signals(self):
        assert _factor_nlp_signals(None) is None
        assert _factor_nlp_signals({}) is None

    def test_only_unrelated_signals(self):
        # ``renovated`` shouldn't generate a discount factor.
        assert _factor_nlp_signals({"renovated": True}) is None

    def test_negociable_adds_discount(self):
        f = _factor_nlp_signals({"negotiable": True})
        assert f is not None
        assert f.discount_pct == -2.0
        assert "negociable" in f.why

    def test_multiple_signals_sum_capped(self):
        f = _factor_nlp_signals({
            "negotiable": True,
            "urgency":    True,
            "needs_work": True,
            "direct":     True,
        })
        assert f is not None
        # negotiable (-2) + urgency (-1.5) + needs_work (-2) + direct (-0.5) = -6
        # capped at -CAP_NLP_SIGNALS (-4)
        assert f.discount_pct == pytest.approx(-CAP_NLP_SIGNALS, abs=0.01)


class TestOverpricedFactor:
    def test_fair_above_asking_no_factor(self):
        # Asking 380k, fair 400k → not overpriced.
        assert _factor_overpriced(380_000, 400_000) is None

    def test_small_gap_within_tolerance(self):
        # Asking 412k, fair 400k = 3% above → within tolerance.
        assert _factor_overpriced(412_000, 400_000) is None

    def test_clear_overprice_produces_discount(self):
        # Asking 440k, fair 400k = 10% above.
        f = _factor_overpriced(440_000, 400_000)
        assert f is not None
        assert f.discount_pct < 0

    def test_huge_overprice_capped(self):
        f = _factor_overpriced(800_000, 400_000)
        assert f is not None
        assert f.discount_pct == pytest.approx(-CAP_OVERPRICED, abs=0.01)


# ──────────────────────────────────────────────────────────────────────
# Aggregation tests
# ──────────────────────────────────────────────────────────────────────


class TestSuggestOffer:
    def test_neutral_listing_yields_at_fair(self):
        """Fresh listing, agency, no drops, no extra signals → mid ≈ fair."""
        s = suggest_offer(
            listing    = _listing(price=400_000, days_on_market=10),
            fair_value = 400_000,
        )
        assert s.factors == []
        assert s.total_discount_pct == 0.0
        assert s.suggested_mid == 400_000

    def test_full_buyer_leverage_combines(self):
        listing = _listing(
            price          = 400_000,
            days_on_market = 200,
            num_drops      = 3,
            total_drop_pct = 8.0,
            seller_type    = "Particular",
        )
        ph = SimpleNamespace(republication_count=2)
        s = suggest_offer(
            listing          = listing,
            fair_value       = 400_000,
            fair_confidence  = "high",
            property_history = ph,
            nlp_signals      = {"negotiable": True},
        )
        # At least 5 factors fire.
        assert len(s.factors) >= 5
        # Mid is meaningfully below fair.
        assert s.suggested_mid < 400_000
        # Total discount never exceeds the global cap.
        assert s.total_discount_pct >= -MAX_DISCOUNT_PCT

    def test_total_discount_capped_at_max(self):
        """Synthetic extreme listing — every factor maxed out — caps total."""
        listing = _listing(
            price          = 1_000_000,
            days_on_market = 10 * 365,
            num_drops      = 50,
            total_drop_pct = 100.0,
            seller_type    = "Particular",
        )
        ph = SimpleNamespace(republication_count=99)
        s = suggest_offer(
            listing          = listing,
            fair_value       = 500_000,                # also overpriced 2x
            property_history = ph,
            nlp_signals      = {"negotiable": True, "urgency": True, "needs_work": True},
        )
        # All factor caps + global cap.
        assert s.total_discount_pct == -MAX_DISCOUNT_PCT
        assert s.suggested_mid == int(round(500_000 * (1 - MAX_DISCOUNT_PCT / 100)))

    def test_suggested_high_never_above_fair_value(self):
        s = suggest_offer(
            listing    = _listing(price=400_000, days_on_market=10),
            fair_value = 400_000,
        )
        assert s.suggested_high <= s.fair_value

    def test_band_width_shrinks_with_confidence(self):
        l = _listing(price=400_000, days_on_market=200, num_drops=3, total_drop_pct=10)
        s_high = suggest_offer(l, 400_000, fair_confidence="high")
        s_low  = suggest_offer(l, 400_000, fair_confidence="low")
        # Wider band on lower confidence.
        spread_high = s_high.suggested_high - s_high.suggested_low
        spread_low  = s_low.suggested_high - s_low.suggested_low
        assert spread_low > spread_high

    def test_is_above_fair_value(self):
        s = suggest_offer(_listing(price=450_000), 400_000)
        assert s.is_above_fair_value is True
        s = suggest_offer(_listing(price=380_000), 400_000)
        assert s.is_above_fair_value is False

    def test_discount_vs_asking_zero_for_neutral(self):
        s = suggest_offer(_listing(price=400_000, days_on_market=10), 400_000)
        assert s.discount_vs_asking_pct == 0.0

    def test_bargain_listing_never_suggests_above_asking(self):
        """Asking below fair (a chollo) — sugerencia never exceeds asking.

        Regression for the bug where an aggressive fair_value (much higher
        than asking) caused the engine to suggest paying more than the
        seller's own price.  The discount must apply to
        ``min(fair, asking)``.
        """
        s = suggest_offer(
            listing    = _listing(price=209_000, days_on_market=118,
                                  num_drops=2, total_drop_pct=7.2,
                                  seller_type="Particular"),
            fair_value = 331_000,                  # 58% above asking
            fair_confidence = "high",
        )
        # Mid never exceeds asking price.
        assert s.suggested_mid    <= 209_000
        assert s.suggested_high   <= 209_000
        assert s.suggested_low    <= s.suggested_mid
        # Discount vs asking is positive (we still suggest below asking).
        assert s.discount_vs_asking_pct > 0

    def test_bargain_listing_applies_leverage_below_asking(self):
        """A chollo with strong leverage signals still gets a meaningful
        below-asking suggestion — buyer leverage doesn't disappear just
        because the listing happens to be mispriced low to begin with."""
        s = suggest_offer(
            listing = _listing(price=200_000, days_on_market=180,
                               num_drops=3, total_drop_pct=10.0,
                               seller_type="Particular"),
            fair_value = 280_000,
        )
        # Expect a non-trivial discount applied even though asking < fair.
        assert s.discount_vs_asking_pct >= 5.0
