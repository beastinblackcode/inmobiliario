"""
Unit tests for the barrio-context features added to the RF price model
(ROADMAP §2.3 "Features adicionales al RF").

Covers the pure feature-engineering helpers (no DB / no model fit):
  - coordinates.distance_to_sol
  - PricePredictor._compute_barrio_context  (median €/m², supply, velocity)
  - PricePredictor._lookup_barrio           (fallback for unseen barrios)
  - PricePredictor._attach_barrio_features  (vectorised attach)
"""

from __future__ import annotations

import pandas as pd
import pytest

pytestmark = pytest.mark.unit


# ── distance_to_sol ─────────────────────────────────────────────────────────

def test_distance_to_sol_is_zero_at_sol():
    from coordinates import distance_to_sol
    # Centro/Sol's centroid is Puerta del Sol itself.
    assert distance_to_sol("Centro", "Sol") == pytest.approx(0.0, abs=0.01)


def test_distance_to_sol_known_barrio_is_positive_and_plausible():
    from coordinates import distance_to_sol
    d = distance_to_sol("Salamanca", "Goya")
    assert d is not None
    # Goya is ~3 km from Sol — sanity bounds, not an exact geodesic.
    assert 1.0 < d < 6.0


def test_distance_to_sol_unknown_barrio_is_none():
    from coordinates import distance_to_sol
    # Unknown barrio must return None (NOT silently fall back to city centre,
    # which would wrongly read as distance 0).
    assert distance_to_sol("Nonexistent", "Nowhere") is None


# ── barrio context aggregation ──────────────────────────────────────────────

def _predictor():
    from predictive_model import PricePredictor
    return PricePredictor()


def _ctx_frame() -> pd.DataFrame:
    """Synthetic cleaned frame: the columns _compute_barrio_context needs."""
    return pd.DataFrame([
        # Barrio A: 2 active (€/m² 4000, 6000 → median 5000), 1 sold @ 10 days
        {"distrito": "D1", "barrio": "A", "price_sqm": 4000, "status": "active",       "days_on_market": None},
        {"distrito": "D1", "barrio": "A", "price_sqm": 6000, "status": "active",       "days_on_market": None},
        {"distrito": "D1", "barrio": "A", "price_sqm": 5000, "status": "sold_removed", "days_on_market": 10},
        # Barrio B: 1 active (€/m² 3000), no sold → velocity None
        {"distrito": "D1", "barrio": "B", "price_sqm": 3000, "status": "active",       "days_on_market": None},
    ])


def test_compute_barrio_context_values():
    p = _predictor()
    ctx, default = p._compute_barrio_context(_ctx_frame())

    a = ctx[p._barrio_key("D1", "A")]
    assert a["median_sqm"] == pytest.approx(5000.0)
    assert a["supply"] == 2          # two active listings
    assert a["velocity"] == 10.0     # single sold @ 10 days

    b = ctx[p._barrio_key("D1", "B")]
    assert b["supply"] == 1
    assert b["velocity"] is None     # no sold listings in barrio B

    # City defaults
    assert default["median_sqm"] == pytest.approx(4500.0)  # median of 4000,6000,5000,3000
    assert default["velocity"] == 10.0                     # only sold row


def test_lookup_barrio_uses_default_for_missing_velocity_and_unseen():
    p = _predictor()
    p.barrio_context, p._barrio_default = p._compute_barrio_context(_ctx_frame())

    # Barrio B has no velocity → falls back to the city default (10.0).
    b = p._lookup_barrio("D1", "B")
    assert b["barrio_supply"] == 1
    assert b["barrio_velocity"] == 10.0

    # Completely unseen barrio → all context comes from the default.
    unseen = p._lookup_barrio("ZZ", "Unknown")
    assert unseen["barrio_median_sqm"] == pytest.approx(4500.0)
    assert unseen["barrio_supply"] == p._barrio_default["supply"]
    # Unknown barrio has no coordinates → distance is None (imputed downstream).
    assert unseen["barrio_dist_sol"] is None


def test_attach_barrio_features_adds_all_columns():
    p = _predictor()
    df = _ctx_frame()
    p.barrio_context, p._barrio_default = p._compute_barrio_context(df)
    p._attach_barrio_features(df)

    for col in ("barrio_median_sqm", "barrio_dist_sol", "barrio_supply", "barrio_velocity"):
        assert col in df.columns
    # Barrio A rows carry A's median €/m².
    a_rows = df[df["barrio"] == "A"]
    assert (a_rows["barrio_median_sqm"] == 5000.0).all()
