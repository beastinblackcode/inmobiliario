"""
Unit tests for barrio_profiles.py — pure logic, no DB.

Covers:
    compute_verdict          — deterministic rules over KPIs
    get_distribution_for_barrio — buckets, rooms, amenities, year
    get_neighbor_barrios     — same-distrito + nearest-by-€/m²
"""

from __future__ import annotations

import pandas as pd
import pytest

from barrio_profiles import (
    compute_verdict,
    get_distribution_for_barrio,
    get_neighbor_barrios,
)

pytestmark = pytest.mark.unit


# ──────────────────────────────────────────────────────────────────────────
# compute_verdict — deterministic rules
# ──────────────────────────────────────────────────────────────────────────


class TestComputeVerdict:
    def test_insufficient_data(self):
        v = compute_verdict(
            kpis={"price_per_sqm": None, "active_count": 0},
            distrito_avg_sqm=5_000,
            madrid_avg_sqm=5_000,
            madrid_avg_days=60,
        )
        assert v["emoji"] == "⚪"
        assert v["label"] == "Datos insuficientes"

    def test_few_listings_returns_insufficient(self):
        # active_count < 5 → "Datos insuficientes" regardless of price
        v = compute_verdict(
            kpis={"price_per_sqm": 5_000, "active_count": 3},
            distrito_avg_sqm=5_000,
            madrid_avg_sqm=5_000,
            madrid_avg_days=60,
        )
        assert v["label"] == "Datos insuficientes"

    def test_tensionado_when_price_well_above_distrito(self):
        v = compute_verdict(
            kpis={
                "price_per_sqm": 7_000,    # ratio 1.40 vs distrito (5000)
                "avg_days_market": 60,
                "pct_with_drops": 5.0,
                "active_count": 50,
            },
            distrito_avg_sqm=5_000,
            madrid_avg_sqm=5_000,
            madrid_avg_days=60,
        )
        assert "tensionado" in v["label"].lower() or "🟡" in v["emoji"]

    def test_infravalorado_when_price_well_below_distrito(self):
        v = compute_verdict(
            kpis={
                "price_per_sqm": 4_000,    # ratio 0.80 vs distrito (5000) — at threshold
                "avg_days_market": 60,
                "pct_with_drops": 5.0,
                "active_count": 50,
            },
            distrito_avg_sqm=5_000,
            madrid_avg_sqm=5_000,
            madrid_avg_days=60,
        )
        # 0.80 ≤ _TENSION_LOW (0.85) → infravalorado
        assert "infravalorado" in v["label"].lower() or v["emoji"] == "🟢"

    def test_equilibrado_when_price_close_to_distrito(self):
        v = compute_verdict(
            kpis={
                "price_per_sqm": 5_000,    # ratio 1.0 — exactly at distrito
                "avg_days_market": 60,
                "pct_with_drops": 5.0,
                "active_count": 30,
            },
            distrito_avg_sqm=5_000,
            madrid_avg_sqm=5_000,
            madrid_avg_days=60,
        )
        # Should be the "Equilibrado" base label (🔵)
        assert v["emoji"] in ("🔵", "🟢", "🟡")  # base label is Equilibrado here
        assert "Equilibrado" in v["label"] or v["emoji"] == "🔵"

    def test_inflexion_bajista_modifier(self):
        # high pct_drops (>= 18%) + slow days (ratio >= 1.20) → bajista
        v = compute_verdict(
            kpis={
                "price_per_sqm": 5_000,
                "avg_days_market": 100,    # ratio 100/60 = 1.66 → SLOW
                "pct_with_drops": 25.0,
                "active_count": 30,
            },
            distrito_avg_sqm=5_000,
            madrid_avg_sqm=5_000,
            madrid_avg_days=60,
        )
        assert "inflexión bajista" in v["label"].lower()
        assert v["emoji"] == "🟠"

    def test_returned_dict_has_all_keys(self):
        v = compute_verdict(
            kpis={
                "price_per_sqm": 5_000,
                "avg_days_market": 60,
                "pct_with_drops": 5.0,
                "active_count": 30,
            },
            distrito_avg_sqm=5_000,
            madrid_avg_sqm=5_000,
            madrid_avg_days=60,
        )
        assert set(v.keys()) == {"label", "emoji", "summary", "recommendation"}
        assert all(isinstance(v[k], str) and v[k] for k in v)


# ──────────────────────────────────────────────────────────────────────────
# get_distribution_for_barrio
# ──────────────────────────────────────────────────────────────────────────


class TestDistribution:
    def test_empty_df_returns_zero_sample(self):
        empty_df = pd.DataFrame(columns=["price", "rooms", "listing_id"])
        d = get_distribution_for_barrio(empty_df, amenities_map={})
        assert d["sample_size"] == 0
        assert d["price_buckets"] == []

    def test_price_buckets_sum_to_total(self):
        df = pd.DataFrame([
            {"listing_id": "A", "price": 150_000, "rooms": 2, "size_sqm": 60},
            {"listing_id": "B", "price": 350_000, "rooms": 3, "size_sqm": 80},
            {"listing_id": "C", "price": 600_000, "rooms": 4, "size_sqm": 100},
            {"listing_id": "D", "price": 1_500_000, "rooms": 5, "size_sqm": 200},
        ])
        d = get_distribution_for_barrio(df, amenities_map={})
        assert sum(b["count"] for b in d["price_buckets"]) == len(df)
        assert d["sample_size"] == 4

    def test_rooms_distribution_categories(self):
        df = pd.DataFrame([
            {"listing_id": "A", "price": 100_000, "rooms": 1, "size_sqm": 40},
            {"listing_id": "B", "price": 200_000, "rooms": 2, "size_sqm": 60},
            {"listing_id": "C", "price": 300_000, "rooms": 3, "size_sqm": 80},
            {"listing_id": "D", "price": 400_000, "rooms": 5, "size_sqm": 120},
        ])
        d = get_distribution_for_barrio(df, amenities_map={})
        assert d["rooms_distribution"] == {"1": 1, "2": 1, "3": 1, "4+": 1}

    def test_amenities_pct_requires_min_3_listings(self):
        df = pd.DataFrame([
            {"listing_id": "A", "price": 100_000, "rooms": 2, "size_sqm": 60},
            {"listing_id": "B", "price": 200_000, "rooms": 3, "size_sqm": 80},
        ])
        amen = {
            "A": {"has_terraza": True,  "construction_year": 2010},
            "B": {"has_terraza": False, "construction_year": 2020},
        }
        d = get_distribution_for_barrio(df, amen)
        # Less than 3 listings with amenities → empty pct
        assert d["amenities_pct"] == {}
        assert d["amenities_sample_size"] == 2

    def test_amenities_pct_computed_with_3_or_more(self):
        df = pd.DataFrame([
            {"listing_id": str(i), "price": 200_000, "rooms": 2, "size_sqm": 70}
            for i in range(4)
        ])
        amen = {
            "0": {"has_terraza": True,  "has_garaje": False, "construction_year": 2010},
            "1": {"has_terraza": True,  "has_garaje": True,  "construction_year": 2015},
            "2": {"has_terraza": False, "has_garaje": True,  "construction_year": 2020},
            "3": {"has_terraza": True,  "has_garaje": False, "construction_year": 1995},
        }
        d = get_distribution_for_barrio(df, amen)
        assert d["amenities_pct"]["has_terraza"] == 75.0   # 3/4
        assert d["amenities_pct"]["has_garaje"]  == 50.0   # 2/4
        assert d["median_construction_year"] in (2010, 2012, 2015)  # exact median can vary by parity


# ──────────────────────────────────────────────────────────────────────────
# get_neighbor_barrios
# ──────────────────────────────────────────────────────────────────────────


class TestNeighborBarrios:
    def _summaries(self):
        return [
            {"barrio": "Sol",        "distrito": "Centro",    "price_per_sqm": 6_000, "median_price": 400_000, "active_count": 100},
            {"barrio": "Justicia",   "distrito": "Centro",    "price_per_sqm": 6_500, "median_price": 450_000, "active_count": 80},
            {"barrio": "Embajadores","distrito": "Centro",    "price_per_sqm": 5_200, "median_price": 350_000, "active_count": 90},
            {"barrio": "Goya",       "distrito": "Salamanca", "price_per_sqm": 8_500, "median_price": 800_000, "active_count": 70},
            {"barrio": "Recoletos",  "distrito": "Salamanca", "price_per_sqm": 9_200, "median_price": 950_000, "active_count": 60},
        ]

    def test_returns_same_distrito_first(self):
        out = get_neighbor_barrios(
            target_barrio="Sol",
            target_distrito="Centro",
            target_ppsqm=6_000,
            all_summaries=self._summaries(),
            n=3,
        )
        # Same-distrito barrios should appear first
        assert out[0]["distrito"] == "Centro"
        assert "Sol" not in [s["barrio"] for s in out]   # never returns the target itself

    def test_excludes_target(self):
        out = get_neighbor_barrios(
            target_barrio="Sol",
            target_distrito="Centro",
            target_ppsqm=6_000,
            all_summaries=self._summaries(),
            n=10,
        )
        names = [s["barrio"] for s in out]
        assert "Sol" not in names

    def test_diff_pct_computed(self):
        out = get_neighbor_barrios(
            target_barrio="Sol",
            target_distrito="Centro",
            target_ppsqm=6_000,
            all_summaries=self._summaries(),
            n=2,
        )
        for s in out:
            if s["price_per_sqm"]:
                # diff_pct must reflect (neighbour / target - 1) * 100
                expected = round((s["price_per_sqm"] / 6_000 - 1) * 100, 1)
                assert s["diff_pct"] == expected

    def test_empty_summaries_returns_empty(self):
        assert get_neighbor_barrios("Sol", "Centro", 6_000, [], n=5) == []
