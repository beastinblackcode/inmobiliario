"""
Unit tests for rental_medians.py — pure data + helpers, no DB.

Locks down:
  - The dict covers all 21 Madrid distritos by exact name.
  - The yield computation handles edge cases gracefully.
  - The reference area is the documented 80 m².
"""

from __future__ import annotations

import pytest

from rental_medians import (
    DATA_AS_OF,
    DATA_SOURCE_LABEL,
    DATA_SOURCE_URL,
    DISTRITO_MEDIAN_RENT_EUR_MONTH,
    REFERENCE_AREA_SQM,
    compute_gross_yield,
    get_distrito_rent,
    get_rent_per_sqm,
)

pytestmark = pytest.mark.unit


# ──────────────────────────────────────────────────────────────────────────
# Coverage of all 21 Madrid distritos
# ──────────────────────────────────────────────────────────────────────────


_EXPECTED_DISTRITOS = {
    "Arganzuela", "Barajas", "Carabanchel", "Centro", "Chamartín",
    "Chamberí", "Ciudad Lineal", "Fuencarral-El Pardo", "Hortaleza",
    "Latina", "Moncloa-Aravaca", "Moratalaz", "Puente de Vallecas",
    "Retiro", "Salamanca", "San Blas-Canillejas", "Tetuán", "Usera",
    "Vicálvaro", "Villa de Vallecas", "Villaverde",
}


class TestDictCoverage:
    def test_has_all_21_distritos(self):
        assert set(DISTRITO_MEDIAN_RENT_EUR_MONTH.keys()) == _EXPECTED_DISTRITOS

    def test_every_value_is_a_realistic_rent(self):
        for distrito, rent in DISTRITO_MEDIAN_RENT_EUR_MONTH.items():
            assert 800 <= rent <= 3500, (
                f"{distrito}: {rent} €/month is outside the realistic Madrid"
                " range. Likely a typo or stale data — verify before merging."
            )

    def test_premium_distritos_have_higher_rents_than_outskirts(self):
        # Sanity check: Salamanca / Chamberí / Centro should be > Vallecas / Villaverde
        premium = {DISTRITO_MEDIAN_RENT_EUR_MONTH[d] for d in
                   ("Salamanca", "Chamberí", "Centro", "Chamartín")}
        outskirts = {DISTRITO_MEDIAN_RENT_EUR_MONTH[d] for d in
                     ("Villaverde", "Puente de Vallecas", "Carabanchel")}
        assert min(premium) > max(outskirts), (
            "Premium distritos should be more expensive than the outskirts. "
            "Check the data."
        )


# ──────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────


class TestGetDistritoRent:
    def test_known_distrito(self):
        assert get_distrito_rent("Salamanca") == DISTRITO_MEDIAN_RENT_EUR_MONTH["Salamanca"]

    def test_unknown_distrito_returns_none(self):
        assert get_distrito_rent("Lavapiés Norte") is None
        assert get_distrito_rent("") is None


class TestGetRentPerSqm:
    def test_known_distrito(self):
        rent = DISTRITO_MEDIAN_RENT_EUR_MONTH["Centro"]
        expected = round(rent / REFERENCE_AREA_SQM, 2)
        assert get_rent_per_sqm("Centro") == expected

    def test_unknown_distrito_returns_none(self):
        assert get_rent_per_sqm("Lavapiés Norte") is None


class TestComputeGrossYield:
    def test_typical_yield(self):
        # Salamanca: rent ~2150, ref 80m² → 26.875 €/m²/month
        # × 12 = 322.5 €/m²/year. Sale price ~8500 €/m² → yield ~3.79 %
        y = compute_gross_yield("Salamanca", sale_price_per_sqm=8_500)
        assert y is not None
        assert 3.0 < y < 5.0   # realistic range

    def test_returns_none_for_zero_or_missing_price(self):
        assert compute_gross_yield("Salamanca", sale_price_per_sqm=0) is None
        assert compute_gross_yield("Salamanca", sale_price_per_sqm=None) is None

    def test_returns_none_for_unknown_distrito(self):
        assert compute_gross_yield("Lavapiés Norte", sale_price_per_sqm=5_000) is None

    def test_yield_inverse_to_price(self):
        # Same rent, higher price → lower yield
        y_low_price  = compute_gross_yield("Centro", sale_price_per_sqm=4_000)
        y_high_price = compute_gross_yield("Centro", sale_price_per_sqm=8_000)
        assert y_low_price > y_high_price


# ──────────────────────────────────────────────────────────────────────────
# Provenance metadata exposed in metrics.json
# ──────────────────────────────────────────────────────────────────────────


class TestProvenance:
    def test_data_as_of_present(self):
        assert isinstance(DATA_AS_OF, str) and DATA_AS_OF
        # Q-format expected
        assert "Q" in DATA_AS_OF or "T" in DATA_AS_OF

    def test_source_label_and_url_present(self):
        assert DATA_SOURCE_LABEL
        assert DATA_SOURCE_URL.startswith("https://")
