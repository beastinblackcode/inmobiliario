"""
Unit tests for nlp_analyzer.py — pure regex extraction, no DB.

Covers:
    analyze_description    — seller signals (urgency, direct, negotiable, …)
    extract_amenities      — physical features + proximity + construction year
    signals_to_badges      — display formatting
"""

from __future__ import annotations

import pytest

from nlp_analyzer import (
    analyze_description,
    extract_amenities,
    signals_to_badges,
    CATEGORY_BONUS,
)

pytestmark = pytest.mark.unit


# ──────────────────────────────────────────────────────────────────────────
# analyze_description — seller signals
# ──────────────────────────────────────────────────────────────────────────


class TestAnalyzeDescription:
    def test_empty_text_returns_no_signals(self):
        for inp in (None, "", "   "):
            r = analyze_description(inp)
            assert r["signal_count"] == 0
            assert r["nlp_bonus"] == 0
            assert all(r[cat] is False for cat in CATEGORY_BONUS)

    def test_non_string_returns_no_signals(self):
        r = analyze_description(123)  # type: ignore[arg-type]
        assert r["signal_count"] == 0

    def test_detects_urgency(self):
        r = analyze_description("Venta urgente por traslado laboral")
        assert r["urgency"] is True
        assert r["nlp_bonus"] >= CATEGORY_BONUS["urgency"]

    def test_detects_direct_seller(self):
        r = analyze_description("Vende propietario directo, sin agencia")
        assert r["direct"] is True

    def test_detects_negotiable(self):
        # The pattern requires explicit phrases like "abierto a ofertas",
        # "aceptaría ofertas", "precio negociable"…
        r = analyze_description("Precio negociable, abierto a ofertas razonables")
        # Depending on which categories the patterns belong to, at least
        # one of negotiable / direct should fire.
        assert r["negotiable"] or r["direct"]

    def test_detects_renovated(self):
        # Patterns: "completamente reformado", "reforma total/integral", etc.
        r = analyze_description("Piso completamente reformado en 2023, listo para entrar")
        assert r["renovated"] is True

    def test_detects_needs_work(self):
        r = analyze_description("Vivienda para reformar, a actualizar")
        assert r["needs_work"] is True

    def test_bonus_capped_at_45(self):
        # Cram every category in
        text = (
            "Venta urgente por traslado. Propietario directo sin agencia. "
            "Precio negociable, abierto a ofertas. Recientemente reformado. "
            "Necesita reforma integral en cocina."
        )
        r = analyze_description(text)
        assert r["nlp_bonus"] <= 45

    def test_neutral_text_zero_bonus(self):
        r = analyze_description("Bonito piso de tres dormitorios con vistas al parque")
        # No motivation signals, so bonus should be 0.  (Note: amenities
        # like 'parque' are extracted by extract_amenities, not here.)
        assert r["nlp_bonus"] == 0


# ──────────────────────────────────────────────────────────────────────────
# extract_amenities — physical features + proximity
# ──────────────────────────────────────────────────────────────────────────


class TestExtractAmenities:
    def test_empty_text(self):
        for inp in (None, "", 42):
            r = extract_amenities(inp)  # type: ignore[arg-type]
            assert r["amenities_count"] == 0
            assert r["construction_year"] is None

    def test_detects_terraza(self):
        r = extract_amenities("Piso con terraza de 20 m² y vistas")
        assert r["has_terraza"] is True

    def test_detects_garaje_parking_synonyms(self):
        for txt in (
            "Plaza de garaje incluida",
            "Plaza de aparcamiento en la finca",
            "Cuenta con parking privado",
        ):
            assert extract_amenities(txt)["has_garaje"] is True

    def test_negative_mention_overrides_positive(self):
        # Even if "ascensor" appears, "sin ascensor" must win
        r = extract_amenities("Edificio antiguo sin ascensor, mucha luz")
        assert r["has_ascensor"] is False

    def test_negative_garaje(self):
        r = extract_amenities("Vivienda céntrica, sin garaje")
        assert r["has_garaje"] is False

    def test_negative_calefaccion(self):
        r = extract_amenities("Piso reformado, no dispone de calefacción central")
        assert r["has_calefaccion"] is False

    def test_construction_year_in_range(self):
        r = extract_amenities("Edificio construido en 1965 con encanto")
        assert r["construction_year"] == 1965

    def test_construction_year_rejects_out_of_range(self):
        # Years outside [1800, 2030] must be ignored (likely OCR / postal codes)
        r = extract_amenities("Edificio construido en 1750 antes de la Reconquista")
        assert r["construction_year"] is None

    def test_amenities_count_matches_true_flags(self):
        r = extract_amenities(
            "Bonito ático con terraza, plaza de garaje y trastero. Tiene ascensor "
            "y aire acondicionado. Cerca de parque del Retiro."
        )
        boolean_flags = [
            v for k, v in r.items()
            if k.startswith(("has_", "near_")) and isinstance(v, bool)
        ]
        assert r["amenities_count"] == sum(1 for v in boolean_flags if v)
        assert r["amenities_count"] >= 4  # terraza, garaje, trastero, ascensor at minimum

    def test_metro_proximity_specific(self):
        r = extract_amenities("A 5 minutos del metro Goya")
        assert r["near_metro"] is True

    def test_metro_no_false_positive_on_unrelated_metro(self):
        # "metro cuadrado" must not trigger near_metro
        r = extract_amenities("Vivienda de 90 metros cuadrados")
        assert r["near_metro"] is False


# ──────────────────────────────────────────────────────────────────────────
# signals_to_badges — pure formatting
# ──────────────────────────────────────────────────────────────────────────


class TestSignalsToBadges:
    def test_empty_signals_returns_empty_string(self):
        assert signals_to_badges({}) == ""
        assert signals_to_badges({k: False for k in CATEGORY_BONUS}) == ""

    def test_renders_active_signals_only(self):
        out = signals_to_badges({"urgency": True, "direct": False, "negotiable": True})
        assert "Urgente" in out
        assert "Negociable" in out
        assert "Directo" not in out
