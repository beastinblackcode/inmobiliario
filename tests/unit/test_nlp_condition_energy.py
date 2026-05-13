"""
Unit tests for the v2 NLP extractors: ``construction_year`` (improved
patterns), ``energy_certification`` (new), ``condition`` (new).

Pure functions, no DB.  The DB write/read path is exercised
end-to-end by the existing integration tests.
"""

from __future__ import annotations

import pytest

from nlp_analyzer import (
    extract_amenities,
    _extract_condition,
    _extract_energy_certification,
)


# ──────────────────────────────────────────────────────────────────────
# Construction year — extended patterns
# ──────────────────────────────────────────────────────────────────────


class TestConstructionYear:
    """Anchor patterns + the new ones added in this PR."""

    @pytest.mark.parametrize("text, expected", [
        ("Edificio construido en 1985, fachada original.", 1985),
        ("Año construcción: 1965.", 1965),
        ("Fecha de construcción 2010.", 2010),
        ("Edificio del año 1972.", 1972),
        ("Edificio de 1965, sin obras.", 1965),
        ("Vivienda del 2018, lista para entrar.", 2018),
        ("Promoción del año 2021.", 2021),
        # NEW pattern: "del año NNNN" stand-alone
        ("El piso es del año 1958.", 1958),
        # NEW pattern: "promoción NNNN"
        ("Promoción de 2015 con todas las garantías.", 2015),
        # NEW pattern: ``piso del`` / ``inmueble del``
        ("Inmueble del año 2008.", 2008),
    ])
    def test_year_extracted(self, text, expected):
        result = extract_amenities(text)
        assert result["construction_year"] == expected

    def test_year_out_of_range_ignored(self):
        # 3000 shouldn't slip through even if pattern matches.
        result = extract_amenities("Edificio del año 3000.")
        assert result["construction_year"] is None

    def test_no_year_returns_none(self):
        result = extract_amenities("Piso reformado con dos baños.")
        assert result["construction_year"] is None


# ──────────────────────────────────────────────────────────────────────
# Energy certification — letter A-G + special states
# ──────────────────────────────────────────────────────────────────────


class TestEnergyCertification:
    @pytest.mark.parametrize("text, expected", [
        ("Certificación energética: B.",                  "B"),
        ("Calificación energética C.",                    "C"),
        ("Etiqueta energética: A",                        "A"),
        ("Consumo energético: D (98 kWh/m²).",            "D"),
        ("Clasificación energética E.",                   "E"),
        # Labelled form via ``energético``
        ("Energético: F",                                 "F"),
        # Parens
        ("Certificado energético: (G)",                   "G"),
    ])
    def test_letter_grades(self, text, expected):
        assert _extract_energy_certification(text) == expected

    def test_exento(self):
        assert _extract_energy_certification(
            "Certificación energética: exento por antigüedad.") == "exento"

    def test_en_tramite(self):
        assert _extract_energy_certification(
            "Certificación energética en trámite, se entregará en breve."
        ) == "en_tramite"

    def test_no_signal_returns_none(self):
        assert _extract_energy_certification(
            "Piso luminoso con terraza."
        ) is None

    def test_bare_letter_not_matched(self):
        """A standalone "B" without the energy label should NOT match —
        otherwise floor numbers ("planta B") would be misread."""
        assert _extract_energy_certification("Planta B con ascensor.") is None


# ──────────────────────────────────────────────────────────────────────
# Condition — categorical 5-way classification
# ──────────────────────────────────────────────────────────────────────


class TestConditionCategory:
    @pytest.mark.parametrize("text, expected", [
        ("Obra nueva con todas las garantías.",                  "obra_nueva"),
        ("Vivienda nueva, lista para entrar.",                   "obra_nueva"),
        ("Piso a estrenar en zona Madrid Río.",                  "obra_nueva"),
        ("Totalmente reformado en 2020.",                        "reformado"),
        ("Piso recientemente reformado, calidades premium.",     "reformado"),
        ("Vivienda reformada con materiales modernos.",          "reformado"),
        ("Buen estado, no necesita reforma.",                    "buen_estado"),
        ("En perfecto estado, para entrar a vivir.",             "buen_estado"),
        ("A reformar a tu gusto, oportunidad de inversión.",     "a_reformar"),
        ("Piso para reformar — buena ubicación.",                "a_reformar"),
        ("Vivienda con reforma integral pendiente.",             "para_reformar"),
        ("Reforma completa pendiente.",                          "para_reformar"),
    ])
    def test_categories(self, text, expected):
        assert _extract_condition(text) == expected

    def test_priority_obra_nueva_over_a_reformar(self):
        # If both phrasings appear, the first hit (priority order in
        # ``_CONDITION_PATTERNS``) wins.  Obra nueva is at the top.
        text = "Obra nueva — antes era piso a reformar."
        assert _extract_condition(text) == "obra_nueva"

    def test_priority_para_reformar_over_a_reformar(self):
        # Reforma integral > reforma simple in the priority table.
        text = "Necesita reforma integral; básicamente para reformar."
        assert _extract_condition(text) == "para_reformar"

    def test_no_match_returns_none(self):
        assert _extract_condition("Piso con tres dormitorios.") is None


# ──────────────────────────────────────────────────────────────────────
# Integration: extract_amenities returns all v2 fields together
# ──────────────────────────────────────────────────────────────────────


class TestExtractAmenitiesV2Integration:
    def test_all_fields_populated_when_present(self):
        text = (
            "Magnífica obra nueva del año 2022 en Chamartín. "
            "Certificación energética: A. Edificio con piscina y ascensor."
        )
        out = extract_amenities(text)
        assert out["construction_year"]    == 2022
        assert out["energy_certification"] == "A"
        assert out["condition"]            == "obra_nueva"
        assert out["has_piscina"]          is True
        assert out["has_ascensor"]         is True

    def test_partial_extraction_other_fields_none(self):
        text = "Piso con tres habitaciones y dos baños."
        out = extract_amenities(text)
        assert out["construction_year"]    is None
        assert out["energy_certification"] is None
        assert out["condition"]            is None

    def test_empty_description_safe(self):
        out = extract_amenities("")
        assert out["construction_year"]    is None
        assert out["energy_certification"] is None
        assert out["condition"]            is None
        assert out["amenities_count"]      == 0


# ──────────────────────────────────────────────────────────────────────
# Offer engine: new factors fire correctly
# ──────────────────────────────────────────────────────────────────────


class TestOfferEngineV2Factors:
    def _listing(self, **overrides):
        base = {
            "price":          400_000,
            "days_on_market":      10,
            "num_drops":            0,
            "total_drop_pct":     0.0,
            "seller_type": "Agencia",
        }
        base.update(overrides)
        return base

    def test_para_reformar_adds_discount(self):
        from offer_engine import suggest_offer
        s = suggest_offer(
            listing       = self._listing(),
            fair_value    = 400_000,
            nlp_amenities = {"condition": "para_reformar"},
        )
        assert any(f.label == "Estado de la propiedad" for f in s.factors)
        # -5% per the table.
        cond_factor = next(f for f in s.factors if f.label == "Estado de la propiedad")
        assert cond_factor.discount_pct == -5.0

    def test_reformado_doesnt_add_factor(self):
        """Reformado is neutral — no factor row."""
        from offer_engine import suggest_offer
        s = suggest_offer(
            listing       = self._listing(),
            fair_value    = 400_000,
            nlp_amenities = {"condition": "reformado"},
        )
        assert not any(f.label == "Estado de la propiedad" for f in s.factors)

    def test_energy_g_adds_discount(self):
        from offer_engine import suggest_offer
        s = suggest_offer(
            listing       = self._listing(),
            fair_value    = 400_000,
            nlp_amenities = {"energy_certification": "G"},
        )
        cert_factor = next(
            f for f in s.factors if f.label == "Certificación energética"
        )
        assert cert_factor.discount_pct == -2.0

    def test_energy_a_adds_positive(self):
        """A class → less leverage (smaller positive discount, not negative)."""
        from offer_engine import suggest_offer
        s = suggest_offer(
            listing       = self._listing(),
            fair_value    = 400_000,
            nlp_amenities = {"energy_certification": "A"},
        )
        cert_factor = next(
            f for f in s.factors if f.label == "Certificación energética"
        )
        assert cert_factor.discount_pct > 0

    def test_old_construction_year_adds_discount(self):
        from offer_engine import suggest_offer
        s = suggest_offer(
            listing       = self._listing(),
            fair_value    = 400_000,
            nlp_amenities = {"construction_year": 1920},
        )
        age_factor = next(
            f for f in s.factors if f.label == "Antigüedad del edificio"
        )
        assert age_factor.discount_pct < 0

    def test_new_construction_year_adds_positive(self):
        from offer_engine import suggest_offer
        s = suggest_offer(
            listing       = self._listing(),
            fair_value    = 400_000,
            nlp_amenities = {"construction_year": 2022},
        )
        age_factor = next(
            f for f in s.factors if f.label == "Antigüedad del edificio"
        )
        assert age_factor.discount_pct > 0

    def test_no_amenities_skip_factors(self):
        from offer_engine import suggest_offer
        s = suggest_offer(
            listing       = self._listing(),
            fair_value    = 400_000,
            nlp_amenities = None,
        )
        labels = {f.label for f in s.factors}
        assert "Estado de la propiedad"      not in labels
        assert "Certificación energética"    not in labels
        assert "Antigüedad del edificio"     not in labels
