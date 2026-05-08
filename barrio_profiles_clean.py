"""
Phase-2 clean-room builder for barrios_profiles.json.

Replaces the legacy ``barrio_profiles.build_all_barrio_profiles`` (which
read the listings table and is now retired) with an exporter that uses
**only public, official, non-listing-derived sources**:

  - ``notarial_prices``                      → distrito €/m² (CIEN)
  - ``market-thermometer/public/district_opendata.json``
                                             → per-barrio renta media
                                               (Open Data Madrid)
  - ``coordinates.BARRIO_COORDINATES``       → canonical 139 barrios

Schema (must match ``lib/barrioProfilesShared.ts`` in the front)::

    {
      "metadata":        { generated_at, version, barrio_count, schema_notes },
      "madrid_baseline": { median_price_per_sqm, median_renta_hogar },
      "profiles": {
        "<barrio>": {
          "barrio":   str,
          "distrito": str,
          "kpis":     { price_per_sqm, vs_distrito_pct, vs_madrid_pct },
          "verdict":  { label, emoji, summary, recommendation },
          "neighbours": [ { barrio, distrito, price_per_sqm, diff_pct }, ... ]
        }
      }
    }

Limitations
-----------

Notarial CIEN releases €/m² at distrito granularity, so every barrio in
a distrito gets the same proxy ``price_per_sqm``.  That makes
``vs_distrito_pct`` always 0 and ``vs_madrid_pct`` constant per
distrito.  We surface ``vs_distrito_pct`` as ``None`` (the front hides
the pill in that case) and let the verdict differentiate barrios by
crossing the distrito-level price tier with the **per-barrio income
tier** from Open Data Madrid.

Entry point
-----------

``build_clean_barrio_profiles()`` returns the full dict ready for
``json.dumps``.  The CLI wrapper lives in ``export_barrio_profiles_clean.py``.
"""

from __future__ import annotations

import json
import os
import sqlite3
import statistics
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

# ──────────────────────────────────────────────────────────────────────
# Tier thresholds — band around Madrid median
# ──────────────────────────────────────────────────────────────────────

# Below 70 % of Madrid median → "low"; above 130 % → "high"; else "mid".
# Empirically these align with the 25th/75th percentile both for
# household income (Open Data) and for distrito €/m² (Notarial).
_LOW_BAND  = 0.70
_HIGH_BAND = 1.30


def _tier(value: Optional[float], median: Optional[float]) -> str:
    """Bucket *value* relative to *median*: returns 'low' | 'mid' | 'high'.

    Falls back to 'mid' when either side is missing so the verdict
    function never raises.
    """
    if not value or not median or median <= 0:
        return "mid"
    ratio = value / median
    if ratio <= _LOW_BAND:
        return "low"
    if ratio >= _HIGH_BAND:
        return "high"
    return "mid"


# ──────────────────────────────────────────────────────────────────────
# Verdict matrix — cross income tier × price tier
# ──────────────────────────────────────────────────────────────────────

# Each cell is (label, emoji, summary_template, recommendation).
# Summary template uses placeholders {price_pct} (€/m² vs Madrid) and
# {income_pct} (renta vs Madrid).
_VERDICT_MATRIX: Dict[Tuple[str, str], Dict[str, str]] = {
    # income tier, price tier
    ("high", "high"): {
        "label":  "Premium consolidado",
        "emoji":  "🟦",
        "summary": (
            "Renta y precios alineados muy por encima de la media: zona "
            "consolidada de poder adquisitivo alto donde el precio refleja "
            "la demanda estructural."
        ),
        "recommendation": (
            "Actuar sólo con margen claro sobre la mediana del distrito o "
            "por motivos de ubicación — no esperes descuentos significativos."
        ),
    },
    ("high", "mid"): {
        "label":  "Renta alta · precios moderados",
        "emoji":  "🟢",
        "summary": (
            "Renta por encima de la media de Madrid pero precios en la "
            "horquilla central. Combinación poco habitual."
        ),
        "recommendation": (
            "Buen perfil para inversión a medio plazo: la renta sostiene "
            "la demanda y deja margen de revalorización."
        ),
    },
    ("high", "low"): {
        "label":  "Oportunidad relativa",
        "emoji":  "🟢",
        "summary": (
            "Renta media por encima de Madrid con precios todavía por "
            "debajo de la media. Posible infravaloración."
        ),
        "recommendation": (
            "Vigilar evolución 6-12 meses — si la dinámica de demanda se "
            "mantiene, los precios tienden a converger al alza."
        ),
    },
    ("mid", "high"): {
        "label":  "Tensión por precio",
        "emoji":  "🟠",
        "summary": (
            "Precios por encima de la media de Madrid sin que la renta "
            "del barrio acompañe. Esfuerzo de compra elevado para residentes."
        ),
        "recommendation": (
            "Negociar a la baja es realista — el ratio precio/renta limita "
            "el pool de compradores locales."
        ),
    },
    ("mid", "mid"): {
        "label":  "Equilibrado",
        "emoji":  "🔵",
        "summary": (
            "Renta y precios en la franja central de Madrid. Mercado sin "
            "tensiones particulares."
        ),
        "recommendation": (
            "Decisión de compra en función de la propiedad concreta y el "
            "score de calidad — no de la dinámica del barrio."
        ),
    },
    ("mid", "low"): {
        "label":  "Asequible · renta media",
        "emoji":  "🟢",
        "summary": (
            "Precios por debajo de la media de Madrid con renta en la "
            "horquilla central. Accesibilidad razonable."
        ),
        "recommendation": (
            "Buen perfil para primera vivienda con financiación: mensualidad "
            "asumible para rentas medias."
        ),
    },
    ("low", "high"): {
        "label":  "Tensionado · gentrificación",
        "emoji":  "🔴",
        "summary": (
            "Precios muy por encima de la renta media del barrio. Síntoma "
            "típico de gentrificación o presión turística."
        ),
        "recommendation": (
            "Dinámica volátil — analizar si la subida de precios es "
            "sostenible antes de comprar a estos niveles."
        ),
    },
    ("low", "mid"): {
        "label":  "Asequibilidad ajustada",
        "emoji":  "🟠",
        "summary": (
            "Precios en la franja central pero renta del barrio por debajo "
            "de la media. Esfuerzo de compra elevado para residentes."
        ),
        "recommendation": (
            "Posible margen de negociación — el mercado local tiene poca "
            "capacidad de pago a estos precios."
        ),
    },
    ("low", "low"): {
        "label":  "Asequible",
        "emoji":  "🟢",
        "summary": (
            "Precios y renta por debajo de la media de Madrid: zona "
            "tradicional con accesibilidad clara."
        ),
        "recommendation": (
            "Foco en la calidad del inmueble y los servicios del barrio, "
            "más que en la negociación del precio."
        ),
    },
}


def _build_verdict(
    barrio_income: Optional[float],
    distrito_ppsqm: Optional[float],
    madrid_income: Optional[float],
    madrid_ppsqm: Optional[float],
) -> Dict[str, str]:
    """Pick the verdict cell + interpolate ``%`` deltas in the summary."""
    if not barrio_income or not distrito_ppsqm:
        return {
            "label":          "Datos insuficientes",
            "emoji":          "⚪",
            "summary":        "Faltan datos oficiales para emitir un veredicto fiable.",
            "recommendation": "Consulta los barrios cercanos del mismo distrito.",
        }

    income_tier = _tier(barrio_income, madrid_income)
    price_tier  = _tier(distrito_ppsqm, madrid_ppsqm)
    cell = _VERDICT_MATRIX[(income_tier, price_tier)].copy()

    # Append the magnitude in parentheses for transparency, only when
    # the deltas are big enough to matter (>5 %).
    extras: List[str] = []
    if madrid_ppsqm and distrito_ppsqm:
        d_price = (distrito_ppsqm / madrid_ppsqm - 1) * 100
        if abs(d_price) >= 5:
            sign = "+" if d_price > 0 else ""
            extras.append(f"€/m² {sign}{d_price:.0f}% vs Madrid")
    if madrid_income and barrio_income:
        d_income = (barrio_income / madrid_income - 1) * 100
        if abs(d_income) >= 5:
            sign = "+" if d_income > 0 else ""
            extras.append(f"renta {sign}{d_income:.0f}% vs Madrid")
    if extras:
        cell["summary"] = cell["summary"] + " (" + " · ".join(extras) + ")"

    return cell


# ──────────────────────────────────────────────────────────────────────
# Neighbours — 5 barrios closest by €/m², excluding same distrito
# ──────────────────────────────────────────────────────────────────────


def _pick_neighbours(
    target_distrito: str,
    target_ppsqm: Optional[float],
    all_barrios: List[Dict],
    n: int = 5,
) -> List[Dict]:
    """Return up to *n* barrios from **other distritos** with the closest
    €/m² to *target_ppsqm*.

    Same-distrito barrios are skipped because they share the proxy
    €/m² (delta = 0) and would crowd the list.
    """
    if not target_ppsqm:
        return []

    candidates = [
        b for b in all_barrios
        if b.get("distrito") != target_distrito and b.get("price_per_sqm")
    ]
    candidates.sort(key=lambda b: abs(b["price_per_sqm"] - target_ppsqm))
    out: List[Dict] = []
    seen_distritos: set = set()
    for b in candidates:
        # One barrio per neighbouring distrito → 5 distinct distritos
        if b["distrito"] in seen_distritos:
            continue
        seen_distritos.add(b["distrito"])
        ppsqm = b["price_per_sqm"]
        diff  = (ppsqm / target_ppsqm - 1) * 100
        out.append({
            "barrio":        b["barrio"],
            "distrito":      b["distrito"],
            "price_per_sqm": ppsqm,
            "diff_pct":      round(diff, 1),
        })
        if len(out) >= n:
            break
    return out


# ──────────────────────────────────────────────────────────────────────
# Data loaders (kept private)
# ──────────────────────────────────────────────────────────────────────


def _load_distrito_prices(db_path: str) -> Dict[str, int]:
    """Latest-period €/m² per distrito from Notarial CIEN."""
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("""
            SELECT distrito, precio_m2
              FROM notarial_prices
             WHERE periodo = (SELECT MAX(periodo) FROM notarial_prices)
        """).fetchall()
    finally:
        conn.close()
    return {d: round(p) for d, p in rows if p is not None}


def _load_barrio_incomes(opendata_path: str) -> Dict[str, float]:
    """Per-barrio ``renta_media_hogar`` from Open Data Madrid."""
    if not os.path.exists(opendata_path):
        return {}
    with open(opendata_path, "r", encoding="utf-8") as fh:
        od = json.load(fh)
    return {
        name: row["renta_media_hogar"]
        for name, row in (od.get("barrios") or {}).items()
        if isinstance(row.get("renta_media_hogar"), (int, float))
    }


# ──────────────────────────────────────────────────────────────────────
# Public entry point
# ──────────────────────────────────────────────────────────────────────


def build_clean_barrio_profiles(
    db_path: Optional[str] = None,
    opendata_path: Optional[str] = None,
) -> Dict:
    """
    Build the full ``barrios_profiles.json`` payload from clean sources.

    Args:
        db_path:        Path to ``real_estate.db``.  Defaults to the one
                        next to this module.
        opendata_path:  Path to ``district_opendata.json``.  Defaults to
                        ``market-thermometer/public/`` next to this
                        module (the in-tree mirror).

    Returns:
        A dict ready for ``json.dump``.
    """
    here = os.path.dirname(os.path.abspath(__file__))
    db_path = db_path or os.path.join(here, "real_estate.db")
    opendata_path = opendata_path or os.path.join(
        here, "market-thermometer", "public", "district_opendata.json"
    )

    # 1. Lazy import to avoid heavy deps when this module is type-checked
    from coordinates import BARRIO_COORDINATES

    distrito_ppsqm = _load_distrito_prices(db_path)
    barrio_incomes = _load_barrio_incomes(opendata_path)

    # Madrid baselines — medians across the canonical universe
    madrid_ppsqm   = (
        statistics.median(distrito_ppsqm.values())
        if distrito_ppsqm else None
    )
    madrid_income  = (
        statistics.median(barrio_incomes.values())
        if barrio_incomes else None
    )

    # First pass: assemble the lightweight list used by the neighbour
    # picker.  Each barrio inherits its distrito's €/m² as proxy.
    flat: List[Dict] = []
    for (distrito, barrio), _coord in BARRIO_COORDINATES.items():
        ppsqm = distrito_ppsqm.get(distrito)
        flat.append({
            "barrio":        barrio,
            "distrito":      distrito,
            "price_per_sqm": ppsqm,
        })

    profiles: Dict[str, Dict] = {}
    for entry in flat:
        barrio   = entry["barrio"]
        distrito = entry["distrito"]
        ppsqm    = entry["price_per_sqm"]
        income   = barrio_incomes.get(barrio)

        # vs_distrito_pct stays None on purpose — with distrito-proxy
        # data it would always be 0 and the front hides null pills.
        vs_madrid_pct = (
            round((ppsqm / madrid_ppsqm - 1) * 100, 1)
            if (ppsqm and madrid_ppsqm) else None
        )

        verdict    = _build_verdict(income, ppsqm, madrid_income, madrid_ppsqm)
        neighbours = _pick_neighbours(distrito, ppsqm, flat, n=5)

        profiles[barrio] = {
            "barrio":   barrio,
            "distrito": distrito,
            "kpis": {
                "price_per_sqm":   ppsqm,
                "vs_distrito_pct": None,  # see docstring
                "vs_madrid_pct":   vs_madrid_pct,
            },
            "verdict":    verdict,
            "neighbours": neighbours,
        }

    return {
        "metadata": {
            "generated_at": datetime.now(timezone.utc)
                .isoformat(timespec="seconds"),
            "version":      "2.2",
            "barrio_count": len(profiles),
            "schema_notes": (
                "Phase 2.2: profiles populated from clean sources only "
                "(Notarial CIEN distrito €/m² + Open Data Madrid renta "
                "media hogar per barrio). Listing-derived fields stay "
                "out. vs_distrito_pct is null because Notarial granularity "
                "is distrito-level."
            ),
            "data_sources": [
                "Notariado CIEN",
                "Open Data Madrid — Indicadores de Distritos y Barrios",
            ],
        },
        "madrid_baseline": {
            "median_price_per_sqm": round(madrid_ppsqm) if madrid_ppsqm else None,
            "median_renta_hogar":   round(madrid_income) if madrid_income else None,
        },
        "profiles": profiles,
    }
