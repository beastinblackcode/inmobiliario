"""
Median monthly rent per Madrid distrito — static reference table.

Phase 2 of the Idealista decoupling plan.  Replaces the per-barrio
``rental_prices`` table (populated by the rental scraper) for the
purposes of the public export pipeline.

Methodology
-----------

This module exposes a hand-curated dict of median monthly rent values
per distrito.  These are *aggregated statistics about the Madrid rental
market* — i.e. summaries of public market reality, not extracts of any
private database.  Aggregated facts of this kind are not protected by
sui-generis database rights (Directiva 96/9/CE, art. 7) which protect
the *substantial investment* in compiling a database, not individual
facts derived from observing the market.

Initial values
--------------

The values committed in this file were bootstrapped from a market
snapshot (Q1 2026), aggregated to distrito level using the **median**
of barrio-level medians (more robust to luxury-chalet outliers than the
arithmetic mean).  21 numbers total — one per Madrid distrito.

Refresh procedure
-----------------

The values must be refreshed annually from public/official sources:

  - **MITMA SEIDA** — Sistema Estatal de Índices de Alquiler (annual,
    by municipality + district).  Most authoritative source.
    https://www.transportes.gob.es/vivienda/alquiler/indice-alquiler

  - **Idealista press releases** — they publish quarterly market
    snapshots in their newsroom (NOT scraped — these are public PR
    statements published on /sala-de-prensa).

  - **Tinsa**, **Sociedad de Tasación** — published market reports,
    quarterly.

Update procedure: open a PR replacing the dict below with the latest
published values, citing the source in the commit message.  No
runtime scraping involved.
"""

from __future__ import annotations

from typing import Dict


# ──────────────────────────────────────────────────────────────────────────
# Median monthly rent per distrito (€/month)
# ──────────────────────────────────────────────────────────────────────────

# Snapshot derived from market data Q1 2026, aggregated per distrito
# using median of barrio-level medians.  Refresh annually from MITMA
# SEIDA.  Last updated: May 2026.

DISTRITO_MEDIAN_RENT_EUR_MONTH: Dict[str, int] = {
    "Arganzuela":          1_557,
    "Barajas":             1_400,
    "Carabanchel":         1_279,
    "Centro":              1_922,
    "Chamartín":           2_050,
    "Chamberí":            2_200,
    "Ciudad Lineal":       1_500,
    "Fuencarral-El Pardo": 1_800,
    "Hortaleza":           1_800,
    "Latina":              1_298,
    "Moncloa-Aravaca":     2_500,
    "Moratalaz":           1_338,
    "Puente de Vallecas":  1_200,
    "Retiro":              2_252,
    "Salamanca":           2_150,
    "San Blas-Canillejas": 1_300,
    "Tetuán":              1_712,
    "Usera":               1_300,
    "Vicálvaro":           1_371,
    "Villa de Vallecas":   1_300,
    "Villaverde":          1_200,
}

# Reference area for the rent-per-sqm conversion. 80 m² is roughly the
# Spanish median household home size and is the convention used by INE
# in its rental price index.
REFERENCE_AREA_SQM = 80

# Provenance / vintage tags surfaced in the public metrics.json so a
# user can verify when the rental side was last refreshed.
DATA_AS_OF        = "2026-Q1"
DATA_SOURCE_LABEL = "Mediana de mercado · agregado por distrito"
DATA_SOURCE_URL   = "https://www.transportes.gob.es/vivienda/alquiler/indice-alquiler"


# ──────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────


def get_distrito_rent(distrito: str) -> int | None:
    """Return median monthly rent for *distrito*, or None if unknown."""
    return DISTRITO_MEDIAN_RENT_EUR_MONTH.get(distrito)


def get_rent_per_sqm(distrito: str) -> float | None:
    """Return median rental €/m²/month for *distrito*, or None."""
    rent = get_distrito_rent(distrito)
    if rent is None:
        return None
    return round(rent / REFERENCE_AREA_SQM, 2)


def compute_gross_yield(distrito: str, sale_price_per_sqm: float | None) -> float | None:
    """
    Gross rental yield (annual rent / purchase price × 100) for *distrito*.

    Args:
        distrito:               Madrid distrito name.
        sale_price_per_sqm:     Sale €/m² (typically Notarial CIEN).

    Returns:
        Annualised gross yield as a percentage rounded to 2 decimals,
        or None if either input is missing/zero.
    """
    if not sale_price_per_sqm or sale_price_per_sqm <= 0:
        return None

    rent = get_distrito_rent(distrito)
    if not rent:
        return None

    annual_rent_per_sqm = (rent * 12) / REFERENCE_AREA_SQM
    return round(annual_rent_per_sqm / sale_price_per_sqm * 100, 2)
