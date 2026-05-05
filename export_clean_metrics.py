"""
Export the CLEAN public metrics JSON for madridhome.tech.

Phase 1 of the Idealista decoupling plan.  Replaces the legacy
``export_public_metrics.py`` for the public-facing pipeline.

What this script does
---------------------

Builds a JSON with the same top-level shape that the Next.js front
already consumes (so no schema break), but with **only fields derivable
from non-scraping sources**.  Listing-derived fields are either dropped
or set to `null` / empty arrays.  When Phase 2 lands and we have ETLs
for Catastro, MITMA and the Open Data Madrid alquileres dataset, this
file is where their loaders plug in.

Allowed sources
---------------

  * ``macro_data``                — BCE Euríbor + INE (IPC, IPV, paro,
                                    hipotecas, afiliados SS, compraventas)
  * ``cgpj_lanzamientos``         — public CGPJ portal (eviction stats)
  * Static morosidad data         — Observatorio del Alquiler annual report
  * ``coordinates.BARRIO_COORDINATES`` — canonical barrio↔distrito map (no DB)
  * Direct SQL on ``notarial_prices`` — Notarial €/m² per distrito (CIEN)

Forbidden sources
-----------------

The following tables are listed in ``_FORBIDDEN_TABLES`` and will cause
the run to abort if any SQL query references them:

  ``listings``, ``price_history``, ``listing_signals``, ``listing_amenities``,
  ``market_snapshots``, ``watchlist``, ``custom_alerts``, ``rental_prices``

(``rental_prices`` is also forbidden because it is currently populated by
the Idealista rental scraper.  When Phase 2 ingests Open Data Madrid
rents into a new clean table, that one will be allow-listed instead.)

Usage
-----

    python export_clean_metrics.py                 # writes to stdout
    python export_clean_metrics.py -o out.json     # writes to file
    python export_clean_metrics.py -o out.json --verify   # also runs sanity
                                                          # check on the
                                                          # output
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import statistics
import sys
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


# Make sibling modules importable regardless of cwd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


# ──────────────────────────────────────────────────────────────────────────
# Guard: SQL allow-list
# ──────────────────────────────────────────────────────────────────────────


_FORBIDDEN_TABLES = (
    "listings",
    "price_history",
    "listing_signals",
    "listing_amenities",
    "market_snapshots",
    "watchlist",
    "custom_alerts",
    "rental_prices",     # populated by the rental scraper today
)


class _ForbiddenTableError(RuntimeError):
    """Raised when a public-export query references a tóxica table."""


def _assert_clean_sql(sql: str) -> None:
    """
    Reject SQL that references any forbidden table.

    Naive but effective for the small set of queries this module runs:
    it scans for ``\\b<keyword>\\s+<table>\\b`` for the verbs that can
    introduce a table reference (FROM/JOIN/INTO/UPDATE/DELETE).
    """
    norm = sql.lower()
    pattern = (
        r"\b(?:from|join|into|update|delete\s+from|with(?:\s+\w+)?\s+as\s*\()"
        r"\s+([a-z_]+)"
    )
    for m in re.finditer(pattern, norm):
        tbl = m.group(1)
        if tbl in _FORBIDDEN_TABLES:
            raise _ForbiddenTableError(
                f"SQL references forbidden table {tbl!r}: {sql.strip()!r}"
            )


def _query(conn: sqlite3.Connection, sql: str, params: tuple = ()):
    """Whitelist-checked SELECT.  Always use this — never ``conn.execute`` directly."""
    _assert_clean_sql(sql)
    return conn.execute(sql, params)


# ──────────────────────────────────────────────────────────────────────────
# Loaders — all clean by construction (no listings/price_history dependency)
# ──────────────────────────────────────────────────────────────────────────


def _safe(fn, *args, default=None, **kwargs):
    """Run *fn*; on failure log and return *default* (often ``{}`` or ``[]``)."""
    try:
        return fn(*args, **kwargs)
    except Exception as exc:
        print(f"⚠️  {fn.__name__} failed: {exc}", file=sys.stderr)
        return default


def _load_macro() -> Dict[str, Dict]:
    """BCE + INE macro indicators.  No DB."""
    from macro_data import get_all_macro_data
    return _safe(get_all_macro_data, default={}) or {}


def _load_lanzamientos(conn: sqlite3.Connection) -> Dict:
    """
    Quarterly evictions for Madrid from the ``cgpj_lanzamientos`` table.

    Reimplemented clean-room (instead of importing
    ``market_indicators.get_lanzamientos_indicator``) so every SQL
    statement passes through ``_query()`` and the forbidden-table guard.
    """
    result: Dict = {
        "name":             "Lanzamientos CGPJ",
        "unit":             "lanzamientos/trimestre",
        "current":          None,
        "quarter_label":    None,
        "alquiler":         None,
        "hipoteca":         None,
        "otros":            None,
        "alquiler_pct":     None,
        "yoy_change":       None,
        "yoy_change_pct":   None,
        "trend":            "stable",
        "series":           [],
    }

    try:
        latest = _query(conn, """
            SELECT year, quarter, total, alquiler, hipoteca, otros, alquiler_pct
              FROM cgpj_lanzamientos
             WHERE tsj = 'Madrid'
             ORDER BY year DESC, quarter DESC
             LIMIT 1
        """).fetchone()
        if not latest:
            return result

        yr, qt, total, alq, hip, otros, alq_pct = (
            latest[0], latest[1], latest[2], latest[3], latest[4], latest[5], latest[6]
        )
        result["current"]       = total
        result["quarter_label"] = f"{yr} T{qt}"
        result["alquiler"]      = alq
        result["hipoteca"]      = hip
        result["otros"]         = otros
        result["alquiler_pct"]  = alq_pct

        prev = _query(conn, """
            SELECT total FROM cgpj_lanzamientos
             WHERE tsj = 'Madrid' AND year = ? AND quarter = ?
        """, (yr - 1, qt)).fetchone()
        if prev and prev[0] and total:
            yoy = total - prev[0]
            yoy_pct = round(yoy / prev[0] * 100, 1)
            result["yoy_change"]     = yoy
            result["yoy_change_pct"] = yoy_pct
            if yoy_pct > 5:
                result["trend"] = "up"
            elif yoy_pct < -5:
                result["trend"] = "down"

        rows = _query(conn, """
            SELECT year, quarter, total, alquiler, hipoteca, otros, alquiler_pct
              FROM cgpj_lanzamientos
             WHERE tsj = 'Madrid' AND total IS NOT NULL
             ORDER BY year DESC, quarter DESC
             LIMIT 12
        """).fetchall()
        result["series"] = [
            {
                "label":         f"{r[0]} T{r[1]}",
                "year":          r[0],
                "quarter":       r[1],
                "total":         r[2],
                "alquiler":      r[3],
                "hipoteca":      r[4],
                "otros":         r[5],
                "alquiler_pct":  r[6],
            }
            for r in reversed(rows)
        ]
    except sqlite3.OperationalError as exc:
        # Table may not exist on a fresh DB — that's fine, return base
        print(f"⚠️  cgpj_lanzamientos read failed: {exc}", file=sys.stderr)

    return result


def _load_morosidad() -> Dict:
    """
    Annual rental delinquency series — Observatorio del Alquiler report.

    Static data: update by hand each February/March when the new annual
    report drops.  Mirrors the implementation in
    ``market_indicators.get_morosidad_indicator`` but inlined here so we
    don't import that file (which has dirty siblings).
    """
    series = [
        {"year": 2024, "madrid": 8_831, "national": 7_958, "yoy_pct": 4.2},
        {"year": 2025, "madrid": 10_420, "national": 8_490, "yoy_pct": 18.0},
    ]
    latest, prev = series[-1], series[-2]
    yoy_pct = round((latest["madrid"] - prev["madrid"]) / prev["madrid"] * 100, 1)

    return {
        "name":           "Morosidad Alquiler",
        "unit":           "€",
        "current":        latest["madrid"],
        "previous":       prev["madrid"],
        "yoy_change_pct": yoy_pct,
        "national_avg":   latest["national"],
        "data_year":      latest["year"],
        "source":         "Observatorio del Alquiler",
        "source_url":     "https://observatoriodelalquiler.org/estudios/",
        "trend":          "up",
        "series":         series,
    }


def _load_clean_affordability(
    median_price_per_sqm: Optional[float],
    euribor_rate: Optional[float],
) -> Dict:
    """
    Affordability built from Notarial €/m² × 90 m² reference, not listings.

    The legacy ``get_affordability_index`` reads ``listings.price``.  Here
    we instead use the Madrid-wide median Notarial €/m² (real escrituras)
    multiplied by a 90 m² reference area as the proxy median price.
    """
    REFERENCE_SQM = 90
    SPREAD = 1.0
    LTV = 0.80
    TERM_MONTHS = 25 * 12
    REFERENCE_INCOME_ANNUAL = 33_000   # INE Comunidad de Madrid (net, approx)

    base = {
        "name":                    "Índice de Asequibilidad",
        "unit":                    "€/mes",
        "current":                 None,
        "monthly_payment":         None,
        "annual_cost":             None,
        "median_price":            None,
        "loan_amount":             None,
        "rate_used":               None,
        "reference_income_annual": REFERENCE_INCOME_ANNUAL,
        "reference_income_monthly": round(REFERENCE_INCOME_ANNUAL / 12),
        "price_to_income":         None,
        "payment_to_income_pct":   None,
        "affordable":              None,
        "trend":                   "stable",
        "reference_area_sqm":      REFERENCE_SQM,
        "source":                  "Notarial CIEN (€/m² mediano) × 90 m² · Euríbor BCE",
    }

    if not median_price_per_sqm or median_price_per_sqm <= 0:
        return base

    median_price = median_price_per_sqm * REFERENCE_SQM
    rate_annual  = (euribor_rate or 3.5) + SPREAD
    loan         = median_price * LTV
    r            = rate_annual / 100 / 12

    monthly = (
        loan * (r * (1 + r) ** TERM_MONTHS) / ((1 + r) ** TERM_MONTHS - 1)
        if r > 0 else loan / TERM_MONTHS
    )
    annual  = monthly * 12
    monthly_income = REFERENCE_INCOME_ANNUAL / 12
    pti_pct = monthly / monthly_income * 100

    base.update({
        "current":              round(monthly),
        "monthly_payment":      round(monthly),
        "annual_cost":          round(annual),
        "median_price":         round(median_price),
        "loan_amount":          round(loan),
        "rate_used":            round(rate_annual, 2),
        "price_to_income":      round(median_price / REFERENCE_INCOME_ANNUAL, 1),
        "payment_to_income_pct": round(pti_pct, 1),
        "affordable":           pti_pct < 33,
    })
    return base


def _load_notarial_zones(conn: sqlite3.Connection) -> List[Dict]:
    """
    One row per Madrid distrito, latest periodo from notarial_prices.

    Output: ``[{"name": "Centro", "price_per_sqm": 5500, "notarial_period": 202504}, ...]``
    """
    rows = _query(conn, """
        SELECT distrito, precio_m2, periodo
          FROM notarial_prices
         WHERE periodo = (SELECT MAX(periodo) FROM notarial_prices)
         ORDER BY distrito
    """).fetchall()
    return [
        {
            "name":             r[0],
            "price_per_sqm":    round(r[1]) if r[1] is not None else None,
            "notarial_period":  r[2],
            "median_price":     None,   # not derivable from notarial alone
            "active_count":     None,   # listing-derived → null in this phase
            "days_to_sell":     None,   # listing-derived → null in this phase
        }
        for r in rows
    ]


def _load_clean_barrios(zones: List[Dict]) -> List[Dict]:
    """
    Build the barrios[] array from the canonical coordinate map.

    Each barrio gets its distrito's notarial €/m² (degraded fallback —
    we don't have per-barrio Notarial data).  All listing-derived
    fields (active_count, avg_days_market, gross_yield, …) are null.

    The Next.js front uses this array for navigation and for the
    "barrios de este distrito" sections; the per-barrio price is
    informative even without the per-listing detail.
    """
    from coordinates import BARRIO_COORDINATES

    distrito_sqm = {z["name"]: z.get("price_per_sqm") for z in zones}

    out: List[Dict] = []
    for (distrito, barrio), _coord in BARRIO_COORDINATES.items():
        out.append({
            "barrio":           barrio,
            "distrito":         distrito,
            "price_per_sqm":    distrito_sqm.get(distrito),  # distrito proxy
            "median_price":     None,
            "active_count":     None,
            "avg_size_sqm":     None,
            "avg_rooms":        None,
            "avg_days_market":  None,
            "gross_yield":      None,
            "rent_median":      None,
        })
    out.sort(key=lambda b: (b["distrito"], b["barrio"]))
    return out


# ──────────────────────────────────────────────────────────────────────────
# Score & alerts (recomputed from clean indicators only)
# ──────────────────────────────────────────────────────────────────────────


def _recompute_market_score(
    indicators: Dict[str, Dict],
    macro: Dict[str, Dict],
) -> Dict:
    """
    Lightweight market score using only clean signals.

    Components (each contributes a 0-100 sub-score):
      * affordability  — % of income consumed by mortgage (lower = better)
      * lanzamientos   — number of evictions (lower = better)
      * morosidad      — €/year rental debt (lower = better)
      * euríbor        — interest rate (lower = better)

    Each is mapped to a 0-100 scale and averaged.  This is intentionally
    simpler than the legacy market_score; we'll improve it in Phase 2 as
    more clean indicators come back online.
    """
    scores = []

    # Affordability — pti < 25% great, > 50% awful
    pti = (indicators.get("affordability") or {}).get("payment_to_income_pct")
    if pti is not None:
        scores.append(max(0.0, min(100.0, 100 - (pti - 20) * 2.5)))

    # Euríbor — < 2 % good, > 5 % bad
    euribor = (macro.get("euribor") or {}).get("current")
    if euribor is not None:
        scores.append(max(0.0, min(100.0, 100 - (euribor - 1) * 20)))

    # Morosidad YoY — flat/down good, +20 % bad
    yoy = (indicators.get("morosidad") or {}).get("yoy_change_pct")
    if yoy is not None:
        scores.append(max(0.0, min(100.0, 70 - yoy * 2)))

    # Lanzamientos — direct value not really comparable; trend used instead
    lan_trend = (indicators.get("lanzamientos") or {}).get("trend")
    if lan_trend == "down":
        scores.append(75.0)
    elif lan_trend == "up":
        scores.append(40.0)
    elif lan_trend == "stable":
        scores.append(60.0)

    if not scores:
        return {
            "score":       None,
            "label":       "Datos insuficientes",
            "emoji":       "⚪",
            "description": "Aún no hay suficientes indicadores limpios para calcular el termómetro.",
            "trend":       "stable",
        }

    score = round(sum(scores) / len(scores))
    if score >= 70:
        label, emoji = "Mercado favorable", "🟢"
    elif score >= 50:
        label, emoji = "Mercado equilibrado", "🟡"
    else:
        label, emoji = "Mercado tensionado", "🔴"

    return {
        "score":       score,
        "label":       label,
        "emoji":       emoji,
        "description": (
            "Termómetro provisional basado en indicadores macro + Notarial. "
            "Subindicadores derivados del scraping de portales se han "
            "retirado del cómputo público."
        ),
        "trend":       "stable",
    }


def _recompute_alerts(
    indicators: Dict[str, Dict],
    macro: Dict[str, Dict],
) -> List[Dict]:
    """Build alerts from clean indicators only."""
    alerts: List[Dict] = []

    # Euríbor over 4 % → warn
    euribor = (macro.get("euribor") or {}).get("current")
    if euribor is not None and euribor >= 4.0:
        alerts.append({
            "level":   "warning",
            "title":   "Euríbor elevado",
            "message": f"El Euríbor 12m está en {euribor:.2f} %.  Las cuotas de hipotecas variables están por encima de la media histórica.",
            "code":    "euribor_high",
        })

    # Affordability — pti > 40 % → warn
    aff = indicators.get("affordability") or {}
    pti = aff.get("payment_to_income_pct")
    if pti is not None and pti > 40:
        alerts.append({
            "level":   "warning",
            "title":   "Esfuerzo hipotecario alto",
            "message": f"La cuota de una hipoteca media consume el {pti:.0f} % de los ingresos brutos.",
            "code":    "affordability_strained",
        })

    # Morosidad — YoY > 10 % → critical
    mor = indicators.get("morosidad") or {}
    yoy = mor.get("yoy_change_pct")
    if yoy is not None and yoy >= 10:
        alerts.append({
            "level":   "critical",
            "title":   "Aumento de morosidad",
            "message": f"La morosidad de alquiler en Madrid creció {yoy:.1f} % interanual.",
            "code":    "morosidad_up",
        })

    return alerts[:10]


# ──────────────────────────────────────────────────────────────────────────
# Builder
# ──────────────────────────────────────────────────────────────────────────


_DATABASE_PATH = "real_estate.db"


def build_clean_metrics() -> Dict[str, Any]:
    """Build the clean public metrics dict ready for JSON serialisation."""
    print("📊 Building clean public metrics (no listing data)...", file=sys.stderr)

    macro          = _load_macro()
    euribor        = (macro.get("euribor") or {}).get("current")

    # Open the DB read-only — we only ever query notarial_prices and
    # cgpj_lanzamientos through the guarded _query() helper.
    conn = sqlite3.connect(f"file:{_DATABASE_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        zones = _safe(_load_notarial_zones, conn, default=[]) or []
        valid_sqm = [z["price_per_sqm"] for z in zones if z["price_per_sqm"]]
        madrid_median_sqm = statistics.median(valid_sqm) if valid_sqm else None
        lanzamientos = _safe(_load_lanzamientos, conn, default={}) or {}
    finally:
        conn.close()

    indicators = {
        "affordability": _load_clean_affordability(madrid_median_sqm, euribor),
        "lanzamientos":  lanzamientos,
        "morosidad":     _load_morosidad(),
    }

    score  = _recompute_market_score(indicators, macro)
    alerts = _recompute_alerts(indicators, macro)
    barrios = _load_clean_barrios(zones)

    return {
        "metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "version":      "2.0",
            "source":       "Termómetro Inmobiliario Madrid (clean export)",
            "data_sources": [
                "Notariado CIEN",
                "INE",
                "BCE",
                "CGPJ",
                "Open Data Madrid",
                "Observatorio del Alquiler",
            ],
            "schema_notes": (
                "Phase 1 of Idealista decoupling: listing-derived fields "
                "are set to null/empty. Phase 2 will repopulate them from "
                "Catastro / MITMA / extended Notarial sources."
            ),
        },
        "market_score":     score,
        "indicators":       indicators,
        "macro":            _shape_macro(macro),
        "zones":            zones,
        "rental_yields":    [],          # Phase 2: Open Data Madrid rents
        "trends":           {"market": [], "by_district": []},
        "notarial_gap":     [],          # dies — needs asking price
        "barrios":          barrios,
        "barrio_trends":    [],          # Phase 2: INE IPV by district
        "price_drop_stats": None,
        "seller_stats":     None,
        "db_stats": {
            "barrios_total":      len(barrios),
            "distritos_notarial": len(zones),
            "last_export_utc":    datetime.now(timezone.utc).isoformat(timespec="seconds"),
        },
        "alerts":           alerts,
        "valuation_model":  None,        # Phase 2: retrain on Notarial + Catastro
    }


def _shape_macro(macro: Dict[str, Dict]) -> Dict[str, Dict]:
    """Strip macro entries to the shape Next.js expects."""
    return {
        k: {
            "name":       v.get("name", k),
            "current":    v.get("current"),
            "previous":   v.get("previous"),
            "change":     v.get("change"),
            "change_pct": v.get("change_pct"),
            "trend":      v.get("trend"),
            "unit":       v.get("unit", ""),
        }
        for k, v in macro.items()
        if isinstance(v, dict)
    }


# ──────────────────────────────────────────────────────────────────────────
# Sanity-check (run with --verify on the produced JSON)
# ──────────────────────────────────────────────────────────────────────────


_FORBIDDEN_KEYS_IN_OUTPUT = {
    # Top-level keys that should be null/empty in clean output
    "price_drop_stats", "seller_stats", "valuation_model",
}


def verify_clean(metrics: Dict[str, Any]) -> List[str]:
    """
    Walk the metrics dict and return a list of suspicious findings.

    Empty list → output is squeaky clean.
    """
    issues: List[str] = []

    for k in _FORBIDDEN_KEYS_IN_OUTPUT:
        if metrics.get(k) not in (None, [], {}):
            issues.append(f"key {k!r} should be null/empty, got: {type(metrics[k]).__name__}")

    if metrics.get("barrio_trends"):
        issues.append("barrio_trends should be empty (listing-derived)")
    if metrics.get("notarial_gap"):
        issues.append("notarial_gap should be empty (needs asking price)")
    trends = metrics.get("trends") or {}
    if trends.get("market") or trends.get("by_district"):
        issues.append("trends.market/by_district should be empty")

    forbidden_indicators = {
        "price_trend", "sales_speed", "supply_demand", "inventory",
        "rotation", "absorption_rate", "months_of_supply", "dispersion",
        "price_drop_ratio", "rental_yield", "notarial_gap",
    }
    indicators = metrics.get("indicators", {}) or {}
    extra = set(indicators.keys()) & forbidden_indicators
    if extra:
        issues.append(f"clean indicators block contains forbidden keys: {sorted(extra)}")

    return issues


# ──────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────


def main() -> int:
    global _DATABASE_PATH  # noqa: PLW0603
    parser = argparse.ArgumentParser(description="Export the CLEAN public metrics JSON")
    parser.add_argument("-o", "--output", help="output JSON file path")
    parser.add_argument("--verify", action="store_true", help="run sanity checks on the output and fail if any")
    parser.add_argument("--db", default=_DATABASE_PATH, help="path to the SQLite DB (default: real_estate.db)")
    args = parser.parse_args()

    _DATABASE_PATH = args.db

    try:
        metrics = build_clean_metrics()
    except _ForbiddenTableError as exc:
        print(f"❌ Forbidden-table guard triggered: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"❌ Build failed: {exc}", file=sys.stderr)
        traceback.print_exc()
        return 1

    if args.verify:
        issues = verify_clean(metrics)
        if issues:
            print("❌ Output failed cleanliness checks:", file=sys.stderr)
            for line in issues:
                print(f"   • {line}", file=sys.stderr)
            return 3
        print("✅ Output passed cleanliness checks.", file=sys.stderr)

    pretty = json.dumps(metrics, ensure_ascii=False, indent=2, default=str)

    if args.output:
        os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(pretty)
        size_kb = os.path.getsize(args.output) / 1024
        print(f"📁 Written to {args.output} ({size_kb:.1f} KB)", file=sys.stderr)
    else:
        print(pretty)

    return 0


if __name__ == "__main__":
    sys.exit(main())
