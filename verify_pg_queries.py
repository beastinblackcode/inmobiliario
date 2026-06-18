#!/usr/bin/env python3
"""
Barrido sistemático de queries contra Postgres (Neon).

Motivación
----------
La migración SQLite→Postgres dejó SQL incompatible disperso (SQLite-isms)
que rompe en silencio: ``ROUND(double precision, int)``, ``strftime``/
``strptime``, ``date(col, '-30 days')``, alias del SELECT en ``HAVING``,
columnas seleccionadas fuera del ``GROUP BY``, etc.  Muchas funciones
envuelven su SQL en ``except Exception`` y devuelven ``{}``/``[]`` tras
imprimir ``"Error ...: <msg>"``, así que el fallo no se ve hasta que un
usuario abre la página y lee "No hay datos disponibles".

Este script ejercita **todas las funciones de lectura** de ``database`` y
``market_indicators`` contra Postgres y reporta las que fallan, detectando
tanto excepciones lanzadas como errores tragados (capturando stdout/stderr
y buscando firmas de error de Postgres).

No es un test unitario: no comprueba *qué* devuelven, solo que la query es
válida en Postgres.  Resultado vacío != fallo.

Uso
---
    python verify_pg_queries.py            # fuerza DB_BACKEND=postgres
    python verify_pg_queries.py --verbose  # muestra el output capturado

Sale con código 1 si alguna función falla (apto para CI).
"""

from __future__ import annotations

import argparse
import inspect
import io
import os
import re
import sys
import traceback
from contextlib import redirect_stderr, redirect_stdout

# El propósito del script es ejercitar Postgres: forzamos el backend antes
# de importar nada que toque la BD.  .env aporta DATABASE_URL en local.
try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass
os.environ["DB_BACKEND"] = "postgres"

import database  # noqa: E402
import market_indicators  # noqa: E402

# ---------------------------------------------------------------------------
# Firmas de errores de Postgres que delatan un SQLite-ism (o cualquier SQL
# inválido).  Se buscan en el stdout/stderr capturado y en el texto de las
# excepciones, de forma case-insensitive.
# ---------------------------------------------------------------------------
PG_ERROR_MARKERS = [
    "does not exist",          # función/columna/relación inexistente
    "syntax error at",
    "operator does not exist",
    "undefinedfunction",
    "undefinedcolumn",
    "undefinedtable",
    "groupingerror",           # columna fuera de GROUP BY
    "must appear in the group by",
    "column reference",        # ambiguous / no existe
    "function round",          # ROUND(double, int)
    "invalid input syntax",
    "psycopg",
    "programmingerror",
]

# Funciones a NO ejecutar: mutan datos, hacen DDL, o no son queries.
SKIP = {
    "init_database",
    "init_alerts_table",
    "purge_stale_listings",
    "mark_stale_as_sold",
    "download_database_from_cloud",
    "is_streamlit_cloud",
    # los migrate_* se descartan por prefijo más abajo
}


def _looks_like_pg_error(text: str) -> str | None:
    """Devuelve la primera línea ofensiva si el texto contiene una firma
    de error de Postgres; ``None`` si parece limpio."""
    low = text.lower()
    for marker in PG_ERROR_MARKERS:
        if marker in low:
            # Devuelve la línea concreta para el reporte
            for line in text.splitlines():
                if marker in line.lower():
                    return line.strip()
            return marker
    return None


def _run_one(label: str, call):
    """Ejecuta ``call`` capturando stdout/stderr.  Devuelve
    (status, detail) donde status ∈ {PASS, FAIL, SKIP}."""
    out, err = io.StringIO(), io.StringIO()
    try:
        with redirect_stdout(out), redirect_stderr(err):
            call()
    except Exception as exc:  # excepción no tragada
        captured = out.getvalue() + err.getvalue()
        detail = f"{type(exc).__name__}: {exc}".splitlines()[0]
        return "FAIL", detail, captured
    captured = out.getvalue() + err.getvalue()
    offending = _looks_like_pg_error(captured)
    if offending:
        return "FAIL", f"error tragado → {offending}", captured
    return "PASS", "", captured


def _sample_args() -> dict:
    """Obtiene valores reales de la BD para las funciones parametrizadas.
    Si la BD está vacía o algo falla, los faltantes se omiten (la función
    asociada se marcará SKIP)."""
    s: dict = {}
    # Source ids/barrios straight from the table so sampling doesn't depend
    # on (and isn't broken by) the very functions under test.
    try:
        with database.get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT listing_id FROM listings WHERE status = 'active' LIMIT 5"
            )
            ids = [r[0] for r in cur.fetchall()]
            if ids:
                s["listing_id"] = ids[0]
                s["listing_ids"] = ids
            cur.execute(
                "SELECT DISTINCT barrio FROM listings "
                "WHERE barrio IS NOT NULL AND barrio != '' LIMIT 3"
            )
            barrios = [r[0] for r in cur.fetchall()]
            if barrios:
                s["barrios"] = barrios
    except Exception:
        pass
    try:
        alerts = database.get_alerts() or []
        if alerts:
            s["alert"] = alerts[0]
    except Exception:
        pass
    if "listing_id" in s:
        s["url_or_id"] = s["listing_id"]
    return s


def _curated_calls(s: dict):
    """Funciones de lectura parametrizadas, con args reales de la BD.
    Cada entrada: (label, callable-o-None).  None ⇒ SKIP (sin sample)."""
    calls = []

    def add(label, needed, fn):
        if all(k in s for k in needed):
            calls.append((label, lambda fn=fn, needed=needed: fn(*[s[k] for k in needed])))
        else:
            calls.append((label, None))

    db = database
    add("database.get_current_price", ["listing_id"], db.get_current_price)
    add("database.get_price_history", ["listing_id"], db.get_price_history)
    add("database.get_property_price_stats", ["listing_id"], db.get_property_price_stats)
    add("database.is_in_watchlist", ["listing_id"], db.is_in_watchlist)
    add("database.get_price_history_for_listings", ["listing_ids"], db.get_price_history_for_listings)
    add("database.get_drop_counts_for_listings", ["listing_ids"], db.get_drop_counts_for_listings)
    add("database.get_barrio_summary", ["barrios"], db.get_barrio_summary)
    add("database.get_price_evolution_by_barrio", ["barrios"], db.get_price_evolution_by_barrio)
    add("database.get_listing_by_url", ["url_or_id"], db.get_listing_by_url)
    add("database.get_alert_matches", ["alert"], db.get_alert_matches)
    add("database.count_alert_new_matches", ["alert"], db.count_alert_new_matches)

    # Snapshots: scope/metric fijos habituales; resultado vacío no es fallo.
    calls.append((
        "database.get_latest_snapshots",
        lambda: db.get_latest_snapshots("district", "median_price"),
    ))
    calls.append((
        "database.get_snapshot_series",
        lambda: db.get_snapshot_series("district", "Centro", "median_price"),
    ))
    return calls


def _auto_zero_arg_calls():
    """Todas las funciones públicas ZERO-ARG de lectura de ambos módulos."""
    calls = []
    for mod in (database, market_indicators):
        for name, fn in inspect.getmembers(mod, inspect.isfunction):
            if fn.__module__ != mod.__name__:
                continue
            if name.startswith("_") or name.startswith("migrate_"):
                continue
            if name in SKIP:
                continue
            sig = inspect.signature(fn)
            required = [
                p.name for p in sig.parameters.values()
                if p.default is inspect._empty
                and p.kind in (p.POSITIONAL_OR_KEYWORD, p.POSITIONAL_ONLY)
            ]
            if required:
                continue  # parametrizadas → van por la vía curada
            label = f"{mod.__name__}.{name}"
            calls.append((label, fn))
    return calls


def _composite_calls():
    """``calculate_market_score`` y ``generate_diagnosis`` necesitan los dicts
    de los otros indicadores: se alimentan de sus propias salidas."""
    mi = market_indicators
    calls = []
    try:
        pt = mi.get_weekly_price_evolution()
        ss = mi.get_weekly_sales_speed()
        sd = mi.get_supply_demand_ratio()
        inv = mi.get_inventory_evolution()
        rot = mi.get_rotation_rate()
        disp = mi.get_price_dispersion()
        calls.append((
            "market_indicators.calculate_market_score",
            lambda: mi.calculate_market_score(pt, ss, sd, inv),
        ))
        calls.append((
            "market_indicators.generate_diagnosis",
            lambda: mi.generate_diagnosis(pt, ss, sd, inv, rot, disp),
        ))
    except Exception:
        calls.append(("market_indicators.calculate_market_score", None))
        calls.append(("market_indicators.generate_diagnosis", None))
    return calls


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--verbose", action="store_true", help="muestra output capturado de los fallos")
    args = ap.parse_args()

    print(f"Backend: {os.environ.get('DB_BACKEND')}  ", end="")
    try:
        from db.dialect import active_backend
        print(f"(active={active_backend()})")
    except Exception:
        print()

    sample = _sample_args()
    print(f"Sample data: {', '.join(sorted(sample)) or '∅ (BD vacía?)'}\n")

    plan = _auto_zero_arg_calls() + _curated_calls(sample) + _composite_calls()
    plan.sort(key=lambda x: x[0])

    passed, failed, skipped = [], [], []
    for label, call in plan:
        if call is None:
            skipped.append((label, "sin datos de muestra"))
            print(f"  SKIP  {label}  (sin datos de muestra)")
            continue
        status, detail, captured = _run_one(label, call)
        if status == "PASS":
            passed.append(label)
            print(f"  ok    {label}")
        else:
            failed.append((label, detail))
            print(f"  FAIL  {label}\n          → {detail}")
            if args.verbose and captured.strip():
                for line in captured.strip().splitlines():
                    print(f"            | {line}")

    print("\n" + "=" * 60)
    print(f"RESUMEN: {len(passed)} ok · {len(failed)} FALLOS · {len(skipped)} omitidas")
    if failed:
        print("\nFunciones que fallan en Postgres:")
        for label, detail in failed:
            print(f"  ✗ {label}\n      {detail}")
    print("=" * 60)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
