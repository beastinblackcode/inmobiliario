"""
SQL dialect helpers for the SQLite → Postgres transition.

The codebase is being moved off SQLite-on-Drive onto a managed Postgres
(Supabase, Phase 3 of the data-layer roadmap).  These helpers emit the
right snippet for whichever backend is active so the runtime files
(``database.py``, ``market_indicators.py``, ``compute_snapshots.py``)
can support both backends until the cutover lands.

Active backend
--------------
Selected via the ``DB_BACKEND`` environment variable (mirrors
``db.connection``):

  - ``DB_BACKEND=sqlite``   (default) → SQLite snippets
  - ``DB_BACKEND=postgres``           → Postgres snippets

The default is ``sqlite`` so the SQLite suite keeps passing while we
translate functions one by one.  After Phase D cutover the SQLite
branches can be deleted.

Helpers
-------

Each helper returns an SQL **fragment** to splice into a larger query.
Bind parameters use the qmark (``?``) convention; the connection
wrapper in ``db.connection`` rewrites them to ``%s`` for psycopg.

  ``current_date()``           — today as a DATE
  ``current_timestamp()``      — now as TIMESTAMPTZ / ISO datetime
  ``julianday_diff(a, b)``     — integer days between two date columns
  ``julianday_now_diff(col)``  — integer days between today and *col*
  ``iso_week(col)``            — ``'YYYY-WW'`` ISO-week label
  ``date_offset_days(expr)``   — date *expr* days from today (parameterised)
  ``datetime_offset_hours(e)`` — timestamp *e* hours from now (parameterised)
  ``has_table_sql(name)``      — ``(sql, params)`` pair to check existence
  ``insert_or_ignore_clause``  — INSERT ... that no-ops on conflict
"""

from __future__ import annotations

import os
from typing import Tuple


# ──────────────────────────────────────────────────────────────────────
# Backend selection
# ──────────────────────────────────────────────────────────────────────


def active_backend() -> str:
    """Return ``'postgres'`` or ``'sqlite'``.

    Reads from the ``DB_BACKEND`` env var; anything other than
    ``postgres`` (case-insensitive) means SQLite — the default.
    """
    return "postgres" if os.environ.get("DB_BACKEND", "sqlite").lower() == "postgres" else "sqlite"


def is_postgres() -> bool:
    return active_backend() == "postgres"


def is_sqlite() -> bool:
    return active_backend() == "sqlite"


# ──────────────────────────────────────────────────────────────────────
# Date / time
# ──────────────────────────────────────────────────────────────────────


def current_date() -> str:
    """SQL expression that yields today's date as a DATE."""
    return "CURRENT_DATE" if is_postgres() else "date('now')"


def current_timestamp() -> str:
    """SQL expression that yields the current instant.

    Postgres returns TIMESTAMPTZ; SQLite returns an ISO-string
    timestamp. Both compare correctly against the schema's datetime
    columns.
    """
    return "CURRENT_TIMESTAMP" if is_postgres() else "datetime('now')"


def julianday_diff(later: str, earlier: str) -> str:
    """Days between two date expressions, as an integer.

    >>> julianday_diff("last_seen_date", "first_seen_date")
    # postgres → "(last_seen_date::date - first_seen_date::date)"
    # sqlite   → "CAST(julianday(last_seen_date) - julianday(first_seen_date) AS INTEGER)"
    """
    if is_postgres():
        return f"({later}::date - {earlier}::date)"
    return f"CAST(julianday({later}) - julianday({earlier}) AS INTEGER)"


def julianday_now_diff(col: str) -> str:
    """Days between today and *col* (integer).

    Equivalent to ``julianday('now') - julianday(col)`` in SQLite.
    """
    if is_postgres():
        return f"(CURRENT_DATE - {col}::date)"
    return f"CAST(julianday('now') - julianday({col}) AS INTEGER)"


def iso_week(col: str) -> str:
    """ISO year-week label (``'YYYY-WW'``) for a date column / expression.

    SQLite's ``%Y-%W`` and Postgres's ``IYYY-IW`` produce the same
    Mon-Sun ISO week format.
    """
    if is_postgres():
        return f"to_char({col}::date, 'IYYY-IW')"
    return f"strftime('%Y-%W', {col})"


def week_start(col: str) -> str:
    """Monday of the week that *col* falls in, as a DATE.

    SQLite uses the ``date(col, 'weekday 1', '-7 days')`` idiom; Postgres
    has the more readable ``date_trunc('week', ...)::date`` (weeks start
    on Monday by default).
    """
    if is_postgres():
        return f"date_trunc('week', {col}::date)::date"
    return f"date({col}, 'weekday 1', '-7 days')"


def date_plus_days(col: str, days_expr: str) -> str:
    """Add an integer number of days to a date column.

    SQLite uses ``date(col, '+N days')``; Postgres uses
    ``col::date + INTERVAL 'N days'``.

    *days_expr* is the SQL fragment for the day count (literal string,
    placeholder, or expression).
    """
    if is_postgres():
        return f"({col}::date + ({days_expr} || ' days')::interval)::date"
    return f"date({col}, {days_expr} || ' days')"


def date_offset_days(days_expr: str) -> str:
    """Date *days_expr* days from today.

    *days_expr* is a SQL fragment that evaluates to a number of days
    (typically a ``?`` placeholder or a string-concat like
    ``? || ' days'``).  Used heavily for "last N days" filters.

    Postgres expects an integer; SQLite expects a ``'<+|->N days'``
    modifier string.  Helper accepts both shapes:

      >>> date_offset_days("?")
      # postgres → "(CURRENT_DATE + (? || ' days')::interval)"
      # sqlite   → "date('now', ? || ' days')"
    """
    if is_postgres():
        # ``? || ' days'`` works with INTERVAL casting in Postgres too,
        # so the calling code passes the same parameter shape unchanged
        # ('-7', '-30', etc.).
        return f"(CURRENT_DATE + ({days_expr} || ' days')::interval)::date"
    return f"date('now', {days_expr} || ' days')"


def datetime_offset_hours(hours_expr: str) -> str:
    """Timestamp *hours_expr* hours from now (used by alerts module).

      >>> datetime_offset_hours("?")
      # postgres → "(CURRENT_TIMESTAMP + (? || ' hours')::interval)"
      # sqlite   → "datetime('now', ? || ' hours')"
    """
    if is_postgres():
        return f"(CURRENT_TIMESTAMP + ({hours_expr} || ' hours')::interval)"
    return f"datetime('now', {hours_expr} || ' hours')"


# ──────────────────────────────────────────────────────────────────────
# Schema introspection
# ──────────────────────────────────────────────────────────────────────


def has_table_sql(table_name: str) -> Tuple[str, tuple]:
    """SQL + bound params that returns a single row iff *table_name* exists.

    Used by defensive checks throughout ``database.py``.  Returns
    ``(sql, params)`` ready for ``cursor.execute``.
    """
    if is_postgres():
        return (
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name = ? "
            "LIMIT 1",
            (table_name,),
        )
    return (
        "SELECT 1 FROM sqlite_master "
        "WHERE type='table' AND name=? "
        "LIMIT 1",
        (table_name,),
    )


# ──────────────────────────────────────────────────────────────────────
# Insert helpers
# ──────────────────────────────────────────────────────────────────────


def insert_or_ignore_clause(
    table: str,
    columns: list[str],
    *,
    conflict_target: str,
) -> str:
    """Return an INSERT statement that silently no-ops on conflict.

    *conflict_target* must match a UNIQUE constraint or PRIMARY KEY.
    SQLite emits ``INSERT OR IGNORE``; Postgres emits
    ``INSERT ... ON CONFLICT (target) DO NOTHING``.

    >>> insert_or_ignore_clause(
    ...     "watchlist",
    ...     ["listing_id", "added_date", "note"],
    ...     conflict_target="listing_id",
    ... )
    """
    cols = ", ".join(columns)
    placeholders = ", ".join("?" * len(columns))
    if is_postgres():
        return (
            f"INSERT INTO {table} ({cols}) VALUES ({placeholders}) "
            f"ON CONFLICT ({conflict_target}) DO NOTHING"
        )
    return f"INSERT OR IGNORE INTO {table} ({cols}) VALUES ({placeholders})"
