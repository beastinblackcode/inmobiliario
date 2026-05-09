"""
SQL helpers for Postgres queries.

After Phase B of the data-layer migration, the runtime backend is
Postgres only.  These helpers exist to keep query construction
readable — ``iso_week('first_seen_date')`` is friendlier in a
multi-line SQL string than the raw ``to_char(... ::date, 'IYYY-IW')``.

If we ever need to support a second backend again, the helpers are the
single point where dialect-specific snippets live.

Note on placeholders
--------------------
SQL strings throughout the codebase use ``?`` (qmark) as the parameter
placeholder.  The connection wrapper in ``db.connection`` rewrites
``?`` → ``%s`` (psycopg's expected style) at execute time, so callers
never have to think about it.
"""

from __future__ import annotations

from typing import Tuple


# ──────────────────────────────────────────────────────────────────────
# Date / time
# ──────────────────────────────────────────────────────────────────────


def current_date() -> str:
    """SQL expression that yields today's date as a DATE."""
    return "CURRENT_DATE"


def current_timestamp() -> str:
    """SQL expression that yields the current instant as TIMESTAMPTZ."""
    return "CURRENT_TIMESTAMP"


def julianday_diff(later: str, earlier: str) -> str:
    """Days between two date expressions, as an integer.

    Args:
        later:   SQL expression evaluating to the later date (e.g. column,
                 ``COALESCE(...)``, etc.).
        earlier: SQL expression evaluating to the earlier date.

    Example:
        >>> julianday_diff("last_seen_date", "first_seen_date")
        '(last_seen_date::date - first_seen_date::date)'
    """
    return f"({later}::date - {earlier}::date)"


def iso_week(col: str) -> str:
    """ISO year-week label ('YYYY-WW') for a date column / expression.

    Equivalent to SQLite's old ``strftime('%Y-%W', col)``. Both produce
    Mon-Sun ISO week format suitable for bucketing rows by week.
    """
    return f"to_char({col}::date, 'IYYY-IW')"


# ──────────────────────────────────────────────────────────────────────
# Schema introspection
# ──────────────────────────────────────────────────────────────────────


def has_table_sql(table_name: str) -> Tuple[str, tuple]:
    """SQL + bound params that returns a single row iff *table_name* exists.

    Used in defensive checks throughout ``database.py`` (was a
    ``SELECT name FROM sqlite_master WHERE type='table' AND name=?``
    pattern). Returns ``(sql, params)`` ready for ``cursor.execute``.
    """
    return (
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = 'public' AND table_name = ? "
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
    Becomes ``INSERT ... ON CONFLICT (target) DO NOTHING``.

    Example:
        >>> insert_or_ignore_clause(
        ...     "watchlist",
        ...     ["listing_id", "added_date", "note"],
        ...     conflict_target="listing_id",
        ... )
        'INSERT INTO watchlist (listing_id, added_date, note) VALUES (?, ?, ?) ON CONFLICT (listing_id) DO NOTHING'
    """
    cols = ", ".join(columns)
    placeholders = ", ".join("?" * len(columns))
    return (
        f"INSERT INTO {table} ({cols}) VALUES ({placeholders}) "
        f"ON CONFLICT ({conflict_target}) DO NOTHING"
    )
