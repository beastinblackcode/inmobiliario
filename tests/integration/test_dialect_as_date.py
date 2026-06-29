"""
Regression test for db.dialect.as_date — the cross-backend date cast that
replaced the SQLite-only ``DATE(col)`` (which raises UndefinedFunction on
Postgres) in the admin "Propiedades por Distrito y Fecha" query.

Runs the actual grouping query against the live backend (SQLite locally,
Postgres in CI), so it would have caught the original crash on Neon.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from db.dialect import as_date, date_offset_days
from db.connection import get_db

pytestmark = pytest.mark.integration


def test_as_date_grouping_query_runs(tmp_db_seeded: Path):
    """The admin query (as_date + date_offset_days) executes and groups by day."""
    conn = get_db()
    rows = conn.execute(
        f"""
        SELECT {as_date('first_seen_date')} AS date,
               distrito,
               COUNT(*) AS properties
        FROM listings
        WHERE first_seen_date >= {date_offset_days("'-3650'")}
          AND distrito IS NOT NULL
        GROUP BY {as_date('first_seen_date')}, distrito
        ORDER BY date DESC, properties DESC
        """
    ).fetchall()

    # The seed has active + sold listings across several distritos/dates.
    assert len(rows) > 0
    # Every grouped row carries a date, a distrito and a positive count.
    for r in rows:
        assert r[0] is not None
        assert r[1] is not None
        assert r[2] >= 1


def test_as_date_emits_backend_appropriate_sql():
    out = as_date("first_seen_date")
    assert "first_seen_date" in out
    # Either the Postgres cast or the SQLite function form — never DATE(...).
    assert "::date" in out or out.startswith("date(")
