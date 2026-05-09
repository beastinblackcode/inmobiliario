"""Unit tests for the backend-dispatching connection layer.

Covers two pieces that have non-trivial logic and are independent of any
real database:

  * ``_translate_placeholders`` — qmark-to-pyformat translation that the
    Postgres backend uses to keep the ~50 SQLite call sites working
    unchanged. Must NOT touch ``?`` characters that live inside SQL
    string literals, and must double stray ``%`` characters so psycopg
    doesn't mistake them for placeholders.

  * ``HybridRow`` — dict-like row that also supports positional
    indexing, mirroring ``sqlite3.Row`` semantics. The Postgres cursor
    wrapper hands these back so callers that mix ``row[0]`` and
    ``row['col']`` keep working.

These tests run against pure Python — no Docker, no Postgres, no
DB_BACKEND env var. They exist to catch regressions in the placeholder
translator or row class without spinning up a container.
"""

from __future__ import annotations

import pytest

from db.connection import HybridRow, _translate_placeholders


# ──────────────────────────────────────────────────────────────────────
# _translate_placeholders
# ──────────────────────────────────────────────────────────────────────


class TestTranslatePlaceholders:
    """Coverage for the qmark→pyformat translator."""

    def test_simple_qmark(self):
        assert _translate_placeholders("SELECT * FROM t WHERE a = ?") == \
               "SELECT * FROM t WHERE a = %s"

    def test_multiple_qmarks(self):
        assert _translate_placeholders(
            "INSERT INTO t (a, b, c) VALUES (?, ?, ?)"
        ) == "INSERT INTO t (a, b, c) VALUES (%s, %s, %s)"

    def test_qmark_inside_single_quotes_is_left_alone(self):
        # The literal "what?" must not be translated.
        sql = "SELECT 'what?' AS prompt FROM t WHERE id = ?"
        translated = _translate_placeholders(sql)
        assert translated == "SELECT 'what?' AS prompt FROM t WHERE id = %s"

    def test_doubled_quote_inside_literal(self):
        # SQL escapes a single quote inside a literal as '': "it''s ok"
        # The translator must keep the literal intact and still translate
        # placeholders outside it.
        sql = "SELECT 'it''s ok?' FROM t WHERE x = ?"
        translated = _translate_placeholders(sql)
        assert translated == "SELECT 'it''s ok?' FROM t WHERE x = %s"

    def test_percent_outside_literal_is_doubled(self):
        # psycopg interprets a bare "%" as the start of a placeholder
        # spec, so we double it. (LIKE patterns build the pattern via
        # parameter binding in this codebase, so the % stays a literal.)
        sql = "SELECT 100 % 3 AS r"
        translated = _translate_placeholders(sql)
        assert translated == "SELECT 100 %% 3 AS r"

    def test_percent_inside_literal_is_doubled(self):
        # ``LIKE 'foo%'`` — the % must survive as a literal pattern.
        sql = "SELECT * FROM t WHERE name LIKE 'foo%'"
        translated = _translate_placeholders(sql)
        assert translated == "SELECT * FROM t WHERE name LIKE 'foo%%'"

    def test_no_placeholders(self):
        sql = "SELECT 1"
        assert _translate_placeholders(sql) == "SELECT 1"

    def test_qmark_in_complex_query(self):
        sql = """
            SELECT id, name
              FROM listings
             WHERE status = ?
               AND price BETWEEN ? AND ?
             ORDER BY price DESC
             LIMIT ?
        """
        translated = _translate_placeholders(sql)
        assert translated.count("%s") == 4
        assert "?" not in translated


# ──────────────────────────────────────────────────────────────────────
# HybridRow
# ──────────────────────────────────────────────────────────────────────


class TestHybridRow:
    """Coverage for the row class that supports both index and key access."""

    @pytest.fixture
    def row(self) -> HybridRow:
        return HybridRow(("id", "name", "price"), (42, "Sol", 350_000))

    def test_key_access(self, row: HybridRow):
        assert row["id"] == 42
        assert row["name"] == "Sol"
        assert row["price"] == 350_000

    def test_index_access(self, row: HybridRow):
        assert row[0] == 42
        assert row[1] == "Sol"
        assert row[2] == 350_000

    def test_negative_index(self, row: HybridRow):
        assert row[-1] == 350_000

    def test_slice(self, row: HybridRow):
        assert row[0:2] == (42, "Sol")

    def test_dict_conversion(self, row: HybridRow):
        # ``dict(row)`` is used heavily in the codebase to convert sqlite
        # rows into plain dicts for further processing.
        assert dict(row) == {"id": 42, "name": "Sol", "price": 350_000}

    def test_keys(self, row: HybridRow):
        # ``sqlite3.Row.keys()`` returns the list of column names.
        assert row.keys() == ["id", "name", "price"]

    def test_iteration_yields_values(self, row: HybridRow):
        # ``sqlite3.Row`` iterates over values, not keys (unlike a dict).
        assert list(row) == [42, "Sol", 350_000]

    def test_membership_uses_keys(self, row: HybridRow):
        # The dict half of HybridRow gives ``"id" in row`` semantics
        # via dict.__contains__ — which checks keys, matching how dict
        # subclasses behave by default.
        assert "id" in row
        assert "missing" not in row

    def test_empty_row(self):
        empty = HybridRow((), ())
        assert dict(empty) == {}
        assert list(empty) == []
        assert empty.keys() == []
