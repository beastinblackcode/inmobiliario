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


# ──────────────────────────────────────────────────────────────────────
# Pool liveness check
# ──────────────────────────────────────────────────────────────────────


class TestPoolLivenessCheck:
    """The pool must validate a connection before handing it out.

    Neon terminates every open connection when it auto-suspends the
    compute. The pool does not learn about it: the socket is dead but
    ``conn.closed`` is still ``False``, so the connection is handed out
    as healthy and the *caller's* first query dies with
    ``AdminShutdown`` / ``OperationalError: the connection is lost`` —
    which surfaced as a full-page Streamlit crash.

    Passing ``check=ConnectionPool.check_connection`` makes the pool
    round-trip the connection before checkout and transparently swap in
    a fresh one when that fails. These tests pin the wiring; the
    behaviour itself is psycopg_pool's.
    """

    @staticmethod
    def _captured_pool_kwargs(monkeypatch, env_value: str | None) -> dict:
        """Reload connection_pg under ``env_value`` and capture pool kwargs."""
        import importlib

        import db.connection_pg as pg

        if env_value is None:
            monkeypatch.delenv("PG_POOL_CHECK", raising=False)
        else:
            monkeypatch.setenv("PG_POOL_CHECK", env_value)

        pg = importlib.reload(pg)
        captured: dict = {}

        # Subclass the real pool so ``ConnectionPool.check_connection``
        # keeps its identity — get_pool() reads it off this same name.
        class _FakePool(pg.ConnectionPool):
            def __init__(self, **kwargs):  # noqa: WPS612 — deliberately no super()
                captured.update(kwargs)

            def open(self, wait=False):
                pass

        monkeypatch.setattr(pg, "ConnectionPool", _FakePool)
        monkeypatch.setattr(pg, "_resolve_url", lambda: "postgresql://u:p@h/db")
        monkeypatch.setattr(pg, "_pool", None)
        pg.get_pool()

        # Leave a clean module for the rest of the suite.
        importlib.reload(pg)
        return captured

    def test_check_is_enabled_by_default(self, monkeypatch):
        from psycopg_pool import ConnectionPool

        kwargs = self._captured_pool_kwargs(monkeypatch, None)
        assert kwargs["check"] is ConnectionPool.check_connection

    def test_check_can_be_disabled_via_env(self, monkeypatch):
        kwargs = self._captured_pool_kwargs(monkeypatch, "0")
        assert kwargs["check"] is None

    @pytest.mark.parametrize("value", ["1", "true", "yes", "anything-else"])
    def test_truthy_env_values_keep_the_check(self, monkeypatch, value):
        from psycopg_pool import ConnectionPool

        kwargs = self._captured_pool_kwargs(monkeypatch, value)
        assert kwargs["check"] is ConnectionPool.check_connection

    @pytest.mark.parametrize("value", ["0", "false", "no", "FALSE", "No"])
    def test_falsy_env_values_disable_the_check(self, monkeypatch, value):
        kwargs = self._captured_pool_kwargs(monkeypatch, value)
        assert kwargs["check"] is None
