"""
Integration tests for ``user_preferences`` — both backends.

The helper is the only thing that interacts with the new
``user_preferences`` Postgres table.  Tests cover the file fallback
(always) and the Postgres path (when the testcontainer fixture is
available).
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterator

import pytest


pytestmark = pytest.mark.integration


# ──────────────────────────────────────────────────────────────────────
# File fallback — works on every backend
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture
def file_only(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Force the file-only path: backend=sqlite, redirect tmp dir."""
    monkeypatch.setenv("DB_BACKEND", "sqlite")
    import user_preferences as up
    monkeypatch.setattr(up, "_FILE_DIR", tmp_path)


def test_file_get_missing_returns_default(file_only):
    from user_preferences import get_user_pref
    assert get_user_pref("alice", "any_key", default={"x": 1}) == {"x": 1}
    assert get_user_pref("alice", "any_key") is None


def test_file_set_then_get_roundtrip(file_only):
    from user_preferences import get_user_pref, set_user_pref
    value = {"barrios": ["Sol"], "max_price": 400_000}
    set_user_pref("alice", "mi_zona_criteria", value)
    assert get_user_pref("alice", "mi_zona_criteria") == value


def test_file_per_user_isolation(file_only):
    from user_preferences import get_user_pref, set_user_pref
    set_user_pref("alice", "mi_zona_criteria", {"x": 1})
    set_user_pref("bob",   "mi_zona_criteria", {"x": 2})
    assert get_user_pref("alice", "mi_zona_criteria") == {"x": 1}
    assert get_user_pref("bob",   "mi_zona_criteria") == {"x": 2}


def test_file_unsafe_chars_in_username_are_sanitised(file_only, tmp_path: Path):
    """Filename must not contain shell-unsafe characters."""
    from user_preferences import set_user_pref
    set_user_pref("alice/../etc", "mi_zona_criteria", {"x": 1})
    written = list(tmp_path.glob("*.json"))
    assert len(written) == 1
    assert "/" not in written[0].name and ".." not in written[0].name


def test_file_overwrite_preserves_only_latest(file_only):
    from user_preferences import get_user_pref, set_user_pref
    set_user_pref("alice", "k", {"v": 1})
    set_user_pref("alice", "k", {"v": 2})
    assert get_user_pref("alice", "k") == {"v": 2}


def test_file_corrupt_yields_default(file_only, tmp_path: Path):
    from user_preferences import get_user_pref, _file_path
    _file_path("alice", "k").write_text("not json {", encoding="utf-8")
    assert get_user_pref("alice", "k", default={"x": 1}) == {"x": 1}


# ──────────────────────────────────────────────────────────────────────
# Postgres — full round trip, picks the DB path when the table exists
# ──────────────────────────────────────────────────────────────────────


def test_postgres_set_get_roundtrip(tmp_pg_db: str, monkeypatch: pytest.MonkeyPatch):
    """Real PG table populated by Alembic 0003."""
    monkeypatch.setenv("DB_BACKEND", "postgres")
    from db.connection import close_db
    close_db()

    from user_preferences import get_user_pref, set_user_pref, _postgres_available
    assert _postgres_available() is True

    set_user_pref("luis", "mi_zona_criteria",
                  {"barrios": ["Acacias", "Delicias"], "max_price": 450_000})
    got = get_user_pref("luis", "mi_zona_criteria")
    assert got == {"barrios": ["Acacias", "Delicias"], "max_price": 450_000}
    close_db()


def test_postgres_upsert_updates_in_place(tmp_pg_db: str, monkeypatch: pytest.MonkeyPatch):
    """Two writes for the same (username, key) → single row, latest value."""
    monkeypatch.setenv("DB_BACKEND", "postgres")
    import psycopg
    from db.connection import close_db
    close_db()

    from user_preferences import get_user_pref, set_user_pref
    set_user_pref("luis", "k", {"v": 1})
    set_user_pref("luis", "k", {"v": 2})
    assert get_user_pref("luis", "k") == {"v": 2}

    # And only one row in the table.
    close_db()
    with psycopg.connect(tmp_pg_db) as raw:
        n = raw.execute(
            "SELECT COUNT(*) FROM user_preferences WHERE username='luis' AND key='k'"
        ).fetchone()[0]
        assert n == 1


def test_postgres_missing_key_returns_default(tmp_pg_db: str, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DB_BACKEND", "postgres")
    from db.connection import close_db
    close_db()
    from user_preferences import get_user_pref
    assert get_user_pref("nobody", "nothing", default={"x": 9}) == {"x": 9}
    close_db()


def test_postgres_falls_back_to_file_when_db_path_errors(
    tmp_pg_db: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
):
    """If a transient DB issue raises, ``get_user_pref`` should still
    look in the file store rather than crashing the page.
    """
    import user_preferences as up
    monkeypatch.setattr(up, "_FILE_DIR", tmp_path)
    # Seed the file so the fallback has something to return.
    up._file_set("alice", "k", {"from": "file"})

    monkeypatch.setenv("DB_BACKEND", "postgres")
    from db.connection import close_db
    close_db()

    # Force the PG path to raise.
    def boom(*_a, **_kw):
        raise RuntimeError("simulated DB outage")
    monkeypatch.setattr(up, "_pg_get", boom)

    assert up.get_user_pref("alice", "k") == {"from": "file"}
    close_db()
