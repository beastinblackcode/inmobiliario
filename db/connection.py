"""
Database connection layer — backend-dispatching shim.

During the SQLite → Postgres migration this module needs to support
*both* backends so the cutover can be staged:

  - ``DB_BACKEND=sqlite`` (default, legacy) → original thread-local
    sqlite3 connection with WAL/cache PRAGMAs.  Tests and the local
    development workflow keep working exactly as before.
  - ``DB_BACKEND=postgres`` (new) → wrapper over the psycopg pool that
    presents the same surface (``cursor.execute(sql, params)`` with
    ``?`` placeholders, ``row[0]`` and ``row['col']`` both work,
    ``dict(row)`` works) so the ~50 call sites in ``database.py``
    don't have to change.

Phase B is the migration of those call sites' SQL strings to syntax
that Postgres also accepts (``julianday`` → date arithmetic, etc.).
This shim is the foundation that makes that migration possible — it
abstracts the connection differences so we can flip the backend per
process via env var.

Public surface (preserved 1:1)
------------------------------
* ``DATABASE_PATH``               — module-level path (sqlite-only)
* ``set_database_path(path)``     — overrides DATABASE_PATH (sqlite-only)
* ``get_db()``                    — thread-local connection
* ``close_db()``                  — releases the thread-local connection
* ``get_connection()``            — context manager: commit / rollback
"""

from __future__ import annotations

import os
import re
import sqlite3
import threading
from contextlib import contextmanager
from typing import Any, Iterator, Optional, Sequence


# ──────────────────────────────────────────────────────────────────────
# Backend selection
# ──────────────────────────────────────────────────────────────────────


def _active_backend() -> str:
    """Return ``'postgres'`` or ``'sqlite'``.

    Reads from the ``DB_BACKEND`` env var. Anything other than
    ``postgres`` (case-insensitive) selects SQLite — the default.
    """
    return "postgres" if os.environ.get("DB_BACKEND", "sqlite").lower() == "postgres" else "sqlite"


# ──────────────────────────────────────────────────────────────────────
# SQLite path (preserved from the original module)
# ──────────────────────────────────────────────────────────────────────


_local = threading.local()
DATABASE_PATH: str = "real_estate.db"


def set_database_path(path: str) -> None:
    """Override the SQLite database path (no-op for Postgres)."""
    global DATABASE_PATH
    DATABASE_PATH = path


def _get_sqlite_db() -> sqlite3.Connection:
    """Original thread-local sqlite3 singleton with WAL PRAGMAs."""
    conn: sqlite3.Connection | None = getattr(_local, "conn", None)

    if conn is not None:
        try:
            conn.execute("SELECT 1")
        except (sqlite3.ProgrammingError, sqlite3.OperationalError):
            conn = None
            _local.conn = None

    if conn is None:
        conn = sqlite3.connect(
            DATABASE_PATH,
            check_same_thread=False,
            timeout=30.0,
        )
        conn.row_factory = sqlite3.Row

        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA cache_size=-64000")
        conn.execute("PRAGMA mmap_size=268435456")
        conn.execute("PRAGMA synchronous=NORMAL")

        _local.conn = conn

    return conn


def _close_sqlite_db() -> None:
    conn: sqlite3.Connection | None = getattr(_local, "conn", None)
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass
        _local.conn = None


# ──────────────────────────────────────────────────────────────────────
# Postgres path — qmark→pyformat translator and HybridRow factory
# ──────────────────────────────────────────────────────────────────────
#
# The Postgres branch is loaded lazily so installing psycopg is only
# required when ``DB_BACKEND=postgres`` is set.  Local SQLite users
# don't have to install it.

_QMARK_RE = re.compile(r"\?")


def _translate_placeholders(sql: str) -> str:
    """Convert ``?`` qmark placeholders to ``%s``; double stray ``%``.

    Walks the string keeping track of single-quoted SQL string literals
    so we never touch ``?`` characters inside them.  Doubles literal
    ``%`` so psycopg doesn't mistake them for placeholders.
    """
    out: list[str] = []
    in_string = False
    i = 0
    n = len(sql)
    while i < n:
        ch = sql[i]
        if ch == "'":
            # SQL escapes single quote as '' inside a literal.
            if in_string and i + 1 < n and sql[i + 1] == "'":
                out.append("''")
                i += 2
                continue
            in_string = not in_string
            out.append(ch)
            i += 1
            continue
        if ch == "%":
            out.append("%%")  # double for both inside and outside literals
        elif ch == "?" and not in_string:
            out.append("%s")
        else:
            out.append(ch)
        i += 1
    return "".join(out)


class HybridRow(dict):
    """Dict-and-tuple row, mirroring sqlite3.Row's dual access.

    Supports ``row['col']`` (dict-style) **and** ``row[0]`` (positional),
    plus ``dict(row)`` and iteration over values. Used by the Postgres
    cursor wrapper so call sites that mix both styles keep working.
    """

    __slots__ = ("_values",)

    def __init__(self, columns: Sequence[str], values: Sequence[Any]):
        super().__init__(zip(columns, values))
        object.__setattr__(self, "_values", tuple(values))

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, (int, slice)):
            return self._values[key]
        return super().__getitem__(key)

    def __iter__(self):
        return iter(self._values)

    def keys(self):
        return list(super().keys())


def _hybrid_row_factory(cursor: Any) -> Any:
    columns = [c.name for c in (cursor.description or [])]

    def make(values: Sequence[Any]) -> HybridRow:
        return HybridRow(columns, values)

    return make


class _PgCursor:
    """Wraps a psycopg cursor: ``?``→``%s`` translation + HybridRow factory."""

    def __init__(self, raw: Any):
        raw.row_factory = _hybrid_row_factory
        self._raw = raw

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> "_PgCursor":
        translated = _translate_placeholders(sql)
        if params is None:
            self._raw.execute(translated)
        else:
            self._raw.execute(translated, tuple(params))
        return self

    def executemany(self, sql: str, params_seq: Sequence[Sequence[Any]]) -> "_PgCursor":
        translated = _translate_placeholders(sql)
        self._raw.executemany(translated, [tuple(p) for p in params_seq])
        return self

    def fetchone(self) -> Optional[HybridRow]:
        return self._raw.fetchone()

    def fetchall(self) -> list[HybridRow]:
        return list(self._raw.fetchall())

    def fetchmany(self, size: int | None = None) -> list[HybridRow]:
        return list(self._raw.fetchmany(size) if size else self._raw.fetchmany())

    @property
    def rowcount(self) -> int:
        return self._raw.rowcount

    @property
    def lastrowid(self) -> int | None:
        # Postgres has no auto-lastrowid. Code paths that relied on this
        # are expected to use ``RETURNING id`` explicitly post-Phase-B.
        return None

    @property
    def description(self):
        return self._raw.description

    def close(self) -> None:
        self._raw.close()

    def __iter__(self):
        return iter(self._raw)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()


class _PgConnection:
    """Wraps a psycopg connection: ``cursor()``/``execute()`` return ``_PgCursor``."""

    def __init__(self, raw: Any):
        self._raw = raw

    def cursor(self) -> _PgCursor:
        return _PgCursor(self._raw.cursor())

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> _PgCursor:
        cur = self.cursor()
        cur.execute(sql, params)
        return cur

    def executemany(self, sql: str, params_seq: Sequence[Sequence[Any]]) -> _PgCursor:
        cur = self.cursor()
        cur.executemany(sql, params_seq)
        return cur

    def commit(self) -> None:
        self._raw.commit()

    def rollback(self) -> None:
        self._raw.rollback()

    def close(self) -> None:
        try:
            self._raw.close()
        except Exception:
            pass

    @property
    def row_factory(self):
        return _hybrid_row_factory

    @row_factory.setter
    def row_factory(self, value: Any) -> None:
        # Legacy code occasionally set ``conn.row_factory = sqlite3.Row``.
        # Silently ignore — HybridRow already gives the same surface.
        pass


def _get_pg_db() -> _PgConnection:
    """Thread-local Postgres connection drawn from the psycopg pool."""
    conn: Optional[_PgConnection] = getattr(_local, "conn", None)
    if conn is None or not isinstance(conn, _PgConnection):
        from db.connection_pg import get_pool  # lazy to avoid cycles
        raw = get_pool().getconn()
        raw.autocommit = False
        conn = _PgConnection(raw)
        _local.conn = conn
    return conn


def _close_pg_db() -> None:
    conn: Optional[_PgConnection] = getattr(_local, "conn", None)
    if isinstance(conn, _PgConnection):
        try:
            from db.connection_pg import get_pool
            try:
                conn.rollback()
            except Exception:
                pass
            get_pool().putconn(conn._raw)
        finally:
            _local.conn = None


# ──────────────────────────────────────────────────────────────────────
# Public dispatching API
# ──────────────────────────────────────────────────────────────────────


def get_db():
    """Return the thread-local connection for the active backend."""
    if _active_backend() == "postgres":
        return _get_pg_db()
    return _get_sqlite_db()


def close_db() -> None:
    """Close (or pool-release) the thread-local connection."""
    if _active_backend() == "postgres":
        _close_pg_db()
    else:
        _close_sqlite_db()


@contextmanager
def get_connection() -> Iterator[Any]:
    """Context manager around the thread-local connection.

    Commits on clean exit, rolls back on exception. Mirrors the
    pre-Phase-B behaviour exactly so the database.py call sites need
    no edits.
    """
    conn = get_db()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
