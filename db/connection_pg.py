"""
Postgres connection layer (Phase 3 of the data-layer migration).

Coexists with ``db.connection`` (SQLite) during the transition.  Once
every reader/writer in the codebase has migrated, the SQLite module
gets retired.

Configuration
-------------
The connection URL is read from, in order of preference:

  1. ``st.secrets["postgres"]["url"]``  — Streamlit Cloud
  2. ``os.environ["DATABASE_URL"]``     — GitHub Actions, local dev
  3. ``os.environ["POSTGRES_URL"]``     — alternative env var name

URL format (whichever pooler you use)::

    postgresql://postgres.<ref>:<PWD>@aws-1-us-east-1.pooler.supabase.com:6543/postgres

The recommended target for a long-running process (Streamlit, scraper)
is the **Transaction pooler** (port 6543) — pgbouncer fronts it and gives
us free IPv4.  Migrations and admin scripts that need
``CREATE INDEX CONCURRENTLY`` / ``COPY`` should hit the **Session pooler**
(port 5432) directly.

API
---

``get_pool()`` lazily creates the global ``psycopg_pool.ConnectionPool``
and returns it.  ``get_pg_conn()`` is a context manager that checks out
a pooled connection, sets ``dict_row`` factory (so the rest of the
codebase keeps treating rows as dicts) and returns it on exit.

This module deliberately *does not* expose a thread-local singleton —
psycopg's pool is already thread-safe and reuses connections across
calls.  Fewer moving parts than the SQLite module.

Tests
-----
The fixture in ``tests/conftest.py`` (added in Fase A) spins up a
disposable Postgres via ``testcontainers`` and points ``DATABASE_URL``
at it for the duration of the run.
"""

from __future__ import annotations

import os
import threading
from contextlib import contextmanager
from typing import Iterator, Optional

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool


# Pool sizing: Supabase free-tier transaction pooler exposes 200
# pgbouncer connections shared across the project.  We deliberately stay
# well below that — the scraper is single-threaded and Streamlit will
# rarely have more than a handful of concurrent users.  Tune via env if
# needed.
_DEFAULT_MIN_SIZE = int(os.environ.get("PG_POOL_MIN", "1"))
_DEFAULT_MAX_SIZE = int(os.environ.get("PG_POOL_MAX", "5"))
_DEFAULT_TIMEOUT  = float(os.environ.get("PG_POOL_TIMEOUT", "30"))

_pool_lock = threading.Lock()
_pool: Optional[ConnectionPool] = None


def _resolve_url() -> str:
    """Return the active Postgres connection URL or raise.

    Lookup order:
      1. ``st.secrets["postgres"]["url"]`` (Streamlit Cloud / local Streamlit)
      2. ``DATABASE_URL`` env var
      3. ``POSTGRES_URL`` env var
    """
    # 1. Streamlit secrets — only when running inside a Streamlit context.
    try:
        import streamlit as st  # noqa: WPS433

        url = st.secrets.get("postgres", {}).get("url")
        if url:
            return url
    except Exception:
        # Streamlit not available, or no secret configured. Fall through.
        pass

    # 2./3. Environment variables.
    url = os.environ.get("DATABASE_URL") or os.environ.get("POSTGRES_URL")
    if url:
        return url

    raise RuntimeError(
        "No Postgres connection URL configured. "
        "Set DATABASE_URL (env) or st.secrets['postgres']['url'] (Streamlit)."
    )


def get_pool() -> ConnectionPool:
    """Lazily create and return the global ``ConnectionPool``.

    The pool is created on first call and reused for the lifetime of the
    process.  Safe to call from multiple threads — initialisation is
    guarded with a lock.
    """
    global _pool
    if _pool is not None:
        return _pool

    with _pool_lock:
        if _pool is not None:
            return _pool
        url = _resolve_url()
        # ``open=True`` opens connections eagerly; we let the pool open
        # them lazily on first checkout to keep cold-start fast.
        _pool = ConnectionPool(
            conninfo=url,
            min_size=_DEFAULT_MIN_SIZE,
            max_size=_DEFAULT_MAX_SIZE,
            timeout=_DEFAULT_TIMEOUT,
            kwargs={"row_factory": dict_row, "autocommit": False},
            open=False,
        )
        _pool.open(wait=False)
    return _pool


@contextmanager
def get_pg_conn() -> Iterator[psycopg.Connection]:
    """Check a connection out of the pool for the duration of the with-block.

    On normal exit, the connection's open transaction is committed; on
    exception, it is rolled back.  In both cases the connection returns
    to the pool.

    Usage::

        with get_pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                row = cur.fetchone()
    """
    pool = get_pool()
    with pool.connection() as conn:
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def close_pool() -> None:
    """Close the global pool. Used in test teardown and clean shutdown."""
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None


def reset_pool_for_tests() -> None:
    """Drop the cached pool so the next ``get_pool()`` re-resolves the URL.

    Necessary for the test fixture that swaps ``DATABASE_URL`` between
    the testcontainers Postgres and Supabase. ``close_pool`` alone would
    leave a closed pool that subsequent calls would try to reuse.
    """
    close_pool()
