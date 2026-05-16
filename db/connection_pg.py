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


# ──────────────────────────────────────────────────────────────────────
# NUMERIC → float type loaders
# ──────────────────────────────────────────────────────────────────────
#
# Postgres returns ``AVG(integer)`` and ``ROUND(numeric, n)`` as
# Python ``decimal.Decimal`` by default.  SQLite returned ``float`` for
# the same queries.  Multiple call sites in ``analytics.py`` /
# ``tabs/opportunities_tab.py`` / ``market_indicators.py`` do
# ``aggregated_value / some_float`` (e.g. percentile of a barrio
# median against a city-wide float), which Python rejects with
# ``TypeError: unsupported operand type(s) for /: 'decimal.Decimal' and 'float'``.
#
# Rather than sprinkle ``float(...)`` casts across hundreds of call
# sites (and risk missing some until the next Streamlit click hits a
# new code path), we tell psycopg to decode NUMERIC as float at the
# read boundary.  Currency precision concerns are nil: we are computing
# ratios and percentages off €-integer columns, not doing accounting.
#
# psycopg3 transports NUMERIC in either TEXT or BINARY wire format and
# picks per-query based on result format negotiation.  We register a
# loader for each — both subclass the built-in numeric loaders so the
# wire parsing logic (which knows about each format) is reused and we
# only cast the final ``Decimal`` to ``float``.


from psycopg.types.numeric import NumericLoader  # noqa: E402

try:
    from psycopg.types.numeric import NumericBinaryLoader  # noqa: E402
except ImportError:  # pragma: no cover — older psycopg without binary loader
    NumericBinaryLoader = None  # type: ignore[assignment]


class _NumericAsFloatTextLoader(NumericLoader):
    """Decode TEXT-format NUMERIC as Python ``float``."""

    def load(self, data):
        v = super().load(data)
        return float(v) if v is not None else None


psycopg.adapters.register_loader("numeric", _NumericAsFloatTextLoader)


if NumericBinaryLoader is not None:
    class _NumericAsFloatBinaryLoader(NumericBinaryLoader):  # type: ignore[misc, valid-type]
        """Decode BINARY-format NUMERIC as Python ``float``."""

        def load(self, data):
            v = super().load(data)
            return float(v) if v is not None else None

    psycopg.adapters.register_loader("numeric", _NumericAsFloatBinaryLoader)


# Pool sizing.  Defaults are tuned for the post-migration deployment
# on Neon (eu-central-1) which auto-suspends compute on the free tier
# after a few minutes of idle.  The original Supabase defaults
# (``min_size=1``, ``timeout=30``) caused ``PoolTimeout`` on Streamlit
# Cloud when the app warmed up against a suspended Neon project: the
# pool's background fill of the min_size conn raced against the
# compute wake-up and lost.
#
# Changes:
#   * ``min_size=0`` (was 1) — don't maintain idle conns.  Lets Neon
#     auto-suspend cleanly and we open exactly one conn on demand.
#   * ``timeout=60`` (was 30) — Neon wake-up usually 0.5-2 s but worst
#     case observed ~20-30 s under cold cloud-to-cloud + TLS handshake.
#     60 s leaves headroom without blocking the UI indefinitely.
#
# All three are env-overridable for tuning in production without a
# code change.
_DEFAULT_MIN_SIZE = int(os.environ.get("PG_POOL_MIN", "0"))
_DEFAULT_MAX_SIZE = int(os.environ.get("PG_POOL_MAX", "5"))
_DEFAULT_TIMEOUT  = float(os.environ.get("PG_POOL_TIMEOUT", "60"))

_pool_lock = threading.Lock()
_pool: Optional[ConnectionPool] = None


def _shutdown_pool() -> None:
    """``atexit`` hook that closes the global pool on interpreter shutdown.

    Without this, psycopg's pool-worker threads outlive the script and
    print ``couldn't stop thread 'pool-1-worker-X'`` warnings before
    being killed by the runtime.  Cosmetic but noisy.
    """
    global _pool
    if _pool is not None:
        try:
            _pool.close()
        except Exception:
            pass
        _pool = None


import atexit  # noqa: E402  — keep imports at top, hook close to module
atexit.register(_shutdown_pool)


def _resolve_url() -> str:
    """Return the active Postgres connection URL or raise.

    Lookup order:
      1. ``st.secrets["postgres"]["url"]`` (Streamlit Cloud / local Streamlit)
      2. ``DATABASE_URL`` env var
      3. ``POSTGRES_URL`` env var

    The returned URL is normalised so libpq accepts it even if the
    password contains characters that would otherwise be misparsed
    (most commonly ``+``, which libpq's URL parser decodes as a
    literal plus, but psycopg's URL parser treats as space).  We
    re-encode the password component defensively — see
    ``_normalise_url()``.
    """
    # 1. Streamlit secrets — only when running inside a Streamlit context.
    try:
        import streamlit as st  # noqa: WPS433

        url = st.secrets.get("postgres", {}).get("url")
        if url:
            return _normalise_url(url)
    except Exception:
        # Streamlit not available, or no secret configured. Fall through.
        pass

    # 2./3. Environment variables.
    url = os.environ.get("DATABASE_URL") or os.environ.get("POSTGRES_URL")
    if url:
        return _normalise_url(url)

    raise RuntimeError(
        "No Postgres connection URL configured. "
        "Set DATABASE_URL (env) or st.secrets['postgres']['url'] (Streamlit)."
    )


def _normalise_url(url: str) -> str:
    """Re-encode the password if it contains URL-unsafe chars.

    psycopg's URL parser uses ``urllib.parse``, which decodes ``+`` as
    a space inside the userinfo component. Many real-world passwords
    (Supabase included) contain ``+``, leading to confusing
    ``password authentication failed`` errors in production.  We
    detect raw ``+`` and ``%`` in the password and percent-encode them
    so libpq sees the original characters.

    Idempotent: a URL whose password is already correctly encoded
    passes through unchanged.
    """
    import re
    from urllib.parse import quote

    # Match ``scheme://user:password@host…``. Anything before the last
    # ``@`` (before the first ``/`` of the path) is userinfo.
    m = re.match(
        r"^(?P<scheme>[a-zA-Z][a-zA-Z0-9+.\-]*://)"
        r"(?P<user>[^:@/]+)"
        r":(?P<pwd>[^@]*)"
        r"@(?P<rest>.*)$",
        url,
    )
    if not m:
        return url
    pwd = m.group("pwd")
    # Already-encoded sequences (``%xx``) stay as-is; anything that
    # would re-decode wrong gets quoted.  ``safe=''`` quotes everything
    # that isn't an unreserved char, which is the right call for
    # passwords.  We also accept already-encoded passwords by skipping
    # if the pwd contains no raw ``+`` or ``%`` followed by non-hex.
    if "+" not in pwd and not re.search(r"%[^0-9A-Fa-f]", pwd):
        # Looks already URL-safe — leave alone.
        return url
    encoded = quote(pwd, safe="")
    return f"{m.group('scheme')}{m.group('user')}:{encoded}@{m.group('rest')}"


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
