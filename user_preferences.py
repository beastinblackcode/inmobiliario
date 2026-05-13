"""
Per-user preferences with Postgres-first storage and a graceful file
fallback.

Why this exists
---------------
Mi Zona writes the user's hunting criteria so they survive across
sessions.  Doing that with a JSON file in ``.streamlit/`` works
locally but breaks on Streamlit Community Cloud after each redeploy
(container filesystem is wiped).  This module persists the same
data in the ``user_preferences`` table created by Alembic 0003.

Both paths
----------
* When ``DB_BACKEND=postgres`` and the table exists → JSONB row,
  survives redeploys, supports future multi-user scenarios.
* When ``DB_BACKEND=sqlite`` (local dev) or the table is missing
  (fresh deploy before ``alembic upgrade head``) → falls back to
  ``.streamlit/<prefix>_<user>_<key>.json`` exactly as before.

The fallback path keeps the local dev story unchanged: no need to
spin up Postgres just to play with Mi Zona, and no behavioural
regression for users who haven't run the new migration yet.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Optional

from db.connection import get_connection


_FILE_DIR = Path(".streamlit")
_FILE_TEMPLATE = "{prefix}_{user}_{key}.json"


# ──────────────────────────────────────────────────────────────────────
# Path helpers
# ──────────────────────────────────────────────────────────────────────


def _safe_segment(s: str) -> str:
    """Filesystem-safe single segment for use in the fallback filename."""
    return re.sub(r"[^A-Za-z0-9_-]", "_", str(s)) or "default"


def _file_path(username: str, key: str, prefix: str = "userpref") -> Path:
    return _FILE_DIR / _FILE_TEMPLATE.format(
        prefix=_safe_segment(prefix),
        user=_safe_segment(username),
        key=_safe_segment(key),
    )


def _postgres_available() -> bool:
    """True iff DB_BACKEND=postgres AND the user_preferences table exists."""
    import os
    if os.environ.get("DB_BACKEND", "sqlite").lower() != "postgres":
        return False
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            # ``information_schema`` works on both backends but only the
            # Postgres path can produce a row (SQLite doesn't have
            # ``information_schema``).
            cur.execute("""
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'user_preferences'
                LIMIT 1
            """)
            return cur.fetchone() is not None
    except Exception:
        return False


# ──────────────────────────────────────────────────────────────────────
# File fallback (works on every backend)
# ──────────────────────────────────────────────────────────────────────


def _file_get(username: str, key: str) -> Optional[dict]:
    path = _file_path(username, key)
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def _file_set(username: str, key: str, value: dict) -> None:
    _FILE_DIR.mkdir(exist_ok=True)
    path = _file_path(username, key)
    with path.open("w", encoding="utf-8") as f:
        json.dump(value, f, indent=2, ensure_ascii=False)


# ──────────────────────────────────────────────────────────────────────
# Postgres path
# ──────────────────────────────────────────────────────────────────────


def _pg_get(username: str, key: str) -> Optional[dict]:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT value FROM user_preferences WHERE username = ? AND key = ?",
            (username, key),
        )
        row = cur.fetchone()
    if not row:
        return None
    value = row[0]
    # psycopg's default JSONB loader returns native Python objects, so
    # a dict / list comes back already parsed.  Defensive: if some loader
    # configuration returned a raw string instead, decode it.
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return None
    return value if isinstance(value, dict) else None


def _pg_set(username: str, key: str, value: dict) -> None:
    payload = json.dumps(value, ensure_ascii=False)
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO user_preferences (username, key, value)
            VALUES (?, ?, ?::jsonb)
            ON CONFLICT (username, key) DO UPDATE
                SET value      = EXCLUDED.value,
                    updated_at = CURRENT_TIMESTAMP
            """,
            (username, key, payload),
        )


# ──────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────


def get_user_pref(
    username: str, key: str, default: Optional[dict] = None,
) -> Optional[dict]:
    """Read the stored JSON value for ``(username, key)``.

    Tries Postgres first (when configured) then the file fallback.
    Returns ``default`` (typically ``None`` or an empty dict) when
    neither store has the key.
    """
    if _postgres_available():
        try:
            v = _pg_get(username, key)
            if v is not None:
                return v
        except Exception:
            pass    # fall through to file
    v = _file_get(username, key)
    return v if v is not None else default


def set_user_pref(username: str, key: str, value: dict) -> None:
    """Upsert the JSON value for ``(username, key)``.

    Writes to Postgres when available; otherwise to a file.  Never
    writes to both — the active store is determined at call time by
    ``_postgres_available()`` and the other one stays untouched.
    """
    if _postgres_available():
        try:
            _pg_set(username, key, value)
            return
        except Exception:
            pass    # fall through to file
    _file_set(username, key, value)
