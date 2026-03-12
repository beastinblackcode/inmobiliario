"""
Thread-local singleton connection for SQLite.

Streamlit uses one thread per user session, so a thread-local connection
means each session reuses one connection instead of opening/closing on
every database call.  PRAGMAs are executed once at creation time.

Usage (direct):
    from db.connection import get_db
    conn = get_db()
    rows = conn.execute("SELECT ...").fetchall()

The legacy ``database.get_connection()`` context manager has been updated
to use this singleton internally, so all existing call-sites keep working
with zero changes.
"""

import sqlite3
import threading
from pathlib import Path

_local = threading.local()

# Default — overridden by set_database_path() if needed.
DATABASE_PATH: str = "real_estate.db"


def set_database_path(path: str) -> None:
    """Override the database path (call before any get_db())."""
    global DATABASE_PATH
    DATABASE_PATH = path


def get_db() -> sqlite3.Connection:
    """
    Return a reusable SQLite connection for the current thread.

    First call per thread opens the connection and sets all PRAGMAs.
    Subsequent calls return the same connection object.
    """
    conn: sqlite3.Connection | None = getattr(_local, "conn", None)

    # Check the connection is still alive (not closed externally)
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

        # ── PRAGMAs (executed once per connection lifetime) ──────────
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA cache_size=-64000")     # 64 MB page cache
        conn.execute("PRAGMA mmap_size=268435456")   # 256 MB memory-mapped I/O
        conn.execute("PRAGMA synchronous=NORMAL")    # safe with WAL, faster than FULL

        _local.conn = conn

    return conn


def close_db() -> None:
    """Close the thread-local connection (for clean shutdown)."""
    conn: sqlite3.Connection | None = getattr(_local, "conn", None)
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass
        _local.conn = None
