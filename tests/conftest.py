"""
Shared pytest fixtures.

Key fixtures:
    tmp_db          – fresh empty SQLite database per test (auto-rebound).
    tmp_db_seeded   – tmp_db pre-populated with a small, deterministic set
                      of listings + price history rows that most tests need.
    sample_listings – plain Python list of listing dicts (no DB) for unit tests
                      that work on rows directly (e.g. analytics scoring).

The session avoids hitting the real `real_estate.db`. Both `database.py` and
`db.connection` carry their own DATABASE_PATH globals, so we patch both.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterator, List, Dict

import pytest

# Ensure project root is on sys.path so `import database` works regardless
# of where pytest is invoked from.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


# ──────────────────────────────────────────────────────────────────────────
# Database fixtures
# ──────────────────────────────────────────────────────────────────────────


@pytest.fixture
def tmp_db(tmp_path: Path) -> Iterator[Path]:
    """
    Create a fresh empty SQLite DB at a temp path, run init_database(),
    rebind both DATABASE_PATH globals, and clean up afterwards.

    Yields the Path to the temp DB file.
    """
    db_path = tmp_path / "test.db"

    import db.connection as dbconn  # noqa: WPS433
    import database as dbmod        # noqa: WPS433

    # Save originals so we can restore on teardown
    orig_conn_path = dbconn.DATABASE_PATH
    orig_mod_path = dbmod.DATABASE_PATH

    dbconn.set_database_path(str(db_path))
    dbmod.DATABASE_PATH = str(db_path)

    # Reset thread-local connection so the new path is picked up
    dbconn.close_db()

    # Initialize schema (and the optional price_history table that
    # several modules — insert_listing, rank_opportunities, snapshots —
    # assume exists).
    dbmod.init_database()

    from db.connection import get_db
    conn = get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS price_history (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_id      TEXT    NOT NULL,
            price           INTEGER NOT NULL,
            date_recorded   TEXT    NOT NULL,
            change_amount   INTEGER,
            change_percent  REAL,
            FOREIGN KEY (listing_id) REFERENCES listings(listing_id)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_listing_price ON price_history(listing_id, date_recorded)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_date_recorded ON price_history(date_recorded)")
    conn.commit()

    yield db_path

    # Teardown: close connection, restore globals
    dbconn.close_db()
    dbconn.set_database_path(orig_conn_path)
    dbmod.DATABASE_PATH = orig_mod_path


@pytest.fixture
def tmp_db_seeded(tmp_db: Path) -> Path:
    """
    `tmp_db` plus a small, predictable set of listings:

        L001  active   €350 000   80 m²  Centro / Sol         (today)
        L002  active   €500 000  100 m²  Salamanca / Goya     (12 d ago)
        L003  active   €600 000  100 m²  Salamanca / Recoletos (60 d ago)
        L004  active   €700 000   90 m²  Retiro / Ibiza       (15 d ago)
        L005  sold     €800 000  120 m²  Chamberí / Almagro   (30 d ago)
        L006  active   €900 000  150 m²  Centro / Sol         (today)

    L004 is intentionally past the 14-day stale threshold.
    L005 is already sold (last_seen 30 d ago).
    """
    import database as dbmod  # noqa: WPS433

    today = datetime.now().date()

    rows = [
        ("L001", "Piso reformado en Sol", 350_000, "Centro", "Sol", 2, 80, "Particular", today, today, "active"),
        ("L002", "Piso luminoso en Goya", 500_000, "Salamanca", "Goya", 3, 100, "Agencia", today - timedelta(days=12), today - timedelta(days=12), "active"),
        ("L003", "Piso amplio Recoletos", 600_000, "Salamanca", "Recoletos", 3, 100, "Agencia", today - timedelta(days=60), today - timedelta(days=60), "active"),
        ("L004", "Piso con terraza en Ibiza", 700_000, "Retiro", "Ibiza", 4, 90, "Particular", today - timedelta(days=15), today - timedelta(days=15), "active"),
        ("L005", "Piso Almagro Chamberí", 800_000, "Chamberí", "Almagro", 4, 120, "Agencia", today - timedelta(days=120), today - timedelta(days=30), "sold_removed"),
        ("L006", "Ático en Sol", 900_000, "Centro", "Sol", 4, 150, "Particular", today, today, "active"),
    ]

    from db.connection import get_db
    conn = get_db()
    conn.executemany(
        """
        INSERT INTO listings
            (listing_id, title, price, distrito, barrio, rooms, size_sqm,
             seller_type, first_seen_date, last_seen_date, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8].isoformat(), r[9].isoformat(), r[10])
            for r in rows
        ],
    )
    conn.commit()

    return tmp_db


# ──────────────────────────────────────────────────────────────────────────
# Pure-Python sample data (no DB)
# ──────────────────────────────────────────────────────────────────────────


@pytest.fixture
def sample_listings() -> List[Dict]:
    """
    A handful of listing dicts (plain Python, no DB) for unit tests that
    work on rows directly (e.g. scoring functions in analytics.py).
    """
    return [
        {
            "listing_id": "L001",
            "distrito": "Centro",
            "barrio": "Sol",
            "price": 350_000,
            "size_sqm": 80,
            "price_per_sqm": 4_375,
            "rooms": 2,
            "seller_type": "Particular",
            "days_on_market": 5,
            "num_drops": 0,
            "total_drop_pct": 0,
        },
        {
            "listing_id": "L002",
            "distrito": "Salamanca",
            "barrio": "Goya",
            "price": 500_000,
            "size_sqm": 100,
            "price_per_sqm": 5_000,
            "rooms": 3,
            "seller_type": "Agencia",
            "days_on_market": 95,
            "num_drops": 2,
            "total_drop_pct": -10,
        },
        {
            "listing_id": "L003",
            "distrito": "Retiro",
            "barrio": "Ibiza",
            "price": 700_000,
            "size_sqm": 90,
            "price_per_sqm": 7_777,
            "rooms": 4,
            "seller_type": "Particular",
            "days_on_market": 150,
            "num_drops": 3,
            "total_drop_pct": -18,
        },
    ]


@pytest.fixture
def sample_distrito_stats() -> Dict[str, Dict[str, float]]:
    """Reference distrito stats for scoring tests."""
    return {
        "Centro":    {"avg_price_sqm": 6_000, "median_price": 500_000},
        "Salamanca": {"avg_price_sqm": 8_500, "median_price": 900_000},
        "Retiro":    {"avg_price_sqm": 7_000, "median_price": 700_000},
    }


@pytest.fixture
def sample_barrio_stats() -> Dict[str, Dict[str, float]]:
    """Reference barrio stats for scoring tests."""
    return {
        "Sol":       {"avg_price_sqm": 5_500, "median_price": 450_000},
        "Goya":      {"avg_price_sqm": 8_500, "median_price": 800_000},
        "Ibiza":     {"avg_price_sqm": 7_200, "median_price": 700_000},
    }
