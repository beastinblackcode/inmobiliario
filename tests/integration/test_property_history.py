"""
Integration tests for ``property_history.get_property_history`` and
``get_republication_counts``.

These run against the real Alembic schema on Postgres (via the
testcontainers fixture from ``conftest.py``) plus an inline SQLite
variant for fast feedback during local dev.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterator

import pytest


pytestmark = pytest.mark.integration


_DESC = (
    "Magnífico piso reformado con tres dormitorios, dos baños, salón "
    "amplio, cocina equipada, terraza y ascensor."
)


# ──────────────────────────────────────────────────────────────────────
# SQLite track — fast local feedback
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture
def sqlite_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """Seed a tmp SQLite with two listings of the same property + one alone."""
    db = tmp_path / "ph.db"
    conn = sqlite3.connect(db)
    conn.executescript("""
        CREATE TABLE listings (
            listing_id      TEXT PRIMARY KEY,
            title           TEXT,
            url             TEXT,
            price           INTEGER,
            distrito        TEXT,
            barrio          TEXT,
            size_sqm        REAL,
            rooms           INTEGER,
            floor           TEXT,
            seller_type     TEXT,
            status          TEXT,
            description     TEXT,
            first_seen_date TEXT,
            last_seen_date  TEXT
        );
        CREATE TABLE price_history (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_id      TEXT NOT NULL,
            price           INTEGER NOT NULL,
            date_recorded   TEXT NOT NULL,
            change_amount   INTEGER,
            change_percent  REAL
        );
        CREATE TABLE property_fingerprints (
            property_id           INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_count         INTEGER NOT NULL DEFAULT 1,
            republication_count   INTEGER NOT NULL DEFAULT 0,
            first_seen_date       TEXT,
            last_seen_date        TEXT,
            total_days_on_market  INTEGER,
            distrito              TEXT,
            barrio                TEXT,
            size_sqm              REAL,
            rooms                 INTEGER,
            floor                 TEXT,
            computed_at           TEXT
        );
        CREATE TABLE listing_property_map (
            listing_id  TEXT PRIMARY KEY REFERENCES listings(listing_id),
            property_id INTEGER NOT NULL REFERENCES property_fingerprints(property_id)
        );

        -- Two listings of the same property (republication).
        INSERT INTO listings VALUES
            ('L1','Piso L1','http://l1', 320000,'Centro','Sol',80.0,3,'3','Agencia','sold_removed','...',
             '2024-03-01','2024-06-15'),
            ('L2','Piso L2','http://l2', 295000,'Centro','Sol',80.0,3,'3','Particular','active','...',
             '2025-10-01','2026-02-10');

        -- An unrelated listing that lives alone (singleton property).
        INSERT INTO listings VALUES
            ('S1','Singleton','http://s1',180000,'Centro','Sol',55.0,2,'5','Agencia','active','...',
             '2026-02-15','2026-02-20');

        -- Price history. L1 dropped twice, L2 dropped once.
        INSERT INTO price_history (listing_id, price, date_recorded, change_amount, change_percent) VALUES
            ('L1', 320000, '2024-03-01',  NULL,  NULL),
            ('L1', 310000, '2024-04-10', -10000, -3.12),
            ('L1', 305000, '2024-05-20',  -5000, -1.61),
            ('L2', 300000, '2025-10-01',  NULL,  NULL),
            ('L2', 295000, '2026-01-15',  -5000, -1.66);

        -- Two property rows: one with L1+L2, one with S1.
        INSERT INTO property_fingerprints VALUES
            (1, 2, 1, '2024-03-01','2026-02-10', 240, 'Centro','Sol',80.0,3,'3','2026-05-12'),
            (2, 1, 0, '2026-02-15','2026-02-20',   5, 'Centro','Sol',55.0,2,'5','2026-05-12');

        INSERT INTO listing_property_map VALUES
            ('L1', 1),
            ('L2', 1),
            ('S1', 2);
    """)
    conn.commit()
    conn.close()

    monkeypatch.setenv("DB_BACKEND", "sqlite")
    from db.connection import set_database_path, close_db
    close_db()
    set_database_path(str(db))

    yield db

    close_db()
    set_database_path("real_estate.db")


def test_sqlite_property_history_for_republished_listing(sqlite_db: Path):
    """``L1`` and ``L2`` resolve to the same property with 2 listings."""
    from property_history import get_property_history

    ph_l1 = get_property_history("L1")
    ph_l2 = get_property_history("L2")
    assert ph_l1 is not None and ph_l2 is not None
    assert ph_l1.property_id == ph_l2.property_id == 1
    assert ph_l1.listing_count == 2
    assert ph_l1.republication_count == 1
    assert [l.listing_id for l in ph_l1.listings] == ["L1", "L2"]

    # Per-listing detail: L1 has 2 drops (initial row's change is NULL,
    # so the two subsequent rows count).
    l1, l2 = ph_l1.listings
    assert l1.n_drops == 2
    assert l1.accumulated_drop_eur == -15000   # -10k + -5k
    assert l1.initial_price == 320000           # earliest stored price
    assert l1.final_price   == 320000           # current listings.price (sold, hasn't changed)
    assert l2.n_drops == 1
    assert l2.initial_price == 300000
    assert l2.final_price   == 295000

    # Cross-listing aggregates.
    assert ph_l1.first_asking_price == 320000   # earliest listing's opening price
    assert ph_l1.final_price_overall == 295000  # current price of latest listing
    assert ph_l1.cumulative_change_eur == -25000
    assert abs(ph_l1.cumulative_change_pct - (-7.81)) < 0.01


def test_sqlite_singleton_returns_single_entry(sqlite_db: Path):
    """``S1`` resolves to a singleton fingerprint with one listing."""
    from property_history import get_property_history

    ph = get_property_history("S1")
    assert ph is not None
    assert ph.listing_count == 1
    assert ph.republication_count == 0
    assert len(ph.listings) == 1
    assert ph.listings[0].listing_id == "S1"


def test_sqlite_unmapped_listing_returns_none(sqlite_db: Path):
    """A listing absent from ``listing_property_map`` yields ``None``."""
    from property_history import get_property_history
    assert get_property_history("DOES_NOT_EXIST") is None


def test_sqlite_bulk_republication_counts(sqlite_db: Path):
    """``get_republication_counts`` returns 0 for singletons and unknowns."""
    from property_history import get_republication_counts

    counts = get_republication_counts(["L1", "L2", "S1", "MISSING"])
    assert counts["L1"]      == 1
    assert counts["L2"]      == 1
    assert counts["S1"]      == 0
    assert counts["MISSING"] == 0


def test_sqlite_empty_input_returns_empty(sqlite_db: Path):
    from property_history import get_republication_counts
    assert get_republication_counts([]) == {}


# ──────────────────────────────────────────────────────────────────────
# Postgres track — same scenarios, real Alembic schema
# ──────────────────────────────────────────────────────────────────────


def test_postgres_end_to_end(tmp_pg_db: str, monkeypatch: pytest.MonkeyPatch):
    """Seed PG, run the helper, verify property + listing aggregation."""
    import psycopg

    monkeypatch.setenv("DB_BACKEND", "postgres")

    with psycopg.connect(tmp_pg_db) as raw:
        raw.execute("""
            INSERT INTO listings (listing_id, title, url, price, distrito, barrio,
                                  size_sqm, rooms, floor, seller_type, status,
                                  description, first_seen_date, last_seen_date)
            VALUES
              ('L1','Piso L1','http://l1', 320000,'Centro','Sol',80.0,3,'3','Agencia',  'sold_removed',%s,'2024-03-01','2024-06-15'),
              ('L2','Piso L2','http://l2', 295000,'Centro','Sol',80.0,3,'3','Particular','active',     %s,'2025-10-01','2026-02-10')
        """, (_DESC, _DESC))
        raw.execute("""
            INSERT INTO price_history (listing_id, price, date_recorded, change_amount, change_percent)
            VALUES
              ('L1', 320000, '2024-03-01', NULL,   NULL),
              ('L1', 310000, '2024-04-10', -10000, -3.12),
              ('L1', 305000, '2024-05-20', -5000,  -1.61),
              ('L2', 300000, '2025-10-01', NULL,   NULL),
              ('L2', 295000, '2026-01-15', -5000,  -1.66)
        """)
        cur = raw.execute("""
            INSERT INTO property_fingerprints (
                listing_count, republication_count,
                first_seen_date, last_seen_date, total_days_on_market,
                distrito, barrio, size_sqm, rooms, floor
            ) VALUES (2, 1, '2024-03-01', '2026-02-10', 240, 'Centro', 'Sol', 80.0, 3, '3')
            RETURNING property_id
        """)
        pid = cur.fetchone()[0]
        raw.execute(
            "INSERT INTO listing_property_map (listing_id, property_id) VALUES (%s, %s), (%s, %s)",
            ("L1", pid, "L2", pid),
        )
        raw.commit()

    from db.connection import close_db
    close_db()
    from property_history import get_property_history, get_republication_counts

    ph = get_property_history("L1")
    assert ph is not None
    assert ph.listing_count == 2
    assert ph.republication_count == 1
    assert ph.cumulative_change_eur == -25000

    counts = get_republication_counts(["L1", "L2", "UNKNOWN"])
    assert counts == {"L1": 1, "L2": 1, "UNKNOWN": 0}

    close_db()
