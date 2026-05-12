"""
Integration tests for the property-fingerprint pipeline.

These exercise ``compute_property_fingerprints._wipe / _write_property /
_write_mappings`` against the real Alembic schema on Postgres (when
Docker is available) and on SQLite (always).  We assert that:

  * The matcher's output gets persisted faithfully.
  * Idempotency: re-running wipes and re-creates.
  * Schema integrity: FK to ``listings`` is honoured, cascades on
    delete clean both tables.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterator

import pytest

from property_fingerprints import cluster_listings


pytestmark = pytest.mark.integration


_SHARED_DESC = (
    "Magnífico piso reformado en pleno centro del barrio. Tres dormitorios, "
    "dos baños, salón comedor amplio, cocina equipada. Edificio con ascensor."
)


# ──────────────────────────────────────────────────────────────────────
# SQLite track
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture
def sqlite_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """Build a tmp SQLite with the 3 tables we touch, plus a few listings."""
    db = tmp_path / "fp.db"
    conn = sqlite3.connect(db)
    conn.executescript("""
        CREATE TABLE listings (
            listing_id      TEXT PRIMARY KEY,
            distrito        TEXT,
            barrio          TEXT,
            size_sqm        REAL,
            rooms           INTEGER,
            floor           TEXT,
            description     TEXT,
            first_seen_date TEXT,
            last_seen_date  TEXT
        );
        CREATE TABLE property_fingerprints (
            property_id          INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_count        INTEGER NOT NULL DEFAULT 1,
            republication_count  INTEGER NOT NULL DEFAULT 0,
            first_seen_date      TEXT,
            last_seen_date       TEXT,
            total_days_on_market INTEGER,
            distrito             TEXT,
            barrio               TEXT,
            size_sqm             REAL,
            rooms                INTEGER,
            floor                TEXT,
            computed_at          TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE listing_property_map (
            listing_id  TEXT PRIMARY KEY REFERENCES listings(listing_id) ON DELETE CASCADE,
            property_id INTEGER NOT NULL REFERENCES property_fingerprints(property_id) ON DELETE CASCADE
        );
    """)
    conn.executemany(
        "INSERT INTO listings VALUES (?,?,?,?,?,?,?,?,?)",
        [
            # Two clones: same flat republished.
            ("L1", "Arganzuela", "Acacias", 90.0, 3, "3", _SHARED_DESC, "2024-03-01", "2024-06-15"),
            ("L2", "Arganzuela", "Acacias", 90.5, 3, "3", _SHARED_DESC, "2025-10-01", "2026-02-10"),
            # Unrelated listing: different barrio.
            ("L3", "Centro",     "Sol",     60.0, 2, "5", _SHARED_DESC, "2026-01-15", "2026-02-20"),
        ],
    )
    conn.commit()
    conn.close()

    monkeypatch.setenv("DB_BACKEND", "sqlite")
    from db.connection import set_database_path, close_db
    close_db()
    set_database_path(str(db))

    yield db

    close_db()
    set_database_path("real_estate.db")


def test_sqlite_end_to_end_writes_expected_rows(sqlite_db: Path):
    """Load → cluster → write produces the expected shape on SQLite."""
    from compute_property_fingerprints import (
        _load_all_listings,
        _wipe,
        _write_property,
        _write_mappings,
    )
    from db.connection import get_connection

    with get_connection() as conn:
        rows = _load_all_listings(conn)
        assert len(rows) == 3

        props = cluster_listings(rows)
        assert len(props) == 2  # {L1,L2} clustered + L3 singleton

        _wipe(conn)
        mappings = []
        for p in props:
            pid = _write_property(conn, p)
            mappings.extend((lid, pid) for lid in p.listing_ids)
        _write_mappings(conn, mappings)

    # Verify both tables.
    conn = sqlite3.connect(sqlite_db)
    fps = conn.execute(
        "SELECT listing_count, republication_count FROM property_fingerprints "
        "ORDER BY listing_count DESC"
    ).fetchall()
    assert fps == [(2, 1), (1, 0)]

    n_map = conn.execute("SELECT COUNT(*) FROM listing_property_map").fetchone()[0]
    assert n_map == 3
    conn.close()


def test_sqlite_rerun_is_idempotent(sqlite_db: Path):
    """Re-running wipes + rebuilds; final state matches the first run."""
    from compute_property_fingerprints import (
        _load_all_listings,
        _wipe,
        _write_property,
        _write_mappings,
    )
    from db.connection import get_connection

    def _populate():
        with get_connection() as conn:
            rows = _load_all_listings(conn)
            props = cluster_listings(rows)
            _wipe(conn)
            mappings = []
            for p in props:
                pid = _write_property(conn, p)
                mappings.extend((lid, pid) for lid in p.listing_ids)
            _write_mappings(conn, mappings)

    _populate()
    _populate()  # second time

    conn = sqlite3.connect(sqlite_db)
    assert conn.execute("SELECT COUNT(*) FROM property_fingerprints").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM listing_property_map").fetchone()[0] == 3
    conn.close()


# ──────────────────────────────────────────────────────────────────────
# Postgres track — runs only when Docker is available.
# ──────────────────────────────────────────────────────────────────────


def test_postgres_end_to_end(tmp_pg_db: str, monkeypatch: pytest.MonkeyPatch):
    """Same end-to-end against a real Postgres provisioned by Alembic."""
    import psycopg

    monkeypatch.setenv("DB_BACKEND", "postgres")

    # Seed listings table.
    with psycopg.connect(tmp_pg_db) as raw:
        raw.execute("""
            INSERT INTO listings (listing_id, distrito, barrio, size_sqm,
                                  rooms, floor, description,
                                  first_seen_date, last_seen_date, status)
            VALUES
                ('L1','Arganzuela','Acacias',90.0,3,'3',%s,'2024-03-01','2024-06-15','sold_removed'),
                ('L2','Arganzuela','Acacias',90.5,3,'3',%s,'2025-10-01','2026-02-10','active'),
                ('L3','Centro',    'Sol',    60.0,2,'5',%s,'2026-01-15','2026-02-20','active')
        """, (_SHARED_DESC, _SHARED_DESC, _SHARED_DESC))
        raw.commit()

    from compute_property_fingerprints import (
        _load_all_listings,
        _wipe,
        _write_property,
        _write_mappings,
    )
    from db.connection import get_connection, close_db
    close_db()  # drop any pool conn left from another test

    with get_connection() as conn:
        rows = _load_all_listings(conn)
        assert len(rows) == 3
        props = cluster_listings(rows)
        assert len(props) == 2
        _wipe(conn)
        mappings = []
        for p in props:
            pid = _write_property(conn, p)
            mappings.extend((lid, pid) for lid in p.listing_ids)
        _write_mappings(conn, mappings)

    close_db()
    # Verify with a fresh raw connection so we see committed state.
    with psycopg.connect(tmp_pg_db) as raw:
        fps = list(raw.execute(
            "SELECT listing_count, republication_count FROM property_fingerprints "
            "ORDER BY listing_count DESC"
        ).fetchall())
        assert fps == [(2, 1), (1, 0)]
        assert raw.execute("SELECT COUNT(*) FROM listing_property_map").fetchone()[0] == 3


def test_postgres_cascade_delete_cleans_map(tmp_pg_db: str, monkeypatch: pytest.MonkeyPatch):
    """Deleting a listing should cascade through ``listing_property_map``."""
    import psycopg

    monkeypatch.setenv("DB_BACKEND", "postgres")

    with psycopg.connect(tmp_pg_db) as raw:
        raw.execute("""
            INSERT INTO listings (listing_id, distrito, barrio, size_sqm,
                                  rooms, floor, description,
                                  first_seen_date, last_seen_date, status)
            VALUES ('A','Centro','Sol',60.0,2,'5',%s,'2026-01-15','2026-02-20','active')
        """, (_SHARED_DESC,))
        raw.commit()

    from compute_property_fingerprints import _write_property, _write_mappings
    from db.connection import get_connection, close_db
    close_db()
    with get_connection() as conn:
        from property_fingerprints import Property
        prop = Property(
            listing_ids=["A"], distrito="Centro", barrio="Sol",
            size_sqm=60.0, rooms=2, floor="5",
        )
        pid = _write_property(conn, prop)
        _write_mappings(conn, [("A", pid)])

    close_db()
    with psycopg.connect(tmp_pg_db) as raw:
        raw.execute("DELETE FROM listings WHERE listing_id = 'A'")
        raw.commit()
        n = raw.execute("SELECT COUNT(*) FROM listing_property_map").fetchone()[0]
        assert n == 0  # cascade removed the map row
