"""
Integration tests for ``tabs.detail_tab._get_comparables``.

The helper is the only piece of the comparables view that's
unit-testable without a Streamlit runtime — the rest is rendering.
We exercise the SQL filter cascade (barrio → distrito fallback) and
the structural filters (size ±20%, rooms ±1) on a tmp SQLite seeded
with a hand-built mini-universe.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterator

import pytest


pytestmark = pytest.mark.integration


@pytest.fixture
def seeded_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """Mini-universe with a clear target + comparables + distractors."""
    db = tmp_path / "comps.db"
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

        INSERT INTO listings (listing_id, title, url, price, distrito, barrio,
                              size_sqm, rooms, floor, seller_type, status,
                              description, first_seen_date, last_seen_date)
        VALUES
            -- Target: Centro·Sol, 80 m², 3 rooms, €400k
            ('TARGET', 'Target', 'http://t', 400000, 'Centro', 'Sol',
             80.0, 3, '3', 'Agencia', 'active', '', '2026-01-01', '2026-05-01'),

            -- Comparables: same barrio, size 64-96 m², rooms 2-4.
            ('C1', 'Comp 1', 'http://c1', 380000, 'Centro', 'Sol',
             75.0, 3, '2', 'Agencia',    'active', '', '2026-01-15', '2026-05-01'),
            ('C2', 'Comp 2', 'http://c2', 420000, 'Centro', 'Sol',
             85.0, 3, '4', 'Particular', 'active', '', '2026-02-01', '2026-05-01'),
            ('C3', 'Comp 3', 'http://c3', 360000, 'Centro', 'Sol',
             70.0, 2, '1', 'Agencia',    'active', '', '2026-02-10', '2026-05-01'),

            -- Excluded by size (60 m² is below 80*0.8=64)
            ('FAR_SIZE', 'Too small', 'http://fs', 280000, 'Centro', 'Sol',
             50.0, 2, '5', 'Agencia',    'active', '', '2026-02-01', '2026-05-01'),

            -- Excluded by rooms (5 is beyond 3+1)
            ('FAR_ROOMS', 'Too big',   'http://fr', 500000, 'Centro', 'Sol',
             80.0, 5, '6', 'Agencia',    'active', '', '2026-02-01', '2026-05-01'),

            -- Same distrito, different barrio: only matters in pass 2.
            ('DIST1', 'Other barrio', 'http://d1', 390000, 'Centro', 'Lavapiés',
             78.0, 3, '3', 'Agencia',    'active', '', '2026-02-01', '2026-05-01'),

            -- Sold listing: excluded.
            ('SOLD',  'Sold',         'http://sold', 410000, 'Centro', 'Sol',
             80.0, 3, '4', 'Agencia',    'sold_removed', '',
             '2025-09-01', '2025-12-01');
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


def _target() -> dict:
    return {
        "listing_id": "TARGET", "price": 400000, "distrito": "Centro",
        "barrio": "Sol", "size_sqm": 80.0, "rooms": 3,
    }


# ──────────────────────────────────────────────────────────────────────
# Filter behaviour
# ──────────────────────────────────────────────────────────────────────


def test_barrio_pass_returns_only_structurally_similar(seeded_db: Path):
    from tabs.detail_tab import _get_comparables
    rows, scope = _get_comparables(_target(), limit=10)
    ids = {r["listing_id"] for r in rows}
    assert scope == "barrio"
    # C1, C2, C3 match.  FAR_SIZE excluded by size, FAR_ROOMS by rooms,
    # DIST1 by barrio, SOLD by status, TARGET by listing_id.
    assert ids == {"C1", "C2", "C3"}


def test_size_band_is_20_percent(seeded_db: Path):
    """A 96.1 m² listing should still match (within 80×1.2=96)."""
    from tabs.detail_tab import _get_comparables
    rows, _scope = _get_comparables(
        {**_target(), "size_sqm": 80.0}, limit=10,
    )
    # All returned listings within size_sqm ∈ [64, 96]
    for r in rows:
        assert 64.0 <= r["size_sqm"] <= 96.0


def test_room_band_is_plus_minus_one(seeded_db: Path):
    """Target rooms=3 should match comps with 2-4 rooms only."""
    from tabs.detail_tab import _get_comparables
    rows, _scope = _get_comparables(_target(), limit=10)
    for r in rows:
        assert 2 <= int(r["rooms"]) <= 4


def test_excludes_self_and_sold_removed(seeded_db: Path):
    from tabs.detail_tab import _get_comparables
    rows, _ = _get_comparables(_target(), limit=10)
    ids = {r["listing_id"] for r in rows}
    assert "TARGET" not in ids
    assert "SOLD"   not in ids


def test_distrito_fallback_when_barrio_empty(seeded_db: Path, monkeypatch):
    """If barrio yields zero, the helper should widen to distrito."""
    # Force the target's barrio to one with zero comparables.
    target_in_empty_barrio = {**_target(), "barrio": "Embajadores"}
    from tabs.detail_tab import _get_comparables
    rows, scope = _get_comparables(target_in_empty_barrio, limit=10)
    ids = {r["listing_id"] for r in rows}
    assert scope == "distrito"
    # DIST1 (Lavapiés) + the Sol comparables (different barrio but same distrito)
    # should now show up.
    assert "DIST1" in ids
    assert "C1" in ids  # Sol listings also in Centro distrito


def test_size_zero_returns_empty(seeded_db: Path):
    """A listing without size info is uncomparable."""
    from tabs.detail_tab import _get_comparables
    rows, _ = _get_comparables({**_target(), "size_sqm": None}, limit=10)
    assert rows == []


def test_limit_respected(seeded_db: Path):
    from tabs.detail_tab import _get_comparables
    rows, _ = _get_comparables(_target(), limit=2)
    assert len(rows) == 2


def test_ordering_by_size_proximity(seeded_db: Path):
    """The most size-close comparable should appear first."""
    from tabs.detail_tab import _get_comparables
    rows, _ = _get_comparables(_target(), limit=10)
    # Target=80. C2 (85) is 5 away, C1 (75) is 5 away, C3 (70) is 10 away.
    # Within same delta the tiebreak is price-per-sqm closeness.
    # We just assert C3 is last (largest size delta).
    assert rows[-1]["listing_id"] == "C3"
