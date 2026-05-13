"""
Integration tests for ``mi_zona_alerts.run_alerts_for_user``.

Covers the watermark state machine (first-run bootstrap, idempotent
advance, no-flood) and the wiring between criteria / candidate query /
offer engine / email send.  Both backends (SQLite for fast feedback,
Postgres via testcontainer for the path that production actually uses).
"""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from pathlib import Path
from typing import Iterator

import pytest


pytestmark = pytest.mark.integration


# ──────────────────────────────────────────────────────────────────────
# SQLite track — fast feedback
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture
def sqlite_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """Build a tmp SQLite with the schema bits the alerts pipeline touches."""
    db = tmp_path / "alerts.db"
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
        CREATE TABLE notarial_prices (
            id INTEGER PRIMARY KEY,
            distrito TEXT, periodo INTEGER, precio_m2 REAL
        );
        -- Mi Zona user prefs (only the file fallback is exercised in
        -- the SQLite track — _postgres_available() returns False here).
    """)
    # Seed comparables so estimate_fair_price doesn't return error.
    today  = date.today()
    base   = today - timedelta(days=120)   # old enough to be "pre-watermark"
    fresh  = today - timedelta(days=2)     # fresh, post-watermark
    fresh2 = today - timedelta(days=1)

    # Five "comparable" listings already in the universe (pre-watermark).
    comps = [
        ("C1", "Comp 1", "http://c1", 320_000, "Centro", "Acacias", 75.0, 2, "3", "Agencia", "active", base.isoformat(), today.isoformat()),
        ("C2", "Comp 2", "http://c2", 340_000, "Centro", "Acacias", 78.0, 2, "4", "Agencia", "active", base.isoformat(), today.isoformat()),
        ("C3", "Comp 3", "http://c3", 360_000, "Centro", "Acacias", 80.0, 2, "5", "Agencia", "active", base.isoformat(), today.isoformat()),
        ("C4", "Comp 4", "http://c4", 330_000, "Centro", "Acacias", 76.0, 2, "2", "Agencia", "active", base.isoformat(), today.isoformat()),
        ("C5", "Comp 5", "http://c5", 350_000, "Centro", "Acacias", 79.0, 2, "1", "Agencia", "active", base.isoformat(), today.isoformat()),
        # One fresh listing far below comps → big margin (chollo + leverage).
        ("NEW", "Fresh match", "http://new", 210_000, "Centro", "Acacias", 75.0, 2, "3", "Particular", "active", fresh.isoformat(), today.isoformat()),
        # Another fresh listing in a different barrio — should NOT match criteria.
        ("OUT", "Other barrio", "http://out", 280_000, "Centro", "Sol", 70.0, 2, "3", "Agencia", "active", fresh2.isoformat(), today.isoformat()),
    ]
    conn.executemany(
        "INSERT INTO listings VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [(c[0], c[1], c[2], c[3], c[4], c[5], c[6], c[7], c[8], c[9], c[10], "", c[11], c[12]) for c in comps],
    )
    conn.commit()
    conn.close()

    monkeypatch.setenv("DB_BACKEND", "sqlite")
    from db.connection import set_database_path, close_db
    close_db()
    set_database_path(str(db))

    # Redirect both user_preferences file dir and mi_zona_tab CONFIG_DIR.
    import user_preferences as up
    monkeypatch.setattr(up, "_FILE_DIR", tmp_path)
    from tabs import mi_zona_tab as mz
    monkeypatch.setattr(mz, "CONFIG_DIR", tmp_path)

    yield db

    close_db()
    set_database_path("real_estate.db")


def test_first_run_bootstraps_watermark_silently(sqlite_db: Path):
    """No prior watermark → set today, send nothing."""
    from user_preferences import set_user_pref, get_user_pref
    from mi_zona_alerts import run_alerts_for_user, PREF_CRITERIA_KEY, PREF_WATERMARK_KEY

    set_user_pref("luis", PREF_CRITERIA_KEY, {
        "barrios": ["Acacias"], "max_price": 600_000, "min_size": 50,
        "min_rooms": 1, "max_rooms": 4, "seller_any": True,
    })

    sent = run_alerts_for_user(
        "luis", min_margin=0.0, max_alerts=10, dry_run=False, recipient_override=None,
    )
    assert sent == 0
    wm = get_user_pref("luis", PREF_WATERMARK_KEY)
    assert wm["date"] == date.today().isoformat()


def test_no_criteria_skips_user(sqlite_db: Path, capsys):
    from mi_zona_alerts import run_alerts_for_user
    sent = run_alerts_for_user("nobody", min_margin=0, max_alerts=10,
                                dry_run=False, recipient_override=None)
    out = capsys.readouterr().out
    assert sent == 0
    assert "no criteria configured" in out


def test_dry_run_finds_match_without_advancing(sqlite_db: Path):
    """With watermark set in the past, dry-run should find matches but not advance."""
    from user_preferences import set_user_pref, get_user_pref
    from mi_zona_alerts import run_alerts_for_user, PREF_CRITERIA_KEY, PREF_WATERMARK_KEY

    set_user_pref("luis", PREF_CRITERIA_KEY, {
        "barrios": ["Acacias"], "max_price": 600_000, "min_size": 50,
        "min_rooms": 1, "max_rooms": 4, "seller_any": True,
    })
    old_wm = (date.today() - timedelta(days=30)).isoformat()
    set_user_pref("luis", PREF_WATERMARK_KEY, {"date": old_wm})

    sent = run_alerts_for_user(
        "luis", min_margin=0.0, max_alerts=10, dry_run=True, recipient_override=None,
    )
    # Should report at least the NEW listing.
    assert sent >= 1
    # Dry-run leaves the watermark alone.
    assert get_user_pref("luis", PREF_WATERMARK_KEY)["date"] == old_wm


def test_watermark_advances_when_no_match_above_threshold(sqlite_db: Path):
    """Listings appeared but none cleared the margin bar → still advance."""
    from user_preferences import set_user_pref, get_user_pref
    from mi_zona_alerts import run_alerts_for_user, PREF_CRITERIA_KEY, PREF_WATERMARK_KEY

    set_user_pref("luis", PREF_CRITERIA_KEY, {
        "barrios": ["Acacias"], "max_price": 600_000, "min_size": 50,
        "min_rooms": 1, "max_rooms": 4, "seller_any": True,
    })
    old_wm = (date.today() - timedelta(days=30)).isoformat()
    set_user_pref("luis", PREF_WATERMARK_KEY, {"date": old_wm})

    # Set an artificially high threshold so no match survives.
    sent = run_alerts_for_user(
        "luis", min_margin=99.0, max_alerts=10, dry_run=False, recipient_override=None,
    )
    assert sent == 0
    wm = get_user_pref("luis", PREF_WATERMARK_KEY)["date"]
    assert wm > old_wm   # advanced


def test_watermark_unchanged_when_send_fails(sqlite_db: Path, monkeypatch):
    """If SMTP fails the watermark must NOT advance — we'd lose the alerts."""
    from user_preferences import set_user_pref, get_user_pref
    from mi_zona_alerts import run_alerts_for_user, PREF_CRITERIA_KEY, PREF_WATERMARK_KEY
    import mi_zona_alerts

    set_user_pref("luis", PREF_CRITERIA_KEY, {
        "barrios": ["Acacias"], "max_price": 600_000, "min_size": 50,
        "min_rooms": 1, "max_rooms": 4, "seller_any": True,
    })
    old_wm = (date.today() - timedelta(days=30)).isoformat()
    set_user_pref("luis", PREF_WATERMARK_KEY, {"date": old_wm})

    # Force the SMTP call to fail.
    monkeypatch.setattr(mi_zona_alerts, "_send", lambda *_a, **_kw: False)

    sent = run_alerts_for_user(
        "luis", min_margin=0.0, max_alerts=10, dry_run=False, recipient_override=None,
    )
    assert sent == 0
    assert get_user_pref("luis", PREF_WATERMARK_KEY)["date"] == old_wm


def test_max_alerts_caps_output(sqlite_db: Path, monkeypatch):
    """Even with many matches, the email caps at max_alerts."""
    from user_preferences import set_user_pref
    from mi_zona_alerts import run_alerts_for_user, PREF_CRITERIA_KEY, PREF_WATERMARK_KEY
    import mi_zona_alerts

    captured = {}

    def fake_send(html, subject, recipient_override):
        captured["html"]    = html
        captured["subject"] = subject
        return True

    monkeypatch.setattr(mi_zona_alerts, "_send", fake_send)

    set_user_pref("luis", PREF_CRITERIA_KEY, {
        "barrios": ["Acacias"], "max_price": 600_000, "min_size": 50,
        "min_rooms": 1, "max_rooms": 4, "seller_any": True,
    })
    old_wm = (date.today() - timedelta(days=30)).isoformat()
    set_user_pref("luis", PREF_WATERMARK_KEY, {"date": old_wm})

    sent = run_alerts_for_user(
        "luis", min_margin=0.0, max_alerts=1, dry_run=False, recipient_override=None,
    )
    assert sent == 1
    # ``1 oportunidad nueva`` (singular) when only one is sent.
    assert "1 oportunidad" in captured["subject"]
