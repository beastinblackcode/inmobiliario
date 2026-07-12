"""
Unit tests for ``tabs/mi_zona_tab`` — config persistence and the
filter helper.  The rendering layer is not unit-testable without
``streamlit`` runtime; the rest is plain Python over dicts and a
DataFrame.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pytest


# ──────────────────────────────────────────────────────────────────────
# Persistence
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture
def isolated_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point both ``CONFIG_DIR`` (legacy path) and ``user_preferences._FILE_DIR``
    (current path) at a tmp dir so tests don't touch — or read leftover
    state from — the real ``.streamlit/`` folder.
    """
    from tabs import mi_zona_tab as mod
    import user_preferences as up
    monkeypatch.setattr(mod, "CONFIG_DIR", tmp_path)
    monkeypatch.setattr(up,  "_FILE_DIR",  tmp_path)
    # Force a known username so tests don't depend on session state.
    import streamlit as st
    monkeypatch.setitem(st.session_state, "user", "pytest")
    # Default to sqlite so we never hit the Postgres path during unit tests.
    monkeypatch.setenv("DB_BACKEND", "sqlite")
    return tmp_path


def test_load_returns_defaults_when_no_file(isolated_config: Path):
    from tabs.mi_zona_tab import _load_criteria, DEFAULT_CRITERIA
    c = _load_criteria()
    assert c == DEFAULT_CRITERIA


def test_save_and_load_roundtrip(isolated_config: Path):
    from tabs.mi_zona_tab import _load_criteria, _save_criteria
    saved = {
        "barrios":    ["Lavapiés", "Acacias"],
        "max_price":  300_000,
        "min_size":   55,
        "max_size":   80,
        "min_rooms":  2,
        "max_rooms":  3,
        "seller_any": False,
        "ascensor":   True,
    }
    _save_criteria(saved)
    assert _load_criteria() == saved


def test_load_corrupt_file_falls_back_to_defaults(isolated_config: Path):
    from tabs.mi_zona_tab import _load_criteria, DEFAULT_CRITERIA, _config_path
    _config_path().write_text("not valid json {", encoding="utf-8")
    assert _load_criteria() == DEFAULT_CRITERIA


def test_load_partial_file_fills_missing_with_defaults(isolated_config: Path):
    """A pre-existing file from an older version only stored ``barrios``
    and ``max_price``.  ``_load_criteria`` must backfill the rest from
    ``DEFAULT_CRITERIA`` so adding new criteria is always backwards-safe.
    """
    from tabs.mi_zona_tab import _load_criteria, DEFAULT_CRITERIA, _config_path
    _config_path().write_text('{"barrios": ["Sol"], "max_price": 300000}',
                              encoding="utf-8")
    c = _load_criteria()
    assert c["barrios"]    == ["Sol"]
    assert c["max_price"]  == 300_000
    assert c["min_size"]   == DEFAULT_CRITERIA["min_size"]
    assert c["seller_any"] == DEFAULT_CRITERIA["seller_any"]


def test_user_namespace_isolation(isolated_config: Path):
    """Per-user file naming — two users don't clobber each other."""
    from tabs.mi_zona_tab import _load_criteria, _save_criteria
    import streamlit as st

    st.session_state["user"] = "alice"
    _save_criteria({"barrios": ["Centro"], "max_price": 500_000,
                    "min_size": 70, "min_rooms": 2, "max_rooms": 4,
                    "seller_any": True})

    st.session_state["user"] = "bob"
    _save_criteria({"barrios": ["Salamanca"], "max_price": 800_000,
                    "min_size": 90, "min_rooms": 3, "max_rooms": 5,
                    "seller_any": False})

    st.session_state["user"] = "alice"
    assert _load_criteria()["barrios"] == ["Centro"]

    st.session_state["user"] = "bob"
    assert _load_criteria()["barrios"] == ["Salamanca"]


# ──────────────────────────────────────────────────────────────────────
# Filtering
# ──────────────────────────────────────────────────────────────────────


def _sample_df() -> pd.DataFrame:
    """Five listings spanning the filter dimensions."""
    return pd.DataFrame([
        # barrio, price, size_sqm, rooms, seller_type, floor
        {"listing_id": "A", "barrio": "Sol",       "price": 300_000, "size_sqm": 65, "rooms": 2, "seller_type": "Agencia",    "floor": "3ª planta exterior con ascensor"},
        {"listing_id": "B", "barrio": "Sol",       "price": 600_000, "size_sqm": 90, "rooms": 3, "seller_type": "Particular", "floor": "1ª planta exterior sin ascensor"},
        {"listing_id": "C", "barrio": "Lavapiés",  "price": 250_000, "size_sqm": 50, "rooms": 1, "seller_type": "Agencia",    "floor": "Bajo exterior con ascensor"},
        {"listing_id": "D", "barrio": "Lavapiés",  "price": 400_000, "size_sqm": 75, "rooms": 3, "seller_type": "Particular", "floor": None},
        {"listing_id": "E", "barrio": "Salamanca", "price": 800_000, "size_sqm":120, "rooms": 4, "seller_type": "Agencia",    "floor": "4ª planta exterior con ascensor"},
    ])


class TestApplyCriteria:
    def test_barrio_filter(self):
        """With only the barrio filter active (others relaxed)."""
        from tabs.mi_zona_tab import _apply_criteria
        c = {
            "barrios":    ["Sol", "Lavapiés"],
            "max_price":  10_000_000,
            "min_size":   0,
            "min_rooms":  0,
            "max_rooms":  99,
            "seller_any": True,
        }
        out = _apply_criteria(_sample_df(), c)
        assert set(out["listing_id"]) == {"A", "B", "C", "D"}

    def test_price_ceiling(self):
        from tabs.mi_zona_tab import _apply_criteria, DEFAULT_CRITERIA
        c = {**DEFAULT_CRITERIA, "barrios": [], "max_price": 350_000,
             "min_size": 0, "min_rooms": 0, "max_rooms": 99}
        out = _apply_criteria(_sample_df(), c)
        assert set(out["listing_id"]) == {"A", "C"}

    def test_size_floor(self):
        from tabs.mi_zona_tab import _apply_criteria, DEFAULT_CRITERIA
        c = {**DEFAULT_CRITERIA, "barrios": [], "max_price": 10_000_000,
             "min_size": 80, "min_rooms": 0, "max_rooms": 99}
        out = _apply_criteria(_sample_df(), c)
        assert set(out["listing_id"]) == {"B", "E"}

    def test_size_ceiling(self):
        from tabs.mi_zona_tab import _apply_criteria, DEFAULT_CRITERIA
        c = {**DEFAULT_CRITERIA, "barrios": [], "max_price": 10_000_000,
             "min_size": 0, "max_size": 80, "min_rooms": 0, "max_rooms": 99}
        out = _apply_criteria(_sample_df(), c)
        # A 65, C 50, D 75 are ≤ 80; B 90 and E 120 exceed it.
        assert set(out["listing_id"]) == {"A", "C", "D"}

    def test_size_ceiling_none_means_no_bound(self):
        from tabs.mi_zona_tab import _apply_criteria, DEFAULT_CRITERIA
        c = {**DEFAULT_CRITERIA, "barrios": [], "max_price": 10_000_000,
             "min_size": 0, "max_size": None, "min_rooms": 0, "max_rooms": 99}
        out = _apply_criteria(_sample_df(), c)
        assert set(out["listing_id"]) == {"A", "B", "C", "D", "E"}

    def test_ascensor_lenient_excludes_only_explicit_sin(self):
        from tabs.mi_zona_tab import _apply_criteria, DEFAULT_CRITERIA
        c = {**DEFAULT_CRITERIA, "barrios": [], "max_price": 10_000_000,
             "min_size": 0, "min_rooms": 0, "max_rooms": 99, "ascensor": True}
        out = _apply_criteria(_sample_df(), c)
        # A/C/E say "con ascensor" → kept. D floor is unknown → kept (lenient).
        # Only B says "sin ascensor" → dropped.
        assert set(out["listing_id"]) == {"A", "C", "D", "E"}

    def test_rooms_range(self):
        from tabs.mi_zona_tab import _apply_criteria, DEFAULT_CRITERIA
        c = {**DEFAULT_CRITERIA, "barrios": [], "max_price": 10_000_000,
             "min_size": 0, "min_rooms": 3, "max_rooms": 3}
        out = _apply_criteria(_sample_df(), c)
        assert set(out["listing_id"]) == {"B", "D"}

    def test_seller_only_particular(self):
        from tabs.mi_zona_tab import _apply_criteria, DEFAULT_CRITERIA
        c = {**DEFAULT_CRITERIA, "barrios": [], "max_price": 10_000_000,
             "min_size": 0, "min_rooms": 0, "max_rooms": 99,
             "seller_any": False}
        out = _apply_criteria(_sample_df(), c)
        assert set(out["listing_id"]) == {"B", "D"}

    def test_all_filters_combined(self):
        from tabs.mi_zona_tab import _apply_criteria, DEFAULT_CRITERIA
        c = {
            "barrios":    ["Sol", "Lavapiés"],
            "max_price":  500_000,
            "min_size":   60,
            "min_rooms":  2,
            "max_rooms":  3,
            "seller_any": True,
        }
        out = _apply_criteria(_sample_df(), c)
        # A: 65m² 2h, Sol, 300k → matches.
        # B: 90m² 3h, Sol, 600k → fails on price.
        # C: 50m² 1h, Lavapiés → fails on size + rooms.
        # D: 75m² 3h, Lavapiés, 400k, Particular → matches.
        assert set(out["listing_id"]) == {"A", "D"}
