"""
Integration tests for export_clean_metrics.py.

We exercise the real script against a temporary DB seeded with notarial
rows.  Macro / CGPJ / morosidad loaders are monkey-patched to return
deterministic values so the test doesn't hit the network.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Iterator

import pytest

import export_clean_metrics as exp

pytestmark = pytest.mark.integration


# ──────────────────────────────────────────────────────────────────────────
# Fixtures local to this file
# ──────────────────────────────────────────────────────────────────────────


@pytest.fixture
def notarial_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """Build a tiny SQLite DB with a populated `notarial_prices` table only."""
    db = tmp_path / "clean_test.db"
    conn = sqlite3.connect(db)
    conn.execute("""
        CREATE TABLE notarial_prices (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            distrito  TEXT NOT NULL,
            periodo   INTEGER NOT NULL,
            precio_m2 REAL NOT NULL,
            UNIQUE(distrito, periodo)
        )
    """)
    conn.executemany(
        "INSERT INTO notarial_prices(distrito, periodo, precio_m2) VALUES (?, ?, ?)",
        [
            # Latest periodo (2025T4) — these are the ones the export should pick
            ("Centro",       202504, 6_000),
            ("Salamanca",    202504, 8_500),
            ("Chamberí",     202504, 7_200),
            # Older periodo — must be ignored
            ("Centro",       202503, 5_900),
        ],
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(exp, "_DATABASE_PATH", str(db))

    # Stub out the network-bound loaders
    monkeypatch.setattr(exp, "_load_macro", lambda: {
        "euribor":  {"name": "Euríbor 12m", "current": 3.2, "trend": "down", "unit": "%"},
        "ipc":      {"name": "IPC", "current": 2.8, "trend": "stable", "unit": "%"},
        "ipv":      {"name": "IPV", "current": 6.5, "trend": "up", "unit": "%"},
    })
    # _load_lanzamientos now takes the SQLite conn — match the signature
    monkeypatch.setattr(exp, "_load_lanzamientos", lambda conn: {
        "name": "Lanzamientos", "current": 240, "unit": "trim", "trend": "stable",
    })

    yield db


# ──────────────────────────────────────────────────────────────────────────
# Builder integration
# ──────────────────────────────────────────────────────────────────────────


class TestBuildCleanMetrics:
    def test_build_returns_expected_shape(self, notarial_db: Path):
        metrics = exp.build_clean_metrics()
        # Top-level keys must match the legacy schema (so the front doesn't break)
        for key in (
            "metadata", "market_score", "indicators", "macro", "zones",
            "rental_yields", "trends", "notarial_gap", "barrios",
            "barrio_trends", "price_drop_stats", "seller_stats", "db_stats",
            "alerts", "valuation_model",
        ):
            assert key in metrics, f"missing top-level key: {key}"

    def test_zones_only_pick_latest_notarial_periodo(self, notarial_db: Path):
        metrics = exp.build_clean_metrics()
        zones = metrics["zones"]
        # 3 distritos at periodo 202504; 1 at 202503 must NOT be returned
        assert len(zones) == 3
        assert {z["name"] for z in zones} == {"Centro", "Salamanca", "Chamberí"}
        for z in zones:
            assert z["notarial_period"] == 202504
            # Listing-derived fields must be null
            assert z["active_count"] is None
            assert z["days_to_sell"] is None

    def test_indicators_block_only_contains_clean_keys(self, notarial_db: Path):
        metrics = exp.build_clean_metrics()
        keys = set(metrics["indicators"].keys())
        assert keys == {"affordability", "lanzamientos", "morosidad"}

    def test_listing_derived_fields_are_empty(self, notarial_db: Path):
        metrics = exp.build_clean_metrics()
        assert metrics["price_drop_stats"] is None
        assert metrics["seller_stats"] is None
        assert metrics["valuation_model"] is None
        assert metrics["barrio_trends"] == []
        assert metrics["notarial_gap"] == []
        assert metrics["rental_yields"] == []
        assert metrics["trends"] == {"market": [], "by_district": []}

    def test_barrios_array_is_populated_with_distrito_proxy(self, notarial_db: Path):
        metrics = exp.build_clean_metrics()
        barrios = metrics["barrios"]
        assert len(barrios) > 100, "should contain all canonical barrios"

        # Every barrio in Centro must inherit Centro's notarial price (6_000)
        centro_barrios = [b for b in barrios if b["distrito"] == "Centro"]
        assert centro_barrios, "Centro should have barrios"
        for b in centro_barrios:
            assert b["price_per_sqm"] == 6_000
            # All listing-derived fields must be null
            assert b["active_count"] is None
            assert b["avg_days_market"] is None
            assert b["gross_yield"] is None

    def test_affordability_uses_notarial_not_listings(self, notarial_db: Path):
        metrics = exp.build_clean_metrics()
        aff = metrics["indicators"]["affordability"]
        # Median of 6000, 8500, 7200 = 7200; × 90 m² = 648 000 €
        assert aff["median_price"] == 7_200 * 90
        assert aff["reference_area_sqm"] == 90
        assert "Notarial" in aff["source"]
        assert aff["monthly_payment"] is not None
        assert aff["monthly_payment"] > 0

    def test_market_score_recomputed_from_clean_signals(self, notarial_db: Path):
        metrics = exp.build_clean_metrics()
        score = metrics["market_score"]
        assert isinstance(score["score"], int)
        assert 0 <= score["score"] <= 100
        assert score["emoji"] in {"🟢", "🟡", "🔴", "⚪"}

    def test_alerts_only_contain_clean_codes(self, notarial_db: Path):
        metrics = exp.build_clean_metrics()
        codes = {a["code"] for a in metrics["alerts"]}
        # No alert can mention scraper-derived data
        forbidden = {"aged_stock", "drops_high", "supply_demand", "inventory_low"}
        assert not (codes & forbidden), f"forbidden alert codes leaked: {codes & forbidden}"

    def test_lanzamientos_is_invoked_with_db_conn(self, notarial_db: Path):
        """The patched stub must actually receive the conn argument."""
        metrics = exp.build_clean_metrics()
        lan = metrics["indicators"]["lanzamientos"]
        # The stub returned {"current": 240, ...}; if the call signature was
        # broken, _safe() would silently swallow the TypeError and return {}.
        assert lan.get("current") == 240


# ──────────────────────────────────────────────────────────────────────────
# Forbidden-table guard
# ──────────────────────────────────────────────────────────────────────────


class TestForbiddenTableGuard:
    @pytest.mark.parametrize("forbidden_sql", [
        "SELECT * FROM listings",
        "select foo from price_history where x = 1",
        "SELECT a, b FROM listings JOIN price_history USING (listing_id)",
        "  select * from rental_prices",
        "SELECT 1 FROM market_snapshots",
    ])
    def test_blocks_forbidden_select(self, forbidden_sql: str):
        with pytest.raises(exp._ForbiddenTableError):
            exp._assert_clean_sql(forbidden_sql)

    @pytest.mark.parametrize("ok_sql", [
        "SELECT * FROM notarial_prices",
        "SELECT distrito, precio_m2 FROM notarial_prices WHERE periodo = 202504",
        "SELECT MAX(periodo) FROM notarial_prices",
    ])
    def test_allows_clean_select(self, ok_sql: str):
        # Should not raise
        exp._assert_clean_sql(ok_sql)


# ──────────────────────────────────────────────────────────────────────────
# Output sanity-check
# ──────────────────────────────────────────────────────────────────────────


class TestVerifyClean:
    def test_clean_output_has_no_issues(self, notarial_db: Path):
        metrics = exp.build_clean_metrics()
        issues = exp.verify_clean(metrics)
        assert issues == []

    def test_dirty_output_is_caught(self):
        bad = {
            "indicators": {
                "price_trend": {"current": 3000},   # forbidden indicator
                "affordability": {},
            },
            "price_drop_stats": {"overview": {}},  # should be None
            "barrio_trends": [{"barrio": "Sol"}],  # should be []
            "trends": {"market": [{"x": 1}], "by_district": []},
            "notarial_gap": [],
        }
        issues = exp.verify_clean(bad)
        joined = " | ".join(issues)
        assert "price_drop_stats" in joined
        assert "barrio_trends" in joined
        assert "trends" in joined
        assert "price_trend" in joined


# ──────────────────────────────────────────────────────────────────────────
# Round-trip serialisation
# ──────────────────────────────────────────────────────────────────────────


class TestSerialisation:
    def test_metrics_are_json_serialisable(self, notarial_db: Path):
        metrics = exp.build_clean_metrics()
        # Must round-trip without raising
        s = json.dumps(metrics, ensure_ascii=False, default=str)
        assert json.loads(s) == metrics
