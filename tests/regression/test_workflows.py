"""
Regression guards on the GitHub Actions workflows.

These tests fire if anyone reintroduces the legacy public-export step
(or a similar accidental coupling) into ``daily_scraper.yml``, or if
``export-metrics.yml`` regresses from the clean exporter to the
listings-derived one.

The point is to catch a bad merge before it ships scraper-derived data
back to madridhome.tech.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.regression


_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_DAILY_SCRAPER  = _REPO_ROOT / ".github" / "workflows" / "daily_scraper.yml"
_EXPORT_METRICS = _REPO_ROOT / ".github" / "workflows" / "export-metrics.yml"


def _read(path: Path) -> str:
    assert path.exists(), f"workflow not found: {path}"
    return path.read_text(encoding="utf-8")


# ──────────────────────────────────────────────────────────────────────────
# daily_scraper.yml — must NOT push to the public site
# ──────────────────────────────────────────────────────────────────────────


class TestDailyScraperIsolation:
    """The Idealista scraper must never publish to madridhome.tech."""

    def test_does_not_run_legacy_export(self):
        body = _read(_DAILY_SCRAPER)
        assert "export_public_metrics.py" not in body, (
            "daily_scraper.yml is invoking export_public_metrics.py — "
            "this couples the scraper to the public CDN. Move public "
            "exports to export-metrics.yml (clean exporter only)."
        )

    def test_does_not_run_clean_export_either(self):
        # Even the clean exporter shouldn't be triggered by the scraper —
        # it lives in its own workflow with an independent schedule.
        body = _read(_DAILY_SCRAPER)
        assert "export_clean_metrics.py" not in body, (
            "daily_scraper.yml runs the clean exporter directly. "
            "Keep these pipelines decoupled — clean export should run "
            "from export-metrics.yml on its own schedule."
        )

    def test_does_not_clone_market_thermometer(self):
        body = _read(_DAILY_SCRAPER)
        assert "market-thermometer" not in body, (
            "daily_scraper.yml clones the public-site repo — "
            "this would let scraper-derived data reach the public CDN."
        )

    def test_does_not_use_thermometer_pat(self):
        body = _read(_DAILY_SCRAPER)
        assert "THERMOMETER_PAT" not in body, (
            "daily_scraper.yml references the thermometer push token — "
            "remove the secret reference along with the publish step."
        )


# ──────────────────────────────────────────────────────────────────────────
# export-metrics.yml — must use the clean exporter and verify
# ──────────────────────────────────────────────────────────────────────────


class TestExportMetricsUsesCleanExporter:
    def test_runs_clean_exporter(self):
        body = _read(_EXPORT_METRICS)
        assert "export_clean_metrics.py" in body, (
            "export-metrics.yml does not invoke the clean exporter."
        )

    def test_does_not_run_legacy_exporter(self):
        body = _read(_EXPORT_METRICS)
        assert "export_public_metrics.py" not in body, (
            "export-metrics.yml is invoking the LEGACY listings-derived "
            "exporter. Switch to export_clean_metrics.py."
        )

    def test_does_not_run_legacy_barrio_profiles_exporter(self):
        body = _read(_EXPORT_METRICS)
        assert "export_barrio_profiles.py" not in body, (
            "export-metrics.yml is invoking the LEGACY barrio profiles "
            "exporter, which reads from listings. The clean workflow "
            "ships an empty profiles stub until Phase 2."
        )

    def test_runs_with_verify_flag(self):
        body = _read(_EXPORT_METRICS)
        assert "--verify" in body, (
            "export-metrics.yml runs the clean exporter without --verify. "
            "The flag is the safety net that aborts the run if any "
            "scraper-derived key leaks into the output."
        )

    def test_publishes_both_jsons(self):
        body = _read(_EXPORT_METRICS)
        for fname in ("metrics.json", "barrios_profiles.json"):
            assert fname in body, (
                f"export-metrics.yml does not publish {fname}; the public "
                "site needs both files to render barrio pages cleanly."
            )

    def test_runs_on_schedule(self):
        body = _read(_EXPORT_METRICS)
        assert "schedule:" in body, (
            "export-metrics.yml has no schedule — without it the public "
            "site never refreshes. Use a cron entry."
        )
