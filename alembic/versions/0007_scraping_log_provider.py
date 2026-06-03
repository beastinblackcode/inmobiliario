"""scraping_log_provider — per-tier breakdown of each scrape run

Revision ID: 0007
Revises:    0006
Create Date: 2026-06-02 10:05:00.000000

``scraping_log`` already records one row per scraper run with the
aggregate ``cost_estimate_usd`` and ``total_requests``.  That's enough
for the existing burn-rate widget but blind to *where* the cost went:
when Oxylabs starts returning empty payloads (the 2/5 failure mode the
benchmark surfaced) it falls through to BrightData Web Unlocker, the
GB cost spikes, and today's dashboard can't show that.

This table mirrors the in-memory counters that ``scraper.py`` already
maintains (``oxylabs_counter``, ``request_counter`` (= unlocker),
``residential_counter``, ``direct_counter``) and persists them per run
so the admin dashboard can chart success-rate / cost / latency per
provider over time.

Phase 1 only adds the schema; the writer side lands in Phase 2 (where
``log_scraping_execution`` is extended to accept the per-tier dict).
"""

from __future__ import annotations

from alembic import op


revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE scraping_log_provider (
            id                 BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            scraping_log_id    BIGINT NOT NULL REFERENCES scraping_log(id) ON DELETE CASCADE,
            provider           TEXT NOT NULL REFERENCES provider_config(name),
            requests           INTEGER NOT NULL DEFAULT 0,
            successful         INTEGER NOT NULL DEFAULT 0,
            failed             INTEGER NOT NULL DEFAULT 0,
            empty_payload      INTEGER NOT NULL DEFAULT 0,
            bytes_downloaded   BIGINT NOT NULL DEFAULT 0,
            cost_usd           DOUBLE PRECISION NOT NULL DEFAULT 0,
            p50_latency_ms     INTEGER,
            p95_latency_ms     INTEGER
        )
    """)
    op.execute("""
        CREATE UNIQUE INDEX idx_slp_unique_run_provider
        ON scraping_log_provider (scraping_log_id, provider)
    """)
    op.execute("""
        CREATE INDEX idx_slp_provider
        ON scraping_log_provider (provider)
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_slp_provider")
    op.execute("DROP INDEX IF EXISTS idx_slp_unique_run_provider")
    op.execute("DROP TABLE IF EXISTS scraping_log_provider")
