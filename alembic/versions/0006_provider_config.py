"""provider_config — runtime registry of scraping providers

Revision ID: 0006
Revises:    0005
Create Date: 2026-06-02 10:00:00.000000

Until now the scraper picked providers via env-var probing inside
``fetch_page()``: ``if OXYLABS_USER and OXYLABS_PASS`` → use Oxylabs,
``if BRIGHTDATA_USER and BRIGHTDATA_PASS`` → fall back to BrightData
Web Unlocker.  The order was hard-coded and the prices lived as Python
constants (``OXYLABS_COST_PER_REQ``, etc.).

This table makes the registry data-driven so the admin UI can show
what's configured, what each tier costs, and (in a later phase) flip
``enabled`` / re-order ``priority`` without a deploy.  Credentials stay
in env vars — only the *policy* lives here.

Schema
------
* ``name``            — stable slug used as PK and in scraper code
                       (``oxylabs``, ``brightdata_unlocker`` …)
* ``display_name``    — human label for the UI
* ``enabled``         — does ``fetch_page()`` consider this tier at all
* ``priority``        — lower number is tried first.  Direct = 0,
                       Oxylabs = 10, Web Unlocker = 20, Residential = 30
* ``kind``            — ``'per_req'`` or ``'per_gb'``.  Drives which of
                       the two cost columns is meaningful for billing
                       math; the other is left NULL
* ``cost_per_req``    — flat USD charge per HTTP call (Oxylabs)
* ``cost_per_gb``     — USD per GB of response payload (BrightData
                       residential & Web Unlocker bill bandwidth)
* ``notes``           — free-form admin note

Seed mirrors the current ``scraper.py`` state on 2026-06-02:
``direct`` (free, hybrid mode only), ``oxylabs`` (primary paid tier),
``brightdata_unlocker`` (fallback), ``brightdata_residential``
(disabled — Idealista blocks it, see comment in ``fetch_page``).
"""

from __future__ import annotations

from alembic import op


revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE provider_config (
            name             TEXT PRIMARY KEY,
            display_name     TEXT NOT NULL,
            enabled          BOOLEAN NOT NULL DEFAULT TRUE,
            priority         INTEGER NOT NULL,
            kind             TEXT NOT NULL CHECK (kind IN ('per_req', 'per_gb')),
            cost_per_req     DOUBLE PRECISION,
            cost_per_gb      DOUBLE PRECISION,
            notes            TEXT,
            updated_at       TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    op.execute("""
        CREATE INDEX idx_provider_config_enabled_priority
        ON provider_config (enabled, priority)
    """)

    op.execute("""
        INSERT INTO provider_config
            (name, display_name, enabled, priority, kind, cost_per_req, cost_per_gb, notes)
        VALUES
            ('direct',                  'Direct (curl_cffi)',         TRUE,  0,  'per_req', 0.0,     NULL, 'Free tier. Hybrid mode only — auto-degrades after consecutive failures.'),
            ('oxylabs',                 'Oxylabs Web Scraper API',    TRUE,  10, 'per_req', 0.00135, NULL, 'Primary paid tier since 2026-05-31 (PR #61).'),
            ('brightdata_unlocker',     'BrightData Web Unlocker',    TRUE,  20, 'per_gb',  NULL,    15.0, 'Fallback when Oxylabs exhausts retries. ~200KB avg payload.'),
            ('brightdata_residential',  'BrightData Residential',     FALSE, 30, 'per_gb',  NULL,    8.0,  'Disabled — Idealista blocks residential proxies (99% failure rate).')
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_provider_config_enabled_priority")
    op.execute("DROP TABLE IF EXISTS provider_config")
