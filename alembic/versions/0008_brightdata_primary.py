"""provider_config — promote BrightData to primary, disable Oxylabs

Revision ID: 0008
Revises:    0007
Create Date: 2026-06-03 14:30:00.000000

Policy change, not schema.  The Oxylabs Web Scraper API was the primary
paid tier from 2026-05-31 (PR #61) but carried a ~€50/mo subscription
floor while real usage ran ~$10/mo.  We cancelled the subscription and
promoted BrightData Web Unlocker — pay-as-you-go, no monthly minimum,
already proven on Idealista's DataDome — back to primary in
``scraper.fetch_page``.

This migration realigns the runtime registry so the admin dashboard
reflects the live tier order:

  * ``brightdata_unlocker`` → enabled, priority 10 (primary)
  * ``oxylabs``             → disabled, priority 20 (dormant rescue)

The seed in 0006 / ``database._PROVIDER_SEED`` only fires on a fresh DB
(``INSERT … ON CONFLICT DO NOTHING``); existing Postgres rows need this
explicit UPDATE.  Oxylabs stays in the table (not deleted) so re-adding
the ``OXYLABS_*`` secrets — the only switch needed to reactivate it —
keeps a matching registry row.
"""

from __future__ import annotations

from alembic import op


revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        UPDATE provider_config
           SET enabled    = TRUE,
               priority   = 10,
               notes      = 'Primary paid tier since 2026-06-03. Pay-as-you-go, no monthly floor. ~200KB avg payload.',
               updated_at = now()
         WHERE name = 'brightdata_unlocker'
    """)
    op.execute("""
        UPDATE provider_config
           SET enabled    = FALSE,
               priority   = 20,
               notes      = 'Disabled rescue tier — subscription cancelled 2026-06-03 to drop the ~€50/mo floor. Re-add OXYLABS_* secrets to reactivate.',
               updated_at = now()
         WHERE name = 'oxylabs'
    """)


def downgrade() -> None:
    # Restore the pre-0008 state seeded by 0006: Oxylabs primary, BrightData fallback.
    op.execute("""
        UPDATE provider_config
           SET enabled    = TRUE,
               priority   = 10,
               notes      = 'Primary paid tier since 2026-05-31 (PR #61).',
               updated_at = now()
         WHERE name = 'oxylabs'
    """)
    op.execute("""
        UPDATE provider_config
           SET enabled    = TRUE,
               priority   = 20,
               notes      = 'Fallback when Oxylabs exhausts retries. ~200KB avg payload.',
               updated_at = now()
         WHERE name = 'brightdata_unlocker'
    """)
