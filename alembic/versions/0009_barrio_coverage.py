"""barrio_coverage — track when each barrio was last swept to full depth

Revision ID: 0009
Revises:    0008
Create Date: 2026-07-19 16:00:00.000000

``mark_stale_as_sold`` Tier 1 only marks a listing sold when its barrio
"was scraped recently", but the pre-0009 test for that was:

    barrio IN (SELECT DISTINCT barrio FROM listings
               WHERE last_seen_date >= cutoff)

A single refreshed listing satisfied it.  In ``lite`` mode the scraper
only reads page 1 of each barrio (~30 listings), so every barrio looked
"covered" while its pages 2..N went unread for weeks — and everything
buried past page 1 got marked sold the moment it crossed the staleness
threshold.  Post-mortem 2026-07-19: 42 days between complete sweeps
(2026-06-07 → 2026-07-19) produced ~5.6k false ``sold_removed`` rows,
all reactivated in one go by the next full sweep.

This table records the last time each barrio was walked to the *end of
its pagination*, which is the signal Tier 1 actually needs.  Only
complete full-mode sweeps are recorded: a barrio cut short by a 404/502,
a fetch failure, or the MAX_PAGES cap does not count as covered.

Empty table = no barrio qualifies for Tier 1, so a fresh deploy degrades
to the Tier 2 hard cutoff alone rather than to the old buggy behaviour.
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0009"
down_revision = "0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "barrio_coverage",
        sa.Column("distrito", sa.Text(), nullable=False),
        sa.Column("barrio", sa.Text(), nullable=False),
        sa.Column("last_deep_scrape_date", sa.Date(), nullable=False),
        sa.Column("pages_scraped", sa.Integer(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("distrito", "barrio"),
    )
    # Tier 1 probes this by (distrito, barrio) + date on every stale sweep.
    op.create_index(
        "ix_barrio_coverage_date",
        "barrio_coverage",
        ["last_deep_scrape_date"],
    )

    # Seed the sweep that ran today (2026-07-19).  It walked all 91 barrios
    # to completion, so its coverage is real — without this seed the first
    # post-deploy run would have an empty table and skip Tier 1 entirely
    # until the next full sweep.
    # pages_scraped is left NULL (unknown for a backfilled sweep); it is
    # informational only, Tier 1 reads just the date.
    op.execute("""
        INSERT INTO barrio_coverage (distrito, barrio, last_deep_scrape_date)
        SELECT DISTINCT distrito, barrio, DATE '2026-07-19'
          FROM listings
         WHERE last_seen_date = DATE '2026-07-19'
           AND distrito IS NOT NULL
           AND barrio   IS NOT NULL
        ON CONFLICT (distrito, barrio) DO NOTHING
    """)


def downgrade() -> None:
    op.drop_index("ix_barrio_coverage_date", table_name="barrio_coverage")
    op.drop_table("barrio_coverage")
