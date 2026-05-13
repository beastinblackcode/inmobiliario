"""property_fingerprints.cluster_type — classify republication kind

Revision ID: 0004
Revises:    0003
Create Date: 2026-05-13 11:00:00.000000

Distinguishes the three phenomena the original matcher conflated:

  * ``singleton``   — single listing, no republication signal.
  * ``temporal``    — same flat re-published after delisting (real
                      seller-fatigue signal — what a buyer wants to
                      see).
  * ``parallel``    — same flat listed by multiple agencies in
                      parallel.  Informative but **not** fatigue.
  * ``obra_nueva``  — same-day cluster of consecutive listing IDs
                      with identical generic descriptions: different
                      units in the same development, not the same
                      flat at all.  False positive of the matcher;
                      the UI hides republication framing for these.

Existing rows default to ``singleton`` so post-migration queries don't
hit NULL.  The classification is recomputed on the next
``compute_property_fingerprints.py`` run.
"""

from __future__ import annotations

from alembic import op


revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE property_fingerprints "
        "ADD COLUMN cluster_type TEXT NOT NULL DEFAULT 'singleton'"
    )
    # ``cluster_type='temporal'`` is the one queries are going to
    # filter on the most (buyer-facing dashboards / alerts).  Partial
    # index covers it specifically.
    op.execute(
        "CREATE INDEX idx_fingerprints_temporal "
        "ON property_fingerprints (republication_count DESC) "
        "WHERE cluster_type = 'temporal'"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_fingerprints_temporal")
    op.execute("ALTER TABLE property_fingerprints DROP COLUMN cluster_type")
