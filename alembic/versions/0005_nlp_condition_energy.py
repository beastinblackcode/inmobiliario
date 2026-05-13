"""listing_amenities — add energy_certification and condition fields

Revision ID: 0005
Revises:    0004
Create Date: 2026-05-13 22:30:00.000000

Two new NLP-derived fields the offer engine can use as buyer-side
signals:

* ``energy_certification`` (TEXT, nullable) — A/B/C/D/E/F/G or
  'exento'.  A property at A/B is cheap to run; F/G points to costly
  maintenance and is increasingly hard to sell after the 2026 EU
  energy-class disclosure rules.

* ``condition`` (TEXT, nullable) — categorical state of the property
  as described by the seller:
    * ``obra_nueva``     — brand new (developer listing)
    * ``reformado``      — recently renovated, move-in ready
    * ``buen_estado``    — habitable as-is, no major works needed
    * ``a_reformar``     — needs renovation
    * ``para_reformar``  — strong reform required (often "promoción")
  ``NULL`` when no clear signal is in the description.

Both are populated by ``nlp_analyzer.extract_amenities`` and consumed
by the offer engine as additional discount/boost factors.
"""

from __future__ import annotations

from alembic import op


revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE listing_amenities "
        "ADD COLUMN energy_certification TEXT"
    )
    op.execute(
        "ALTER TABLE listing_amenities "
        "ADD COLUMN condition TEXT"
    )
    # Index for "find me listings in good condition" queries (Mi Zona
    # filter, search).  Partial: only rows where we extracted a value.
    op.execute(
        "CREATE INDEX idx_amenities_condition "
        "ON listing_amenities (condition) "
        "WHERE condition IS NOT NULL"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_amenities_condition")
    op.execute("ALTER TABLE listing_amenities DROP COLUMN condition")
    op.execute("ALTER TABLE listing_amenities DROP COLUMN energy_certification")
