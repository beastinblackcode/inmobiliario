"""property fingerprints — group listings by physical property

Revision ID: 0002
Revises:    0001
Create Date: 2026-05-12 12:00:00.000000

Adds two tables that let us recognise when the same physical property
re-appears under a different ``listing_id``.  Idealista (and Spanish
agencies in general) routinely re-publish unsold flats: the seller
delists, changes agency, and re-publishes weeks or months later with a
fresh ``listing_id`` so the property looks "new".  For a buyer this
hides the real history — total days on market, accumulated price drops,
seller fatigue.  These tables expose that history.

Schema overview
---------------

``property_fingerprints``
    One row per *physical* property (regardless of how many times it
    has been listed).  Stores aggregated lifecycle stats (first seen,
    last seen, republication count, total days on market) plus the
    canonical attributes copied from the most recent listing.

``listing_property_map``
    Thin mapping table: every ``listing_id`` maps to exactly one
    ``property_id``.  Lets us join from a listing back to its property
    and vice versa.  ``ON DELETE CASCADE`` so removing a listing
    cleans up its mapping (the parent fingerprint stays until
    explicitly cleaned).

The fingerprints are populated by ``compute_property_fingerprints.py``
which runs as a post-scrape step.  It re-builds the tables from
scratch on every run — the matcher is deterministic and cheap enough
(~20s for 25k listings) that an incremental algorithm isn't worth
the complexity at our scale.
"""

from __future__ import annotations

from alembic import op


revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ─── property_fingerprints ──────────────────────────────────────────
    op.execute("""
        CREATE TABLE property_fingerprints (
            property_id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

            -- Aggregated lifecycle stats across every listing for this property.
            listing_count         INTEGER NOT NULL DEFAULT 1,
            republication_count   INTEGER NOT NULL DEFAULT 0,
            first_seen_date       DATE,
            last_seen_date        DATE,
            total_days_on_market  INTEGER,

            -- Canonical attributes (snapshot from the most recent listing
            -- belonging to this property).  Useful for filtering / display
            -- without joining back to listings.
            distrito              TEXT,
            barrio                TEXT,
            size_sqm              DOUBLE PRECISION,
            rooms                 INTEGER,
            floor                 TEXT,

            -- Diagnostics: when was this fingerprint last (re)computed.
            computed_at           TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Filter the dashboard by republished-only with a single index hit.
    op.execute("""
        CREATE INDEX idx_fingerprints_republications
        ON property_fingerprints (republication_count DESC)
        WHERE republication_count > 0
    """)
    op.execute("CREATE INDEX idx_fingerprints_barrio ON property_fingerprints (barrio)")

    # ─── listing_property_map ───────────────────────────────────────────
    op.execute("""
        CREATE TABLE listing_property_map (
            listing_id   TEXT   PRIMARY KEY REFERENCES listings(listing_id) ON DELETE CASCADE,
            property_id  BIGINT NOT NULL    REFERENCES property_fingerprints(property_id) ON DELETE CASCADE
        )
    """)
    op.execute("CREATE INDEX idx_lpm_property ON listing_property_map (property_id)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS listing_property_map")
    op.execute("DROP TABLE IF EXISTS property_fingerprints")
