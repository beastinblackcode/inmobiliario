"""user preferences — generic per-user JSON storage

Revision ID: 0003
Revises:    0002
Create Date: 2026-05-13 09:00:00.000000

Replaces the file-on-disk persistence for Mi Zona criteria with a
Postgres table so the data survives Streamlit Cloud redeploys
(container filesystem is ephemeral after a deploy).  Kept generic
``(username, key, value)`` triple so future per-user settings drop
in without another migration.

Schema
------
``value`` is ``JSONB`` so we can query into nested fields if we ever
need to (e.g. "every user watching barrio X").  Today Mi Zona only
stores a flat dict, but JSONB costs us nothing extra vs TEXT.

The UNIQUE constraint enforces one row per (username, key) so the
``set_user_pref`` helper can use ``INSERT … ON CONFLICT … DO
UPDATE`` for a clean upsert.
"""

from __future__ import annotations

from alembic import op


revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE user_preferences (
            id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            username    TEXT        NOT NULL,
            key         TEXT        NOT NULL,
            value       JSONB       NOT NULL,
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (username, key)
        )
    """)
    op.execute("CREATE INDEX idx_userprefs_username ON user_preferences (username)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS user_preferences")
