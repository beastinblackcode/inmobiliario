"""Alembic environment configuration.

Reads the connection URL from, in order of preference:

  1. ``DATABASE_URL`` env var (CI, local dev, ``alembic upgrade head``)
  2. ``POSTGRES_URL`` env var (alternative)
  3. ``sqlalchemy.url`` from ``alembic.ini`` (fallback only — usually empty)

The repo's runtime config lives in ``db/connection_pg.py`` which honours
the same env vars plus ``st.secrets["postgres"]["url"]`` for Streamlit
deployments.  Alembic CLI runs are always env-driven.
"""

from __future__ import annotations

import os
from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool

from alembic import context


# ── Alembic config object (parsed from alembic.ini) ─────────────────────
config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)


# ── Resolve the DB URL at runtime ───────────────────────────────────────
def _resolve_url() -> str:
    """Pick the active connection URL.

    SQLAlchemy needs ``postgresql+psycopg://...`` (with the ``+psycopg``
    dialect tag) to use psycopg 3 instead of the legacy psycopg2.  The
    Supabase / DATABASE_URL strings come without the tag, so we patch
    the prefix here once.

    We also run the URL through ``db.connection_pg._normalise_url`` so
    passwords containing ``+`` (common with Supabase auto-generated
    secrets) are percent-encoded before SQLAlchemy's URL parser sees
    them — otherwise ``urllib.parse`` decodes ``+`` as space and
    auth fails with a confusing ``password authentication failed``.
    The runtime already does this; keeping Alembic in sync means the
    same ``DATABASE_URL`` works in both contexts.
    """
    raw = (
        os.environ.get("DATABASE_URL")
        or os.environ.get("POSTGRES_URL")
        or config.get_main_option("sqlalchemy.url")
        or ""
    )
    if not raw:
        raise RuntimeError(
            "No DB URL configured. Set DATABASE_URL or POSTGRES_URL."
        )
    # Lazy import: alembic env.py is loaded before sys.path tweaks, but
    # db/ is at the repo root which is already on sys.path when alembic
    # CLI runs from the project directory.
    from db.connection_pg import _normalise_url
    raw = _normalise_url(raw)
    if raw.startswith("postgresql://"):
        return "postgresql+psycopg://" + raw[len("postgresql://"):]
    if raw.startswith("postgres://"):
        return "postgresql+psycopg://" + raw[len("postgres://"):]
    return raw


# Override the URL in the Alembic config so engine_from_config picks it up.
config.set_main_option("sqlalchemy.url", _resolve_url())


# ── No SQLAlchemy models (we use raw SQL migrations) ────────────────────
target_metadata = None


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode (emit SQL to stdout)."""
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations against a live database connection."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            # Render long type names compactly in autogenerate output.
            render_as_batch=False,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
