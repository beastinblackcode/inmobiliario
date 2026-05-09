"""initial schema — Postgres-native port of the SQLite database

Revision ID: 0001
Revises:
Create Date: 2026-05-09 14:30:00.000000

This is the bootstrap revision.  It defines every table that the
SQLite codebase creates inline in ``database.init_database()`` plus the
auxiliary migration scripts (``migration_add_price_history.py``, etc.).

Type translations applied while porting:

  - ``INTEGER PRIMARY KEY AUTOINCREMENT`` → ``BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY``
  - Date strings (``TEXT 'YYYY-MM-DD'``)  → ``DATE``
  - Datetime strings (``TEXT ISO``)      → ``TIMESTAMPTZ``
  - ``REAL``                              → ``DOUBLE PRECISION``
  - ``INTEGER`` storing 0/1              → ``BOOLEAN`` (only where the column
                                           is genuinely boolean, e.g.
                                           ``alert_on_drop``, ``has_terraza``)
  - ``date('now')`` defaults             → ``CURRENT_DATE``
  - ``datetime('now')`` defaults         → ``CURRENT_TIMESTAMP``

Indexes mirror the SQLite ones one-for-one. Foreign keys that were
implicit in SQLite are made explicit here (Postgres enforces them).
"""

from __future__ import annotations

from alembic import op


# Alembic revision identifiers
revision    = "0001"
down_revision = None
branch_labels = None
depends_on    = None


def upgrade() -> None:
    # ─── listings ───────────────────────────────────────────────────────
    # The core table: one row per scraped property. ``listing_id`` is
    # the Idealista ID and stays as the natural primary key.
    op.execute("""
        CREATE TABLE listings (
            listing_id          TEXT             PRIMARY KEY,
            title               TEXT,
            url                 TEXT,
            price               INTEGER,
            distrito            TEXT,
            barrio              TEXT,
            rooms               INTEGER,
            size_sqm            DOUBLE PRECISION,
            floor               TEXT,
            orientation         TEXT,
            seller_type         TEXT,
            is_new_development  BOOLEAN          NOT NULL DEFAULT FALSE,
            description         TEXT,
            first_seen_date     DATE,
            last_seen_date      DATE,
            status              TEXT             NOT NULL DEFAULT 'active'
        )
    """)
    op.execute("CREATE INDEX idx_status            ON listings (status)")
    op.execute("CREATE INDEX idx_distrito          ON listings (distrito)")
    op.execute("CREATE INDEX idx_last_seen         ON listings (last_seen_date)")
    op.execute("CREATE INDEX idx_active_distrito_price ON listings (status, distrito, price)")
    op.execute("CREATE INDEX idx_active_barrio_price   ON listings (status, barrio, price)")
    op.execute("CREATE INDEX idx_status_last_seen ON listings (status, last_seen_date DESC)")

    # ─── price_history ──────────────────────────────────────────────────
    # One row per (listing, day) — UNIQUE constraint enforces this
    # invariant directly (the SQLite version learned the same lesson via
    # migration_dedupe_price_history.py + an idx_ph_unique index).
    op.execute("""
        CREATE TABLE price_history (
            id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            listing_id      TEXT             NOT NULL REFERENCES listings(listing_id) ON DELETE CASCADE,
            price           INTEGER          NOT NULL,
            date_recorded   DATE             NOT NULL,
            change_amount   INTEGER,
            change_percent  DOUBLE PRECISION,
            UNIQUE (listing_id, date_recorded)
        )
    """)
    op.execute("CREATE INDEX idx_price_history_listing_date ON price_history (listing_id, date_recorded)")
    op.execute("CREATE INDEX idx_date_recorded ON price_history (date_recorded)")

    # ─── rental_prices ──────────────────────────────────────────────────
    # Daily snapshot of median rent per barrio.  UNIQUE(barrio, date)
    # so the rental scraper can be re-run idempotently.
    op.execute("""
        CREATE TABLE rental_prices (
            id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            distrito        TEXT             NOT NULL,
            barrio          TEXT             NOT NULL,
            date_recorded   DATE             NOT NULL,
            median_rent     DOUBLE PRECISION NOT NULL,
            listing_count   INTEGER          NOT NULL DEFAULT 0,
            UNIQUE (barrio, date_recorded)
        )
    """)
    op.execute("CREATE INDEX idx_rental_barrio_date ON rental_prices (barrio, date_recorded)")

    # ─── watchlist ──────────────────────────────────────────────────────
    op.execute("""
        CREATE TABLE watchlist (
            id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            listing_id      TEXT             NOT NULL UNIQUE REFERENCES listings(listing_id) ON DELETE CASCADE,
            added_date      DATE             NOT NULL DEFAULT CURRENT_DATE,
            note            TEXT,
            price_at_add    INTEGER,
            alert_on_drop   BOOLEAN          NOT NULL DEFAULT TRUE
        )
    """)
    op.execute("CREATE INDEX idx_watchlist_listing ON watchlist (listing_id)")

    # ─── notarial_prices ────────────────────────────────────────────────
    # Real escritura prices per distrito and quarter from the Notarial
    # CSV import.  ``periodo`` encoded as e.g. ``2025`` (annual) or
    # ``202504`` (year-quarter) — INTEGER for trivial range queries.
    op.execute("""
        CREATE TABLE notarial_prices (
            id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            distrito    TEXT             NOT NULL,
            periodo     INTEGER          NOT NULL,
            precio_m2   DOUBLE PRECISION NOT NULL,
            UNIQUE (distrito, periodo)
        )
    """)
    op.execute("CREATE INDEX idx_notarial_distrito ON notarial_prices (distrito)")

    # ─── market_snapshots ───────────────────────────────────────────────
    # Pre-computed metrics produced by ``compute_snapshots.py``. Acts as
    # a cheap KPI cache so the dashboard can render without re-running
    # the heavy aggregations every page load.
    op.execute("""
        CREATE TABLE market_snapshots (
            id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            date_computed   DATE             NOT NULL,
            scope_type      TEXT             NOT NULL,
            scope_value     TEXT,
            metric_name     TEXT             NOT NULL,
            metric_value    DOUBLE PRECISION,
            UNIQUE (date_computed, scope_type, scope_value, metric_name)
        )
    """)
    op.execute("""
        CREATE INDEX idx_snapshots_lookup
        ON market_snapshots (scope_type, scope_value, metric_name, date_computed)
    """)

    # ─── scraping_log ───────────────────────────────────────────────────
    # One row per scraper run for cost / coverage observability.
    op.execute("""
        CREATE TABLE scraping_log (
            id                    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            start_time            TIMESTAMPTZ,
            end_time              TIMESTAMPTZ,
            duration_minutes      DOUBLE PRECISION,
            properties_processed  INTEGER,
            new_listings          INTEGER,
            updated_listings      INTEGER,
            total_requests        INTEGER,
            cost_estimate_usd     DOUBLE PRECISION,
            status                TEXT
        )
    """)

    # ─── custom_alerts ──────────────────────────────────────────────────
    # User-saved alert criteria.  Stored as denormalised JSON-in-TEXT
    # columns originally; kept that way in PG for now (no behavioural
    # change), but candidates for ``JSONB`` in a future refactor.
    op.execute("""
        CREATE TABLE custom_alerts (
            id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            name          TEXT NOT NULL,
            distritos     TEXT,
            barrios       TEXT,
            max_price     INTEGER,
            min_size      INTEGER,
            max_sqm_price INTEGER,
            min_rooms     INTEGER,
            seller_type   TEXT,
            min_score     INTEGER,
            last_checked  TIMESTAMPTZ,
            active        BOOLEAN     NOT NULL DEFAULT TRUE,
            created_at    TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ─── listing_signals ────────────────────────────────────────────────
    # NLP-derived per-listing flags.  In SQLite these were INTEGER 0/1;
    # promoted to BOOLEAN here so query writers stop having to remember
    # ``= 1`` vs ``IS TRUE``.
    op.execute("""
        CREATE TABLE listing_signals (
            listing_id    TEXT             PRIMARY KEY REFERENCES listings(listing_id) ON DELETE CASCADE,
            urgency       BOOLEAN          NOT NULL DEFAULT FALSE,
            direct        BOOLEAN          NOT NULL DEFAULT FALSE,
            negotiable    BOOLEAN          NOT NULL DEFAULT FALSE,
            renovated     BOOLEAN          NOT NULL DEFAULT FALSE,
            needs_work    BOOLEAN          NOT NULL DEFAULT FALSE,
            nlp_bonus     INTEGER          NOT NULL DEFAULT 0,
            signal_count  INTEGER          NOT NULL DEFAULT 0,
            analyzed_at   TIMESTAMPTZ      NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ─── listing_amenities ──────────────────────────────────────────────
    # Per-listing amenity flags + construction year extracted from the
    # description by the NLP analyser.  Same BOOLEAN promotion as above.
    op.execute("""
        CREATE TABLE listing_amenities (
            listing_id              TEXT             PRIMARY KEY REFERENCES listings(listing_id) ON DELETE CASCADE,
            has_terraza             BOOLEAN          NOT NULL DEFAULT FALSE,
            has_balcon              BOOLEAN          NOT NULL DEFAULT FALSE,
            has_garaje              BOOLEAN          NOT NULL DEFAULT FALSE,
            has_trastero            BOOLEAN          NOT NULL DEFAULT FALSE,
            has_piscina             BOOLEAN          NOT NULL DEFAULT FALSE,
            has_ascensor            BOOLEAN          NOT NULL DEFAULT FALSE,
            has_portero             BOOLEAN          NOT NULL DEFAULT FALSE,
            has_aire_acondicionado  BOOLEAN          NOT NULL DEFAULT FALSE,
            has_calefaccion         BOOLEAN          NOT NULL DEFAULT FALSE,
            has_armarios_empotrados BOOLEAN          NOT NULL DEFAULT FALSE,
            near_metro              BOOLEAN          NOT NULL DEFAULT FALSE,
            near_parque             BOOLEAN          NOT NULL DEFAULT FALSE,
            near_colegio            BOOLEAN          NOT NULL DEFAULT FALSE,
            near_hospital           BOOLEAN          NOT NULL DEFAULT FALSE,
            construction_year       INTEGER,
            amenities_count         INTEGER          NOT NULL DEFAULT 0,
            analyzed_at             TIMESTAMPTZ      NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    op.execute("CREATE INDEX idx_amenities_count ON listing_amenities (amenities_count DESC)")
    op.execute("""
        CREATE INDEX idx_amenities_year ON listing_amenities (construction_year)
        WHERE construction_year IS NOT NULL
    """)

    # ─── cgpj_lanzamientos ──────────────────────────────────────────────
    # Imported quarterly from the CGPJ Excel bulletins (see
    # ``cgpj_lanzamientos.py`` importer).  No date column — periodo
    # split into year + quarter.
    op.execute("""
        CREATE TABLE cgpj_lanzamientos (
            id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            year            INTEGER          NOT NULL,
            quarter         INTEGER          NOT NULL,
            tsj             TEXT             NOT NULL,
            provincia       TEXT,
            total           INTEGER,
            alquiler        INTEGER,
            hipoteca        INTEGER,
            otros           INTEGER,
            alquiler_pct    DOUBLE PRECISION,
            UNIQUE (year, quarter, tsj)
        )
    """)


def downgrade() -> None:
    # Reverse order so foreign key references unwind cleanly.
    op.execute("DROP TABLE IF EXISTS cgpj_lanzamientos")
    op.execute("DROP TABLE IF EXISTS listing_amenities")
    op.execute("DROP TABLE IF EXISTS listing_signals")
    op.execute("DROP TABLE IF EXISTS custom_alerts")
    op.execute("DROP TABLE IF EXISTS scraping_log")
    op.execute("DROP TABLE IF EXISTS market_snapshots")
    op.execute("DROP TABLE IF EXISTS notarial_prices")
    op.execute("DROP TABLE IF EXISTS watchlist")
    op.execute("DROP TABLE IF EXISTS rental_prices")
    op.execute("DROP TABLE IF EXISTS price_history")
    op.execute("DROP TABLE IF EXISTS listings")
