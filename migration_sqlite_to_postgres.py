"""
One-shot backfill: SQLite ``real_estate.db`` → Postgres (Supabase).

Part of Phase C of the SQLite → Postgres migration.  Run this **once**,
after ``alembic upgrade head`` has provisioned the target schema, to
copy every row from the local SQLite snapshot into the Postgres
backend.  After it succeeds, Phase D (cutover) flips ``DB_BACKEND`` and
the SQLite file becomes archival.

Prerequisites
-------------
1. The target Postgres has the schema from
   ``alembic/versions/0001_initial_schema.py`` already applied::

       alembic upgrade head

   (set ``DATABASE_URL`` first so Alembic targets the right DB).

2. ``DATABASE_URL`` (or ``POSTGRES_URL``, or ``st.secrets['postgres']['url']``)
   resolves to the Postgres you want to load.  We reuse
   ``db.connection_pg._resolve_url`` so the same secret precedence
   applies as the runtime.

3. The source SQLite file is reachable at ``--sqlite`` (defaults to
   ``real_estate.db`` in the cwd).

Usage
-----
::

    # 1. Dry-run: walk the SQLite side, print what would be loaded,
    #    don't touch Postgres at all.
    python migration_sqlite_to_postgres.py --dry-run

    # 2. Full load against a *clean* Postgres (post-alembic upgrade,
    #    before any data is written).  Aborts if any target table
    #    already has rows — pass --truncate to overwrite.
    python migration_sqlite_to_postgres.py

    # 3. Idempotent re-run: TRUNCATE every target table first (resets
    #    identity sequences), then load.  Use this if a previous run
    #    failed halfway through.
    python migration_sqlite_to_postgres.py --truncate

    # 4. Load from a non-default snapshot.
    python migration_sqlite_to_postgres.py --sqlite /path/to/backup.db

What the script does
--------------------
For each table, in foreign-key-safe order:

  1. ``SELECT`` every row from SQLite.
  2. Coerce values:
       - ``BOOLEAN`` columns (SQLite stores as int 0/1)        → ``bool``
       - ``DATE`` columns   (SQLite stores as ``'YYYY-MM-DD'``) → ``datetime.date``
       - ``TIMESTAMPTZ`` columns (SQLite stores as ISO string)  → ``datetime`` (UTC)
  3. Stream into Postgres via ``COPY ... FROM STDIN`` (text mode).

The SQLite ``id`` autoincrement columns are **not** carried over.  No
foreign key in the schema references them, so Postgres's
``GENERATED ALWAYS AS IDENTITY`` assigns fresh ids — simpler and
side-steps having to reset identity sequences manually.

The whole run executes in a single transaction.  On any error
everything rolls back; the only way to leave the Postgres in a partial
state is to interrupt the process mid-COPY and lose the transaction.

After loading, the script re-counts every table on both sides and
asserts equality.  Mismatches are listed and the run exits non-zero.

Timestamps
----------
SQLite stores ``scraping_log.start_time`` / ``end_time`` etc. as naive
ISO strings (whatever the writer's local clock was).  We attach UTC
tzinfo on load — the wall-clock value is preserved, but it is now
explicit.  These columns are observability / logging fields, not
correctness-critical, so the small drift (1-2h for runs that happened
on a local CET machine instead of CI in UTC) is acceptable.

Safety
------
*Read-only on the SQLite side.*  Even ``--truncate`` only TRUNCATEs the
Postgres target.  The SQLite file is never written to.
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as _dt
import os
import sqlite3
import sys
from typing import Any, Callable, Iterable, Sequence

# psycopg3 is imported lazily so ``--dry-run`` works without Postgres
# being reachable (useful in CI sanity checks).


# ──────────────────────────────────────────────────────────────────────
# Coercion helpers
# ──────────────────────────────────────────────────────────────────────


def _to_bool(v: Any) -> bool | None:
    """SQLite stores BOOLEAN columns as ``INTEGER`` 0/1. Coerce to ``bool``."""
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, int):
        return bool(v)
    # Defensive: ``'0'`` / ``'1'`` text just in case.
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("1", "true", "t", "yes"):
            return True
        if s in ("0", "false", "f", "no", ""):
            return False
    raise ValueError(f"cannot coerce {v!r} ({type(v).__name__}) to bool")


def _to_date(v: Any) -> _dt.date | None:
    """ISO ``'YYYY-MM-DD'`` → ``datetime.date``. ``None`` passes through."""
    if v is None or v == "":
        return None
    if isinstance(v, _dt.date) and not isinstance(v, _dt.datetime):
        return v
    if isinstance(v, _dt.datetime):
        return v.date()
    if isinstance(v, str):
        # Trim possible trailing time component (``'YYYY-MM-DD HH:MM:SS'``).
        s = v.strip().split(" ", 1)[0].split("T", 1)[0]
        return _dt.date.fromisoformat(s)
    raise ValueError(f"cannot coerce {v!r} ({type(v).__name__}) to date")


def _to_utc_dt(v: Any) -> _dt.datetime | None:
    """ISO datetime (with or without ``T``) → tz-aware UTC ``datetime``."""
    if v is None or v == "":
        return None
    if isinstance(v, _dt.datetime):
        return v if v.tzinfo else v.replace(tzinfo=_dt.timezone.utc)
    if isinstance(v, str):
        s = v.strip().replace(" ", "T")
        # ``fromisoformat`` since Python 3.11 accepts ``'+00:00'``;
        # earlier versions need a hand-rolled split, but we are on 3.11+.
        dt = _dt.datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=_dt.timezone.utc)
    raise ValueError(f"cannot coerce {v!r} ({type(v).__name__}) to datetime")


# ──────────────────────────────────────────────────────────────────────
# Per-table column specs
# ──────────────────────────────────────────────────────────────────────
#
# ``columns`` lists the *target* (Postgres) columns in INSERT order.
# ``coerce`` maps column name → coercer.  Columns absent from the map
# are passed through unchanged.  The SQLite SELECT uses the same names
# in the same order — both schemas agree on every name we copy.
#
# We deliberately do **not** copy SQLite ``id`` columns; Postgres
# assigns new identity values.  No FK references id, so this is safe.


class TableSpec:
    __slots__ = ("name", "columns", "coerce", "select_sql", "fk_parent")

    def __init__(
        self,
        name: str,
        columns: Sequence[str],
        coerce: dict[str, Callable[[Any], Any]] | None = None,
        select_sql: str | None = None,
        fk_parent: str | None = None,
    ):
        self.name = name
        self.columns = tuple(columns)
        self.coerce = coerce or {}
        # ``fk_parent``: if set, this table has a NOT NULL FK
        # (``listing_id``) to that parent. Postgres enforces the FK
        # (SQLite did not), so we filter orphans on read to keep the
        # load idempotent across slightly-inconsistent SQLite
        # snapshots.  See ``_listings_orphan_filter``.
        self.fk_parent = fk_parent
        # ``select_sql`` defaults to a plain SELECT of every named column.
        # Override when a table needs dedup or filtering on the SQLite
        # side — e.g. ``price_history`` had a writer bug that left dups
        # in older snapshots; we keep MAX(id) per (listing_id, date)
        # which matches ``migration_dedupe_price_history.py`` semantics.
        base = select_sql or f"SELECT {', '.join(columns)} FROM {name}"
        if fk_parent == "listings":
            # Wrap the user's query so orphan filtering composes with
            # any dedup logic inside ``select_sql``.
            base = (
                f"SELECT * FROM ({base}) AS _src "
                f"WHERE listing_id IN (SELECT listing_id FROM listings)"
            )
        self.select_sql = base


TABLES: list[TableSpec] = [
    TableSpec(
        "listings",
        [
            "listing_id", "title", "url", "price",
            "distrito", "barrio", "rooms", "size_sqm",
            "floor", "orientation", "seller_type",
            "is_new_development", "description",
            "first_seen_date", "last_seen_date", "status",
        ],
        {
            "is_new_development": _to_bool,
            "first_seen_date":    _to_date,
            "last_seen_date":     _to_date,
        },
    ),
    TableSpec(
        "price_history",
        ["listing_id", "price", "date_recorded", "change_amount", "change_percent"],
        {"date_recorded": _to_date},
        # Dedup at read time: keep one row per (listing_id, date_recorded),
        # the one with the highest ``id`` — matches the kept row from
        # ``migration_dedupe_price_history.py``.  Old SQLite snapshots
        # (pre-March 2026) still contain a handful (~15) of intra-day
        # duplicates that the Postgres UNIQUE constraint rejects, and
        # we want this script to work on any snapshot, not just the
        # latest one.
        select_sql="""
            SELECT listing_id, price, date_recorded, change_amount, change_percent
            FROM price_history
            WHERE id IN (
                SELECT MAX(id) FROM price_history
                GROUP BY listing_id, date_recorded
            )
        """,
        fk_parent="listings",
    ),
    TableSpec(
        "rental_prices",
        ["distrito", "barrio", "date_recorded", "median_rent", "listing_count"],
        {"date_recorded": _to_date},
    ),
    TableSpec(
        "watchlist",
        ["listing_id", "added_date", "note", "price_at_add", "alert_on_drop"],
        {"added_date": _to_date, "alert_on_drop": _to_bool},
        fk_parent="listings",
    ),
    TableSpec(
        "notarial_prices",
        ["distrito", "periodo", "precio_m2"],
    ),
    TableSpec(
        "market_snapshots",
        ["date_computed", "scope_type", "scope_value", "metric_name", "metric_value"],
        {"date_computed": _to_date},
    ),
    TableSpec(
        "scraping_log",
        [
            "start_time", "end_time", "duration_minutes",
            "properties_processed", "new_listings", "updated_listings",
            "total_requests", "cost_estimate_usd", "status",
        ],
        {"start_time": _to_utc_dt, "end_time": _to_utc_dt},
    ),
    TableSpec(
        "custom_alerts",
        [
            "name", "distritos", "barrios",
            "max_price", "min_size", "max_sqm_price",
            "min_rooms", "seller_type", "min_score",
            "last_checked", "active", "created_at",
        ],
        {"active": _to_bool, "last_checked": _to_utc_dt, "created_at": _to_utc_dt},
    ),
    TableSpec(
        "listing_signals",
        [
            "listing_id",
            "urgency", "direct", "negotiable", "renovated", "needs_work",
            "nlp_bonus", "signal_count", "analyzed_at",
        ],
        {
            "urgency": _to_bool, "direct": _to_bool, "negotiable": _to_bool,
            "renovated": _to_bool, "needs_work": _to_bool,
            "analyzed_at": _to_utc_dt,
        },
        fk_parent="listings",
    ),
    TableSpec(
        "listing_amenities",
        [
            "listing_id",
            "has_terraza", "has_balcon", "has_garaje", "has_trastero",
            "has_piscina", "has_ascensor", "has_portero",
            "has_aire_acondicionado", "has_calefaccion",
            "has_armarios_empotrados",
            "near_metro", "near_parque", "near_colegio", "near_hospital",
            "construction_year", "amenities_count", "analyzed_at",
        ],
        {
            c: _to_bool for c in (
                "has_terraza", "has_balcon", "has_garaje", "has_trastero",
                "has_piscina", "has_ascensor", "has_portero",
                "has_aire_acondicionado", "has_calefaccion",
                "has_armarios_empotrados",
                "near_metro", "near_parque", "near_colegio", "near_hospital",
            )
        } | {"analyzed_at": _to_utc_dt},
        fk_parent="listings",
    ),
    TableSpec(
        "cgpj_lanzamientos",
        [
            "year", "quarter", "tsj", "provincia",
            "total", "alquiler", "hipoteca", "otros", "alquiler_pct",
        ],
    ),
]


# ──────────────────────────────────────────────────────────────────────
# Sqlite read / coerce
# ──────────────────────────────────────────────────────────────────────


def _open_sqlite(path: str) -> sqlite3.Connection:
    if not os.path.exists(path):
        raise SystemExit(f"SQLite file not found: {path}")
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _coerce_row(spec: TableSpec, row: sqlite3.Row) -> tuple[Any, ...]:
    out: list[Any] = []
    for col in spec.columns:
        v = row[col]
        fn = spec.coerce.get(col)
        out.append(fn(v) if fn is not None else v)
    return tuple(out)


def _iter_rows(
    sqlite_conn: sqlite3.Connection, spec: TableSpec, batch: int,
) -> Iterable[tuple[Any, ...]]:
    cur = sqlite_conn.cursor()
    cur.execute(spec.select_sql)
    while True:
        rows = cur.fetchmany(batch)
        if not rows:
            return
        for r in rows:
            yield _coerce_row(spec, r)


def _sqlite_count(sqlite_conn: sqlite3.Connection, spec: TableSpec) -> int:
    """Count rows as ``select_sql`` would yield them (after any dedup)."""
    return sqlite_conn.execute(
        f"SELECT COUNT(*) FROM ({spec.select_sql}) AS _t"
    ).fetchone()[0]


# ──────────────────────────────────────────────────────────────────────
# Postgres write
# ──────────────────────────────────────────────────────────────────────


def _connect_postgres():
    """Open a single Postgres connection using the project's URL resolver."""
    import psycopg
    from db.connection_pg import _resolve_url  # noqa: WPS437 — internal but stable

    url = _resolve_url()
    return psycopg.connect(url, autocommit=False)


def _pg_count(pg_conn, table: str) -> int:
    with pg_conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        return cur.fetchone()[0]


def _truncate_all(pg_conn) -> None:
    """``TRUNCATE`` every target table.  CASCADE for FK-bearing children."""
    with pg_conn.cursor() as cur:
        # Single statement so it's one fast pass and resets identities atomically.
        names = ", ".join(spec.name for spec in TABLES)
        cur.execute(f"TRUNCATE TABLE {names} RESTART IDENTITY CASCADE")


def _preflight_empty_or_fail(pg_conn) -> None:
    """Refuse to load if any target table already has rows. Hints at ``--truncate``."""
    non_empty = []
    for spec in TABLES:
        n = _pg_count(pg_conn, spec.name)
        if n > 0:
            non_empty.append((spec.name, n))
    if non_empty:
        msg = "\n".join(f"  {t}: {n} rows" for t, n in non_empty)
        raise SystemExit(
            "Aborting: target Postgres has existing data:\n"
            f"{msg}\n"
            "Pass --truncate to wipe these tables before loading."
        )


def _copy_table(
    pg_conn, sqlite_conn: sqlite3.Connection, spec: TableSpec, batch: int,
) -> int:
    """Stream a single table via ``COPY ... FROM STDIN``. Returns rows loaded."""
    from psycopg import sql

    cols_sql = sql.SQL(", ").join(sql.Identifier(c) for c in spec.columns)
    copy_sql = sql.SQL("COPY {tbl} ({cols}) FROM STDIN").format(
        tbl=sql.Identifier(spec.name), cols=cols_sql,
    )

    loaded = 0
    with pg_conn.cursor() as cur:
        with cur.copy(copy_sql) as cp:
            for row in _iter_rows(sqlite_conn, spec, batch):
                cp.write_row(row)
                loaded += 1
    return loaded


# ──────────────────────────────────────────────────────────────────────
# Driver
# ──────────────────────────────────────────────────────────────────────


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--sqlite", default="real_estate.db", help="Source SQLite path (default: real_estate.db).")
    p.add_argument("--truncate", action="store_true", help="TRUNCATE every target table before loading.")
    p.add_argument("--dry-run", action="store_true", help="Walk SQLite, coerce rows, print counts. Don't open Postgres.")
    p.add_argument("--batch", type=int, default=5000, help="Rows per SQLite fetchmany() (default: 5000).")
    args = p.parse_args()

    sqlite_conn = _open_sqlite(args.sqlite)

    # ── Dry run: only exercise the SQLite read + coercion path. ─────
    if args.dry_run:
        print(f"[dry-run] source: {args.sqlite}")
        total = 0
        for spec in TABLES:
            n_src = _sqlite_count(sqlite_conn, spec)
            n_walked = 0
            try:
                for _ in _iter_rows(sqlite_conn, spec, args.batch):
                    n_walked += 1
            except Exception as e:
                print(f"  {spec.name:22s}  ✗ coercion error: {e}")
                return 1
            assert n_walked == n_src, f"{spec.name}: walked {n_walked} ≠ source {n_src}"
            print(f"  {spec.name:22s}  {n_src:>7d} rows ✓ coercion clean")
            total += n_src
        print(f"  total                {total:>7d} rows")
        return 0

    # ── Real load. ──────────────────────────────────────────────────
    pg_conn = _connect_postgres()

    try:
        if args.truncate:
            print("Truncating target tables (RESTART IDENTITY CASCADE)…")
            _truncate_all(pg_conn)
        else:
            _preflight_empty_or_fail(pg_conn)

        print(f"Loading from {args.sqlite}")
        total = 0
        for spec in TABLES:
            n_src = _sqlite_count(sqlite_conn, spec)
            # If we filter orphans, surface the drop so silent data loss
            # is visible.  Compare against the raw COUNT(*).
            n_raw = sqlite_conn.execute(
                f"SELECT COUNT(*) FROM {spec.name}"
            ).fetchone()[0]
            if n_raw > n_src:
                print(f"  {spec.name:22s}  ({n_raw - n_src} filtered: orphans / dedup)")
            if n_src == 0:
                print(f"  {spec.name:22s}  (empty, skipping)")
                continue
            n_loaded = _copy_table(pg_conn, sqlite_conn, spec, args.batch)
            assert n_loaded == n_src, f"{spec.name}: loaded {n_loaded} ≠ source {n_src}"
            print(f"  {spec.name:22s}  {n_loaded:>7d} rows loaded")
            total += n_loaded

        # Validate before committing — if anything is off, we roll back.
        print("Verifying row counts…")
        mismatches = []
        for spec in TABLES:
            n_src = _sqlite_count(sqlite_conn, spec)
            n_dst = _pg_count(pg_conn, spec.name)
            tag = "✓" if n_src == n_dst else "✗"
            print(f"  {spec.name:22s}  src={n_src:>7d}  dst={n_dst:>7d}  {tag}")
            if n_src != n_dst:
                mismatches.append((spec.name, n_src, n_dst))

        if mismatches:
            pg_conn.rollback()
            print("\nRow count mismatch — rolled back.")
            for t, s, d in mismatches:
                print(f"  {t}: source={s} dst={d}")
            return 1

        pg_conn.commit()
        print(f"\nCommitted. Total: {total} rows across {len(TABLES)} tables.")
        return 0

    except BaseException:
        with contextlib.suppress(Exception):
            pg_conn.rollback()
        raise
    finally:
        with contextlib.suppress(Exception):
            pg_conn.close()


if __name__ == "__main__":
    sys.exit(main())
