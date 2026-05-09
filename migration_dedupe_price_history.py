"""
One-time migration: dedupe ``price_history`` to one row per
``(listing_id, date_recorded)`` and add a UNIQUE index that prevents
the bug from coming back.

Context
-------
Two scraper runs on the same day can both insert a row for the same
listing because ``_insert_price_change_internal`` looks up the *last*
price by ``date_recorded DESC`` (no day-bound) and inserts
unconditionally.  When the second run sees a different price, both
rows survive — same listing, same date, two different prices.

Empirically the fallout is small (15 groups of 2 rows on the local
snapshot, ~0.06 % of the table) but the data model the chart and the
score logic assume is "1 row = 1 day", so we collapse it.

What this script does
---------------------
For every ``(listing_id, date_recorded)`` group with > 1 row:

1. ``keep_id`` = ``MAX(id)`` (the latest insert; represents the end-of-day
   state — the same row a future ``ON CONFLICT DO UPDATE`` would land on).
2. Recomputes ``change_amount`` and ``change_percent`` for the kept row
   against the **last different-day** price for that listing (NULL if
   none).  The previously-stored values referenced an intra-day row
   that is about to be deleted, so they would otherwise lie.
3. Deletes every other row in the group.

After the loop:

4. Creates ``CREATE UNIQUE INDEX IF NOT EXISTS idx_ph_unique_listing_date
   ON price_history(listing_id, date_recorded)`` so the writer's new
   ``ON CONFLICT(listing_id, date_recorded) DO UPDATE`` clause has
   something to target — and so a future bug can't reintroduce dups.

Usage
-----
    python migration_dedupe_price_history.py --dry-run
    python migration_dedupe_price_history.py            # apply
    python migration_dedupe_price_history.py --db /path/to/other.db

The script makes a backup before mutating anything (unless ``--no-backup``).
Run is idempotent: re-running on a clean DB is a no-op.
"""

from __future__ import annotations

import argparse
import shutil
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Tuple


def _find_duplicate_groups(cur: sqlite3.Cursor) -> List[Tuple[str, str, int]]:
    """Return (listing_id, date_recorded, count) for groups with > 1 row."""
    cur.execute("""
        SELECT listing_id, date_recorded, COUNT(*) AS n
          FROM price_history
         GROUP BY listing_id, date_recorded
         HAVING n > 1
         ORDER BY listing_id, date_recorded
    """)
    return cur.fetchall()


def _prior_different_day_price(
    cur: sqlite3.Cursor, listing_id: str, date_recorded: str
) -> int | None:
    """Latest ``price_history.price`` for *listing_id* with a date
    strictly earlier than *date_recorded*.  ``None`` if no such row
    exists (i.e. the duplicate group is the listing's earliest record).
    """
    cur.execute("""
        SELECT price
          FROM price_history
         WHERE listing_id = ?
           AND date_recorded < ?
         ORDER BY date_recorded DESC, id DESC
         LIMIT 1
    """, (listing_id, date_recorded))
    row = cur.fetchone()
    return row[0] if row else None


def _max_id_for_group(
    cur: sqlite3.Cursor, listing_id: str, date_recorded: str
) -> int:
    cur.execute("""
        SELECT MAX(id) FROM price_history
         WHERE listing_id = ? AND date_recorded = ?
    """, (listing_id, date_recorded))
    return cur.fetchone()[0]


def _kept_row_price(cur: sqlite3.Cursor, row_id: int) -> int:
    cur.execute("SELECT price FROM price_history WHERE id = ?", (row_id,))
    return cur.fetchone()[0]


def _backup_db(db_path: Path) -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = db_path.with_name(f"{db_path.stem}_dedup_backup_{stamp}{db_path.suffix}")
    shutil.copy2(db_path, backup)
    size_mb = backup.stat().st_size / (1024 * 1024)
    print(f"📦 Backup created: {backup} ({size_mb:.1f} MB)")
    return backup


def _create_unique_index(cur: sqlite3.Cursor) -> None:
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_ph_unique_listing_date
        ON price_history(listing_id, date_recorded)
    """)


def dedupe(db_path: Path, dry_run: bool = False, do_backup: bool = True) -> dict:
    if not db_path.exists():
        print(f"❌ Database not found: {db_path}")
        sys.exit(1)

    if not dry_run and do_backup:
        _backup_db(db_path)

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    groups = _find_duplicate_groups(cur)

    print("=" * 60)
    print("📊 Pre-migration scan")
    print("=" * 60)
    print(f"Duplicate groups found: {len(groups)}")
    rows_to_delete = sum(n - 1 for (_, _, n) in groups)
    print(f"Rows to delete:         {rows_to_delete}")
    print(f"Rows to recompute:      {len(groups)}")
    print()

    if not groups:
        print("✅ Nothing to do — every (listing_id, date_recorded) is already unique.")
        # Still ensure the unique index exists.
        if not dry_run:
            _create_unique_index(cur)
            conn.commit()
            print("✅ UNIQUE index ensured on (listing_id, date_recorded).")
        conn.close()
        return {"groups": 0, "deleted": 0, "recomputed": 0}

    if dry_run:
        # Show first few groups for sanity-check
        print("📝 Sample of groups (first 5):")
        for listing_id, date_recorded, n in groups[:5]:
            keep = _max_id_for_group(cur, listing_id, date_recorded)
            kept_price = _kept_row_price(cur, keep)
            prior = _prior_different_day_price(cur, listing_id, date_recorded)
            new_change = (
                None if prior is None else kept_price - prior
            )
            new_pct = (
                None if (prior is None or prior == 0)
                else round((kept_price - prior) / prior * 100, 4)
            )
            print(
                f"  listing={listing_id} date={date_recorded} n={n} "
                f"keep_id={keep} kept_price={kept_price} prior={prior} "
                f"new_change={new_change} new_pct={new_pct}"
            )
        print()
        print("🔍 DRY RUN — no changes applied. Re-run without --dry-run to commit.")
        conn.close()
        return {"groups": len(groups), "deleted": 0, "recomputed": 0}

    print(f"⏳ Deduping {len(groups)} groups…")

    deleted = 0
    recomputed = 0
    for listing_id, date_recorded, _n in groups:
        keep_id = _max_id_for_group(cur, listing_id, date_recorded)
        kept_price = _kept_row_price(cur, keep_id)
        prior = _prior_different_day_price(cur, listing_id, date_recorded)

        new_change = None if prior is None else (kept_price - prior)
        new_pct = (
            None if (prior is None or prior == 0)
            else (kept_price - prior) / prior * 100.0
        )

        cur.execute(
            "UPDATE price_history "
            "   SET change_amount = ?, change_percent = ? "
            " WHERE id = ?",
            (new_change, new_pct, keep_id),
        )
        recomputed += 1

        cur.execute(
            "DELETE FROM price_history "
            " WHERE listing_id = ? AND date_recorded = ? AND id <> ?",
            (listing_id, date_recorded, keep_id),
        )
        deleted += cur.rowcount

    print(f"✅ Recomputed {recomputed} rows, deleted {deleted}.")

    print("⏳ Creating UNIQUE index on (listing_id, date_recorded)…")
    _create_unique_index(cur)
    print("✅ UNIQUE index in place.")

    # Sanity check — no dups left
    leftover = _find_duplicate_groups(cur)
    if leftover:
        print(
            f"❌ Sanity check failed: {len(leftover)} duplicate groups remain. "
            "Rolling back."
        )
        conn.rollback()
        conn.close()
        sys.exit(2)

    conn.commit()
    conn.close()
    print()
    print("=" * 60)
    print("✅ MIGRATION COMPLETE")
    print("=" * 60)
    return {"groups": len(groups), "deleted": deleted, "recomputed": recomputed}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument(
        "--db",
        default="real_estate.db",
        help="Path to the SQLite DB (default: ./real_estate.db).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report only — do not modify the database.",
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="Skip the pre-migration backup file (use only on already-backed-up clones).",
    )
    args = parser.parse_args()

    dedupe(
        db_path=Path(args.db).resolve(),
        dry_run=args.dry_run,
        do_backup=not args.no_backup,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
