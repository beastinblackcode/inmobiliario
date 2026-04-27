"""
One-time migration: backfill missing initial entries in price_history.

Context
-------
A bug in older scraping pipelines (mainly the bulk-import on 2026-01-14
and the days that followed) left ~54 % of active listings without any
row in `price_history`.  As a result, the property-detail screen on the
Streamlit dashboard reported "no data" for those properties even though
their `first_seen_date` and `price` were perfectly valid.

What this script does
---------------------
For every listing in the `listings` table that has NO entries in
`price_history`, insert a single synthetic initial entry using:

  - listing_id     = listing.listing_id
  - price          = listing.price                (current price)
  - date_recorded  = listing.first_seen_date      (when we first saw it)
  - change_amount  = NULL  (initial entry)
  - change_percent = NULL

Listings without `first_seen_date` or `price` are skipped (they cannot
be backfilled meaningfully).

This is idempotent: running it twice will not double-insert, because
the WHERE NOT EXISTS clause filters listings that already have history.

Usage
-----
    python migration_backfill_initial_history.py            # apply
    python migration_backfill_initial_history.py --dry-run  # report only
"""
import sqlite3
import sys
from pathlib import Path

DATABASE_PATH = Path(__file__).parent / "real_estate.db"


def backfill(dry_run: bool = False) -> dict:
    if not DATABASE_PATH.exists():
        print(f"❌ Database not found: {DATABASE_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(str(DATABASE_PATH))
    cur  = conn.cursor()

    # Count candidates first (informative)
    cur.execute("""
        SELECT COUNT(*) FROM listings l
        WHERE l.price IS NOT NULL
          AND l.price > 0
          AND l.first_seen_date IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM price_history ph WHERE ph.listing_id = l.listing_id
          )
    """)
    candidates = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*) FROM listings l
        WHERE NOT EXISTS (
            SELECT 1 FROM price_history ph WHERE ph.listing_id = l.listing_id
        )
    """)
    no_history_total = cur.fetchone()[0]

    skipped = no_history_total - candidates

    print("=" * 60)
    print("📊 Pre-migration scan")
    print("=" * 60)
    print(f"Listings without price_history entries: {no_history_total:,}")
    print(f"  → backfillable:                         {candidates:,}")
    print(f"  → skipped (missing first_seen/price):   {skipped:,}")
    print()

    if dry_run:
        print("🔍 DRY RUN — no changes applied. Re-run without --dry-run to commit.")
        conn.close()
        return {"candidates": candidates, "inserted": 0, "skipped": skipped}

    if candidates == 0:
        print("✅ Nothing to do — all listings already have history entries.")
        conn.close()
        return {"candidates": 0, "inserted": 0, "skipped": skipped}

    print(f"⏳ Backfilling {candidates:,} initial entries…")

    cur.execute("""
        INSERT INTO price_history (listing_id, price, date_recorded, change_amount, change_percent)
        SELECT
            l.listing_id,
            l.price,
            -- normalise the date to YYYY-MM-DD (strip any time component)
            substr(l.first_seen_date, 1, 10),
            NULL,
            NULL
        FROM listings l
        WHERE l.price IS NOT NULL
          AND l.price > 0
          AND l.first_seen_date IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM price_history ph WHERE ph.listing_id = l.listing_id
          )
    """)

    inserted = cur.rowcount
    conn.commit()
    conn.close()

    print(f"✅ Inserted {inserted:,} initial price_history rows.")
    return {"candidates": candidates, "inserted": inserted, "skipped": skipped}


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    backfill(dry_run=dry_run)
