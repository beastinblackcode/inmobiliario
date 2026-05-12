"""
Populate ``property_fingerprints`` + ``listing_property_map`` from
the current ``listings`` table.

The matcher lives in ``property_fingerprints.cluster_listings``; this
module is the I/O wrapper: pulls listings out of the DB, runs the
matcher, writes the resulting clusters back into the two fingerprint
tables.  It rebuilds from scratch every run — the matcher is cheap
enough (~20s on 25k listings) that an incremental algorithm isn't
worth the complexity at our scale.

Usage
-----

::

    # Default: rebuild fingerprints in the DB pointed at by DB_BACKEND.
    python compute_property_fingerprints.py

    # Tweak knobs:
    python compute_property_fingerprints.py \
        --threshold 0.55 \
        --size-tolerance 3 \
        --min-description-chars 80

    # Dry-run: cluster + print stats, don't touch the DB.
    python compute_property_fingerprints.py --dry-run

Designed to be invoked as a post-scrape step in
``.github/workflows/daily_scraper.yml`` once the matcher behaviour is
validated against a few snapshots.
"""

from __future__ import annotations

import argparse
import sys
import time
from typing import Any, Iterable, Sequence

from db.connection import get_connection
from property_fingerprints import (
    DEFAULT_MIN_DESCRIPTION_CHARS,
    DEFAULT_SIZE_TOLERANCE,
    DEFAULT_THRESHOLD,
    Property,
    cluster_listings,
)


# ──────────────────────────────────────────────────────────────────────
# Loaders
# ──────────────────────────────────────────────────────────────────────


def _load_all_listings(conn) -> list[dict[str, Any]]:
    """Pull every listing with the columns the matcher cares about.

    ``conn`` is the dispatching connection from ``db.connection`` —
    works against SQLite and Postgres identically thanks to the
    ``?``-placeholder translator and dict-style row factory.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT listing_id, distrito, barrio,
               size_sqm, rooms, floor,
               description,
               first_seen_date, last_seen_date
        FROM listings
    """)
    return [dict(row) for row in cur.fetchall()]


# ──────────────────────────────────────────────────────────────────────
# Writers
# ──────────────────────────────────────────────────────────────────────


def _wipe(conn) -> None:
    """Drop existing fingerprints so the rebuild starts clean.

    ``listing_property_map`` cascades from ``property_fingerprints``
    on FK, so a single TRUNCATE-equivalent wipes both.  We delete
    rather than TRUNCATE to keep this portable across SQLite and
    Postgres (SQLite has no TRUNCATE).
    """
    cur = conn.cursor()
    cur.execute("DELETE FROM listing_property_map")
    cur.execute("DELETE FROM property_fingerprints")


_PROPERTY_COLS = (
    "listing_count", "republication_count",
    "first_seen_date", "last_seen_date", "total_days_on_market",
    "distrito", "barrio", "size_sqm", "rooms", "floor",
)


def _property_row(p: Property) -> tuple:
    return (
        p.listing_count, p.republication_count,
        p.first_seen_date, p.last_seen_date, p.total_days_on_market,
        p.distrito, p.barrio, p.size_sqm, p.rooms, p.floor,
    )


def _write_properties(conn, properties: Sequence[Property]) -> list[int]:
    """Bulk-insert all properties, return their ``property_id`` in input order.

    Uses a single multi-VALUES ``INSERT … RETURNING property_id`` per
    batch.  One row at a time would be ~33k network round trips over
    the Madrid↔Supabase link — ~65 minutes of pure latency.  Batching
    1000 rows per query drops it to ~30 round trips, finishing in
    seconds.  Postgres's hard cap is 65 535 bind parameters per query;
    at 10 columns/row the safe batch is 6 500 — we leave headroom.

    Works on SQLite too — the multi-VALUES syntax is portable.
    ``RETURNING`` returns rows in input order on both backends.
    """
    if not properties:
        return []

    ids: list[int] = []
    cur = conn.cursor()
    batch_size = 1000
    cols_sql = ", ".join(_PROPERTY_COLS)
    placeholder_tuple = "(" + ", ".join(["?"] * len(_PROPERTY_COLS)) + ")"

    for start in range(0, len(properties), batch_size):
        batch = properties[start:start + batch_size]
        values_sql = ", ".join([placeholder_tuple] * len(batch))
        flat_params: list = []
        for p in batch:
            flat_params.extend(_property_row(p))

        cur.execute(
            f"INSERT INTO property_fingerprints ({cols_sql}) "
            f"VALUES {values_sql} "
            f"RETURNING property_id",
            flat_params,
        )
        ids.extend(r[0] for r in cur.fetchall())
    return ids


def _write_mappings(conn, mappings: Sequence[tuple[str, int]]) -> None:
    """Bulk-insert ``(listing_id, property_id)`` pairs.

    Same batching reasoning as ``_write_properties`` — ``executemany``
    in psycopg3 issues separate round trips, which is unworkable over
    a high-latency link.  Multi-VALUES INSERT batches by 2 500 rows
    (2 cols × 2 500 = 5 000 params, well under the 65 535 cap).
    """
    if not mappings:
        return
    cur = conn.cursor()
    batch_size = 2500
    for start in range(0, len(mappings), batch_size):
        batch = mappings[start:start + batch_size]
        values_sql = ", ".join(["(?, ?)"] * len(batch))
        flat = [v for pair in batch for v in pair]
        cur.execute(
            f"INSERT INTO listing_property_map (listing_id, property_id) "
            f"VALUES {values_sql}",
            flat,
        )


# ──────────────────────────────────────────────────────────────────────
# Driver
# ──────────────────────────────────────────────────────────────────────


def _report(properties: list[Property]) -> None:
    """Print a compact summary of the clustering result."""
    n_props = len(properties)
    n_listings = sum(p.listing_count for p in properties)
    n_reposts  = sum(1 for p in properties if p.republication_count > 0)
    listings_in_reposts = sum(p.listing_count for p in properties if p.republication_count > 0)

    print(f"  Listings procesados:        {n_listings:>6d}")
    print(f"  Propiedades únicas:         {n_props:>6d}")
    print(f"  Grupos de republicación:    {n_reposts:>6d}  "
          f"({listings_in_reposts} listings en total)")

    if not n_reposts:
        return
    # Top-5 most republished as a sanity-check sample.
    top = sorted(properties, key=lambda p: p.republication_count, reverse=True)[:5]
    print(f"\n  Top republicaciones:")
    for p in top:
        attrs = f"{p.barrio} · {p.size_sqm:.0f}m² · {p.rooms}h"
        print(f"    {p.listing_count} listings  ({p.total_days_on_market}d en mercado)  {attrs}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD,
                        help=f"TF-IDF cosine cutoff (default: {DEFAULT_THRESHOLD})")
    parser.add_argument("--size-tolerance", type=float, default=DEFAULT_SIZE_TOLERANCE,
                        help=f"size_sqm bucket width in m² (default: {DEFAULT_SIZE_TOLERANCE})")
    parser.add_argument("--min-description-chars", type=int,
                        default=DEFAULT_MIN_DESCRIPTION_CHARS,
                        help="skip clustering for descriptions shorter than this "
                             f"(default: {DEFAULT_MIN_DESCRIPTION_CHARS})")
    parser.add_argument("--dry-run", action="store_true",
                        help="cluster + print stats, don't write to DB")
    args = parser.parse_args()

    with get_connection() as conn:
        t0 = time.time()
        rows = _load_all_listings(conn)
        print(f"  Cargados {len(rows)} listings en {time.time()-t0:.1f}s", file=sys.stderr)

        t1 = time.time()
        properties = cluster_listings(
            rows,
            threshold              = args.threshold,
            size_tolerance         = args.size_tolerance,
            min_description_chars  = args.min_description_chars,
        )
        print(f"  Clustering: {time.time()-t1:.1f}s", file=sys.stderr)

        print()
        _report(properties)

        if args.dry_run:
            print("\n[dry-run] DB sin tocar.")
            return 0

        t2 = time.time()
        _wipe(conn)
        # We commit on context exit; until then, the wipe is reversible.
        property_ids = _write_properties(conn, properties)
        mappings: list[tuple[str, int]] = []
        for p, pid in zip(properties, property_ids):
            mappings.extend((lid, pid) for lid in p.listing_ids)
        _write_mappings(conn, mappings)
        print(f"\n  Escritura: {time.time()-t2:.1f}s ({len(mappings)} mappings)", file=sys.stderr)

    print("\n✓ Fingerprints actualizados.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
