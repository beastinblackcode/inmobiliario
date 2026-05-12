"""
Property-level history queries built on top of the fingerprint tables.

The dashboard already knows how to render a single ``listing_id``:
price history, days on market, etc.  This module lifts that view from
the listing level to the *physical property* level by joining through
``listing_property_map`` and ``property_fingerprints`` (populated by
``compute_property_fingerprints.py``).

For a buyer this matters because Idealista routinely republishes the
same flat under multiple ``listing_id``s.  Looking at a single anuncio
that "first appeared 14 days ago" hides the fact that the same
property has been on the market for 14 *months* across three
relistings, with €40k accumulated price drops.  That history is the
single biggest piece of leverage a buyer has when negotiating an
offer.

Public surface
--------------

``get_property_history(listing_id) -> PropertyHistory | None``

    Returns ``None`` when:
      * the listing doesn't exist, or
      * it has not yet been mapped to a property fingerprint (fresh
        listing, or ``compute_property_fingerprints.py`` hasn't run
        since the last scrape).

    The caller (``tabs/detail_tab``) gracefully degrades to the
    single-listing view in those cases — no functional regression
    vs the pre-fingerprint dashboard.

``get_republication_counts(listing_ids) -> dict[str, int]``

    Bulk helper used by card-style views (oportunidades, búsqueda) to
    decorate each row with a ``🔄 Nx`` chip.  One query per page,
    not one per card.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Iterable, Optional, Sequence

from db.connection import get_connection


# ──────────────────────────────────────────────────────────────────────
# Data types
# ──────────────────────────────────────────────────────────────────────


@dataclass
class PriceChange:
    """One stored row of ``price_history``."""
    date_recorded:  date
    price:          int
    change_amount:  Optional[int]
    change_percent: Optional[float]


@dataclass
class ListingHistoryEntry:
    """One listing's contribution to a property's history."""
    listing_id:       str
    title:            Optional[str]
    url:              Optional[str]
    status:           str
    seller_type:      Optional[str]
    first_seen_date:  Optional[date]
    last_seen_date:   Optional[date]
    days_on_market:   int                  # last_seen - first_seen, clipped at 0
    initial_price:    Optional[int]        # earliest known price (history[0] or current)
    final_price:      Optional[int]        # listings.price (current)
    price_changes:    list[PriceChange] = field(default_factory=list)

    @property
    def n_drops(self) -> int:
        return sum(1 for c in self.price_changes if (c.change_amount or 0) < 0)

    @property
    def accumulated_drop_eur(self) -> int:
        """Sum of all negative ``change_amount`` rows. Always ≤ 0."""
        return sum(c.change_amount for c in self.price_changes if (c.change_amount or 0) < 0)


@dataclass
class PropertyHistory:
    """The full lifecycle of a single physical property."""
    property_id:          int
    listing_count:        int
    republication_count:  int
    first_seen_date:      Optional[date]
    last_seen_date:       Optional[date]
    total_days_on_market: int
    distrito:             Optional[str]
    barrio:               Optional[str]
    size_sqm:             Optional[float]
    rooms:                Optional[int]
    floor:                Optional[str]
    listings:             list[ListingHistoryEntry] = field(default_factory=list)

    @property
    def initial_price_overall(self) -> Optional[int]:
        """First observed price across every listing of this property."""
        candidates = [l.initial_price for l in self.listings if l.initial_price is not None]
        return min(candidates) if candidates else None

    @property
    def final_price_overall(self) -> Optional[int]:
        """Most-recent (current) price across every listing of this property."""
        # The "most recent" listing is the one with the latest last_seen_date.
        active = max(
            (l for l in self.listings if l.last_seen_date is not None),
            key=lambda l: l.last_seen_date,
            default=None,
        )
        return active.final_price if active else None

    @property
    def cumulative_change_eur(self) -> Optional[int]:
        """final_price_overall − first_price_overall.

        Negative means the property has dropped over its lifetime —
        the buyer-facing signal.  ``None`` when prices are missing.

        Note this uses the *first ever* price (lowest of the initial
        prices, since reposts often start lower than the previous
        listing ended) rather than the highest ever asking price.  We
        want "how much has the price actually moved while the same
        property has been on the market", not the bull/bear range.
        """
        first = self.first_asking_price
        last  = self.final_price_overall
        if first is None or last is None:
            return None
        return last - first

    @property
    def first_asking_price(self) -> Optional[int]:
        """Initial price of the *earliest* listing — the original ask.

        Different from ``initial_price_overall`` which takes the min
        across listings. Here we want the chronologically-first listing's
        opening price, which is the right reference point for "how much
        has the property dropped since first published".
        """
        earliest = min(
            (l for l in self.listings if l.first_seen_date is not None),
            key=lambda l: l.first_seen_date,
            default=None,
        )
        return earliest.initial_price if earliest else None

    @property
    def cumulative_change_pct(self) -> Optional[float]:
        first = self.first_asking_price
        delta = self.cumulative_change_eur
        if first is None or delta is None or first == 0:
            return None
        return round(delta * 100.0 / first, 2)


# ──────────────────────────────────────────────────────────────────────
# Coercion: SQLite and Postgres return dates in different types.
# ──────────────────────────────────────────────────────────────────────


def _to_date(v: Any) -> Optional[date]:
    if v is None or v == "":
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, str):
        return date.fromisoformat(v.split(" ", 1)[0].split("T", 1)[0])
    raise TypeError(f"cannot coerce {v!r} to date")


def _days_between(a: Optional[date], b: Optional[date]) -> int:
    if not a or not b:
        return 0
    return max(0, (b - a).days)


# ──────────────────────────────────────────────────────────────────────
# Queries
# ──────────────────────────────────────────────────────────────────────


def _resolve_property_id(conn, listing_id: str) -> Optional[int]:
    cur = conn.cursor()
    cur.execute(
        "SELECT property_id FROM listing_property_map WHERE listing_id = ?",
        (listing_id,),
    )
    row = cur.fetchone()
    return row[0] if row else None


def _load_property_meta(conn, property_id: int) -> Optional[dict]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT property_id, listing_count, republication_count,
               first_seen_date, last_seen_date, total_days_on_market,
               distrito, barrio, size_sqm, rooms, floor
        FROM property_fingerprints
        WHERE property_id = ?
        """,
        (property_id,),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def _load_listings_for_property(conn, property_id: int) -> list[dict]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT l.listing_id, l.title, l.url, l.price, l.status,
               l.seller_type, l.first_seen_date, l.last_seen_date
        FROM listings l
        JOIN listing_property_map m ON m.listing_id = l.listing_id
        WHERE m.property_id = ?
        ORDER BY l.first_seen_date
        """,
        (property_id,),
    )
    return [dict(r) for r in cur.fetchall()]


def _load_price_changes_for_property(conn, property_id: int) -> dict[str, list[PriceChange]]:
    """Return ``{listing_id: [PriceChange, …]}`` for every listing of the property."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT ph.listing_id, ph.date_recorded, ph.price,
               ph.change_amount, ph.change_percent
        FROM price_history ph
        JOIN listing_property_map m ON m.listing_id = ph.listing_id
        WHERE m.property_id = ?
        ORDER BY ph.listing_id, ph.date_recorded
        """,
        (property_id,),
    )
    out: dict[str, list[PriceChange]] = {}
    for r in cur.fetchall():
        lid = r["listing_id"]
        out.setdefault(lid, []).append(PriceChange(
            date_recorded  = _to_date(r["date_recorded"]),
            price          = r["price"],
            change_amount  = r["change_amount"],
            change_percent = r["change_percent"],
        ))
    return out


# ──────────────────────────────────────────────────────────────────────
# Public surface
# ──────────────────────────────────────────────────────────────────────


def get_property_history(listing_id: str) -> Optional[PropertyHistory]:
    """Build the full ``PropertyHistory`` for the property a listing belongs to.

    Returns ``None`` if the listing has no fingerprint mapping yet
    (fresh listing scraped after the last fingerprint job, or the job
    has never run).  Callers should fall back to per-listing rendering
    in that case.
    """
    with get_connection() as conn:
        pid = _resolve_property_id(conn, listing_id)
        if pid is None:
            return None

        meta = _load_property_meta(conn, pid)
        if meta is None:
            return None  # shouldn't happen, but defensive

        listing_rows  = _load_listings_for_property(conn, pid)
        price_changes = _load_price_changes_for_property(conn, pid)

    entries: list[ListingHistoryEntry] = []
    for row in listing_rows:
        lid     = row["listing_id"]
        changes = price_changes.get(lid, [])
        initial = changes[0].price if changes else row["price"]
        first   = _to_date(row["first_seen_date"])
        last    = _to_date(row["last_seen_date"])
        entries.append(ListingHistoryEntry(
            listing_id      = lid,
            title           = row["title"],
            url             = row["url"],
            status          = row["status"],
            seller_type     = row["seller_type"],
            first_seen_date = first,
            last_seen_date  = last,
            days_on_market  = _days_between(first, last),
            initial_price   = initial,
            final_price     = row["price"],
            price_changes   = changes,
        ))

    return PropertyHistory(
        property_id          = meta["property_id"],
        listing_count        = meta["listing_count"],
        republication_count  = meta["republication_count"],
        first_seen_date      = _to_date(meta["first_seen_date"]),
        last_seen_date       = _to_date(meta["last_seen_date"]),
        total_days_on_market = meta["total_days_on_market"] or 0,
        distrito             = meta["distrito"],
        barrio               = meta["barrio"],
        size_sqm             = meta["size_sqm"],
        rooms                = meta["rooms"],
        floor                = meta["floor"],
        listings             = entries,
    )


def get_republication_counts(listing_ids: Sequence[str]) -> dict[str, int]:
    """Bulk: ``listing_id → republication_count`` for cards / table views.

    One query for the whole page; entries missing from the map default
    to 0.  The caller can drop a ``🔄 Nx`` chip with ``v > 0`` to flag
    properties that have been republished.
    """
    listing_ids = list(listing_ids)
    if not listing_ids:
        return {}

    placeholders = ",".join(["?"] * len(listing_ids))
    out: dict[str, int] = {lid: 0 for lid in listing_ids}
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT m.listing_id, p.republication_count
            FROM listing_property_map m
            JOIN property_fingerprints p ON p.property_id = m.property_id
            WHERE m.listing_id IN ({placeholders})
              AND p.republication_count > 0
            """,
            listing_ids,
        )
        for r in cur.fetchall():
            out[r[0]] = r[1]
    return out
