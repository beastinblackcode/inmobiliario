"""
Property fingerprint matcher — groups listings into physical properties.

The Madrid Idealista dataset is keyed by ``listing_id`` but a single
physical property routinely shows up under several listing ids: the
owner delists after months without selling, changes agency or goes
particular, and re-publishes with a fresh anuncio.  For a buyer this
hides the true days-on-market and accumulated price drops.

This module exposes a pure function ``cluster_listings(...)`` that
takes a list of listing dicts and returns a list of properties, where
each property carries the ``listing_id``s that belong to it plus
aggregated lifecycle stats.  No database I/O — that lives in
``compute_property_fingerprints.py`` so the matcher stays trivially
testable.

Algorithm
---------

1. **Bucket** by exact attributes that a republished listing can't
   change: ``(barrio, rooms, size_band, floor)``.  ``size_band`` is
   ``size_sqm`` rounded to ``size_tolerance`` m² (default 2 m²), which
   accepts the slight remeasurement jitter you see when an agency
   re-publishes the same flat.

2. **Within each bucket** of size > 1, compute pairwise cosine
   similarity over TF-IDF vectors of the descriptions.  Two listings
   merge into the same property iff their cosine ≥ ``threshold``
   (default 0.60).  Listings with empty or very short descriptions
   (< ``min_description_chars``, default 50) are treated as singletons
   — we'd rather miss a republication than wrongly fuse two distinct
   flats in the same building.

3. **Union-find** the resulting pair set so transitive matches join
   the same property cluster.

4. **Aggregate** lifecycle stats over each cluster: earliest
   ``first_seen_date``, latest ``last_seen_date``, sum of
   per-listing days on market, listing count, republication count
   (= listing_count − 1), and a canonical attribute snapshot taken
   from the most recent listing.

Why TF-IDF (and not embedding models)
-------------------------------------
Speed and reproducibility.  ~25k listings cluster in seconds with
scikit-learn alone, no GPU, no extra deps, and the result is the same
on every machine.  Embeddings would catch paraphrased descriptions
better but real-estate descriptions are highly templated by agency
(same property republished by the same agency reuses ~80% of the
text) so cosine on TF-IDF already gets us most of the way.

Tunable knobs
-------------

``threshold``           cosine similarity cutoff. Lower → more aggressive
                        clustering, higher false-positive risk. Default 0.60
                        chosen empirically — same agency reposts cluster
                        at ~0.85+, different-agency reposts at ~0.55-0.75,
                        unrelated listings same-building stay < 0.45.

``size_tolerance``      m² wiggle for the bucket key. 2 m² absorbs the
                        2-3 m² rounding agencies do when they remeasure.

``min_description_chars`` skip clustering for stubby descriptions —
                        nothing reliable to match on. They become
                        singletons (no false fusions).
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Iterable, Mapping, Optional, Sequence

# ──────────────────────────────────────────────────────────────────────
# Defaults
# ──────────────────────────────────────────────────────────────────────


DEFAULT_THRESHOLD = 0.60
DEFAULT_SIZE_TOLERANCE = 2.0          # m²
DEFAULT_MIN_DESCRIPTION_CHARS = 50


# ──────────────────────────────────────────────────────────────────────
# Data types
# ──────────────────────────────────────────────────────────────────────


@dataclass
class Property:
    """A clustered physical property and its lifecycle stats."""

    listing_ids:          list[str]                       = field(default_factory=list)
    first_seen_date:      Optional[date]                  = None
    last_seen_date:       Optional[date]                  = None
    total_days_on_market: int                             = 0
    # Canonical attribute snapshot from the most recent listing.
    distrito:             Optional[str]                   = None
    barrio:               Optional[str]                   = None
    size_sqm:             Optional[float]                 = None
    rooms:                Optional[int]                   = None
    floor:                Optional[str]                   = None

    @property
    def listing_count(self) -> int:
        return len(self.listing_ids)

    @property
    def republication_count(self) -> int:
        # ``republications = additional listings beyond the first``.
        return max(0, self.listing_count - 1)


# ──────────────────────────────────────────────────────────────────────
# Union-find — minimal local impl to keep the module dep-free.
# ──────────────────────────────────────────────────────────────────────


class _UnionFind:
    """Path-compression union-find over integer ids ``[0, n)``."""

    __slots__ = ("_parent",)

    def __init__(self, n: int):
        self._parent = list(range(n))

    def find(self, x: int) -> int:
        # Iterative with path compression.
        root = x
        while self._parent[root] != root:
            root = self._parent[root]
        while self._parent[x] != root:
            self._parent[x], x = root, self._parent[x]
        return root

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self._parent[ra] = rb

    def groups(self) -> list[list[int]]:
        out: dict[int, list[int]] = defaultdict(list)
        for i in range(len(self._parent)):
            out[self.find(i)].append(i)
        return list(out.values())


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────


def _coerce_date(v: Any) -> Optional[date]:
    """Accept ``date`` / ``datetime`` / ISO string, return ``date`` or None."""
    if v is None or v == "":
        return None
    if isinstance(v, date):
        # ``datetime`` is a subclass — collapse to date.
        return v if not hasattr(v, "hour") else v.date()
    if isinstance(v, str):
        return date.fromisoformat(v.split(" ", 1)[0].split("T", 1)[0])
    raise TypeError(f"cannot coerce {v!r} to date")


def _size_band(size_sqm: Optional[float], tolerance: float) -> Optional[int]:
    """Round ``size_sqm`` to the nearest tolerance step, for bucket grouping.

    >>> _size_band(91.5, 2.0)
    46
    >>> _size_band(92.4, 2.0)
    46
    >>> _size_band(None, 2.0) is None
    True
    """
    if size_sqm is None:
        return None
    if tolerance <= 0:
        return int(size_sqm)
    # Bucket so that adjacent ±tolerance/2 m² fall in the same band.
    return int(round(size_sqm / tolerance))


def _bucket_key(
    listing: Mapping[str, Any], size_tolerance: float,
) -> Optional[tuple]:
    """Tuple used to group listings before TF-IDF.

    Returns ``None`` for listings missing one of the required attributes,
    so they end up as singletons rather than getting wrongly bucketed.
    """
    barrio = listing.get("barrio")
    size   = listing.get("size_sqm")
    rooms  = listing.get("rooms")
    floor  = listing.get("floor")
    if not barrio or size is None or rooms is None:
        return None
    return (barrio, int(rooms), _size_band(float(size), size_tolerance), floor or "")


def _aggregate_property(listings: Sequence[Mapping[str, Any]]) -> Property:
    """Build a ``Property`` from a non-empty list of listings."""
    dates_first = [_coerce_date(l.get("first_seen_date")) for l in listings]
    dates_last  = [_coerce_date(l.get("last_seen_date"))  for l in listings]
    first_seen = min((d for d in dates_first if d), default=None)
    last_seen  = max((d for d in dates_last  if d), default=None)

    # Per-listing days on market summed: captures total exposure even
    # across delistings + reposts.  When dates are missing we skip the
    # contribution; aggregate may underestimate but never inflate.
    total_days = 0
    for f, l in zip(dates_first, dates_last):
        if f and l:
            total_days += max(0, (l - f).days)

    # Canonical attributes: pick the listing with the latest
    # ``last_seen_date`` (or first_seen, or first in list as a tiebreaker).
    def _recency(l: Mapping[str, Any]):
        return _coerce_date(l.get("last_seen_date")) or _coerce_date(l.get("first_seen_date")) or date.min
    canonical = max(listings, key=_recency)

    return Property(
        listing_ids          = [l["listing_id"] for l in listings],
        first_seen_date      = first_seen,
        last_seen_date       = last_seen,
        total_days_on_market = total_days,
        distrito             = canonical.get("distrito"),
        barrio               = canonical.get("barrio"),
        size_sqm             = canonical.get("size_sqm"),
        rooms                = canonical.get("rooms"),
        floor                = canonical.get("floor"),
    )


# ──────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────


def cluster_listings(
    listings:               Iterable[Mapping[str, Any]],
    *,
    threshold:              float = DEFAULT_THRESHOLD,
    size_tolerance:         float = DEFAULT_SIZE_TOLERANCE,
    min_description_chars:  int   = DEFAULT_MIN_DESCRIPTION_CHARS,
) -> list[Property]:
    """Cluster *listings* into physical properties.

    *listings* is any iterable of mapping-like objects (dict, sqlite3.Row,
    HybridRow) carrying at least ``listing_id`` plus the bucket and
    description attributes.  Missing optional fields are tolerated.

    Returns a list of ``Property`` objects, one per cluster, in no
    particular order.  Every input ``listing_id`` appears in exactly one
    property — including the singletons (description too short, or no
    other candidate in its bucket).
    """
    listings = list(listings)

    # ── Step 1: bucket by exact attributes. ─────────────────────────
    buckets: dict[Any, list[Mapping[str, Any]]] = defaultdict(list)
    singletons: list[Mapping[str, Any]] = []
    for l in listings:
        key = _bucket_key(l, size_tolerance)
        if key is None:
            singletons.append(l)
        else:
            buckets[key].append(l)

    properties: list[Property] = [_aggregate_property([l]) for l in singletons]

    # ── Step 2 & 3: per bucket, TF-IDF cosine + union-find. ─────────
    # Imported lazily so the module can be imported in environments
    # without scikit-learn (e.g. lightweight CI lint jobs).
    needs_clustering = [b for b in buckets.values() if len(b) > 1]
    if needs_clustering:
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.metrics.pairwise import cosine_similarity
    for bucket in buckets.values():
        if len(bucket) == 1:
            properties.append(_aggregate_property(bucket))
            continue

        # Listings with usable descriptions go through TF-IDF; the rest
        # become singletons regardless of bucket-mates.  We refuse to
        # claim two listings are the same property based only on
        # ``(barrio, rooms, size, floor)`` — too many false positives
        # in a city full of identical-layout buildings.
        clusterable: list[Mapping[str, Any]] = []
        stubby:      list[Mapping[str, Any]] = []
        for l in bucket:
            desc = (l.get("description") or "").strip()
            if len(desc) >= min_description_chars:
                clusterable.append(l)
            else:
                stubby.append(l)
        properties.extend(_aggregate_property([l]) for l in stubby)

        if len(clusterable) < 2:
            properties.extend(_aggregate_property([l]) for l in clusterable)
            continue

        descriptions = [l["description"] for l in clusterable]
        vec = TfidfVectorizer(
            lowercase=True,
            strip_accents="unicode",
            ngram_range=(1, 2),
            min_df=1,
        )
        X = vec.fit_transform(descriptions)
        sim = cosine_similarity(X)

        uf = _UnionFind(len(clusterable))
        n = len(clusterable)
        for i in range(n):
            for j in range(i + 1, n):
                if sim[i, j] >= threshold:
                    uf.union(i, j)

        for indices in uf.groups():
            cluster = [clusterable[i] for i in indices]
            properties.append(_aggregate_property(cluster))

    return properties
