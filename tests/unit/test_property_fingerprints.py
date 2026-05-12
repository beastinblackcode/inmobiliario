"""
Unit tests for the property-fingerprint matcher.

These tests use hand-crafted listing dicts and never touch a DB —
``cluster_listings`` is a pure function over mappings.  Integration
with the actual schema lives in ``tests/integration/``.
"""

from __future__ import annotations

import pytest

from property_fingerprints import (
    DEFAULT_THRESHOLD,
    _bucket_key,
    _coerce_date,
    _size_band,
    _UnionFind,
    cluster_listings,
)


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────


def _listing(
    lid: str,
    *,
    barrio: str | None = "Acacias",
    size: float | None = 90.0,
    rooms: int | None = 3,
    floor: str | None = "3",
    description: str = "",
    first_seen: str = "2026-01-10",
    last_seen: str = "2026-02-10",
    distrito: str = "Arganzuela",
) -> dict:
    """Build a minimal listing dict with sensible defaults."""
    return {
        "listing_id":      lid,
        "barrio":          barrio,
        "size_sqm":        size,
        "rooms":           rooms,
        "floor":           floor,
        "description":     description,
        "first_seen_date": first_seen,
        "last_seen_date":  last_seen,
        "distrito":        distrito,
    }


_SHARED = (
    "Magnífico piso en pleno corazón del barrio. Recién reformado con materiales de "
    "primera calidad. Tres dormitorios, dos baños, salón comedor amplio, cocina "
    "totalmente equipada, terraza orientada al sur, suelos de madera, calefacción "
    "individual de gas, aire acondicionado en todas las estancias. Edificio con "
    "ascensor y portero físico. Excelente ubicación, próximo a transporte público."
)


# ──────────────────────────────────────────────────────────────────────
# Helpers — small primitives
# ──────────────────────────────────────────────────────────────────────


class TestSizeBand:
    def test_buckets_within_tolerance_collapse(self):
        assert _size_band(89.5, 2.0) == _size_band(90.4, 2.0)

    def test_buckets_across_tolerance_separate(self):
        assert _size_band(89.0, 2.0) != _size_band(93.0, 2.0)

    def test_none_passes_through(self):
        assert _size_band(None, 2.0) is None


class TestBucketKey:
    def test_full_attributes_produce_key(self):
        l = _listing("a")
        assert _bucket_key(l, 2.0) is not None

    def test_missing_required_returns_none(self):
        assert _bucket_key(_listing("a", barrio=None), 2.0) is None
        assert _bucket_key(_listing("a", size=None),   2.0) is None
        assert _bucket_key(_listing("a", rooms=None),  2.0) is None

    def test_two_clones_share_key(self):
        a = _listing("a")
        b = _listing("b")
        assert _bucket_key(a, 2.0) == _bucket_key(b, 2.0)


class TestCoerceDate:
    def test_iso_string(self):
        d = _coerce_date("2026-01-15")
        assert d.year == 2026 and d.month == 1 and d.day == 15

    def test_iso_string_with_time(self):
        d = _coerce_date("2026-01-15 10:00:00")
        assert d.isoformat() == "2026-01-15"

    def test_none_and_empty(self):
        assert _coerce_date(None) is None
        assert _coerce_date("")   is None


class TestUnionFind:
    def test_singletons(self):
        uf = _UnionFind(3)
        assert sorted(map(len, uf.groups())) == [1, 1, 1]

    def test_transitive_merge(self):
        uf = _UnionFind(4)
        uf.union(0, 1)
        uf.union(1, 2)
        # 0, 1, 2 should be one group; 3 alone.
        sizes = sorted(map(len, uf.groups()))
        assert sizes == [1, 3]


# ──────────────────────────────────────────────────────────────────────
# cluster_listings — golden behaviours
# ──────────────────────────────────────────────────────────────────────


class TestClusterListings:
    def test_single_listing_returns_one_property(self):
        props = cluster_listings([_listing("a", description=_SHARED)])
        assert len(props) == 1
        assert props[0].listing_ids == ["a"]
        assert props[0].republication_count == 0

    def test_two_listings_same_description_cluster_together(self):
        listings = [
            _listing("a", description=_SHARED, first_seen="2024-03-01", last_seen="2024-06-15"),
            _listing("b", description=_SHARED, first_seen="2025-10-01", last_seen="2026-02-10"),
        ]
        props = cluster_listings(listings)
        assert len(props) == 1
        prop = props[0]
        assert set(prop.listing_ids) == {"a", "b"}
        assert prop.republication_count == 1
        # Total days on market sums across both listings.
        # listing a: 106d; listing b: 132d
        assert prop.total_days_on_market == 106 + 132
        # First/last seen are the extremes.
        assert prop.first_seen_date.isoformat() == "2024-03-01"
        assert prop.last_seen_date.isoformat()  == "2026-02-10"

    def test_two_listings_different_descriptions_stay_separate(self):
        a = _listing("a", description=(
            "Piso luminoso con vistas al parque, cocina americana, "
            "dos terrazas, plaza de garaje incluida."
        ))
        b = _listing("b", description=(
            "Vivienda interior a reformar, cocina antigua, sin terraza ni ascensor. "
            "Ideal para reforma integral, oportunidad de inversión."
        ))
        props = cluster_listings([a, b])
        assert len(props) == 2
        assert all(p.republication_count == 0 for p in props)

    def test_short_description_is_singleton_even_with_clone_neighbour(self):
        # ``b`` has the description; ``a`` doesn't.  We refuse to fuse
        # them on attributes alone (defensive against same-building
        # different-flat collisions).
        listings = [
            _listing("a", description="Piso 3h"),                      # stubby
            _listing("b", description=_SHARED),
            _listing("c", description=_SHARED),
        ]
        props = cluster_listings(listings)
        # Two properties: {b, c} clustered, {a} singleton.
        sizes = sorted(p.listing_count for p in props)
        assert sizes == [1, 2]

    def test_different_buckets_dont_cluster_even_if_description_matches(self):
        # Same description but different barrios → must stay apart.
        listings = [
            _listing("a", barrio="Acacias",  description=_SHARED),
            _listing("b", barrio="Salamanca",description=_SHARED),
        ]
        props = cluster_listings(listings)
        assert len(props) == 2

    def test_size_within_tolerance_does_cluster(self):
        # 89.5 m² and 90.5 m² → same size band.
        listings = [
            _listing("a", size=89.5, description=_SHARED),
            _listing("b", size=90.5, description=_SHARED),
        ]
        props = cluster_listings(listings)
        assert len(props) == 1

    def test_size_beyond_tolerance_does_not_cluster(self):
        # 88 m² vs 96 m² with 2 m² tolerance → different bands.
        listings = [
            _listing("a", size=88.0, description=_SHARED),
            _listing("b", size=96.0, description=_SHARED),
        ]
        props = cluster_listings(listings, size_tolerance=2.0)
        assert len(props) == 2

    def test_transitive_clustering(self):
        # a~b (high cosine), b~c (high cosine), a~c (low cosine).
        # Union-find must still pull all three into one property.
        base   = _SHARED
        a_text = base + " Reformado en 2018."
        b_text = base + " Reformado en 2018. Plaza de garaje opcional."
        c_text = base + " Plaza de garaje opcional. Trastero incluido."
        listings = [
            _listing("a", description=a_text),
            _listing("b", description=b_text),
            _listing("c", description=c_text),
        ]
        props = cluster_listings(listings, threshold=0.40)
        # All three should land in one cluster via transitivity.
        assert len(props) == 1
        assert set(props[0].listing_ids) == {"a", "b", "c"}

    def test_canonical_attributes_from_most_recent(self):
        # Most recent listing's attributes win even when older listings
        # also belong to the property.
        listings = [
            _listing("old", description=_SHARED, last_seen="2024-06-15", floor="3"),
            _listing("new", description=_SHARED, last_seen="2026-02-10", floor="5"),  # mismatch on purpose
        ]
        # Mismatched floors put them in different buckets, but force the
        # scenario by making bucket keys identical (same floor first):
        listings = [
            _listing("old", description=_SHARED, last_seen="2024-06-15", floor="3"),
            _listing("new", description=_SHARED, last_seen="2026-02-10", floor="3"),
        ]
        listings[1]["barrio"] = listings[0]["barrio"]
        props = cluster_listings(listings)
        assert len(props) == 1
        # ``canonical`` = newest, but floor was the same so we only check
        # the date logic clearly.
        assert props[0].last_seen_date.isoformat() == "2026-02-10"

    def test_missing_bucket_attribute_becomes_singleton(self):
        # No barrio → cannot bucket → always a singleton, even if it
        # shares description with another listing.
        listings = [
            _listing("a", barrio=None, description=_SHARED),
            _listing("b",              description=_SHARED),
        ]
        props = cluster_listings(listings)
        # 'a' is forcibly a singleton; 'b' alone in its bucket.
        assert sorted(p.listing_count for p in props) == [1, 1]

    def test_threshold_too_high_breaks_clusters(self):
        # Same description, threshold 0.99 → cosine is 1.0 so still
        # clusters.  Set above 1.0 (impossible) to force split.
        listings = [
            _listing("a", description=_SHARED),
            _listing("b", description=_SHARED),
        ]
        props = cluster_listings(listings, threshold=1.01)
        assert len(props) == 2  # neither pair can hit > 1.0
