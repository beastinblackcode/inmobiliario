"""
Unit tests for parse_listing orientation extraction.

Regression: until 2026-05-29, 96.7 % of active listings had
orientation=NULL in production because Idealista bundles floor and
orientation in the same item-detail span (e.g. "Piso exterior,
3ª planta"). The legacy if/elif chain matched the floor branch first
on `'piso' in text`, consumed the span and never evaluated the
orientation branches. Fix in scraper.py:953 splits them into
independent matches.
"""

from __future__ import annotations

import pytest
from bs4 import BeautifulSoup

from scraper import parse_listing

pytestmark = pytest.mark.unit


def _article(*detail_spans: str) -> BeautifulSoup:
    """Build a minimal Idealista-shaped <article> with the given details."""
    details_html = "".join(f'<span class="item-detail">{s}</span>' for s in detail_spans)
    html = f"""
    <article class="item" data-element-id="999">
      <a class="item-link" href="/inmueble/999/">Test listing</a>
      <span class="item-price">300.000</span>
      {details_html}
    </article>
    """
    return BeautifulSoup(html, 'html.parser').find('article')


# ──────────────────────────────────────────────────────────────────────────
# Orientation: the regression
# ──────────────────────────────────────────────────────────────────────────

def test_floor_and_orientation_bundled_piso_exterior():
    """The exact pattern that was producing 96.7 % NULL in prod."""
    art = _article("3 hab.", "90 m²", "Piso exterior, 3ª planta")
    out = parse_listing(art, "Centro", "Sol")
    assert out['orientation'] == 'Exterior'
    assert out['floor'] == "Piso exterior, 3ª planta"
    assert out['rooms'] == 3
    assert out['size_sqm'] == 90.0


def test_floor_and_orientation_bundled_piso_interior():
    art = _article("2 hab.", "65 m²", "Piso interior, 2ª planta")
    out = parse_listing(art, "Centro", "Sol")
    assert out['orientation'] == 'Interior'
    assert out['floor'] == "Piso interior, 2ª planta"


def test_floor_bajo_with_orientation():
    art = _article("1 hab.", "45 m²", "Bajo exterior")
    out = parse_listing(art, "X", "Y")
    assert out['orientation'] == 'Exterior'
    assert out['floor'] == "Bajo exterior"


def test_floor_atico_with_orientation():
    art = _article("3 hab.", "110 m²", "Ático exterior")
    out = parse_listing(art, "X", "Y")
    assert out['orientation'] == 'Exterior'
    assert out['floor'] == "Ático exterior"


def test_orientation_alone():
    """Edge case Idealista sometimes uses for new builds: orientation as its own span."""
    art = _article("2 hab.", "70 m²", "Exterior")
    out = parse_listing(art, "X", "Y")
    assert out['orientation'] == 'Exterior'
    assert out['floor'] is None  # 'Exterior' alone is not a floor descriptor


def test_no_orientation_keeps_null():
    """When Idealista really doesn't ship the data, we still return None."""
    art = _article("2 hab.", "70 m²", "3ª planta")
    out = parse_listing(art, "X", "Y")
    assert out['orientation'] is None
    assert out['floor'] == "3ª planta"


# ──────────────────────────────────────────────────────────────────────────
# Regression guard for the other detail fields — make sure splitting the
# elif chain didn't break rooms / size / floor parsing.
# ──────────────────────────────────────────────────────────────────────────

def test_rooms_and_size_still_parsed():
    art = _article("4 hab.", "120 m²", "Ático interior")
    out = parse_listing(art, "X", "Y")
    assert out['rooms'] == 4
    assert out['size_sqm'] == 120.0
    assert out['orientation'] == 'Interior'
    assert out['floor'] == "Ático interior"


def test_size_alternative_m2_spelling():
    art = _article("80 m2")
    out = parse_listing(art, "X", "Y")
    assert out['size_sqm'] == 80.0


def test_only_floor_no_orientation_word():
    art = _article("3 hab.", "85 m²", "2ª planta")
    out = parse_listing(art, "X", "Y")
    assert out['floor'] == "2ª planta"
    assert out['orientation'] is None
