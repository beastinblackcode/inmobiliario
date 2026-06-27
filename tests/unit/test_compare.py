"""
Unit tests for the property comparator's pure logic (no Streamlit):
  - compute_radar_scores  (min-max normalisation + direction + neutrals)
  - build_comparison_table (side-by-side, vs-barrio row)
"""

from __future__ import annotations

import pytest

from tabs.compare_tab import compute_radar_scores, build_comparison_table

pytestmark = pytest.mark.unit


def _props():
    return [
        {"listing_id": "A", "price": 300000, "price_per_sqm": 4000, "size_sqm": 75,
         "rooms": 2, "days_on_market": 10, "distrito": "D1", "barrio": "B1",
         "floor": "Planta 2", "seller_type": "Particular", "url": "http://a"},
        {"listing_id": "B", "price": 500000, "price_per_sqm": 6000, "size_sqm": 100,
         "rooms": 3, "days_on_market": 90, "distrito": "D1", "barrio": "B2",
         "floor": "Planta 5", "seller_type": "Agencia", "url": "http://b"},
    ]


def test_radar_direction_cheaper_scores_higher():
    s = compute_radar_scores(_props())
    # A is cheaper → higher 'Precio' and '€/m²' score than B.
    assert s["Precio"] == [100.0, 0.0]
    assert s["€/m²"] == [100.0, 0.0]
    # B is bigger / more rooms / longer on market → higher on those.
    assert s["Tamaño"] == [0.0, 100.0]
    assert s["Habitaciones"] == [0.0, 100.0]
    assert s["Margen negoc."] == [0.0, 100.0]


def test_radar_equal_values_are_neutral():
    a, b = _props()
    b["size_sqm"] = a["size_sqm"]  # equal size on both
    s = compute_radar_scores([a, b])
    assert s["Tamaño"] == [50.0, 50.0]


def test_radar_missing_value_is_neutral():
    a, b = _props()
    a["rooms"] = None
    s = compute_radar_scores([a, b])
    # Missing → neutral 50 for that property; the other keeps a real score.
    assert s["Habitaciones"][0] == 50.0


def test_comparison_table_vs_barrio():
    stats = {"B1": {"median_price_sqm": 5000}, "B2": {"median_price_sqm": 5000}}
    table = build_comparison_table(_props(), stats)
    # Two property columns, attributes as the index.
    assert table.shape[1] == 2
    assert "vs mediana barrio" in table.index
    # A at 4000 vs barrio 5000 → -20%; B at 6000 → +20%.
    vs_row = table.loc["vs mediana barrio"].tolist()
    assert vs_row == ["-20.0%", "+20.0%"]


def test_comparison_table_handles_missing_barrio_stats():
    table = build_comparison_table(_props(), {})
    assert table.loc["vs mediana barrio"].tolist() == ["—", "—"]
