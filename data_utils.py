"""
Shared data-loading utilities for the Madrid Real Estate Dashboard.

Centralises the cached `load_data` function so it can be imported by
app.py and any tab module without duplication.
"""

import streamlit as st
import pandas as pd

from database import get_listings, get_listings_page


@st.cache_data(ttl=300)  # 5-minute cache
def load_data(
    status,
    distritos,
    min_price,
    max_price,
    seller_type,
) -> pd.DataFrame:
    """Load and cache listing data with filters.

    Now uses ``get_listings_page`` which computes ``price_per_sqm`` and
    ``days_on_market`` directly in SQL — no more ``df.apply()`` needed.

    Args:
        status:      'active' | 'sold_removed' | None (all)
        distritos:   list of district names, or None
        min_price:   minimum price filter, or None
        max_price:   maximum price filter, or None
        seller_type: 'All' | 'Particular' | 'Agencia'

    Returns:
        pd.DataFrame of matching listings (with price_per_sqm and
        days_on_market columns already computed).
    """
    status_filter = None if status in (None, "all") else status

    rows, _total = get_listings_page(
        status=status_filter,
        distrito=distritos if distritos else None,
        min_price=min_price,
        max_price=max_price,
        seller_type=seller_type,
        page_size=0,  # no pagination — load all matching rows
    )
    return pd.DataFrame(rows)
