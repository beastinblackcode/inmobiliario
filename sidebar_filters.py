"""
Shared sidebar filters for the multipage Streamlit app.

Pages that need the filtered DataFrame call ``render_sidebar_filters()``
which draws the sidebar widgets and returns the filtered ``pd.DataFrame``.
Filter values are persisted in ``st.session_state`` so they survive
page navigation.
"""

import streamlit as st
import pandas as pd
from data_utils import load_data


ALL_DISTRICTS = [
    "Centro", "Arganzuela", "Retiro", "Salamanca", "Chamartín",
    "Tetuán", "Chamberí", "Fuencarral-El Pardo", "Moncloa-Aravaca",
    "Latina", "Carabanchel", "Usera", "Puente de Vallecas",
    "Moratalaz", "Ciudad Lineal", "Hortaleza", "Villaverde",
    "Villa de Vallecas", "Vicálvaro", "San Blas-Canillejas", "Barajas",
]


def render_sidebar_filters() -> pd.DataFrame:
    """Draw sidebar filter widgets and return the filtered DataFrame.

    Uses ``st.session_state`` keys prefixed with ``sf_`` so values survive
    across page navigations.
    """

    st.sidebar.header("🔍 Filtros")

    status_filter = st.sidebar.radio(
        "Estado",
        options=["active", "sold_removed", "all"],
        format_func=lambda x: {
            "active": "Activos",
            "sold_removed": "Vendidos/Retirados",
            "all": "Todos",
        }[x],
        index=0,
        key="sf_status",
    )
    status_value = None if status_filter == "all" else status_filter

    selected_districts = st.sidebar.multiselect(
        "Distritos", options=ALL_DISTRICTS, default=[], key="sf_distritos",
    )

    st.sidebar.subheader("Rango de Precio")
    col1, col2 = st.sidebar.columns(2)
    with col1:
        min_price = st.number_input(
            "Mín (€)", min_value=0, value=0, step=10000, key="sf_min_price",
        )
    with col2:
        max_price = st.number_input(
            "Máx (€)", min_value=0, value=2000000, step=10000, key="sf_max_price",
        )

    seller_type = st.sidebar.selectbox(
        "Tipo de Vendedor",
        options=["All", "Particular", "Agencia"],
        key="sf_seller_type",
    )

    # Load data (cached in data_utils)
    with st.spinner("Cargando datos..."):
        df = load_data(
            status=status_value,
            distritos=selected_districts if selected_districts else None,
            min_price=min_price if min_price > 0 else None,
            max_price=max_price if max_price < 2000000 else None,
            seller_type=seller_type,
        )

    return df
