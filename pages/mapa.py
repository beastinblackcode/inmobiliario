"""🗺️ Mapa — Map view of listings."""
import streamlit as st
from sidebar_filters import render_sidebar_filters

df = render_sidebar_filters()

if df.empty:
    st.warning("⚠️ No hay datos disponibles.")
    st.stop()

from tabs.map_tab import render_map_tab
render_map_tab(df)
