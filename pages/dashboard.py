"""📊 Dashboard — Main dashboard page."""
import streamlit as st
from sidebar_filters import render_sidebar_filters

df = render_sidebar_filters()

if df.empty:
    st.warning("⚠️ No hay datos disponibles. Ejecuta el scraper primero: `python scraper.py`")
    st.stop()

from tabs.dashboard_tab import render_dashboard_tab
render_dashboard_tab(df)
