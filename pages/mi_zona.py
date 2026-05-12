"""🎯 Mi Zona — personalised buyer landing page."""
import streamlit as st
from sidebar_filters import render_sidebar_filters

df = render_sidebar_filters()

if df.empty:
    st.warning("⚠️ No hay datos disponibles.")
    st.stop()

from tabs.mi_zona_tab import render_mi_zona_tab
render_mi_zona_tab(df)
