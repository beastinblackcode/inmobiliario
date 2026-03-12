"""🎯 Oportunidades — Opportunity detection."""
import streamlit as st
from sidebar_filters import render_sidebar_filters

df = render_sidebar_filters()

if df.empty:
    st.warning("⚠️ No hay datos disponibles.")
    st.stop()

from tabs.opportunities_tab import render_opportunities_tab
render_opportunities_tab(df)
