"""⚙️ Administración — Admin tools and stats."""
import streamlit as st
from sidebar_filters import render_sidebar_filters

df = render_sidebar_filters()

if df.empty:
    st.warning("⚠️ No hay datos disponibles.")
    st.stop()

from tabs.admin_tab import render_admin_tab
render_admin_tab(df)
