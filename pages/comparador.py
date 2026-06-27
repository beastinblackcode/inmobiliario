"""⚖️ Comparador — pon 2-4 pisos lado a lado."""
import streamlit as st
from sidebar_filters import render_sidebar_filters

df = render_sidebar_filters()

from tabs.compare_tab import render_compare_tab
render_compare_tab(df)
