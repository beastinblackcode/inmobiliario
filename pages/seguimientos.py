"""🔔 Mis Seguimientos — alertas (criterios) + watchlist (anuncios concretos)."""
import streamlit as st

st.title("🔔 Mis Seguimientos")
st.caption(
    "Dos formas de seguir el mercado: "
    "**alertas** son criterios de búsqueda que matchean anuncios nuevos; "
    "**watchlist** son anuncios concretos que has marcado para vigilar precio."
)

tab_alerts, tab_watchlist = st.tabs([
    "🔔 Alertas (criterios)",
    "📌 Watchlist (anuncios)",
])

with tab_alerts:
    from tabs.alerts_tab import render_alerts_tab
    render_alerts_tab()

with tab_watchlist:
    from tabs.watchlist_tab import render_watchlist_tab
    render_watchlist_tab()
