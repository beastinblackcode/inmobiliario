"""
Streamlit dashboard for Madrid Real Estate Tracker.

This file is the thin orchestrator: it handles authentication, sidebar filters,
data loading, and tab routing. All tab-specific rendering is delegated to the
modules in the `tabs/` package.
"""

import streamlit as st
import pandas as pd
from datetime import datetime
from pathlib import Path

from database import (
    get_database_stats,
    download_database_from_cloud,
    is_streamlit_cloud,
    DATABASE_PATH,
)
from data_utils import load_data

# ---------------------------------------------------------------------------
# Tab renderers — imported lazily inside main() to avoid import-time side
# effects when Streamlit is still bootstrapping.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Page configuration (must be the first Streamlit call)
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Madrid Real Estate Tracker",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS
st.markdown(
    """
    <style>
    .main {
        padding: 0rem 1rem;
    }
    .stMetric {
        background-color: #f0f2f6;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    h1 {
        color: #1f77b4;
        padding-bottom: 20px;
    }
    h2 {
        color: #2c3e50;
        padding-top: 20px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

def check_password() -> bool:
    """Return True if the user entered correct credentials."""

    def password_entered():
        username = st.session_state.get("username", "")
        password = st.session_state.get("password", "")

        if "auth" in st.secrets and "users" in st.secrets["auth"]:
            users = st.secrets["auth"]["users"]
            if username in users and users[username] == password:
                st.session_state["password_correct"] = True
                st.session_state["current_user"] = username
                st.session_state.pop("username", None)
                st.session_state.pop("password", None)
            else:
                st.session_state["password_correct"] = False
        elif "auth" in st.secrets:
            if (
                username == st.secrets["auth"].get("username", "")
                and password == st.secrets["auth"].get("password", "")
            ):
                st.session_state["password_correct"] = True
                st.session_state["current_user"] = username
                st.session_state.pop("username", None)
                st.session_state.pop("password", None)
            else:
                st.session_state["password_correct"] = False
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        st.markdown("## 🔐 Acceso al Dashboard")
        st.markdown("Por favor, introduce tus credenciales para acceder.")
        st.text_input("Usuario", key="username", autocomplete="username")
        st.text_input(
            "Contraseña", type="password", key="password", autocomplete="current-password"
        )
        st.button("Iniciar Sesión", on_click=password_entered, type="primary")
        return False
    elif not st.session_state["password_correct"]:
        st.markdown("## 🔐 Acceso al Dashboard")
        st.text_input("Usuario", key="username", autocomplete="username")
        st.text_input(
            "Contraseña", type="password", key="password", autocomplete="current-password"
        )
        st.button("Iniciar Sesión", on_click=password_entered, type="primary")
        st.error("😕 Usuario o contraseña incorrectos")
        return False
    else:
        return True


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def calculate_days_on_market(row) -> int:
    """Return days between first and last seen dates."""
    try:
        first = datetime.fromisoformat(row["first_seen_date"])
        last = datetime.fromisoformat(row["last_seen_date"])
        return (last - first).days
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    # Authentication
    if not check_password():
        st.stop()

    # Ensure database is available
    if not download_database_from_cloud():
        st.error(
            "❌ No se pudo cargar la base de datos. Por favor, contacta al administrador."
        )
        st.stop()

    # ------------------------------------------------------------------
    # Page navigation
    # ------------------------------------------------------------------
    st.sidebar.markdown("---")
    page = st.sidebar.radio(
        "📄 Página",
        options=["dashboard", "surveillance"],
        format_func=lambda x: {
            "dashboard": "🏠 Dashboard Principal",
            "surveillance": "🛡️ Vigilancia del Mercado",
        }[x],
        index=0,
    )
    # ── Version & environment info ──────────────────────────────────────────
    st.sidebar.markdown("---")

    # Git commit hash
    try:
        import subprocess
        commit = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(Path(__file__).parent),
            stderr=subprocess.DEVNULL,
        ).decode().strip()
    except Exception:
        commit = "—"

    # Last scrape date from DB
    try:
        from database import get_scraping_log
        log = get_scraping_log(limit=1)
        last_scrape = log[0]["start_time"][:10] if log else "sin datos"
    except Exception:
        last_scrape = "—"

    # DB size
    try:
        db_size_mb = Path(DATABASE_PATH).stat().st_size / (1024 * 1024)
        db_size_str = f"{db_size_mb:.1f} MB"
    except Exception:
        db_size_str = "—"

    env_label = "☁️ Streamlit Cloud" if is_streamlit_cloud() else "💻 Local"
    st.sidebar.caption(f"**{env_label}**")
    if commit != "—":
        st.sidebar.caption(f"🔖 Versión: `{commit}`")
    st.sidebar.caption(f"🕐 Último scrape: {last_scrape}")
    st.sidebar.caption(f"🗄️ Base de datos: {db_size_str}")
    if "current_user" in st.session_state:
        st.sidebar.caption(f"👤 {st.session_state['current_user']}")

    # Route: Market Surveillance (no sidebar filters needed)
    if page == "surveillance":
        from market_surveillance import render_market_surveillance
        render_market_surveillance()
        return

    # ------------------------------------------------------------------
    # Main Dashboard
    # ------------------------------------------------------------------
    st.title("🏠 Madrid Real Estate Tracker")
    st.markdown("**Monitorización diaria del mercado inmobiliario de Madrid**")

    # Sidebar filters
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
    )
    status_value = None if status_filter == "all" else status_filter

    all_districts = [
        "Centro", "Arganzuela", "Retiro", "Salamanca", "Chamartín",
        "Tetuán", "Chamberí", "Fuencarral-El Pardo", "Moncloa-Aravaca",
        "Latina", "Carabanchel", "Usera", "Puente de Vallecas",
        "Moratalaz", "Ciudad Lineal", "Hortaleza", "Villaverde",
        "Villa de Vallecas", "Vicálvaro", "San Blas-Canillejas", "Barajas",
    ]
    selected_districts = st.sidebar.multiselect(
        "Distritos", options=all_districts, default=[]
    )

    st.sidebar.subheader("Rango de Precio")
    col1, col2 = st.sidebar.columns(2)
    with col1:
        min_price = st.number_input("Mín (€)", min_value=0, value=0, step=10000)
    with col2:
        max_price = st.number_input(
            "Máx (€)", min_value=0, value=2000000, step=10000
        )

    seller_type = st.sidebar.selectbox(
        "Tipo de Vendedor", options=["All", "Particular", "Agencia"]
    )

    # ------------------------------------------------------------------
    # Load data
    # ------------------------------------------------------------------
    with st.spinner("Cargando datos..."):
        df = load_data(
            status=status_value,
            distritos=selected_districts,
            min_price=min_price if min_price > 0 else None,
            max_price=max_price if max_price < 2000000 else None,
            seller_type=seller_type,
        )

    if df.empty:
        st.warning(
            "⚠️ No hay datos disponibles. Ejecuta el scraper primero: `python scraper.py`"
        )
        return

    # Derived metrics
    df["price_per_sqm"] = df.apply(
        lambda row: row["price"] / row["size_sqm"]
        if row["size_sqm"] and row["size_sqm"] > 0
        else None,
        axis=1,
    )
    df["days_on_market"] = df.apply(calculate_days_on_market, axis=1)

    # ------------------------------------------------------------------
    # Tabs
    # ------------------------------------------------------------------
    (dashboard_tab, map_tab, prediction_tab, personal_tab,
     watchlist_tab, price_drops_tab, trends_tab,
     alerts_tab, compare_tab, admin_tab) = st.tabs(
        ["📊 Dashboard", "🗺️ Mapa", "🔮 Predicción", "🔍 Mis Búsquedas",
         "⭐ Mi Watchlist", "📉 Bajadas de Precio",
         "📈 Tendencias", "🔔 Mis Alertas",
         "📊 Comparar Barrios", "⚙️ Administración"]
    )

    with dashboard_tab:
        from tabs.dashboard_tab import render_dashboard_tab
        render_dashboard_tab(df)

    with map_tab:
        from tabs.map_tab import render_map_tab
        render_map_tab(df)

    with prediction_tab:
        from tabs.prediction_tab import render_prediction_tab
        render_prediction_tab(df)

    with personal_tab:
        from tabs.search_tab import render_search_tab
        render_search_tab()

    with watchlist_tab:
        from tabs.watchlist_tab import render_watchlist_tab
        render_watchlist_tab()

    with price_drops_tab:
        from tabs.price_drops_tab import render_price_drops_tab
        render_price_drops_tab()

    with trends_tab:
        from tabs.market_trends_tab import render_market_trends_tab
        render_market_trends_tab()

    with alerts_tab:
        from tabs.alerts_tab import render_alerts_tab
        render_alerts_tab()

    with compare_tab:
        from tabs.compare_tab import render_compare_tab
        render_compare_tab()

    with admin_tab:
        from tabs.admin_tab import render_admin_tab
        render_admin_tab(df)

    # ------------------------------------------------------------------
    # Footer
    # ------------------------------------------------------------------
    st.markdown("---")
    st.caption(f"📅 Última actualización: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    st.caption(f"📊 Total de registros mostrados: {len(df):,}")


if __name__ == "__main__":
    main()
