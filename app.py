"""
Streamlit multipage dashboard for Madrid Real Estate Tracker.

Uses ``st.navigation`` (Streamlit 1.36+) so that only the active page
executes on each run.  Replaces the old ``st.tabs()`` + JS-polling hack
which rendered *all* 8 tabs on every interaction.
"""

import streamlit as st
from datetime import datetime
from pathlib import Path

from database import (
    download_database_from_cloud,
    is_streamlit_cloud,
    DATABASE_PATH,
)

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
    .main { padding: 0rem 1rem; }
    .stMetric {
        background-color: #f0f2f6;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    h1 { color: #1f77b4; padding-bottom: 20px; }
    h2 { color: #2c3e50; padding-top: 20px; }
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
# Sidebar: version & environment info
# ---------------------------------------------------------------------------

def _render_sidebar_info():
    """Show version, last scrape, DB size at the bottom of the sidebar."""
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


# ---------------------------------------------------------------------------
# Main entry point — multipage navigation
# ---------------------------------------------------------------------------

def main():
    # Authentication gate
    if not check_password():
        st.stop()

    # Ensure database is available
    if not download_database_from_cloud():
        st.error(
            "❌ No se pudo cargar la base de datos. Por favor, contacta al administrador."
        )
        st.stop()

    # ------------------------------------------------------------------
    # Build page registry with st.navigation (Streamlit ≥1.36)
    # Only the selected page runs — no more rendering all 8 tabs.
    # ------------------------------------------------------------------
    pages = {
        "🏠 Caza": [
            st.Page("pages/oportunidades.py", title="🎯 Oportunidades", default=True),
            st.Page("pages/bajadas.py",       title="📉 Bajadas de Precio"),
            st.Page("pages/busqueda.py",      title="🔍 Búsqueda"),
            st.Page("pages/seguimientos.py",  title="🔔 Mis Seguimientos"),
            st.Page("pages/detalle.py",       title="🔎 Detalle de Anuncio"),
        ],
        "⚙️ Operaciones": [
            st.Page("pages/admin.py",         title="⚙️ Administración"),
            st.Page("pages/vigilancia.py",    title="🛡️ Vigilancia"),
        ],
    }

    pg = st.navigation(pages)

    # Sidebar info — shown on every page
    _render_sidebar_info()

    # Run the selected page
    pg.run()


if __name__ == "__main__":
    main()
