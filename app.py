"""
Streamlit multipage dashboard for Madrid Real Estate Tracker.

Uses ``st.navigation`` (Streamlit 1.36+) so that only the active page
executes on each run.  Replaces the old ``st.tabs()`` + JS-polling hack
which rendered *all* 8 tabs on every interaction.
"""

import os

import streamlit as st
from datetime import datetime
from pathlib import Path


# ---------------------------------------------------------------------------
# Backend bootstrap: bridge Streamlit secrets → DB_BACKEND env var.
#
# Streamlit Community Cloud free tier exposes secrets via ``st.secrets``
# but does NOT expose a UI for arbitrary environment variables.  The DB
# shim in ``db.connection`` reads ``DB_BACKEND`` from ``os.environ`` to
# decide between SQLite (default) and Postgres.  If the deployment has
# a ``[postgres]`` block configured, default to Postgres — that's the
# only reason that block would be there in the first place.  An
# explicit ``DB_BACKEND`` env var (CI workflows, local dev) still wins.
# ---------------------------------------------------------------------------
if "DB_BACKEND" not in os.environ:
    try:
        if "postgres" in st.secrets and st.secrets["postgres"].get("url"):
            os.environ["DB_BACKEND"] = "postgres"
    except Exception:
        pass


# Imports that touch the DB go *after* the bootstrap so they see the
# right backend on first access.
from auth import check_password  # noqa: E402
from database import (  # noqa: E402
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
# Authentication: see auth.py — bcrypt + rate limit + session expiry + audit log
# ---------------------------------------------------------------------------


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
        # ``start_time`` is TEXT on SQLite, datetime on Postgres.
        if log:
            ts = log[0]["start_time"]
            last_scrape = ts.isoformat()[:10] if hasattr(ts, "isoformat") else str(ts)[:10]
        else:
            last_scrape = "sin datos"
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
            st.Page("pages/mi_zona.py",       title="🎯 Mi Zona", default=True),
            st.Page("pages/oportunidades.py", title="🏆 Oportunidades"),
            st.Page("pages/bajadas.py",       title="📉 Bajadas de Precio"),
            st.Page("pages/busqueda.py",      title="🔍 Búsqueda"),
            st.Page("pages/comparador.py",    title="⚖️ Comparador"),
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
