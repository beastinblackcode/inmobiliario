"""📈 Precios por Distrito — dashboard embebido con datos en vivo.

Reutiliza el generador de ``export_distrito_dashboard.py`` (mismo HTML/gráficos
que el artefacto público) pero calculado al vuelo contra la BD en cada visita,
así no hay que regenerar ni republicar nada.
"""
from datetime import date

import streamlit as st
import streamlit.components.v1 as components

from export_distrito_dashboard import fetch_data, compute_forecasts, render_html


@st.cache_data(ttl=600, show_spinner=False)
def _distritos() -> list[str]:
    """Distritos con anuncios activos, más poblados primero."""
    from database import get_connection

    with get_connection() as conn:
        c = conn.cursor()
        c.execute(
            "SELECT distrito, COUNT(*) AS n FROM listings "
            "WHERE status = 'active' AND distrito IS NOT NULL "
            "  AND distrito NOT LIKE '%(General)%' "
            "GROUP BY distrito ORDER BY n DESC"
        )
        return [r["distrito"] for r in c.fetchall()]


@st.cache_data(ttl=600, show_spinner="Calculando dashboard…")
def _dashboard_html(distrito: str, year: int) -> str:
    data = compute_forecasts(fetch_data(distrito, year))
    return render_html(data)


st.title("📈 Precios por Distrito")
st.caption(
    "Evolución de precios de venta, previsión a cierre de año y velocidad de "
    "venta — calculado en vivo desde la base de datos."
)

distritos = _distritos()
if not distritos:
    st.warning("⚠️ No hay datos de distritos disponibles.")
    st.stop()

default_idx = distritos.index("Moratalaz") if "Moratalaz" in distritos else 0

col_sel, col_btn = st.columns([4, 1])
with col_sel:
    distrito = st.selectbox("Distrito", distritos, index=default_idx)
with col_btn:
    st.write("")  # alinea el botón con el selectbox
    if st.button("🔄 Actualizar", help="Recalcula saltando la caché (10 min)"):
        _dashboard_html.clear()
        _distritos.clear()
        st.rerun()

try:
    html = _dashboard_html(distrito, date.today().year)
except SystemExit:
    # fetch_data llama a _die()/sys.exit si el distrito no tiene datos.
    st.error(f"No hay datos suficientes para «{distrito}».")
    st.stop()
except Exception as exc:  # noqa: BLE001
    st.error(f"No se pudo generar el dashboard: {exc}")
    st.stop()

# El HTML es un documento autocontenido → se renderiza en un iframe.
components.html(html, height=2700, scrolling=True)
