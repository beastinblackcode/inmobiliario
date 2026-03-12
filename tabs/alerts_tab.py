"""
Tab: Alertas Personalizadas
Create and manage custom property search alerts.
Matching listings (added since last visit, or last 24h) are shown per alert
and included in the daily email. Supports min_score filter and NLP badges.
"""

import streamlit as st
import json
import pandas as pd
from datetime import datetime

from database import (
    init_alerts_table,
    get_alerts,
    add_alert,
    delete_alert,
    get_alert_matches,
    count_alert_new_matches,
    touch_alert_checked,
)

ALL_DISTRICTS = [
    "Centro", "Arganzuela", "Retiro", "Salamanca", "Chamartín",
    "Tetuán", "Chamberí", "Fuencarral-El Pardo", "Moncloa-Aravaca",
    "Latina", "Carabanchel", "Usera", "Puente de Vallecas",
    "Moratalaz", "Ciudad Lineal", "Hortaleza", "Villaverde",
    "Villa de Vallecas", "Vicálvaro", "San Blas-Canillejas", "Barajas",
]


def _nlp_badges(match: dict) -> str:
    """Return a compact string with NLP signal badges for a match row."""
    parts = []
    if match.get("urgency"):    parts.append("🔴 Urgente")
    if match.get("direct"):     parts.append("💼 Directo")
    if match.get("negotiable"): parts.append("🟡 Negociable")
    if match.get("renovated"):  parts.append("🟢 Reformado")
    if match.get("needs_work"): parts.append("🔧 A reformar")
    return "  ·  ".join(parts) if parts else ""


@st.fragment
def render_alerts_tab():
    init_alerts_table()

    st.markdown(
        "Define criterios de búsqueda y sigue en tiempo real los nuevos pisos "
        "que los cumplan. Las alertas también se envían en el email diario."
    )

    # ── Mensajes de estado ────────────────────────────────────────────────────
    if st.session_state.get("_alert_saved"):
        st.success(f"✅ Alerta «{st.session_state.pop('_alert_saved')}» creada correctamente.")
    if st.session_state.get("_alert_deleted"):
        st.success(st.session_state.pop("_alert_deleted"))

    alerts = get_alerts()

    # ── Crear nueva alerta ────────────────────────────────────────────────────
    with st.expander("➕ Crear nueva alerta", expanded=len(alerts) == 0):
        with st.form("new_alert_form", clear_on_submit=True):
            st.subheader("Nueva alerta")
            col1, col2 = st.columns(2)
            with col1:
                alert_name = st.text_input(
                    "Nombre de la alerta *",
                    placeholder="Ej: Chamberí amplio < 500k",
                )
                sel_distritos = st.multiselect(
                    "Distritos (vacío = todos)",
                    options=ALL_DISTRICTS,
                    default=[],
                )
                max_price = st.number_input(
                    "Precio máximo (€)", min_value=0, value=0, step=10000,
                    help="0 = sin límite",
                )
                min_size = st.number_input(
                    "Superficie mínima (m²)", min_value=0, value=0, step=5,
                    help="0 = sin límite",
                )
            with col2:
                max_sqm_price = st.number_input(
                    "€/m² máximo", min_value=0, value=0, step=100,
                    help="0 = sin límite",
                )
                min_rooms = st.number_input(
                    "Habitaciones mínimas", min_value=0, value=0, step=1,
                    help="0 = sin límite",
                )
                seller_type = st.selectbox(
                    "Tipo de vendedor",
                    options=["Cualquiera", "Particular", "Agencia"],
                )
                min_score = st.number_input(
                    "Score mínimo de oportunidad",
                    min_value=0, max_value=100, value=0, step=5,
                    help="0 = sin filtro. Score calculado por el modelo (0–100). Útil para filtrar sólo los mejores anuncios.",
                )

            submitted = st.form_submit_button("💾 Guardar alerta", type="primary")

            if submitted:
                if not alert_name.strip():
                    st.error("El nombre es obligatorio.")
                else:
                    add_alert(
                        name=alert_name.strip(),
                        distritos=sel_distritos or [],
                        max_price=max_price if max_price > 0 else None,
                        min_size=min_size if min_size > 0 else None,
                        max_sqm_price=max_sqm_price if max_sqm_price > 0 else None,
                        min_rooms=min_rooms if min_rooms > 0 else None,
                        seller_type=seller_type if seller_type != "Cualquiera" else None,
                        min_score=min_score if min_score > 0 else None,
                    )
                    st.session_state["_alert_saved"] = alert_name.strip()

    st.markdown("---")

    # ── Alertas activas ───────────────────────────────────────────────────────
    alerts = get_alerts()  # Refresh after potential create

    if not alerts:
        st.info("No tienes alertas configuradas. Crea una usando el formulario de arriba.")
        return

    # Pre-compute new match counts for all alerts (for badges)
    new_counts = {a["id"]: count_alert_new_matches(a) for a in alerts}
    total_new  = sum(new_counts.values())

    if total_new > 0:
        st.success(f"🔔 **{total_new} nuevo{'s' if total_new > 1 else ''} anuncio{'s' if total_new > 1 else ''}** desde tu última visita en {len([v for v in new_counts.values() if v > 0])} alerta(s).")

    st.subheader(f"📋 Tus alertas ({len(alerts)})")

    for alert in alerts:
        distritos_list = json.loads(alert.get("distritos") or "[]")
        distritos_str  = ", ".join(distritos_list) if distritos_list else "Todos los distritos"

        criteria = []
        if alert.get("max_price"):     criteria.append(f"💰 Máx. {alert['max_price']:,}€")
        if alert.get("min_size"):      criteria.append(f"📐 Mín. {alert['min_size']}m²")
        if alert.get("max_sqm_price"): criteria.append(f"📊 Máx. {alert['max_sqm_price']:,}€/m²")
        if alert.get("min_rooms"):     criteria.append(f"🛏️ Mín. {alert['min_rooms']} hab.")
        if alert.get("seller_type"):   criteria.append(f"👤 {alert['seller_type']}")
        if alert.get("min_score"):     criteria.append(f"⭐ Score ≥ {alert['min_score']}")
        criteria_str = "  ·  ".join(criteria) if criteria else "Sin filtros adicionales"

        n_new = new_counts.get(alert["id"], 0)
        badge = f" 🔴 **{n_new} nuevo{'s' if n_new > 1 else ''}**" if n_new > 0 else ""

        with st.container(border=True):
            col_title, col_del = st.columns([4, 1])
            with col_title:
                st.markdown(f"### 🔔 {alert['name']}{badge}")
                st.caption(f"📍 {distritos_str}  ·  {criteria_str}")
                last_checked = alert.get("last_checked")
                if last_checked:
                    st.caption(f"Última revisión: {last_checked[:16].replace('T', ' ')}")
                else:
                    st.caption("Nunca revisada — mostrando últimas 24h")
            with col_del:
                if st.button("🗑️ Eliminar", key=f"del_{alert['id']}"):
                    delete_alert(alert["id"])
                    st.session_state["_alert_deleted"] = f"Alerta «{alert['name']}» eliminada."
                    st.rerun()

            # ── Selector de ventana de tiempo ─────────────────────────────────
            col_mode, col_hours, _ = st.columns([2, 2, 2])
            with col_mode:
                mode = st.radio(
                    "Ver",
                    options=["Desde mi última visita", "Ventana de tiempo"],
                    key=f"mode_{alert['id']}",
                    horizontal=True,
                )
            with col_hours:
                if mode == "Ventana de tiempo":
                    hours = st.selectbox(
                        "Período",
                        options=[24, 48, 168, 720],
                        format_func=lambda h: {24: "24h", 48: "48h", 168: "7 días", 720: "30 días"}[h],
                        key=f"hours_{alert['id']}",
                    )
                else:
                    hours = None  # unused in "since last check" mode

            # ── Obtener matches ────────────────────────────────────────────────
            if mode == "Desde mi última visita":
                since = alert.get("last_checked")  # None → fallback to 24h inside function
                matches = get_alert_matches(alert, hours=24, since_datetime=since)
                # Mark as checked now
                touch_alert_checked(alert["id"])
            else:
                matches = get_alert_matches(alert, hours=hours)

            # ── Mostrar matches ────────────────────────────────────────────────
            if matches:
                label = "nuevos desde tu última visita" if mode == "Desde mi última visita" else f"en las últimas {hours}h"
                st.success(f"**{len(matches)} propiedad{'es' if len(matches) > 1 else ''}** {label}:")

                for m in matches:
                    price     = f"€{m['price']:,}" if m.get("price") else "—"
                    sqm       = f"{int(m['size_sqm'])}m²" if m.get("size_sqm") else "—"
                    rooms_str = f"{m['rooms']} hab." if m.get("rooms") else "—"
                    sqm_price = f"€{int(m['price_sqm']):,}/m²" if m.get("price_sqm") else "—"
                    score     = m.get("score_oportunidad")
                    score_str = f"⭐ {int(score)}" if score else ""
                    badges    = _nlp_badges(m)
                    first_seen = m.get("first_seen_date", "")[:10] if m.get("first_seen_date") else "—"

                    with st.container(border=False):
                        c1, c2 = st.columns([3, 1])
                        with c1:
                            st.markdown(
                                f"**[{m['title'][:60]}]({m['url']})**  \n"
                                f"📍 {m['barrio']}, {m['distrito']}  ·  "
                                f"{rooms_str}  ·  {sqm}  ·  {sqm_price}  ·  visto: {first_seen}"
                            )
                            if badges:
                                st.caption(badges)
                        with c2:
                            st.metric("Precio", price)
                            if score_str:
                                st.caption(score_str)
                    st.divider()
            else:
                label = "desde tu última visita" if mode == "Desde mi última visita" else f"en las últimas {hours}h"
                st.info(f"Sin nuevas propiedades {label} que cumplan esta alerta.")
