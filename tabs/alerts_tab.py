"""
Tab: Alertas Personalizadas
Create and manage custom property search alerts.
Matching listings (added in the last 24h) are shown per alert and included in the daily email.
"""

import streamlit as st
import json
import pandas as pd

from database import (
    init_alerts_table,
    get_alerts,
    add_alert,
    delete_alert,
    get_alert_matches,
)

ALL_DISTRICTS = [
    "Centro", "Arganzuela", "Retiro", "Salamanca", "Chamartín",
    "Tetuán", "Chamberí", "Fuencarral-El Pardo", "Moncloa-Aravaca",
    "Latina", "Carabanchel", "Usera", "Puente de Vallecas",
    "Moratalaz", "Ciudad Lineal", "Hortaleza", "Villaverde",
    "Villa de Vallecas", "Vicálvaro", "San Blas-Canillejas", "Barajas",
]


def render_alerts_tab():
    init_alerts_table()

    st.markdown(
        "Define criterios de búsqueda y recibe en el email diario las nuevas propiedades "
        "que los cumplan. Las alertas también se comprueban aquí en tiempo real."
    )

    # ── Mensajes de estado (sin rerun) ────────────────────────────────────────
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
                    )
                    st.session_state["_alert_saved"] = alert_name.strip()

    st.markdown("---")

    # ── Alertas activas ───────────────────────────────────────────────────────
    alerts = get_alerts()  # Refresh after potential create

    if not alerts:
        st.info("No tienes alertas configuradas. Crea una usando el formulario de arriba.")
        return

    st.subheader(f"📋 Tus alertas ({len(alerts)})")

    for alert in alerts:
        distritos_list = json.loads(alert.get("distritos") or "[]")
        distritos_str  = ", ".join(distritos_list) if distritos_list else "Todos"

        criteria = []
        if alert.get("max_price"):
            criteria.append(f"💰 Máx. {alert['max_price']:,}€")
        if alert.get("min_size"):
            criteria.append(f"📐 Mín. {alert['min_size']}m²")
        if alert.get("max_sqm_price"):
            criteria.append(f"📊 Máx. {alert['max_sqm_price']:,}€/m²")
        if alert.get("min_rooms"):
            criteria.append(f"🛏️ Mín. {alert['min_rooms']} hab.")
        if alert.get("seller_type"):
            criteria.append(f"👤 {alert['seller_type']}")
        criteria_str = "  ·  ".join(criteria) if criteria else "Sin filtros adicionales"

        with st.container(border=True):
            col_title, col_del = st.columns([4, 1])
            with col_title:
                st.markdown(f"### 🔔 {alert['name']}")
                st.caption(f"📍 {distritos_str}  ·  {criteria_str}")
                st.caption(f"Creada el {alert['created_at'][:10]}")
            with col_del:
                if st.button("🗑️ Eliminar", key=f"del_{alert['id']}"):
                    delete_alert(alert["id"])
                    st.session_state["_alert_deleted"] = f"Alerta «{alert['name']}» eliminada."

            col_hours, _ = st.columns([1, 3])
            with col_hours:
                hours = st.selectbox(
                    "Ver coincidencias de las últimas:",
                    options=[24, 48, 168, 720],
                    format_func=lambda h: {24: "24h", 48: "48h", 168: "7 días", 720: "30 días"}[h],
                    key=f"hours_{alert['id']}",
                )

            matches = get_alert_matches(alert, hours=hours)

            if matches:
                st.success(f"**{len(matches)} propiedades** coinciden con esta alerta:")
                df_m = pd.DataFrame(matches)
                df_m["Precio"] = df_m["price"].apply(lambda x: f"{x:,}€")
                df_m["€/m²"]   = df_m["price_sqm"].apply(lambda x: f"{x:,.0f}" if x else "—")
                df_m["Tamaño"] = df_m["size_sqm"].apply(lambda x: f"{x}m²" if x else "—")
                df_m["Hab."]   = df_m["rooms"].apply(lambda x: str(x) if x else "—")
                df_m["Enlace"] = df_m["url"].apply(
                    lambda u: f'<a href="{u}" target="_blank">Ver</a>' if u else "—"
                )
                df_show = df_m[["title", "barrio", "Precio", "€/m²", "Tamaño", "Hab.", "seller_type", "Enlace"]].copy()
                df_show.columns = ["Título", "Barrio", "Precio", "€/m²", "Tamaño", "Hab.", "Vendedor", "Enlace"]
                df_show["Título"] = df_show["Título"].str[:50]
                st.markdown(df_show.to_html(escape=False, index=False), unsafe_allow_html=True)
            else:
                st.info(f"Sin nuevas propiedades en las últimas {hours}h que cumplan esta alerta.")
