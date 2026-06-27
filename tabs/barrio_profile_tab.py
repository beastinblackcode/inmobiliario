"""🏘️ Perfil de Barrio — ficha rica por barrio para decidir *dónde vivir*.

Consume el builder existente ``barrio_profiles.build_all_barrio_profiles``
(KPIs + veredicto determinista + top oportunidades + distribución + vecinos),
cacheado, y renderiza el perfil del barrio seleccionado.
"""

from __future__ import annotations

from typing import Any, Dict

import pandas as pd
import streamlit as st


# Pretty labels for the amenity keys produced by the distribution builder.
_AMENITY_LABELS = {
    "has_terraza": "Terraza", "has_balcon": "Balcón", "has_garaje": "Garaje",
    "has_trastero": "Trastero", "has_piscina": "Piscina", "has_ascensor": "Ascensor",
    "has_portero": "Portero", "has_aire_acondicionado": "A/A",
    "has_calefaccion": "Calefacción", "has_armarios": "Armarios",
    "has_near_metro": "Cerca metro", "has_near_parque": "Cerca parque",
    "has_near_colegio": "Cerca colegio", "has_near_hospital": "Cerca hospital",
}


@st.cache_data(ttl=600, show_spinner="Construyendo perfiles de barrio…")
def _load_profiles() -> Dict[str, Any]:
    from barrio_profiles import build_all_barrio_profiles
    return build_all_barrio_profiles()


def _render_kpis(kpis: Dict, baseline: Dict) -> None:
    c1, c2, c3, c4 = st.columns(4)
    ppsqm = kpis.get("price_per_sqm")
    with c1:
        st.metric(
            "💶 €/m² mediano",
            f"€{ppsqm:,.0f}" if ppsqm else "—",
            delta=f"{kpis['vs_madrid_pct']:+.1f}% vs Madrid" if kpis.get("vs_madrid_pct") is not None else None,
            delta_color="inverse",  # cheaper than Madrid = good for a buyer
        )
    with c2:
        mp = kpis.get("median_price")
        st.metric("🏠 Precio mediano", f"€{mp:,.0f}" if mp else "—",
                  delta=f"{kpis['vs_distrito_pct']:+.1f}% vs distrito" if kpis.get("vs_distrito_pct") is not None else None,
                  delta_color="inverse")
    with c3:
        days = kpis.get("avg_days_market")
        base_days = baseline.get("median_days_on_market")
        st.metric("⏱️ Días en mercado", f"{days:.0f}" if days else "—",
                  delta=f"Madrid {base_days:.0f}" if base_days else None, delta_color="off")
    with c4:
        st.metric("📦 Pisos activos", f"{kpis.get('active_count', 0):,}")

    c5, c6, c7, c8 = st.columns(4)
    with c5:
        sz = kpis.get("avg_size_sqm")
        st.metric("📐 Tamaño medio", f"{sz:.0f} m²" if sz else "—")
    with c6:
        rooms = kpis.get("avg_rooms")
        st.metric("🛏️ Habitaciones", f"{rooms:.1f}" if rooms else "—")
    with c7:
        pct = kpis.get("pct_with_drops")
        st.metric("📉 Pisos con bajadas", f"{pct:.0f}%" if pct is not None else "—",
                  help="Proporción del stock activo del barrio con ≥1 bajada de precio.")
    with c8:
        ad = kpis.get("avg_drops")
        st.metric("🔁 Bajadas/piso", f"{ad:.1f}" if ad else "—")


def _render_distribution(dist: Dict) -> None:
    import plotly.express as px

    col1, col2 = st.columns(2)
    with col1:
        buckets = dist.get("price_buckets") or []
        nonzero = [b for b in buckets if b["count"] > 0]
        if nonzero:
            bdf = pd.DataFrame(nonzero)
            fig = px.bar(bdf, x="range", y="count", labels={"range": "Precio", "count": "Pisos"})
            fig.update_layout(height=300, margin=dict(l=10, r=10, t=30, b=10),
                              title="Distribución de precios")
            st.plotly_chart(fig, use_container_width=True)
    with col2:
        rooms = dist.get("rooms_distribution") or {}
        rooms = {k: v for k, v in rooms.items() if v}
        if rooms:
            rdf = pd.DataFrame({"Habitaciones": list(rooms.keys()), "Pisos": list(rooms.values())})
            fig = px.bar(rdf, x="Habitaciones", y="Pisos")
            fig.update_layout(height=300, margin=dict(l=10, r=10, t=30, b=10),
                              title="Distribución por habitaciones")
            st.plotly_chart(fig, use_container_width=True)

    amen = dist.get("amenities_pct") or {}
    if amen:
        st.markdown("**Características más frecuentes** "
                    f"<span style='color:#94a3b8;font-size:13px'>(sobre {dist.get('amenities_sample_size', 0)} pisos con datos)</span>",
                    unsafe_allow_html=True)
        items = sorted(amen.items(), key=lambda kv: kv[1], reverse=True)
        cols = st.columns(4)
        for i, (k, v) in enumerate(items):
            with cols[i % 4]:
                st.write(f"{_AMENITY_LABELS.get(k, k)}: **{v:.0f}%**")

    year = dist.get("median_construction_year")
    if year:
        st.caption(f"🏗️ Año de construcción mediano: **{year}**")


def _render_top_opportunities(opps: list) -> None:
    if not opps:
        st.info("Sin oportunidades destacadas en este barrio ahora mismo.")
        return
    df = pd.DataFrame(opps)
    show = df[[
        "title", "price", "price_per_sqm", "size_sqm", "rooms",
        "vs_distrito_pct", "days_on_market", "num_drops",
        "quality_score", "negotiability_score", "url",
    ]].rename(columns={
        "title": "Título", "price": "Precio €", "price_per_sqm": "€/m²",
        "size_sqm": "m²", "rooms": "Hab.", "vs_distrito_pct": "vs Distrito %",
        "days_on_market": "Días", "num_drops": "Bajadas",
        "quality_score": "Calidad", "negotiability_score": "Negociab.", "url": "Enlace",
    })
    st.dataframe(
        show, hide_index=True, use_container_width=True,
        column_config={"Enlace": st.column_config.LinkColumn("Enlace", display_text="ver")},
    )


def _render_neighbours(neighbours: list) -> None:
    if not neighbours:
        return
    df = pd.DataFrame(neighbours)
    show = df[["barrio", "distrito", "price_per_sqm", "median_price", "active_count", "diff_pct"]].rename(columns={
        "barrio": "Barrio", "distrito": "Distrito", "price_per_sqm": "€/m²",
        "median_price": "Precio mediano", "active_count": "Activos", "diff_pct": "Δ €/m² vs este",
    })
    st.dataframe(
        show, hide_index=True, use_container_width=True,
        column_config={"Δ €/m² vs este": st.column_config.NumberColumn(format="%+.1f %%")},
    )


def render_barrio_profile_tab() -> None:
    st.header("🏘️ Perfil de Barrio")
    st.caption("Ficha completa por barrio para decidir *dónde* vivir antes de elegir el piso.")

    data = _load_profiles()
    profiles = data.get("profiles", {})
    baseline = data.get("madrid_baseline", {})

    if not profiles:
        st.warning("No hay perfiles disponibles (sin pisos activos).")
        return

    # Selector grouped/sorted by distrito → barrio.
    ordered = sorted(profiles.values(), key=lambda p: (p.get("distrito") or "", p.get("barrio") or ""))
    labels = {f"{p['barrio']} · {p.get('distrito', '')}": p["barrio"] for p in ordered}
    choice = st.selectbox("Barrio", options=list(labels.keys()), key="barrio_profile_select")
    prof = profiles[labels[choice]]

    # ── Verdict banner ────────────────────────────────────────────────────
    v = prof.get("verdict", {})
    st.subheader(f"{v.get('emoji', '')} {v.get('label', '')}")
    if v.get("summary"):
        st.write(v["summary"])
    if v.get("recommendation"):
        st.info(f"💡 {v['recommendation']}")

    st.markdown("---")
    _render_kpis(prof.get("kpis", {}), baseline)

    st.markdown("---")
    st.subheader("📊 Distribución del stock")
    _render_distribution(prof.get("distribution", {}))

    st.markdown("---")
    st.subheader("🏆 Top oportunidades del barrio")
    _render_top_opportunities(prof.get("top_opportunities", []))

    st.markdown("---")
    st.subheader("🧭 Barrios vecinos comparables")
    _render_neighbours(prof.get("neighbours", []))
