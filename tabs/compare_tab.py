"""
Comparador de Barrios — side-by-side analysis of 2-4 Madrid neighbourhoods.

Entry point: render_compare_tab()

Shows per barrio:
  - Key metrics: price, €/m², active listings, avg size, avg rooms, days on market
  - Gross rental yield (if rental data available)
  - Weekly price/m² evolution chart (last 16 weeks)
  - Opportunity score distribution (% of listings below barrio median)
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go

# Colour palette — one per barrio slot
BARRIO_COLORS = ["#4fc3f7", "#81c784", "#ffb74d", "#f48fb1"]


def _fmt(value, fmt="{:,.0f}", fallback="—"):
    if value is None or (isinstance(value, float) and value != value):
        return fallback
    return fmt.format(value)


def render_compare_tab() -> None:
    """Render all content for the 📊 Comparar Barrios tab."""

    st.subheader("📊 Comparador de Barrios")
    st.caption("Selecciona entre 2 y 4 barrios para comparar sus indicadores clave.")

    # ── Barrio selector ───────────────────────────────────────────────────────
    from database import (
        get_barrio_price_stats,
        get_barrio_summary,
        get_price_evolution_by_barrio,
        get_rental_yields,
    )

    # Build the full list of barrios that have enough listings
    all_barrio_stats = get_barrio_price_stats(min_listings=3)
    all_barrios = sorted(all_barrio_stats.keys())

    if len(all_barrios) < 2:
        st.warning("No hay suficientes datos por barrio todavía. Ejecuta el scraper primero.")
        return

    # Defaults: first 3 barrios alphabetically (user will change them)
    default_selection = all_barrios[:3] if len(all_barrios) >= 3 else all_barrios

    selected = st.multiselect(
        "Barrios a comparar",
        options=all_barrios,
        default=default_selection,
        max_selections=4,
        help="Selecciona entre 2 y 4 barrios.",
    )

    if len(selected) < 2:
        st.info("Selecciona al menos 2 barrios para ver la comparativa.")
        return

    # ── Load data ─────────────────────────────────────────────────────────────
    summaries = get_barrio_summary(selected)
    summary_map = {s["barrio"]: s for s in summaries}

    yields_list = get_rental_yields(min_listings=3)
    yield_map   = {r["barrio"]: r["yield_pct"] for r in yields_list}

    evolution   = get_price_evolution_by_barrio(selected, weeks=16)
    evo_df      = pd.DataFrame(evolution) if evolution else pd.DataFrame()

    # ── KPI comparison cards ──────────────────────────────────────────────────
    st.markdown("### 🔢 Indicadores Clave")

    cols = st.columns(len(selected))
    for i, barrio in enumerate(selected):
        color  = BARRIO_COLORS[i % len(BARRIO_COLORS)]
        s      = summary_map.get(barrio, {})
        y_pct  = yield_map.get(barrio)

        with cols[i]:
            st.markdown(
                f"<div style='border-top: 4px solid {color}; padding-top: 8px;'>"
                f"<b style='font-size:15px;'>{barrio}</b><br>"
                f"<span style='color:#888;font-size:12px;'>{s.get('distrito','')}</span>"
                f"</div>",
                unsafe_allow_html=True,
            )
            st.metric("🏠 Propiedades activas", _fmt(s.get("active_count"), "{:,}"))
            st.metric("💰 Precio medio",         f"€{_fmt(s.get('median_price'))}")
            st.metric("📏 €/m² medio",           f"€{_fmt(s.get('median_price_sqm'))}")
            st.metric("📐 Tamaño medio",          f"{_fmt(s.get('avg_size_sqm'), '{:.0f}')} m²")
            st.metric("🛏️ Habitaciones medias",  _fmt(s.get("avg_rooms"), "{:.1f}"))
            st.metric("⏱️ Días en mercado",       _fmt(s.get("avg_days_market"), "{:.0f}"))
            if y_pct is not None:
                badge = "🟢" if y_pct >= 5.0 else ("🟡" if y_pct >= 3.5 else "🔴")
                st.metric("🏘️ Rentabilidad bruta", f"{badge} {y_pct:.2f}%")
            else:
                st.metric("🏘️ Rentabilidad bruta", "Sin datos")

    # ── Radar / spider chart — normalised scores ──────────────────────────────
    st.markdown("### 🕸️ Perfil Comparativo")
    st.caption("Cada eje normalizado de 0 (peor) a 100 (mejor) dentro de la selección.")

    metrics_labels = [
        "Precio bajo",       # inverse of median_price (cheaper = better)
        "€/m² bajo",         # inverse of median_price_sqm
        "Oferta (listados)", # active_count
        "Tamaño",            # avg_size_sqm
        "Rapidez venta",     # inverse of avg_days_market (fewer days = better)
        "Rentabilidad",      # yield_pct
    ]

    def _normalise(values, inverse=False):
        """Normalise list of floats to 0-100. None → 0."""
        cleaned = [v if v is not None else 0.0 for v in values]
        lo, hi  = min(cleaned), max(cleaned)
        if hi == lo:
            return [50.0] * len(cleaned)
        normed = [(v - lo) / (hi - lo) * 100 for v in cleaned]
        return [100 - n for n in normed] if inverse else normed

    raw = {
        "price":    [summary_map.get(b, {}).get("median_price")     for b in selected],
        "sqm":      [summary_map.get(b, {}).get("median_price_sqm") for b in selected],
        "count":    [summary_map.get(b, {}).get("active_count")     for b in selected],
        "size":     [summary_map.get(b, {}).get("avg_size_sqm")     for b in selected],
        "days":     [summary_map.get(b, {}).get("avg_days_market")  for b in selected],
        "yield":    [yield_map.get(b)                               for b in selected],
    }

    normed = {
        "price":  _normalise(raw["price"],  inverse=True),   # lower price = better
        "sqm":    _normalise(raw["sqm"],    inverse=True),
        "count":  _normalise(raw["count"]),
        "size":   _normalise(raw["size"]),
        "days":   _normalise(raw["days"],   inverse=True),   # fewer days = better
        "yield":  _normalise(raw["yield"]),
    }

    fig_radar = go.Figure()
    for i, barrio in enumerate(selected):
        vals = [
            normed["price"][i],
            normed["sqm"][i],
            normed["count"][i],
            normed["size"][i],
            normed["days"][i],
            normed["yield"][i],
        ]
        # Close the polygon
        vals_closed   = vals + [vals[0]]
        labels_closed = metrics_labels + [metrics_labels[0]]

        fig_radar.add_trace(go.Scatterpolar(
            r     = vals_closed,
            theta = labels_closed,
            fill  = "toself",
            name  = barrio,
            line  = dict(color=BARRIO_COLORS[i % len(BARRIO_COLORS)], width=2),
            fillcolor=BARRIO_COLORS[i % len(BARRIO_COLORS)].replace(")", ", 0.15)").replace("rgb", "rgba")
                  if BARRIO_COLORS[i].startswith("rgb") else BARRIO_COLORS[i % len(BARRIO_COLORS)] + "26",
            hovertemplate=(
                f"<b>{barrio}</b><br>"
                "%{theta}: %{r:.0f}/100<extra></extra>"
            ),
        ))

    fig_radar.update_layout(
        polar=dict(
            radialaxis=dict(visible=True, range=[0, 100], tickfont=dict(size=9)),
            angularaxis=dict(tickfont=dict(size=11)),
        ),
        showlegend=True,
        height=420,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=40, r=40, t=20, b=20),
    )
    st.plotly_chart(fig_radar, use_container_width=True)

    # ── Price evolution chart ─────────────────────────────────────────────────
    st.markdown("### 📈 Evolución del Precio €/m² (últimas 16 semanas)")

    if not evo_df.empty and "week_start" in evo_df.columns:
        fig_evo = go.Figure()
        for i, barrio in enumerate(selected):
            barrio_df = evo_df[evo_df["barrio"] == barrio].copy()
            if barrio_df.empty:
                continue
            barrio_df = barrio_df.sort_values("week_start")
            fig_evo.add_trace(go.Scatter(
                x    = barrio_df["week_start"],
                y    = barrio_df["median_price_sqm"],
                mode = "lines+markers",
                name = barrio,
                line = dict(color=BARRIO_COLORS[i % len(BARRIO_COLORS)], width=3),
                marker=dict(size=7),
                customdata=barrio_df[["listing_count"]].values,
                hovertemplate=(
                    f"<b>{barrio}</b><br>"
                    "Semana: %{x}<br>"
                    "€/m²: %{y:,.0f}<br>"
                    "Inmuebles: %{customdata[0]}<extra></extra>"
                ),
            ))
        fig_evo.update_layout(
            xaxis_title   = "Semana",
            yaxis_title   = "€/m² (promedio)",
            hovermode     = "x unified",
            height        = 380,
            paper_bgcolor = "rgba(0,0,0,0)",
            plot_bgcolor  = "rgba(0,0,0,0)",
            legend        = dict(orientation="h", yanchor="bottom", y=1.02),
            margin        = dict(l=10, r=10, t=30, b=40),
        )
        st.plotly_chart(fig_evo, use_container_width=True)
    else:
        st.info("No hay suficiente historial de precios para mostrar la evolución.")

    # ── Summary table ─────────────────────────────────────────────────────────
    st.markdown("### 📋 Resumen Comparativo")

    rows = []
    for barrio in selected:
        s     = summary_map.get(barrio, {})
        y_pct = yield_map.get(barrio)
        rows.append({
            "Barrio":              barrio,
            "Distrito":            s.get("distrito", "—"),
            "Activos":             int(s["active_count"]) if s.get("active_count") else 0,
            "Precio medio (€)":    round(s["median_price"]) if s.get("median_price") else None,
            "€/m² medio":          round(s["median_price_sqm"]) if s.get("median_price_sqm") else None,
            "Tamaño medio (m²)":   round(s["avg_size_sqm"], 1) if s.get("avg_size_sqm") else None,
            "Hab. medias":         round(s["avg_rooms"], 1) if s.get("avg_rooms") else None,
            "Días mercado":        round(s["avg_days_market"]) if s.get("avg_days_market") else None,
            "Rentabilidad bruta":  f"{y_pct:.2f}%" if y_pct else "—",
        })

    table_df = pd.DataFrame(rows)
    st.dataframe(
        table_df,
        use_container_width=True,
        column_config={
            "Precio medio (€)":   st.column_config.NumberColumn(format="€%d"),
            "€/m² medio":         st.column_config.NumberColumn(format="€%d"),
            "Tamaño medio (m²)":  st.column_config.NumberColumn(format="%.1f m²"),
        },
        hide_index=True,
    )
