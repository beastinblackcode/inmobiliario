"""
Tab: Tendencias del Mercado
Weekly price evolution per district + overall market trend.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np

from database import get_price_trend_by_district, get_market_summary_trend


def _trend_arrow(pct: float) -> str:
    if pct > 2:   return f"🔴 +{pct:.1f}%"
    if pct > 0:   return f"🟡 +{pct:.1f}%"
    if pct > -2:  return f"🟡 {pct:.1f}%"
    return f"🟢 {pct:.1f}%"


def render_market_trends_tab():
    st.header("📈 Tendencias del Mercado")
    st.markdown("Evolución semanal del precio €/m² por distrito. Datos basados en las nuevas publicaciones de cada semana.")

    with st.spinner("Cargando tendencias..."):
        district_data = get_price_trend_by_district()
        market_data   = get_market_summary_trend()

    if not market_data:
        st.warning("Aún no hay suficientes datos históricos. Vuelve en unos días.")
        return

    df_market   = pd.DataFrame(market_data)
    df_district = pd.DataFrame(district_data) if district_data else pd.DataFrame()

    # ── Global KPIs ───────────────────────────────────────────────────────────
    if len(df_market) >= 2:
        first_sqm   = df_market.iloc[0]["avg_sqm"]
        last_sqm    = df_market.iloc[-1]["avg_sqm"]
        change_pct  = 100 * (last_sqm - first_sqm) / first_sqm if first_sqm else 0
        first_price = df_market.iloc[0]["avg_price"]
        last_price  = df_market.iloc[-1]["avg_price"]
        price_chg   = 100 * (last_price - first_price) / first_price if first_price else 0
        n_weeks     = len(df_market)
    else:
        change_pct = price_chg = 0
        last_sqm   = df_market.iloc[-1]["avg_sqm"] if len(df_market) else 0
        last_price = df_market.iloc[-1]["avg_price"] if len(df_market) else 0
        n_weeks    = len(df_market)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("€/m² actual (media Madrid)", f"€{last_sqm:,.0f}")
    c2.metric(
        f"Variación ({n_weeks} semanas)",
        f"{change_pct:+.1f}%",
        delta=f"{'↑ sube' if change_pct > 0 else '↓ baja'}",
        delta_color="inverse",
    )
    c3.metric("Precio medio actual", f"€{last_price:,.0f}")
    c4.metric(
        f"Variación precio ({n_weeks} sem.)",
        f"{price_chg:+.1f}%",
        delta_color="inverse",
    )

    st.markdown("---")

    # ── Overall market trend line ─────────────────────────────────────────────
    st.subheader("🏙️ Mercado global — €/m² semanal")

    fig_global = go.Figure()
    fig_global.add_trace(go.Scatter(
        x=df_market["week_start"],
        y=df_market["avg_sqm"],
        mode="lines+markers",
        name="€/m² Madrid",
        line=dict(color="#4fc3f7", width=3),
        marker=dict(size=8),
        hovertemplate="<b>Semana %{x}</b><br>€/m²: %{y:,.0f}<extra></extra>",
    ))

    # Trend line (linear regression)
    if len(df_market) >= 3:
        x_num = np.arange(len(df_market))
        z     = np.polyfit(x_num, df_market["avg_sqm"].values, 1)
        trend = np.poly1d(z)(x_num)
        color = "#ef5350" if z[0] > 0 else "#66bb6a"
        fig_global.add_trace(go.Scatter(
            x=df_market["week_start"],
            y=trend,
            mode="lines",
            name="Tendencia",
            line=dict(color=color, width=2, dash="dot"),
        ))

    fig_global.update_layout(
        height=280,
        margin=dict(t=10, b=40),
        xaxis_title="Semana",
        yaxis_title="€/m²",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", y=1.1),
    )
    st.plotly_chart(fig_global, use_container_width=True)

    st.markdown("---")

    # ── Per-district evolution ────────────────────────────────────────────────
    st.subheader("🏘️ Evolución por distrito")

    if df_district.empty:
        st.info("Sin datos por distrito.")
        return

    all_districts = sorted(df_district["distrito"].dropna().unique())
    default_sel   = ["Centro", "Chamberí", "Salamanca", "Carabanchel", "Arganzuela"]
    default_sel   = [d for d in default_sel if d in all_districts]

    selected = st.multiselect(
        "Selecciona distritos a comparar",
        options=all_districts,
        default=default_sel,
        key="mt_distritos",
    )

    if not selected:
        st.info("Selecciona al menos un distrito.")
        return

    df_sel = df_district[df_district["distrito"].isin(selected)]

    fig_dist = px.line(
        df_sel,
        x="week_start",
        y="avg_sqm",
        color="distrito",
        markers=True,
        labels={"week_start": "Semana", "avg_sqm": "€/m²", "distrito": "Distrito"},
        title="€/m² semanal por distrito",
        hover_data={"n_listings": True},
    )
    fig_dist.update_layout(
        height=420,
        margin=dict(t=40, b=40),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", y=-0.2),
    )
    st.plotly_chart(fig_dist, use_container_width=True)

    st.markdown("---")

    # ── District trend table ──────────────────────────────────────────────────
    st.subheader("📊 Resumen de tendencia por distrito")
    st.caption("Comparativa entre la primera y última semana disponibles.")

    summary_rows = []
    for dist in all_districts:
        df_d = df_district[df_district["distrito"] == dist].sort_values("week")
        if len(df_d) < 2:
            continue
        first = df_d.iloc[0]["avg_sqm"]
        last  = df_d.iloc[-1]["avg_sqm"]
        chg   = 100 * (last - first) / first if first else 0
        # Linear regression slope
        x_num = np.arange(len(df_d))
        slope = np.polyfit(x_num, df_d["avg_sqm"].values, 1)[0] if len(df_d) >= 3 else 0
        summary_rows.append({
            "Distrito":        dist,
            "€/m² actual":     f"€{last:,.0f}",
            "€/m² hace {n_w} sem.".format(n_w=n_weeks): f"€{first:,.0f}",
            "Variación":       f"{chg:+.1f}%",
            "Tendencia":       _trend_arrow(chg),
            "slope":           slope,
        })

    if summary_rows:
        df_sum = pd.DataFrame(summary_rows).drop(columns=["slope"])
        st.dataframe(df_sum, use_container_width=True, hide_index=True)

    # ── Heatmap: distrito vs semana ────────────────────────────────────────────
    st.markdown("---")
    st.subheader("🔥 Mapa de calor — €/m² por distrito y semana")

    pivot = df_district.pivot_table(
        index="distrito", columns="week_start", values="avg_sqm", aggfunc="mean"
    )
    if not pivot.empty:
        fig_heat = px.imshow(
            pivot,
            color_continuous_scale="RdYlGn_r",
            labels=dict(x="Semana", y="Distrito", color="€/m²"),
            aspect="auto",
        )
        fig_heat.update_layout(
            height=500,
            margin=dict(t=20, b=40),
        )
        st.plotly_chart(fig_heat, use_container_width=True)
