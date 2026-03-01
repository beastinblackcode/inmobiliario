"""
Tab: Tendencias del Mercado
Weekly price evolution per district + overall market trend.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np

from database import (
    get_price_trend_by_district,
    get_market_summary_trend,
    get_notarial_gap_by_district,
    get_notarial_prices,
)


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

    c1, c2 = st.columns(2)
    c1.metric(
        "€/m² actual (media Madrid)",
        f"€{last_sqm:,.0f}",
        f"{change_pct:+.1f}% en {n_weeks} semanas",
        delta_color="inverse",
    )
    direction = "↑ subiendo" if change_pct > 1 else ("↓ bajando" if change_pct < -1 else "→ estable")
    c2.metric("Tendencia del mercado", direction, f"desde €{df_market.iloc[0]['avg_sqm']:,.0f}/m²")

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

    st.markdown("---")

    # ── Notarial: precio real vs precio Idealista ─────────────────────────────
    st.subheader("🏛️ Precio de oferta (Idealista) vs Precio real (Notarial)")
    st.caption(
        "El Colegio de Registradores publica el precio escriturado real de cada "
        "transacción. Comparamos el €/m² medio actual de Idealista con el último "
        "año disponible en el registro notarial. Un gap positivo indica que la "
        "oferta pide más de lo que realmente se paga."
    )

    gap_data = get_notarial_gap_by_district()

    if not gap_data:
        st.info("No hay datos notariales disponibles todavía.")
    else:
        df_gap = pd.DataFrame(gap_data)

        # ── KPIs globales ─────────────────────────────────────────────────────
        avg_gap    = df_gap["gap_pct"].mean()
        max_gap    = df_gap.loc[df_gap["gap_pct"].idxmax()]
        min_gap    = df_gap.loc[df_gap["gap_pct"].idxmin()]
        notarial_yr = int(df_gap["notarial_year"].max())

        g1, g2, g3 = st.columns(3)
        g1.metric(
            "Gap medio Madrid",
            f"{avg_gap:+.1f}%",
            f"Idealista vs Notarial {notarial_yr}",
            delta_color="inverse",
        )
        g2.metric(
            f"Mayor sobreprecio — {max_gap['distrito']}",
            f"{max_gap['gap_pct']:+.1f}%",
            f"€{max_gap['idealista_price']:,}/m² vs €{max_gap['notarial_price']:,}/m²",
            delta_color="inverse",
        )
        g3.metric(
            f"Menor gap — {min_gap['distrito']}",
            f"{min_gap['gap_pct']:+.1f}%",
            f"€{min_gap['idealista_price']:,}/m² vs €{min_gap['notarial_price']:,}/m²",
            delta_color="inverse",
        )

        # ── Bar chart gap por distrito ─────────────────────────────────────────
        fig_gap = px.bar(
            df_gap.sort_values("gap_pct", ascending=True),
            x="gap_pct",
            y="distrito",
            orientation="h",
            color="gap_pct",
            color_continuous_scale="RdYlGn_r",
            text=df_gap.sort_values("gap_pct", ascending=True)["gap_pct"].apply(
                lambda v: f"{v:+.1f}%"
            ),
            labels={
                "gap_pct": "Gap (%)",
                "distrito": "",
            },
            title=f"Sobreprecio de oferta vs escriturado notarial {notarial_yr} (% gap por distrito)",
            custom_data=["notarial_price", "idealista_price"],
        )
        fig_gap.update_traces(
            textposition="outside",
            hovertemplate=(
                "<b>%{y}</b><br>"
                "Gap: %{x:+.1f}%<br>"
                "Idealista: €%{customdata[1]:,}/m²<br>"
                f"Notarial {notarial_yr}: €%{{customdata[0]:,}}/m²"
                "<extra></extra>"
            ),
        )
        fig_gap.update_layout(
            height=560,
            coloraxis_showscale=False,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            xaxis_title="Gap % (positivo = oferta más cara que precio real)",
            margin=dict(l=10, r=80, t=50, b=40),
        )
        st.plotly_chart(fig_gap, use_container_width=True)

        # ── Tabla detalle ─────────────────────────────────────────────────────
        with st.expander("📋 Ver tabla completa de precios"):
            df_disp = df_gap[["distrito", "idealista_price", "notarial_price", "gap_pct", "notarial_year"]].copy()
            df_disp.columns = ["Distrito", "Idealista (€/m²)", f"Notarial (€/m²)", "Gap (%)", "Año notarial"]
            st.dataframe(
                df_disp.style.format({
                    "Idealista (€/m²)": "€{:,.0f}",
                    "Notarial (€/m²)":  "€{:,.0f}",
                    "Gap (%)":          "{:+.1f}%",
                }),
                hide_index=True,
                use_container_width=True,
            )

    st.markdown("---")

    # ── Evolución histórica notarial por distrito ─────────────────────────────
    st.subheader("📜 Evolución histórica del precio real (Notarial) por distrito")
    st.caption(
        f"Precios escriturados reales desde 2014. Fuente: Portal del Notariado / "
        "Colegio de Registradores de Madrid."
    )

    all_notarial = get_notarial_prices()
    if not all_notarial:
        st.info("Sin datos notariales históricos disponibles.")
    else:
        df_not = pd.DataFrame(all_notarial)
        all_dist_not = sorted(df_not["distrito"].unique())
        default_not  = [d for d in ["Centro", "Chamberí", "Salamanca", "Carabanchel"] if d in all_dist_not]

        sel_not = st.multiselect(
            "Distritos a comparar (notarial histórico)",
            options=all_dist_not,
            default=default_not,
            key="mt_notarial_distritos",
        )

        if sel_not:
            df_not_sel = df_not[df_not["distrito"].isin(sel_not)]
            fig_not = px.line(
                df_not_sel,
                x="periodo",
                y="precio_m2",
                color="distrito",
                markers=True,
                labels={"periodo": "Año", "precio_m2": "€/m² (escriturado)", "distrito": "Distrito"},
                title="Precio real escriturado por distrito (€/m²)",
            )
            fig_not.update_layout(
                height=400,
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                legend=dict(orientation="h", y=-0.25),
                xaxis=dict(dtick=1),
            )
            st.plotly_chart(fig_not, use_container_width=True)
        else:
            st.info("Selecciona al menos un distrito.")
