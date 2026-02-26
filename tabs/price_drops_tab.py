"""
Tab: Bajadas de Precio
Comprehensive price-drop analytics: overview KPIs, barrio ranking,
recent drops table and magnitude histogram.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from database import get_price_drop_stats, get_price_trend_by_district, get_daily_price_drops


def render_price_drops_tab():
    st.header("📉 Bajadas de Precio")
    st.markdown("Monitorización de reducciones de precio en el mercado activo de Madrid.")

    with st.spinner("Cargando estadísticas de bajadas..."):
        data = get_price_drop_stats()

    ov = data.get("overview", {})
    by_barrio = data.get("by_barrio", [])
    recent = data.get("recent_drops", [])
    buckets = data.get("drop_magnitude_buckets", {})

    if not ov:
        st.warning("No hay datos disponibles.")
        return

    # ── KPI row ──────────────────────────────────────────────────────────────
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric(
        "Propiedades activas",
        f"{ov['total_active']:,}",
    )
    c2.metric(
        "Con bajada de precio",
        f"{ov['with_drops']:,}",
        f"{ov['drop_pct_of_total']}% del total",
    )
    c3.metric(
        "Bajada media",
        f"{ov['avg_drop_pct']:.1f}%",
        help="Porcentaje medio de reducción sobre el precio anterior",
    )
    c4.metric(
        "Bajada máxima",
        f"{ov['max_drop_pct']:.1f}%",
        help="La reducción más agresiva registrada",
    )
    c5.metric(
        "Días hasta 1ª bajada",
        f"{ov['avg_days_to_drop']:.0f} días",
        help="Tiempo medio desde publicación hasta la primera reducción",
    )

    st.markdown("---")

    # ── Evolución diaria (movido desde Dashboard) ─────────────────────────────
    st.subheader("📅 Evolución diaria de bajadas (últimos 30 días)")
    drops_data = get_daily_price_drops(days=30)
    if drops_data:
        drops_df = pd.DataFrame(drops_data)
        latest = drops_df.iloc[-1]
        prev = drops_df.iloc[-2] if len(drops_df) > 1 else None

        dm1, dm2, dm3 = st.columns(3)
        with dm1:
            delta_drops = int(latest['drop_count'] - prev['drop_count']) if prev is not None else 0
            dm1.metric("Bajadas (último día)", f"{latest['drop_count']}", f"{delta_drops:+} vs ayer",
                       help=f"Fecha: {latest['date']}")
        with dm2:
            delta_pct = latest['drop_pct'] - prev['drop_pct'] if prev is not None else 0
            dm2.metric("% sobre activos", f"{latest['drop_pct']}%", f"{delta_pct:+.2f}%",
                       help="% de inmuebles activos que bajaron de precio")
        with dm3:
            dm3.metric("Total activos (est.)", f"{latest['active_count']:,}",
                       help="Estimación de inmuebles activos en esa fecha")

        fig_drops = go.Figure()
        fig_drops.add_trace(go.Bar(
            x=drops_df['date'], y=drops_df['drop_count'],
            name='Nº Bajadas', marker_color='#e74c3c', opacity=0.7,
        ))
        fig_drops.add_trace(go.Scatter(
            x=drops_df['date'], y=drops_df['drop_pct'],
            name='% del Total', yaxis='y2',
            line=dict(color='#2c3e50', width=3), mode='lines+markers',
        ))
        fig_drops.update_layout(
            xaxis=dict(title='Fecha'),
            yaxis=dict(title='Número de bajadas', side='left'),
            yaxis2=dict(title='% del total activo', side='right', overlaying='y',
                        showgrid=False, tickformat='.1f%'),
            legend=dict(x=0.01, y=0.99),
            hovermode='x unified',
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig_drops, use_container_width=True)
    else:
        st.info("No hay datos suficientes de historial de precios para generar el gráfico.")

    st.markdown("---")

    # ── Bajadas recientes (ancho completo) ───────────────────────────────────
    st.subheader("🕐 Bajadas recientes")
    r7, r30 = ov["recent_7d"], ov["recent_30d"]
    st.markdown(
        f"**{r7}** propiedades bajaron precio en los últimos **7 días** · "
        f"**{r30}** en los últimos **30 días**"
    )

    if recent:
        df_rec = pd.DataFrame(recent)
        df_rec["Bajada"] = df_rec["change_percent"].apply(lambda x: f"{x:.1f}%")
        df_rec["Δ€"] = df_rec["change_amount"].apply(lambda x: f"{x:+,}€")
        df_rec["Precio"] = df_rec["current_price"].apply(lambda x: f"{x:,}€")

        df_show = df_rec[["title", "barrio", "Precio", "Δ€", "Bajada", "date_recorded", "url"]].copy()
        df_show.columns = ["Título", "Barrio", "Precio", "Δ€", "% Bajada", "Fecha", "URL"]

        df_show["Título"] = df_show.apply(
            lambda r: f'<a href="{r["URL"]}" target="_blank">{r["Título"][:55]}…</a>'
            if len(str(r["Título"])) > 55
            else f'<a href="{r["URL"]}" target="_blank">{r["Título"]}</a>',
            axis=1,
        )
        df_show = df_show.drop(columns=["URL"])

        st.markdown(
            df_show.to_html(escape=False, index=False),
            unsafe_allow_html=True,
        )
    else:
        st.info("Sin bajadas en los últimos 7 días.")

    st.markdown("---")

    # ── Distribución de bajadas (ancho completo) ──────────────────────────────
    st.subheader("📊 Distribución de bajadas")
    if buckets:
        col_hist, col_spacer = st.columns([2, 1])
        with col_hist:
            fig_hist = go.Figure(
                go.Bar(
                    x=list(buckets.keys()),
                    y=list(buckets.values()),
                    marker_color=["#81c784", "#4fc3f7", "#ffb74d", "#e57373", "#ab47bc"],
                    text=list(buckets.values()),
                    textposition="outside",
                )
            )
            fig_hist.update_layout(
                xaxis_title="Magnitud de la bajada",
                yaxis_title="Nº de eventos",
                height=320,
                margin=dict(t=20, b=40),
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
            )
            st.plotly_chart(fig_hist, use_container_width=True)

    st.markdown("---")

    # ── Barrio ranking ────────────────────────────────────────────────────────
    st.subheader("🏘️ Barrios con más bajadas")

    if by_barrio:
        df_b = pd.DataFrame(by_barrio)

        col_tbl, col_bar = st.columns([1.2, 1])

        with col_tbl:
            # Filtros
            distritos = sorted(df_b["distrito"].dropna().unique())
            sel_dist = st.multiselect(
                "Filtrar por distrito", options=distritos, default=[], key="pd_dist"
            )
            df_filt = df_b[df_b["distrito"].isin(sel_dist)] if sel_dist else df_b

            df_display = df_filt[
                ["barrio", "distrito", "total", "with_drops", "drop_rate_pct", "avg_drop_pct", "max_drop_pct"]
            ].copy()
            df_display.columns = [
                "Barrio", "Distrito", "Activos", "Con bajada",
                "% con bajada", "Bajada media %", "Bajada máx %"
            ]
            df_display = df_display.sort_values("% con bajada", ascending=False)

            st.dataframe(
                df_display.style.format({
                    "% con bajada":   "{:.1f}%",
                    "Bajada media %": "{:.1f}%",
                    "Bajada máx %":   "{:.1f}%",
                }),
                use_container_width=True,
                height=400,
            )

        with col_bar:
            top15 = df_b.nlargest(15, "drop_rate_pct")
            fig_bar = px.bar(
                top15,
                x="drop_rate_pct",
                y="barrio",
                orientation="h",
                color="avg_drop_pct",
                color_continuous_scale="RdYlGn_r",
                labels={
                    "drop_rate_pct": "% propiedades con bajada",
                    "barrio": "",
                    "avg_drop_pct": "Bajada media %",
                },
                title="Top 15 barrios por tasa de bajadas",
                text="drop_rate_pct",
            )
            fig_bar.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
            fig_bar.update_layout(
                height=450,
                margin=dict(t=40, b=20, l=10, r=20),
                yaxis={"categoryorder": "total ascending"},
                coloraxis_showscale=False,
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
            )
            st.plotly_chart(fig_bar, use_container_width=True)

    st.markdown("---")

    # ── Scatter: drop rate vs avg drop magnitude ──────────────────────────────
    st.subheader("🔍 Frecuencia vs. Magnitud por barrio")
    st.caption("Barrios en el cuadrante superior derecho = bajan con frecuencia Y de forma agresiva (mejor poder de negociación).")

    if by_barrio:
        df_scatter = pd.DataFrame(by_barrio)
        df_scatter = df_scatter[df_scatter["with_drops"] > 0].copy()
        df_scatter["avg_drop_pct_abs"] = df_scatter["avg_drop_pct"].abs()

        fig_sc = px.scatter(
            df_scatter,
            x="drop_rate_pct",
            y="avg_drop_pct_abs",
            size="total",
            color="distrito",
            hover_name="barrio",
            hover_data={
                "total": True,
                "with_drops": True,
                "drop_rate_pct": ":.1f",
                "avg_drop_pct_abs": ":.1f",
                "distrito": False,
            },
            labels={
                "drop_rate_pct":    "% propiedades que han bajado",
                "avg_drop_pct_abs": "Bajada media (% abs)",
                "total":            "Activos",
            },
            title="Frecuencia de bajadas vs. Magnitud media (tamaño = nº activos)",
        )

        # Median lines
        med_x = df_scatter["drop_rate_pct"].median()
        med_y = df_scatter["avg_drop_pct_abs"].median()
        fig_sc.add_vline(x=med_x, line_dash="dot", line_color="gray", opacity=0.5)
        fig_sc.add_hline(y=med_y, line_dash="dot", line_color="gray", opacity=0.5)

        fig_sc.update_layout(
            height=480,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig_sc, use_container_width=True)

    # ── Heatmap €/m² por distrito y semana ───────────────────────────────────
    st.markdown("---")
    st.subheader("🗓️ Evolución semanal €/m² por distrito")
    st.caption("Contexto de tendencia de precios por zona para interpretar mejor las bajadas.")

    trend_data = get_price_trend_by_district()
    if trend_data:
        df_trend = pd.DataFrame(trend_data)
        pivot = df_trend.pivot_table(
            index="distrito", columns="week_start", values="avg_sqm", aggfunc="mean"
        )
        if not pivot.empty:
            fig_heat = go.Figure(go.Heatmap(
                z=pivot.values,
                x=[str(c)[:10] for c in pivot.columns],
                y=list(pivot.index),
                colorscale="RdYlGn_r",
                hoverongaps=False,
                hovertemplate="<b>%{y}</b><br>Semana: %{x}<br>€/m²: %{z:,.0f}<extra></extra>",
                colorbar=dict(title="€/m²"),
            ))
            fig_heat.update_layout(
                height=500,
                margin=dict(t=20, b=40),
                xaxis_title="Semana",
                yaxis_title="Distrito",
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
            )
            st.plotly_chart(fig_heat, use_container_width=True)
