"""
Ranking de Barrios — composite neighbourhood score for Madrid.

Entry point: render_ranking_tab()

Score components (buyer perspective, 0-100 each):
    25 % — Precio bajo      : €/m² vs city median  (lower = better)
    20 % — Tendencia        : weekly price change   (falling = better)
    20 % — Tiempo mercado   : avg days on market    (longer = more negotiating power)
    20 % — Rentabilidad     : gross rental yield    (higher = better)
    15 % — Oferta           : active listing count  (more = better choice)
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px


# Score badge colours
def _badge(score: int) -> str:
    if score >= 70:
        return "🟢"
    if score >= 50:
        return "🟡"
    return "🔴"


def render_ranking_tab():
    from database import get_barrio_ranking

    st.title("🏆 Ranking de Barrios")
    st.markdown(
        "Puntuación compuesta 0-100 para cada barrio de Madrid. "
        "**Mayor score = mejor oportunidad para el comprador.**"
    )

    with st.spinner("Calculando ranking..."):
        data = get_barrio_ranking(min_listings=5)

    if not data:
        st.warning("Sin datos suficientes. Ejecuta el scraper para poblar la base de datos.")
        return

    df = pd.DataFrame(data)

    # ── Filters sidebar ───────────────────────────────────────────────────────
    with st.expander("⚙️ Filtros", expanded=False):
        col1, col2, col3 = st.columns(3)
        with col1:
            distritos = sorted(df["distrito"].dropna().unique())
            sel_dist = st.multiselect("Distritos", distritos, default=[])
        with col2:
            max_sqm = int(df["avg_price_sqm"].max() or 10000)
            price_range = st.slider(
                "€/m² máximo", 1000, max_sqm, max_sqm, step=100
            )
        with col3:
            min_score = st.slider("Score mínimo", 0, 100, 0, step=5)

    filtered = df.copy()
    if sel_dist:
        filtered = filtered[filtered["distrito"].isin(sel_dist)]
    filtered = filtered[filtered["avg_price_sqm"] <= price_range]
    filtered = filtered[filtered["ranking_score"] >= min_score]
    filtered = filtered.reset_index(drop=True)
    filtered["rank"] = filtered.index + 1

    st.caption(f"Mostrando **{len(filtered)}** de {len(df)} barrios")

    # ── Top 3 podium ──────────────────────────────────────────────────────────
    if len(filtered) >= 3:
        st.markdown("### 🥇 Top 3 Barrios")
        podium = filtered.head(3)
        medals = ["🥇", "🥈", "🥉"]
        cols = st.columns(3)
        for i, (_, row) in enumerate(podium.iterrows()):
            with cols[i]:
                score = int(row["ranking_score"])
                yield_str = f"{row['yield_pct']:.1f}%" if row.get("yield_pct") else "—"
                trend_str = (
                    f"{row['price_trend_pct']:+.1f}%" if row.get("price_trend_pct") else "—"
                )
                st.metric(
                    label=f"{medals[i]} {row['barrio']}",
                    value=f"{score}/100",
                    delta=row["distrito"],
                    delta_color="off",
                )
                st.caption(
                    f"€{int(row['avg_price_sqm']):,}/m²  ·  "
                    f"Yield {yield_str}  ·  "
                    f"Tendencia {trend_str}  ·  "
                    f"{int(row['active_count'])} anuncios"
                )

    st.markdown("---")

    # ── Score breakdown radar for top barrio ──────────────────────────────────
    col_left, col_right = st.columns([1, 1])

    with col_left:
        st.subheader("📊 Tabla de Ranking")

        display_df = filtered[[
            "rank", "barrio", "distrito", "ranking_score",
            "avg_price_sqm", "yield_pct", "price_trend_pct",
            "days_on_market", "active_count", "urgency_pct",
        ]].copy()

        display_df["ranking_score"] = display_df["ranking_score"].apply(
            lambda s: f"{_badge(s)} {s}"
        )
        display_df["avg_price_sqm"] = display_df["avg_price_sqm"].apply(
            lambda v: f"€{int(v):,}" if pd.notna(v) else "—"
        )
        display_df["yield_pct"] = display_df["yield_pct"].apply(
            lambda v: f"{v:.1f}%" if pd.notna(v) and v else "—"
        )
        display_df["price_trend_pct"] = display_df["price_trend_pct"].apply(
            lambda v: f"{v:+.1f}%" if pd.notna(v) and v else "—"
        )
        display_df["days_on_market"] = display_df["days_on_market"].apply(
            lambda v: f"{int(v)}d" if pd.notna(v) and v else "—"
        )
        display_df["urgency_pct"] = display_df["urgency_pct"].apply(
            lambda v: f"🔴 {v:.0f}%" if pd.notna(v) and v else "—"
        )
        display_df.columns = [
            "#", "Barrio", "Distrito", "Score",
            "€/m²", "Yield", "Tendencia", "Días mercado", "Anuncios", "🔴 Urgencia",
        ]
        st.dataframe(display_df, use_container_width=True, hide_index=True, height=520)

    with col_right:
        st.subheader("🔬 Detalle de Score")

        top_barrios = filtered["barrio"].head(20).tolist()
        sel_barrio = st.selectbox("Selecciona un barrio", top_barrios)

        if sel_barrio:
            row = filtered[filtered["barrio"] == sel_barrio].iloc[0]
            categories = [
                "Precio bajo", "Tendencia bajista",
                "Tiempo mercado", "Rentabilidad", "Oferta"
            ]
            values = [
                row["score_precio"],
                row["score_tendencia"],
                row["score_tiempo"],
                row["score_rentabilidad"],
                row["score_oferta"],
            ]
            vals_closed = values + [values[0]]
            cats_closed = categories + [categories[0]]

            fig_radar = go.Figure()
            fig_radar.add_trace(go.Scatterpolar(
                r=vals_closed,
                theta=cats_closed,
                fill="toself",
                name=sel_barrio,
                line=dict(color="#4fc3f7", width=2),
                fillcolor="rgba(79,195,247,0.15)",
                hovertemplate="<b>%{theta}</b><br>%{r:.0f}/100<extra></extra>",
            ))
            fig_radar.update_layout(
                polar=dict(
                    radialaxis=dict(visible=True, range=[0, 100],
                                   tickfont=dict(size=9)),
                    angularaxis=dict(tickfont=dict(size=11)),
                ),
                showlegend=False,
                height=320,
                margin=dict(l=40, r=40, t=30, b=30),
                paper_bgcolor="rgba(0,0,0,0)",
            )
            st.plotly_chart(fig_radar, use_container_width=True)

            # Component KPI cards
            kpi_cols = st.columns(5)
            icons = ["💰", "📉", "⏱️", "🏘️", "📦"]
            labels = ["Precio", "Tendencia", "Tiempo", "Yield", "Oferta"]
            raw_vals = [
                f"€{int(row['avg_price_sqm']):,}/m²" if row.get("avg_price_sqm") else "—",
                f"{row['price_trend_pct']:+.1f}%" if row.get("price_trend_pct") else "—",
                f"{int(row['days_on_market'])}d" if row.get("days_on_market") else "—",
                f"{row['yield_pct']:.1f}%" if row.get("yield_pct") else "—",
                str(int(row["active_count"])),
            ]
            scores = [
                row["score_precio"], row["score_tendencia"], row["score_tiempo"],
                row["score_rentabilidad"], row["score_oferta"],
            ]
            for j, col in enumerate(kpi_cols):
                col.metric(
                    label=f"{icons[j]} {labels[j]}",
                    value=raw_vals[j],
                    delta=f"{int(scores[j])}/100",
                    delta_color="off",
                )

    # ── Scatter: precio vs rentabilidad ──────────────────────────────────────
    st.markdown("---")
    st.subheader("📈 Precio vs Rentabilidad por Barrio")

    scatter_df = filtered.dropna(subset=["avg_price_sqm", "yield_pct"]).copy()

    if not scatter_df.empty:
        fig_scatter = px.scatter(
            scatter_df,
            x="avg_price_sqm",
            y="yield_pct",
            size="active_count",
            color="ranking_score",
            color_continuous_scale="RdYlGn",
            range_color=[0, 100],
            hover_name="barrio",
            hover_data={
                "distrito": True,
                "avg_price_sqm": ":.0f",
                "yield_pct": ":.2f",
                "active_count": True,
                "ranking_score": True,
                "days_on_market": ":.0f",
                "urgency_pct": ":.1f",
            },
            labels={
                "avg_price_sqm":  "€/m² (precio venta)",
                "yield_pct":      "Rentabilidad bruta (%)",
                "active_count":   "Anuncios activos",
                "ranking_score":  "Score",
                "days_on_market": "Días en mercado",
                "urgency_pct":    "🔴 Urgentes (%)",
            },
            size_max=30,
        )
        # Quadrant reference lines
        med_price = scatter_df["avg_price_sqm"].median()
        med_yield = scatter_df["yield_pct"].median()
        fig_scatter.add_vline(
            x=med_price, line_dash="dot", line_color="rgba(255,255,255,0.3)",
            annotation_text="Mediana precio",
            annotation_font_color="rgba(255,255,255,0.5)",
        )
        fig_scatter.add_hline(
            y=med_yield, line_dash="dot", line_color="rgba(255,255,255,0.3)",
            annotation_text="Mediana yield",
            annotation_font_color="rgba(255,255,255,0.5)",
        )
        # Annotate top 5
        for _, row in scatter_df.head(5).iterrows():
            fig_scatter.add_annotation(
                x=row["avg_price_sqm"],
                y=row["yield_pct"],
                text=row["barrio"][:15],
                showarrow=False,
                font=dict(size=9, color="white"),
                yshift=12,
            )
        fig_scatter.update_layout(
            height=480,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#e0e0e0"),
            coloraxis_colorbar=dict(title="Score"),
            margin=dict(l=20, r=20, t=20, b=40),
        )
        st.plotly_chart(fig_scatter, use_container_width=True)
        st.caption(
            "Cuadrante ideal: **izquierda-arriba** = bajo precio + alta rentabilidad. "
            "Tamaño del punto = número de anuncios activos. "
            "Color = score compuesto (verde = mejor)."
        )
    else:
        st.info("Sin suficientes datos de rentabilidad para el gráfico. "
                "Se pobla con cada ejecución del scraper.")

    # ── Heatmap: score components per barrio (top 20) ────────────────────────
    st.markdown("---")
    st.subheader("🗺️ Mapa de Calor de Componentes (Top 20)")

    top20 = filtered.head(20)
    hm_data = top20[[
        "barrio", "score_precio", "score_tendencia",
        "score_tiempo", "score_rentabilidad", "score_oferta", "ranking_score"
    ]].set_index("barrio")

    hm_data.columns = [
        "💰 Precio", "📉 Tendencia", "⏱️ Tiempo",
        "🏘️ Yield", "📦 Oferta", "🏆 Score Total"
    ]

    fig_hm = go.Figure(go.Heatmap(
        z=hm_data.values,
        x=hm_data.columns.tolist(),
        y=hm_data.index.tolist(),
        colorscale="RdYlGn",
        zmin=0,
        zmax=100,
        text=[[f"{v:.0f}" for v in row] for row in hm_data.values],
        texttemplate="%{text}",
        textfont=dict(size=11),
        hovertemplate="<b>%{y}</b><br>%{x}: %{z:.0f}/100<extra></extra>",
    ))
    fig_hm.update_layout(
        height=max(350, len(top20) * 26),
        margin=dict(l=10, r=10, t=10, b=40),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#e0e0e0"),
        xaxis=dict(side="top"),
    )
    st.plotly_chart(fig_hm, use_container_width=True)
