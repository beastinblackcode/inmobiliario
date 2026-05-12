"""
Tab: Oportunidades
Top 20 mejores oportunidades por ratio calidad-precio, gangas por distrito,
vendedores desesperados (múltiples bajadas) y chollos por barrio.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime

from data_utils import load_data


def _quality_badge(score: float) -> tuple:
    """Return (emoji, label) for a 0-100 quality score."""
    if score >= 80:
        return "🟢", "Excelente"
    if score >= 70:
        return "🔵", "Muy Bueno"
    if score >= 60:
        return "🟡", "Bueno"
    return "🟠", "Regular"


def _urgency_badge(score: float) -> tuple:
    """Return (emoji, label) for a 0-100 urgency / negotiability score."""
    if score >= 80:
        return "🔥", "Urgente"
    if score >= 60:
        return "⚡", "Alto"
    if score >= 40:
        return "🟡", "Medio"
    return "⚪", "Bajo"


def _render_opportunity_expander(row) -> None:
    """Render one ranked-opportunity row as a Streamlit expander.

    Extracted from the Top-N section so the same layout can be reused
    inside the per-distrito grouping without code duplication.
    """
    from analytics import negotiability_label   # local import: avoids cycle

    score = row["quality_score"]
    badge, label = _quality_badge(score)
    title_preview = row["title"][:70] + "..." if len(row["title"]) > 70 else row["title"]
    reps = int(row.get("republications", 0) or 0)
    rep_chip = f" · 🔄 {reps}x" if reps > 0 else ""

    with st.expander(f"{badge} **{score:.0f}/100** ({label}){rep_chip} — {title_preview}"):
        rc1, rc2, rc3 = st.columns(3)
        with rc1:
            st.metric("💰 Precio", f"€{row['price']:,}")
            st.metric(
                "📐 Tamaño",
                f"{row['size_sqm']:.0f} m²" if pd.notna(row["size_sqm"]) else "N/A",
            )
            st.metric(
                "🛏️ Habitaciones",
                int(row["rooms"]) if pd.notna(row["rooms"]) else "N/A",
            )
        with rc2:
            st.metric(
                "💵 €/m²",
                f"€{row['price_per_sqm']:,.0f}" if pd.notna(row["price_per_sqm"]) else "N/A",
            )
            st.metric(
                "📊 vs Distrito",
                f"{row['vs_distrito_avg']:+.1f}%" if pd.notna(row["vs_distrito_avg"]) else "N/A",
            )
            st.metric(
                "⏱️ Días en mercado",
                f"{row['days_on_market']:.0f}" if pd.notna(row["days_on_market"]) else "N/A",
            )
        with rc3:
            st.metric("📍 Distrito", row["distrito"])
            st.metric("🏘️ Barrio", row["barrio"])
            st.metric("👤 Vendedor", row["seller_type"])
            n_score = row.get("negotiability_score", 0)
            n_badge, n_label = negotiability_label(n_score)
            st.metric(
                f"🤝 Margen {n_badge}",
                f"{n_score:.0f}/100",
                help=(
                    f"Negociabilidad: {n_label}. "
                    "Combina días en mercado, bajadas, gap vs distrito y tipo de vendedor."
                ),
            )
        st.markdown(f"[🔗 Ver en Idealista]({row['url']})")


def _render_negotiability_expander(row) -> None:
    """Render one row from df_ranked as a negotiability-focused expander."""
    from analytics import negotiability_label  # local import: avoids cycle

    n_score = row.get("negotiability_score", 0)
    q_score = row.get("quality_score", 0)
    n_badge, n_label = _urgency_badge(n_score)
    q_badge, q_label = _quality_badge(q_score)
    title_preview = row["title"][:70] + "..." if len(row["title"]) > 70 else row["title"]
    reps = int(row.get("republications", 0) or 0)
    rep_chip = f" · 🔄 {reps}x" if reps > 0 else ""

    with st.expander(f"{n_badge} **{n_score:.0f}/100** ({n_label}){rep_chip} — {title_preview}"):
        rc1, rc2, rc3 = st.columns(3)
        with rc1:
            st.metric("💰 Precio", f"€{row['price']:,}")
            st.metric(
                "💵 €/m²",
                f"€{row['price_per_sqm']:,.0f}" if pd.notna(row["price_per_sqm"]) else "N/A",
            )
            st.metric(
                "⏱️ Días en mercado",
                f"{row['days_on_market']:.0f}" if pd.notna(row["days_on_market"]) else "N/A",
            )
        with rc2:
            st.metric(
                "📉 Bajadas",
                int(row["num_drops"]) if pd.notna(row["num_drops"]) else 0,
            )
            st.metric(
                "📊 Gap vs Distrito",
                f"{row['vs_distrito_avg']:+.1f}%" if pd.notna(row["vs_distrito_avg"]) else "N/A",
            )
            st.metric("👤 Vendedor", row["seller_type"])
        with rc3:
            st.metric(
                f"{q_badge} Calidad",
                f"{q_score:.0f}/100",
                help=f"Score de calidad: {q_label}. Un score alto de negociabilidad junto a calidad alta = oportunidad real.",
            )
            st.metric(
                "📐 m²",
                f"{row['size_sqm']:.0f}" if pd.notna(row["size_sqm"]) else "N/A",
            )
            st.metric(
                "🛏️ Hab.",
                int(row["rooms"]) if pd.notna(row["rooms"]) else "N/A",
            )
        st.markdown(f"[🔗 Ver en Idealista]({row['url']})")


def _render_desperate_expander(row) -> None:
    """Render one desperate-seller row as a Streamlit expander."""
    score = row.get("urgency_score", 0)
    badge, label = _urgency_badge(score)
    title_preview = row["title"][:70] + "..." if len(row["title"]) > 70 else row["title"]
    num_drops = int(row["num_drops"]) if pd.notna(row.get("num_drops")) else 0

    with st.expander(
        f"{badge} **{score:.0f}/100** ({label}) — {title_preview} "
        f"<span style='color:#ef4444;font-size:13px;'>▼{num_drops} bajadas</span>",
        expanded=False,
    ):
        rc1, rc2, rc3 = st.columns(3)
        with rc1:
            st.metric("💰 Precio Actual", f"€{row['current_price']:,}")
            st.metric("🏷️ Precio Inicial", f"€{row['initial_price']:,}")
        with rc2:
            st.metric(
                "📉 Bajada (€)",
                f"−€{row['total_drop']:,.0f}" if pd.notna(row.get("total_drop")) else "N/A",
            )
            st.metric(
                "📉 Bajada (%)",
                f"−{row['total_drop_pct']:.1f}%" if pd.notna(row.get("total_drop_pct")) else "N/A",
            )
            st.metric("🔁 Nº Bajadas", num_drops)
        with rc3:
            st.metric(
                "💵 €/m² actual",
                f"€{row['current_price_sqm']:,.0f}" if pd.notna(row.get("current_price_sqm")) else "N/A",
            )
            st.metric(
                "📐 m²",
                f"{row['size_sqm']:.0f}" if pd.notna(row.get("size_sqm")) else "N/A",
            )
            st.metric(
                "🛏️ Hab.",
                int(row["rooms"]) if pd.notna(row.get("rooms")) else "N/A",
            )
        url = row.get("url", "")
        if url:
            st.markdown(f"[🔗 Ver en Idealista]({url})")


def render_opportunities_tab(df: pd.DataFrame) -> None:
    st.header("🎯 Oportunidades")
    st.markdown("Propiedades con mayor potencial de negociación o mejor relación calidad-precio.")

    from analytics import (
        rank_opportunities,
        identify_bargains,
        get_desperate_sellers_dataframe,
        negotiability_label,
    )

    active_df = df[df["status"] == "active"]

    # ── Mejores Oportunidades por Distrito ────────────────────────────────────
    st.subheader("🏆 Mejores Oportunidades (Score Calidad-Precio)")
    st.info(
        "**Score de Oportunidad (0-100):** "
        "€/m² vs media del barrio (35%) · "
        "€/m² vs media del distrito (15%) · "
        "Historial de bajadas de precio (25%) · "
        "Días en mercado (15%) · "
        "Vendedor particular (10%)"
    )

    df_ranked = rank_opportunities(active_df[active_df["price"] < 500_000])

    # Enrich with property republication counts so each card can flag
    # "🔄 Nx" when the listing has been published before.  One bulk
    # query, not one-per-card.  Defensive: if fingerprints haven't
    # been computed yet (fresh deploy / empty table) the helper
    # returns 0 for every id — the chip simply never renders.
    try:
        from property_history import get_republication_counts
        rep_counts = get_republication_counts(df_ranked["listing_id"].tolist())
        df_ranked = df_ranked.copy()
        df_ranked["republications"] = df_ranked["listing_id"].map(rep_counts).fillna(0).astype(int)
    except Exception:
        df_ranked = df_ranked.copy()
        df_ranked["republications"] = 0

    if df_ranked.empty:
        st.warning("No hay propiedades activas para analizar.")
    else:
        # Controls — let the user tune how dense the per-distrito grouping is
        ctl1, ctl2 = st.columns([1, 1])
        with ctl1:
            n_per_distrito = st.slider(
                "Top por distrito",
                min_value=1, max_value=10, value=3, step=1,
                key="opp_topN_per_distrito",
                help="Cuántas propiedades mostrar por distrito.",
            )
        with ctl2:
            min_score = st.slider(
                "Score mínimo",
                min_value=0, max_value=100, value=0, step=5,
                key="opp_min_quality",
                help="Filtra distritos sin ninguna propiedad por encima del umbral.",
            )

        # df_ranked is already sorted by quality_score DESC → groupby().head(N)
        # naturally returns the top-N per distrito ordered by score.
        grouped = (
            df_ranked[df_ranked["quality_score"] >= min_score]
            .groupby("distrito", sort=False, dropna=True)
            .head(n_per_distrito)
        )

        if grouped.empty:
            st.warning(f"Ningún distrito tiene oportunidades con score ≥ {min_score}.")
        else:
            # Order distritos by their best score (then by group size as tie-break)
            distrito_stats = (
                grouped.groupby("distrito")["quality_score"]
                .agg(best="max", media="mean", count="count")
                .sort_values(by=["best", "count"], ascending=[False, False])
            )
            total_shown = int(distrito_stats["count"].sum())
            st.caption(
                f"📍 {len(distrito_stats)} distritos · {total_shown} oportunidades en total"
            )

            for distrito, stats in distrito_stats.iterrows():
                d_rows = grouped[grouped["distrito"] == distrito]
                st.markdown(
                    f"#### 📍 {distrito} "
                    f"<span style='color:#94a3b8;font-size:14px;font-weight:400;'>"
                    f"· {int(stats['count'])} pisos "
                    f"· mejor {stats['best']:.0f}/100 "
                    f"· media {stats['media']:.0f}/100"
                    f"</span>",
                    unsafe_allow_html=True,
                )

                for _, row in d_rows.iterrows():
                    _render_opportunity_expander(row)

                st.write("")  # small spacer between distritos

    st.markdown("---")

    # ── Mayor Margen de Negociación por Distrito ─────────────────────────────
    st.subheader("🤝 Mayor Margen de Negociación por Distrito")
    st.info(
        "**Score de Negociabilidad (0-100):** mide cuánto margen tienes para "
        "ofertar por debajo del precio publicado. "
        "Días en mercado (35%) · Bajadas previas (30%) · "
        "Sobreprecio vs distrito (20%) · Vendedor particular (15%). "
        "Complementa el score de calidad: alta negociabilidad **+** alta calidad = oportunidad real."
    )

    if not df_ranked.empty:
        neg_ctl1, neg_ctl2 = st.columns([1, 1])
        with neg_ctl1:
            neg_per_distrito = st.slider(
                "Top por distrito",
                min_value=1, max_value=10, value=3, step=1,
                key="neg_topN_per_distrito",
                help="Cuántas propiedades mostrar por distrito.",
            )
        with neg_ctl2:
            min_neg_score = st.slider(
                "Negociabilidad mínima",
                min_value=0, max_value=100, value=0, step=5,
                key="neg_min_score",
                help="Filtra pisos con score de negociabilidad inferior al umbral.",
            )

        neg_sorted = df_ranked.sort_values("negotiability_score", ascending=False)
        neg_grouped = (
            neg_sorted[neg_sorted["negotiability_score"] >= min_neg_score]
            .groupby("distrito", sort=False, dropna=True)
            .head(neg_per_distrito)
        )

        if neg_grouped.empty:
            st.warning(f"Ningún distrito tiene pisos con negociabilidad ≥ {min_neg_score}.")
        else:
            neg_distrito_stats = (
                neg_grouped.groupby("distrito")["negotiability_score"]
                .agg(best="max", media="mean", count="count")
                .sort_values(by=["best", "count"], ascending=[False, False])
            )
            neg_total = int(neg_distrito_stats["count"].sum())
            st.caption(
                f"🤝 {len(neg_distrito_stats)} distritos · {neg_total} propiedades en total"
            )

            for distrito, stats in neg_distrito_stats.iterrows():
                d_rows = neg_grouped[neg_grouped["distrito"] == distrito]
                st.markdown(
                    f"#### 📍 {distrito} "
                    f"<span style='color:#94a3b8;font-size:14px;font-weight:400;'>"
                    f"· {int(stats['count'])} pisos "
                    f"· mejor {stats['best']:.0f}/100 "
                    f"· media {stats['media']:.0f}/100"
                    f"</span>",
                    unsafe_allow_html=True,
                )
                for _, row in d_rows.iterrows():
                    _render_negotiability_expander(row)
                st.write("")  # spacer

        st.caption(
            "💡 *Negociabilidad* = cuánto margen tiene el vendedor. "
            "*Calidad* = si es objetivamente buen precio vs comparables. "
            "El combo de ambos altos es la oportunidad real."
        )
    else:
        st.info("Sin datos suficientes para calcular el ranking.")

    st.markdown("---")

    # ── Gangas por distrito (precio/m² 15% bajo media) ────────────────────────
    st.subheader("💎 Gangas por Distrito")
    st.info("Propiedades con precio/m² **15% o más por debajo** del promedio de su distrito.")

    bargains = identify_bargains(active_df[active_df["price"] < 500_000], threshold=-15.0)

    if not bargains.empty:
        st.success(f"✨ {len(bargains)} gangas potenciales encontradas")

        bargains_display = bargains[[
            "title", "price", "price_per_sqm", "vs_distrito_avg",
            "distrito", "barrio", "rooms", "size_sqm", "quality_score", "url",
        ]].copy()
        bargains_display.columns = [
            "Título", "Precio", "€/m²", "% vs Distrito",
            "Distrito", "Barrio", "Hab.", "m²", "Score", "Link",
        ]
        bargains_display["Precio"] = bargains["price"]
        bargains_display["€/m²"] = bargains["price_per_sqm"]
        bargains_display["% vs Distrito"] = bargains["vs_distrito_avg"]
        bargains_display["Score"] = bargains["quality_score"]
        bargains_display["m²"] = bargains["size_sqm"]
        bargains_display["Link"] = bargains["url"]

        st.dataframe(
            bargains_display, hide_index=True, use_container_width=True, height=400,
            column_config={
                "Precio":        st.column_config.NumberColumn("Precio", format="€%d"),
                "€/m²":          st.column_config.NumberColumn("€/m²", format="€%d"),
                "% vs Distrito": st.column_config.NumberColumn("% vs Distrito", format="%.1f%%"),
                "Score":         st.column_config.ProgressColumn("Score", format="%d", min_value=0, max_value=100),
                "m²":            st.column_config.NumberColumn("m²", format="%d m²"),
                "Link":          st.column_config.LinkColumn("Idealista", display_text="🔗 Ver"),
            },
        )
        csv = bargains.to_csv(index=False)
        st.download_button(
            "📥 Descargar Gangas (CSV)", data=csv,
            file_name=f"gangas_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )
    else:
        st.warning("No se encontraron gangas con el criterio actual (15% por debajo del promedio).")

    st.markdown("---")

    # ── Vendedores Desesperados ───────────────────────────────────────────────
    st.subheader("🔥 Vendedores Desesperados por Distrito")
    st.caption("Propiedades con varias bajadas de precio acumuladas — máximo margen de negociación.")

    desp_ctl1, desp_ctl2, desp_ctl3 = st.columns(3)
    with desp_ctl1:
        min_drops_filter = st.slider("Mínimo de bajadas", 2, 5, 2, 1, key="opp_min_drops")
    with desp_ctl2:
        min_total_drop = st.slider("Bajada total mínima (%)", 5.0, 30.0, 10.0, 5.0, key="opp_min_total")
    with desp_ctl3:
        desp_per_distrito = st.slider(
            "Top por distrito",
            min_value=1, max_value=10, value=3, step=1,
            key="desp_topN_per_distrito",
            help="Cuántas propiedades mostrar por distrito.",
        )

    desperate_df = get_desperate_sellers_dataframe(
        min_drops=min_drops_filter, min_total_drop_pct=min_total_drop
    )
    if not desperate_df.empty:
        desperate_df = desperate_df[desperate_df["current_price"] < 500_000]

    if not desperate_df.empty:
        # Group by distrito, top N per distrito ordered by urgency_score
        desp_grouped = (
            desperate_df  # already sorted by urgency_score DESC from analytics
            .groupby("distrito", sort=False, dropna=True)
            .head(desp_per_distrito)
        )

        desp_distrito_stats = (
            desp_grouped.groupby("distrito")["urgency_score"]
            .agg(best="max", media="mean", count="count")
            .sort_values(by=["best", "count"], ascending=[False, False])
        )
        desp_total = int(desp_distrito_stats["count"].sum())
        st.success(
            f"🔥 {len(desperate_df)} propiedades con múltiples bajadas "
            f"· mostrando {desp_total} en {len(desp_distrito_stats)} distritos"
        )

        for distrito, stats in desp_distrito_stats.iterrows():
            d_rows = desp_grouped[desp_grouped["distrito"] == distrito]
            st.markdown(
                f"#### 📍 {distrito} "
                f"<span style='color:#94a3b8;font-size:14px;font-weight:400;'>"
                f"· {int(stats['count'])} pisos "
                f"· mayor urgencia {stats['best']:.0f}/100 "
                f"· media {stats['media']:.0f}/100"
                f"</span>",
                unsafe_allow_html=True,
            )
            for _, row in d_rows.iterrows():
                _render_desperate_expander(row)
            st.write("")  # spacer

        csv = desperate_df.to_csv(index=False)
        st.download_button(
            "📥 Descargar CSV completo", data=csv,
            file_name=f"vendedores_desesperados_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )
    else:
        st.info(f"No hay propiedades con ≥{min_drops_filter} bajadas y ≥{min_total_drop}% de bajada total.")

    st.markdown("---")

    # ── Chollos por Barrio (z-score) ──────────────────────────────────────────
    st.subheader("🏘️ Chollos por Barrio")
    st.caption("Propiedades con precio/m² significativamente inferior a la media del barrio (z-score < -1.5).")

    all_active = load_data(status="active", distritos=None, min_price=None, max_price=None, seller_type="All")
    chollos_df = all_active[
        (all_active["price"] > 0) & (all_active["size_sqm"] > 0) & (all_active["barrio"].notna())
    ].copy()

    if not chollos_df.empty and len(chollos_df) > 20:
        chollos_df["price_per_sqm"] = chollos_df["price"] / chollos_df["size_sqm"]
        barrio_stats = chollos_df.groupby("barrio").agg(
            {"price_per_sqm": ["mean", "std", "count"]}
        ).reset_index()
        barrio_stats.columns = ["barrio", "mean_price_sqm", "std_price_sqm", "count"]
        barrio_stats = barrio_stats[barrio_stats["count"] >= 5]
        chollos_df = chollos_df.merge(barrio_stats[["barrio", "mean_price_sqm", "std_price_sqm"]], on="barrio", how="left")
        chollos_df["z_score"] = (chollos_df["price_per_sqm"] - chollos_df["mean_price_sqm"]) / chollos_df["std_price_sqm"]
        chollos = chollos_df[chollos_df["z_score"] < -1.5].copy().sort_values("z_score")

        if not chollos.empty:
            st.success(f"🎯 {len(chollos)} chollos potenciales encontrados")
            top_chollos = chollos.head(20)

            display_chollos = pd.DataFrame({
                "Título":      top_chollos["title"],
                "Barrio":      top_chollos["barrio"],
                "Precio":      top_chollos["price"],
                "Precio/m²":   top_chollos["price_per_sqm"],
                "Media Barrio": top_chollos["mean_price_sqm"],
                "Descuento":   (top_chollos["mean_price_sqm"] - top_chollos["price_per_sqm"]) / top_chollos["mean_price_sqm"],
                "Hab.":        top_chollos["rooms"].fillna(0).astype(int),
                "m²":          top_chollos["size_sqm"],
                "URL":         top_chollos["url"],
            })
            st.dataframe(
                display_chollos, hide_index=True, use_container_width=True,
                column_config={
                    "Precio":       st.column_config.NumberColumn("Precio", format="€%d"),
                    "Precio/m²":    st.column_config.NumberColumn("Precio/m²", format="€%d"),
                    "Media Barrio": st.column_config.NumberColumn("Media Barrio", format="€%d"),
                    "Descuento":    st.column_config.NumberColumn("Descuento", format="%.1f%%"),
                    "m²":           st.column_config.NumberColumn("m²", format="%d m²"),
                    "URL":          st.column_config.LinkColumn("Enlace", display_text="Ver oferta"),
                },
            )

            chollos_by_barrio = chollos.groupby("barrio").size().reset_index(name="Chollos")
            chollos_by_barrio = chollos_by_barrio.sort_values("Chollos", ascending=False).head(10)

            cc1, cc2 = st.columns(2)
            with cc1:
                st.dataframe(chollos_by_barrio, hide_index=True, use_container_width=True)
            with cc2:
                fig_ch = px.bar(
                    chollos_by_barrio, x="Chollos", y="barrio", orientation="h",
                    title="Top 10 Barrios con Más Chollos",
                    labels={"Chollos": "Número de chollos", "barrio": ""},
                )
                fig_ch.update_layout(
                    showlegend=False, height=400,
                    plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                )
                st.plotly_chart(fig_ch, use_container_width=True)
        else:
            st.info("No se encontraron chollos significativos en este momento.")
    else:
        st.info("No hay suficientes datos para detectar chollos (mínimo 20 propiedades activas).")
