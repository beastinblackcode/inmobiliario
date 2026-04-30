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

    # ── Top 20 Mejores Oportunidades ──────────────────────────────────────────
    st.subheader("🏆 Top 20 Mejores Oportunidades (Score Calidad-Precio)")
    st.info(
        "**Score de Oportunidad (0-100):** "
        "€/m² vs media del barrio (35%) · "
        "€/m² vs media del distrito (15%) · "
        "Historial de bajadas de precio (25%) · "
        "Días en mercado (15%) · "
        "Vendedor particular (10%)"
    )

    df_ranked = rank_opportunities(active_df[active_df["price"] < 500_000])

    if not df_ranked.empty:
        for _, row in df_ranked.head(20).iterrows():
            score = row["quality_score"]
            if score >= 80:
                badge, label = "🟢", "Excelente"
            elif score >= 70:
                badge, label = "🔵", "Muy Bueno"
            elif score >= 60:
                badge, label = "🟡", "Bueno"
            else:
                badge, label = "🟠", "Regular"

            title_preview = row["title"][:70] + "..." if len(row["title"]) > 70 else row["title"]
            with st.expander(f"{badge} **{score:.0f}/100** ({label}) — {title_preview}"):
                rc1, rc2, rc3 = st.columns(3)
                with rc1:
                    st.metric("💰 Precio", f"€{row['price']:,}")
                    st.metric("📐 Tamaño", f"{row['size_sqm']:.0f} m²" if pd.notna(row["size_sqm"]) else "N/A")
                    st.metric("🛏️ Habitaciones", int(row["rooms"]) if pd.notna(row["rooms"]) else "N/A")
                with rc2:
                    st.metric("💵 €/m²", f"€{row['price_per_sqm']:,.0f}" if pd.notna(row["price_per_sqm"]) else "N/A")
                    st.metric("📊 vs Distrito", f"{row['vs_distrito_avg']:+.1f}%" if pd.notna(row["vs_distrito_avg"]) else "N/A")
                    st.metric("⏱️ Días en mercado", f"{row['days_on_market']:.0f}" if pd.notna(row["days_on_market"]) else "N/A")
                with rc3:
                    st.metric("📍 Distrito", row["distrito"])
                    st.metric("🏘️ Barrio", row["barrio"])
                    st.metric("👤 Vendedor", row["seller_type"])
                    n_score = row.get("negotiability_score", 0)
                    n_badge, n_label = negotiability_label(n_score)
                    st.metric(
                        f"🤝 Margen {n_badge}",
                        f"{n_score:.0f}/100",
                        help=f"Negociabilidad: {n_label}. "
                             f"Combina días en mercado, bajadas, gap vs distrito y tipo de vendedor.",
                    )
                st.markdown(f"[🔗 Ver en Idealista]({row['url']})")
    else:
        st.warning("No hay propiedades activas para analizar.")

    st.markdown("---")

    # ── Top 20 con Mayor Margen de Negociación ───────────────────────────────
    st.subheader("🤝 Top 20 con Mayor Margen de Negociación")
    st.info(
        "**Score de Negociabilidad (0-100):** mide cuánto margen tienes para "
        "ofertar por debajo del precio publicado. "
        "Días en mercado (35%) · Bajadas previas (30%) · "
        "Sobreprecio vs distrito (20%) · Vendedor particular (15%). "
        "Complementa el score de calidad: alta negociabilidad **+** alta calidad = oportunidad real."
    )

    if not df_ranked.empty:
        top_neg = df_ranked.sort_values(
            "negotiability_score", ascending=False
        ).head(20)

        neg_display = top_neg[[
            "title", "distrito", "barrio", "price", "price_per_sqm",
            "vs_distrito_avg", "days_on_market", "num_drops",
            "seller_type", "negotiability_score", "quality_score", "url",
        ]].copy()
        neg_display.columns = [
            "Título", "Distrito", "Barrio", "Precio", "€/m²",
            "% vs Distrito", "Días Mercado", "Bajadas",
            "Vendedor", "Negociabilidad", "Calidad", "Link",
        ]
        st.dataframe(
            neg_display, hide_index=True, use_container_width=True, height=500,
            column_config={
                "Precio":         st.column_config.NumberColumn("Precio", format="€%d"),
                "€/m²":           st.column_config.NumberColumn("€/m²", format="€%d"),
                "% vs Distrito":  st.column_config.NumberColumn("% vs Distrito", format="%+.1f%%"),
                "Días Mercado":   st.column_config.NumberColumn("Días", format="%d"),
                "Bajadas":        st.column_config.NumberColumn("Bajadas", format="%d"),
                "Negociabilidad": st.column_config.ProgressColumn(
                    "Negociabilidad", format="%d", min_value=0, max_value=100
                ),
                "Calidad":        st.column_config.ProgressColumn(
                    "Calidad", format="%d", min_value=0, max_value=100
                ),
                "Link":           st.column_config.LinkColumn("Idealista", display_text="🔗 Ver"),
            },
        )
        st.caption(
            "💡 La columna *Calidad* indica si el piso es objetivamente buen precio "
            "vs comparables. La columna *Negociabilidad* indica cuánto margen "
            "tiene el vendedor para aceptar una oferta inferior."
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
    st.subheader("🔥 Vendedores Desesperados (Múltiples Bajadas)")
    st.caption("Propiedades con varias bajadas de precio acumuladas — máximo margen de negociación.")

    col1, col2 = st.columns(2)
    with col1:
        min_drops_filter = st.slider("Mínimo de bajadas", 2, 5, 2, 1, key="opp_min_drops")
    with col2:
        min_total_drop = st.slider("Bajada total mínima (%)", 5.0, 30.0, 10.0, 5.0, key="opp_min_total")

    desperate_df = get_desperate_sellers_dataframe(
        min_drops=min_drops_filter, min_total_drop_pct=min_total_drop
    )
    if not desperate_df.empty:
        desperate_df = desperate_df[desperate_df["current_price"] < 500_000]

    if not desperate_df.empty:
        st.success(f"✅ {len(desperate_df)} propiedades con múltiples bajadas")

        disp_df = desperate_df[[
            "title", "distrito", "barrio", "initial_price", "current_price",
            "total_drop", "total_drop_pct", "num_drops", "urgency_score", "rooms", "size_sqm",
        ]].head(20).copy()
        disp_df.columns = [
            "Título", "Distrito", "Barrio", "Precio Inicial", "Precio Actual",
            "Bajada (€)", "Bajada (%)", "Nº Bajadas", "Score Urgencia", "Hab.", "m²",
        ]
        st.dataframe(
            disp_df, hide_index=True, use_container_width=True, height=500,
            column_config={
                "Precio Inicial":  st.column_config.NumberColumn("Precio Inicial", format="€%d"),
                "Precio Actual":   st.column_config.NumberColumn("Precio Actual", format="€%d"),
                "Bajada (€)":      st.column_config.NumberColumn("Bajada (€)", format="€%d"),
                "Bajada (%)":      st.column_config.NumberColumn("Bajada (%)", format="%.1f%%"),
                "Score Urgencia":  st.column_config.ProgressColumn("Score Urgencia", format="%d", min_value=0, max_value=100),
                "m²":              st.column_config.NumberColumn("m²", format="%d m²"),
            },
        )
        csv = desperate_df.to_csv(index=False)
        st.download_button(
            "📥 Descargar CSV", data=csv,
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
