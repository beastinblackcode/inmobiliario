"""
Admin tab — scraping activity, cost control, district stats, property lookup.

Entry point: render_admin_tab(df)
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

from database import (
    get_scraping_activity,
    get_scraping_log,
    get_connection,
    get_listing_by_url,
    get_property_price_stats,
)
from analytics import get_property_evolution_dataframe


def render_admin_tab(df: pd.DataFrame) -> None:
    """Render all content for the ⚙️ Administración tab."""

    # =========================================================================
    # Scraping Activity
    # =========================================================================
    st.markdown("---")
    st.subheader("📅 Actividad de Scraping")

    scraping_data = get_scraping_activity(days=30)

    if scraping_data:
        scraping_df = pd.DataFrame(scraping_data)
        scraping_df["date"] = pd.to_datetime(scraping_df["date"])
        scraping_df = scraping_df.sort_values("date")

        avg_scraped = scraping_df["count"].mean()
        max_scraped = scraping_df["count"].max()
        min_scraped = scraping_df["count"].min()
        total_scraped = scraping_df["count"].sum()

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric(
                "Promedio Diario",
                f"{avg_scraped:,.0f}",
                help="Promedio de propiedades nuevas por día",
            )
        with col2:
            st.metric(
                "Máximo en un Día",
                f"{max_scraped:,}",
                help="Mayor cantidad de propiedades scrapeadas en un día",
            )
        with col3:
            st.metric(
                "Mínimo en un Día",
                f"{min_scraped:,}",
                help="Menor cantidad de propiedades scrapeadas en un día",
            )
        with col4:
            st.metric(
                "Total (30 días)",
                f"{total_scraped:,}",
                help="Total de propiedades nuevas en los últimos 30 días",
            )

        # Bar chart
        colors = [
            "#e74c3c"
            if count < avg_scraped * 0.5
            else "#f39c12"
            if count < avg_scraped
            else "#27ae60"
            for count in scraping_df["count"]
        ]

        fig_scraping = go.Figure()
        fig_scraping.add_trace(
            go.Bar(
                x=scraping_df["date"],
                y=scraping_df["count"],
                marker_color=colors,
                hovertemplate="<b>%{x|%d/%m/%Y}</b><br>Propiedades: %{y:,}<extra></extra>",
                name="Propiedades Scrapeadas",
            )
        )
        fig_scraping.add_trace(
            go.Scatter(
                x=scraping_df["date"],
                y=[avg_scraped] * len(scraping_df),
                mode="lines",
                name="Promedio",
                line=dict(color="#3498db", width=2, dash="dash"),
                hovertemplate="<b>Promedio</b>: %{y:,.0f}<extra></extra>",
            )
        )
        fig_scraping.update_layout(
            title="Propiedades Nuevas Descubiertas por Día",
            xaxis_title="Fecha",
            yaxis_title="Cantidad de Propiedades",
            hovermode="x unified",
            height=400,
            showlegend=True,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1,
            ),
            xaxis=dict(showgrid=True, gridcolor="rgba(128, 128, 128, 0.2)"),
            yaxis=dict(showgrid=True, gridcolor="rgba(128, 128, 128, 0.2)"),
        )
        st.plotly_chart(fig_scraping, use_container_width=True)

        # Warning for low scraping days
        low_days = scraping_df[scraping_df["count"] < avg_scraped * 0.5]
        if not low_days.empty:
            st.warning(
                f"⚠️ **{len(low_days)} días con scraping bajo detectados** "
                "(menos del 50% del promedio). "
                "Esto puede indicar scrapes incompletos que marcan incorrectamente "
                "propiedades como vendidas."
            )
            with st.expander("Ver días con scraping bajo"):
                low_days_display = low_days.copy()
                low_days_display["date"] = low_days_display["date"].dt.strftime(
                    "%Y-%m-%d"
                )
                low_days_display.columns = ["Fecha", "Propiedades Scrapeadas"]
                st.dataframe(
                    low_days_display.sort_values("Fecha", ascending=False),
                    hide_index=True,
                    use_container_width=True,
                )
    else:
        st.info("No hay datos de scraping disponibles para los últimos 30 días.")

    # =========================================================================
    # Cost & Duration Section
    # =========================================================================
    st.markdown("---")
    st.subheader("💰 Control de Costes y Rendimiento")

    scraping_log = get_scraping_log(limit=30)

    if scraping_log:
        log_df = pd.DataFrame(scraping_log)
        log_df["start_time"] = pd.to_datetime(log_df["start_time"])

        total_cost = log_df["cost_estimate_usd"].sum()
        avg_duration = log_df["duration_minutes"].mean()
        last_run = log_df.iloc[0]

        m1, m2, m3 = st.columns(3)
        m1.metric("Coste Total (30 ejecuciones)", f"${total_cost:.2f}")
        m2.metric("Duración Promedio", f"{avg_duration:.1f} min")
        m3.metric(
            "Última Ejecución",
            f"${last_run['cost_estimate_usd']:.4f}",
            f"{last_run['duration_minutes']:.1f} min",
        )

        tab_cost, tab_time = st.tabs(
            ["💸 Coste por Ejecución", "⏱️ Duración de Scraping"]
        )
        with tab_cost:
            fig_cost = px.line(
                log_df,
                x="start_time",
                y="cost_estimate_usd",
                markers=True,
                title="Coste Estimado (USD)",
            )
            fig_cost.update_layout(
                yaxis_title="Coste (USD)", xaxis_title="Fecha"
            )
            st.plotly_chart(fig_cost, use_container_width=True)

        with tab_time:
            fig_time = px.line(
                log_df,
                x="start_time",
                y="duration_minutes",
                markers=True,
                title="Duración (Minutos)",
            )
            fig_time.update_layout(
                yaxis_title="Minutos", xaxis_title="Fecha"
            )
            st.plotly_chart(fig_time, use_container_width=True)

        with st.expander("Ver Log Detallado"):
            st.dataframe(
                log_df[
                    [
                        "start_time",
                        "duration_minutes",
                        "cost_estimate_usd",
                        "properties_processed",
                        "new_listings",
                        "status",
                    ]
                ],
                use_container_width=True,
            )
    else:
        st.info(
            "ℹ️ No hay registros de costes aún. Se generarán automáticamente "
            "tras la próxima ejecución del scraper."
        )

    # =========================================================================
    # Properties by District and Date
    # =========================================================================
    st.markdown("---")
    st.subheader("📊 Propiedades Cargadas por Distrito y Fecha")

    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT
                DATE(first_seen_date) as date,
                distrito,
                COUNT(*) as properties
            FROM listings
            WHERE first_seen_date >= date('now', '-30 days')
            AND distrito IS NOT NULL
            GROUP BY DATE(first_seen_date), distrito
            ORDER BY date DESC, properties DESC
            """
        )
        district_data = cursor.fetchall()

    if district_data:
        district_df = pd.DataFrame(
            district_data, columns=["Fecha", "Distrito", "Propiedades"]
        )

        pivot_df = district_df.pivot_table(
            index="Distrito",
            columns="Fecha",
            values="Propiedades",
            fill_value=0,
            aggfunc="sum",
        )
        pivot_df["Total"] = pivot_df.sum(axis=1)
        pivot_df = pivot_df.sort_values("Total", ascending=False)

        col1, col2 = st.columns([3, 1])
        with col1:
            st.markdown(
                "**Propiedades nuevas descubiertas en los últimos 30 días, "
                "agrupadas por distrito y fecha**"
            )
        with col2:
            view_mode = st.selectbox(
                "Vista",
                options=["Tabla Pivote", "Lista Detallada"],
                help="Elige cómo visualizar los datos",
            )

        if view_mode == "Tabla Pivote":
            st.dataframe(
                pivot_df,
                use_container_width=True,
                height=500,
            )
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Distritos", len(pivot_df))
            with col2:
                st.metric("Distrito Más Activo", pivot_df.index[0])
            with col3:
                st.metric("Propiedades Totales", int(pivot_df["Total"].sum()))
        else:
            display_df = district_df.sort_values(
                ["Fecha", "Propiedades"], ascending=[False, False]
            )
            st.dataframe(
                display_df,
                hide_index=True,
                use_container_width=True,
                height=500,
            )
            csv = display_df.to_csv(index=False)
            st.download_button(
                label="📥 Descargar CSV",
                data=csv,
                file_name=f"propiedades_por_distrito_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
            )
    else:
        st.info("No hay datos de propiedades nuevas en los últimos 30 días.")

    # =========================================================================
    # Property Lookup
    # =========================================================================
    st.markdown("---")
    st.subheader("🔍 Buscar Propiedad")
    st.markdown(
        "Introduce la URL de Idealista o el ID del piso para ver su histórico de precios completo."
    )

    col1, col2 = st.columns([4, 1])
    with col1:
        search_input = st.text_input(
            "URL o ID",
            placeholder="https://www.idealista.com/inmueble/110506346/ o 110506346",
            label_visibility="collapsed",
            key="property_search_input",
        )
    with col2:
        search_button = st.button(
            "🔍 Buscar", use_container_width=True, type="primary"
        )

    if search_button and search_input:
        with st.spinner("Buscando propiedad..."):
            listing = get_listing_by_url(search_input)

        if listing:
            st.success(f"✅ Propiedad encontrada: {listing['listing_id']}")
            st.markdown("### 📍 Detalles de la Propiedad")

            status_emoji = "✅" if listing["status"] == "active" else "❌"
            status_text = (
                "Activo" if listing["status"] == "active" else "Vendido/Retirado"
            )
            status_color = "green" if listing["status"] == "active" else "red"

            size_text = (
                f"{listing['size_sqm']:.0f} m²" if listing["size_sqm"] else "N/A m²"
            )
            rooms_text = (
                f"{listing['rooms']} hab" if listing["rooms"] else "N/A hab"
            )
            floor_text = (
                f"Piso {listing['floor']}" if listing["floor"] else "Piso N/A"
            )

            st.markdown(
                f"""
                <div style="background-color: #f0f2f6; padding: 20px; border-radius: 10px; margin-bottom: 20px;">
                    <h4 style="margin-top: 0;">{listing['title']}</h4>
                    <p style="color: #666; margin: 5px 0;">
                        📍 {listing['distrito']} - {listing['barrio']}
                    </p>
                    <p style="font-size: 24px; color: #1f77b4; margin: 10px 0;">
                        💶 {listing['price']:,}€
                    </p>
                    <p style="margin: 5px 0;">
                        📐 {size_text} •
                        🛏️ {rooms_text} •
                        🏢 {floor_text}
                    </p>
                    <p style="margin: 10px 0;">
                        <span style="background-color: {status_color}; color: white; padding: 5px 10px; border-radius: 5px;">
                            {status_emoji} {status_text}
                        </span>
                    </p>
                    <p style="margin: 10px 0; color: #666; font-size: 14px;">
                        📅 Primera aparición: <strong>{listing['first_seen_date']}</strong><br>
                        📅 Última aparición: <strong>{listing['last_seen_date']}</strong>
                    </p>
                    <p style="margin: 10px 0;">
                        <a href="{listing['url']}" target="_blank" style="color: #1f77b4; text-decoration: none;">
                            🔗 Ver en Idealista →
                        </a>
                    </p>
                </div>
                """,
                unsafe_allow_html=True,
            )

            evolution_df = get_property_evolution_dataframe(listing["listing_id"])

            if not evolution_df.empty:
                stats = get_property_price_stats(listing["listing_id"])

                st.markdown("### 📊 Estadísticas de Precio")
                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    st.metric(
                        "Precio Inicial",
                        f"€{stats['initial_price']:,}",
                        help="Primer precio registrado",
                    )
                with col2:
                    st.metric(
                        "Precio Actual",
                        f"€{stats['current_price']:,}",
                        help="Último precio registrado",
                    )
                with col3:
                    change_color = (
                        "inverse" if stats["total_change"] < 0 else "normal"
                    )
                    st.metric(
                        "Cambio Total",
                        f"€{abs(stats['total_change']):,}",
                        f"{stats['total_change_pct']:+.1f}%",
                        delta_color=change_color,
                        help="Diferencia entre precio inicial y actual",
                    )
                with col4:
                    st.metric(
                        "Cambios de Precio",
                        f"{stats['num_changes']}",
                        help="Número de veces que cambió el precio",
                    )

                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric(
                        "Primera Vista",
                        listing["first_seen_date"],
                        help="Primera vez que se detectó esta propiedad",
                    )
                with col2:
                    st.metric(
                        "Última Vista",
                        listing["last_seen_date"],
                        help="Última vez que se vio activa",
                    )
                with col3:
                    if stats["avg_days_between_changes"]:
                        st.metric(
                            "Días entre Cambios",
                            f"{stats['avg_days_between_changes']:.0f}",
                            help="Promedio de días entre cambios de precio",
                        )

                st.markdown("---")
                st.markdown("### 📈 Evolución del Precio")

                fig_evolution = go.Figure()
                fig_evolution.add_trace(
                    go.Scatter(
                        x=evolution_df["date_recorded"],
                        y=evolution_df["price"],
                        mode="lines+markers",
                        name="Precio",
                        line=dict(color="#3498db", width=3),
                        marker=dict(size=12),
                        text=[f"€{p:,}" for p in evolution_df["price"]],
                        hovertemplate="<b>%{x}</b><br>Precio: %{text}<extra></extra>",
                    )
                )

                for _, row in evolution_df.iterrows():
                    if pd.notna(row["change_amount"]) and row["change_amount"] != 0:
                        color = "#e74c3c" if row["change_amount"] < 0 else "#2ecc71"
                        symbol = "▼" if row["change_amount"] < 0 else "▲"
                        fig_evolution.add_annotation(
                            x=row["date_recorded"],
                            y=row["price"],
                            text=f"{symbol} {abs(row['change_percent']):.1f}%",
                            showarrow=True,
                            arrowhead=2,
                            arrowcolor=color,
                            font=dict(color=color, size=12, family="Arial Black"),
                            bgcolor="white",
                            bordercolor=color,
                            borderwidth=2,
                            borderpad=4,
                        )

                fig_evolution.update_layout(
                    title=f"Histórico de Precios - {listing['title'][:50]}...",
                    xaxis_title="Fecha",
                    yaxis_title="Precio (€)",
                    hovermode="x unified",
                    height=500,
                    showlegend=False,
                )
                st.plotly_chart(fig_evolution, use_container_width=True)

                # Detailed history table
                st.markdown("### 📋 Historial Detallado")
                history_display = evolution_df[
                    ["date_recorded", "price", "change_amount", "change_percent"]
                ].copy()
                history_display.columns = [
                    "Fecha",
                    "Precio",
                    "Cambio (€)",
                    "Cambio (%)",
                ]
                history_display["Precio"] = history_display["Precio"].apply(
                    lambda x: f"€{x:,}"
                )
                history_display["Cambio (€)"] = history_display["Cambio (€)"].apply(
                    lambda x: f"{x:+,.0f}€" if pd.notna(x) else "Inicial"
                )
                history_display["Cambio (%)"] = history_display["Cambio (%)"].apply(
                    lambda x: f"{x:+.1f}%" if pd.notna(x) else "-"
                )
                st.dataframe(
                    history_display,
                    hide_index=True,
                    use_container_width=True,
                    height=300,
                )

                csv = evolution_df.to_csv(index=False)
                st.download_button(
                    label="📥 Descargar Histórico (CSV)",
                    data=csv,
                    file_name=f"historico_{listing['listing_id']}_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv",
                )

            else:
                st.info("📊 Esta propiedad aún no tiene cambios de precio registrados.")
                st.caption(
                    "El histórico se irá poblando conforme el scraper detecte cambios."
                )

        else:
            st.error("❌ Propiedad no encontrada")
            st.info(
                """
                **Posibles razones:**
                - El ID o URL no es válido
                - La propiedad no ha sido scrapeada aún
                - El formato de la URL es incorrecto

                **Formatos válidos:**
                - URL completa: `https://www.idealista.com/inmueble/110506346/`
                - Solo ID: `110506346`
                """
            )

    elif search_button and not search_input:
        st.warning("⚠️ Por favor, introduce una URL o ID para buscar.")
