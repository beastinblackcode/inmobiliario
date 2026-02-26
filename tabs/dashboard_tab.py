"""
Dashboard tab — KPIs, charts, analytics, price-history sections.

Entry point: render_dashboard_tab(df)
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

from database import (
    get_sold_last_n_days,
    get_price_trends_by_zone,
    get_connection,
    get_property_price_stats,
)
from data_utils import load_data


def render_dashboard_tab(df: pd.DataFrame) -> None:
    """Render all content for the 📊 Dashboard tab."""

    # =========================================================================
    # KPI Section
    # =========================================================================
    st.markdown("---")
    st.subheader("📊 Métricas Principales")

    col1, col2, col3 = st.columns(3)

    with col1:
        avg_price = df[df['price'] > 0]['price'].mean() if len(df[df['price'] > 0]) > 0 else 0
        st.metric(label="Precio Medio", value=f"€{avg_price:,.0f}", delta=None)

        median_price = df[df['price'] > 0]['price'].median() if len(df[df['price'] > 0]) > 0 else 0
        st.metric(
            label="Precio Mediano",
            value=f"€{median_price:,.0f}",
            delta=None,
            help="Mediana: menos sensible a propiedades de lujo",
        )

    with col2:
        avg_price_sqm = (
            df[df['price_per_sqm'].notna()]['price_per_sqm'].mean()
            if len(df[df['price_per_sqm'].notna()]) > 0 else 0
        )
        st.metric(label="Precio Medio por m²", value=f"€{avg_price_sqm:,.0f}", delta=None)

        median_price_sqm = (
            df[df['price_per_sqm'].notna()]['price_per_sqm'].median()
            if len(df[df['price_per_sqm'].notna()]) > 0 else 0
        )
        st.metric(
            label="Precio Mediano por m²",
            value=f"€{median_price_sqm:,.0f}",
            delta=None,
            help="Mediana: menos sensible a outliers",
        )

    with col3:
        active_count = len(df[df['status'] == 'active']) if 'status' in df.columns else len(df)
        st.metric(label="Inmuebles Activos", value=f"{active_count:,}", delta=None)

        sold_30_days = get_sold_last_n_days(30)
        st.metric(label="Vendidos (30 días)", value=f"{sold_30_days:,}", delta=None)

    # =========================================================================
    # Price Drops trend
    # =========================================================================

    # =========================================================================
    # Ranking de Barrios (integrado desde ranking_tab)
    # =========================================================================
    st.markdown("---")
    with st.expander("🏆 Ranking de Barrios", expanded=False):
        from tabs.ranking_tab import render_ranking_tab
        render_ranking_tab()

    st.markdown("---")

    # =========================================================================
    # Seller type distribution
    # =========================================================================
    st.markdown("#### Distribución por Tipo de Vendedor")
    seller_df = df[df['seller_type'].notna()].copy()

    if not seller_df.empty:
        seller_counts = seller_df['seller_type'].value_counts().reset_index()
        seller_counts.columns = ['Tipo', 'Cantidad']
        total = seller_counts['Cantidad'].sum()
        seller_counts['Porcentaje'] = (seller_counts['Cantidad'] / total * 100).round(1)

        fig_pie = px.pie(
            seller_counts, values='Cantidad', names='Tipo',
            title='Particulares vs Inmobiliarias',
            color_discrete_sequence=['#2ecc71', '#3498db', '#e74c3c'],
        )
        fig_pie.update_traces(
            textposition='inside', textinfo='percent+label',
            hovertemplate='<b>%{label}</b><br>Cantidad: %{value:,}<br>Porcentaje: %{percent}<extra></extra>',
        )
        fig_pie.update_layout(showlegend=True, height=400)
        st.plotly_chart(fig_pie, use_container_width=True)

        col1, col2 = st.columns(2)
        for idx, row in seller_counts.iterrows():
            with col1 if idx % 2 == 0 else col2:
                st.metric(
                    label=row['Tipo'],
                    value=f"{row['Cantidad']:,} propiedades",
                    delta=f"{row['Porcentaje']}%",
                )

    # =========================================================================
    # Price by district — mean vs median
    # =========================================================================
    st.markdown("#### Precio por Distrito: Media vs Mediana")
    distrito_df = df[(df['distrito'].notna()) & (df['price'] > 0)].copy()

    if not distrito_df.empty:
        distrito_stats = distrito_df.groupby('distrito')['price'].agg(
            ['mean', 'median']
        ).reset_index()
        distrito_stats.columns = ['distrito', 'Media', 'Mediana']
        distrito_stats = distrito_stats.sort_values('Mediana', ascending=True)

        fig_bar = go.Figure()
        fig_bar.add_trace(go.Bar(
            name='Precio Medio', x=distrito_stats['Media'],
            y=distrito_stats['distrito'], orientation='h', marker_color='#1f77b4',
        ))
        fig_bar.add_trace(go.Bar(
            name='Precio Mediano', x=distrito_stats['Mediana'],
            y=distrito_stats['distrito'], orientation='h', marker_color='#ff7f0e',
        ))
        fig_bar.update_layout(
            title='Comparación Precio Medio vs Mediano por Distrito',
            xaxis_title='Precio (€)', yaxis_title='Distrito', barmode='group',
            xaxis_tickformat=',.0f', height=600,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        st.plotly_chart(fig_bar, use_container_width=True)

    # =========================================================================
    # Price per m² evolution
    # =========================================================================
    st.markdown("---")
    st.markdown("#### 📈 Evolución del Precio por m²")

    col_period, col_distrito = st.columns([1, 2])

    with col_period:
        time_period = st.radio(
            "Período",
            ["Últimos 7 días", "Últimos 30 días", "Todo el período"],
            horizontal=True,
        )

    with col_distrito:
        distrito_filter = st.selectbox(
            "Distrito",
            ["Todos"] + sorted(df[df['distrito'].notna()]['distrito'].unique().tolist()),
        )

    from analytics import get_price_per_sqm_evolution

    evolution_df = df[df['status'] == 'active'].copy()

    if time_period == "Últimos 7 días":
        cutoff_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        evolution_df = evolution_df[evolution_df['first_seen_date'] >= cutoff_date]
    elif time_period == "Últimos 30 días":
        cutoff_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        evolution_df = evolution_df[evolution_df['first_seen_date'] >= cutoff_date]

    evolution_data = get_price_per_sqm_evolution(
        evolution_df,
        period='D',
        distrito=distrito_filter if distrito_filter != "Todos" else None,
    )

    if not evolution_data.empty and len(evolution_data) > 1:
        current_avg = evolution_data['avg_price_sqm'].iloc[-1]
        change_7d_pct = change_7d_eur = 0
        change_30d_pct = change_30d_eur = 0

        if len(evolution_data) >= 7:
            week_ago = evolution_data['avg_price_sqm'].iloc[-7]
            change_7d_pct = (current_avg - week_ago) / week_ago * 100
            change_7d_eur = current_avg - week_ago

        if len(evolution_data) >= 30:
            month_ago = evolution_data['avg_price_sqm'].iloc[-30]
            change_30d_pct = (current_avg - month_ago) / month_ago * 100
            change_30d_eur = current_avg - month_ago

        if change_7d_pct > 2:
            trend_icon, trend_text = "↗️", "Subiendo"
        elif change_7d_pct < -2:
            trend_icon, trend_text = "↘️", "Bajando"
        else:
            trend_icon, trend_text = "→", "Estable"

        mc = st.columns(4)
        mc[0].metric("Precio Actual (€/m²)", f"{current_avg:,.0f} €")
        mc[1].metric("Cambio 7 días", f"{change_7d_eur:+,.0f} €", f"{change_7d_pct:+.1f}%")
        if change_30d_pct != 0:
            mc[2].metric("Cambio 30 días", f"{change_30d_eur:+,.0f} €", f"{change_30d_pct:+.1f}%")
        else:
            mc[2].metric("Cambio 30 días", "N/A", help="No hay suficientes datos")
        mc[3].metric("Tendencia", f"{trend_icon} {trend_text}", help="Basado en cambio de 7 días")

        fig_evo = go.Figure()
        fig_evo.add_trace(go.Scatter(
            x=evolution_data['date'], y=evolution_data['avg_price_sqm'],
            mode='lines+markers', name='Precio Medio/m²',
            line=dict(color='#3498db', width=3), marker=dict(size=6),
            hovertemplate='<b>%{x|%d/%m/%Y}</b><br>Precio: %{y:,.0f} €/m²<extra></extra>',
        ))
        fig_evo.add_trace(go.Scatter(
            x=evolution_data['date'], y=evolution_data['median_price_sqm'],
            mode='lines+markers', name='Precio Mediano/m²',
            line=dict(color='#e67e22', width=2, dash='dash'), marker=dict(size=4),
            hovertemplate='<b>%{x|%d/%m/%Y}</b><br>Precio: %{y:,.0f} €/m²<extra></extra>',
        ))
        titulo_distrito = f" - {distrito_filter}" if distrito_filter != "Todos" else ""
        fig_evo.update_layout(
            title=f'Evolución del Precio por m²{titulo_distrito}',
            xaxis_title='Fecha', yaxis_title='Precio (€/m²)',
            hovermode='x unified', height=450, showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            xaxis=dict(showgrid=True, gridcolor='rgba(128,128,128,0.2)'),
            yaxis=dict(showgrid=True, gridcolor='rgba(128,128,128,0.2)', tickformat=',.0f'),
        )
        st.plotly_chart(fig_evo, use_container_width=True)
        st.info(
            f"📊 Mostrando datos de {len(evolution_data)} días con un total de "
            f"{evolution_data['count'].sum():,.0f} propiedades analizadas"
        )
    else:
        st.warning(
            "No hay suficientes datos para mostrar la evolución temporal. "
            "Se necesitan al menos 2 días de datos."
        )

    # =========================================================================
    # Advanced Analytics (sub-tabs)
    # =========================================================================
    st.markdown("---")
    st.subheader("📊 Análisis Avanzado")

    from analytics import (
        get_velocity_metrics,
        rank_opportunities,
        identify_bargains,
        get_new_vs_sold_trends,
    )

    analytics_tab1, analytics_tab2, analytics_tab3 = st.tabs([
        "📈 Tendencias Temporales",
        "🎯 Mejores Oportunidades",
        "💎 Gangas (Precio/m²)",
    ])

    with analytics_tab1:
        st.markdown("### Métricas de Velocidad del Mercado")
        velocity = get_velocity_metrics(df)

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Días en Mercado (Media)", f"{velocity['avg_days_on_market']:.0f}",
                  help="Promedio de días que las propiedades están activas")
        c2.metric("Días en Mercado (Mediana)", f"{velocity['median_days_on_market']:.0f}",
                  help="Mediana de días en mercado (menos afectada por outliers)")
        c3.metric("Nuevos (7 días)", f"{velocity['new_last_7_days']:,}",
                  help="Propiedades nuevas en los últimos 7 días")
        c4.metric("Vendidos (7 días)", f"{velocity['sold_last_7_days']:,}",
                  help="Propiedades vendidas en los últimos 7 días")

        st.markdown("---")
        st.markdown("### Nuevos vs Vendidos (Últimos 30 días)")
        trends_data = get_new_vs_sold_trends(df, days=30)

        if trends_data['dates']:
            fig_trends = go.Figure()
            fig_trends.add_trace(go.Scatter(
                x=trends_data['dates'], y=trends_data['new'], name='Nuevos',
                mode='lines+markers', line=dict(color='#2ecc71', width=2), marker=dict(size=6),
            ))
            fig_trends.add_trace(go.Scatter(
                x=trends_data['dates'], y=trends_data['sold'], name='Vendidos',
                mode='lines+markers', line=dict(color='#e74c3c', width=2), marker=dict(size=6),
            ))
            fig_trends.update_layout(
                xaxis_title='Fecha', yaxis_title='Cantidad',
                hovermode='x unified', height=400,
            )
            st.plotly_chart(fig_trends, use_container_width=True)
        else:
            st.info("No hay suficientes datos para mostrar tendencias temporales")

    with analytics_tab2:
        st.markdown("### Top 20 Mejores Oportunidades (Ratio Calidad-Precio)")
        st.info("""
        **Score de Calidad-Precio (0-100):**
        - 📊 Precio/m² vs promedio del distrito (40%)
        - 📐 Tamaño de la propiedad (20%)
        - 👤 Tipo de vendedor (10%)
        - ⏱️ Días en mercado (15%)
        - 🌅 Orientación (15%)

        **Mayor score = Mejor relación calidad-precio**
        """)

        df_ranked = rank_opportunities(df[df['status'] == 'active'])

        if not df_ranked.empty:
            for idx, row in df_ranked.head(20).iterrows():
                score = row['quality_score']
                if score >= 80:
                    badge, label = "🟢", "Excelente"
                elif score >= 70:
                    badge, label = "🔵", "Muy Bueno"
                elif score >= 60:
                    badge, label = "🟡", "Bueno"
                else:
                    badge, label = "🟠", "Regular"

                title_preview = row['title'][:70] + "..." if len(row['title']) > 70 else row['title']
                with st.expander(f"{badge} **{score:.0f}/100** ({label}) - {title_preview}"):
                    rc1, rc2, rc3 = st.columns(3)
                    with rc1:
                        st.metric("💰 Precio", f"€{row['price']:,}")
                        st.metric("📐 Tamaño", f"{row['size_sqm']:.0f} m²" if pd.notna(row['size_sqm']) else "N/A")
                        st.metric("🛏️ Habitaciones", int(row['rooms']) if pd.notna(row['rooms']) else "N/A")
                    with rc2:
                        st.metric("💵 €/m²", f"€{row['price_per_sqm']:,.0f}" if pd.notna(row['price_per_sqm']) else "N/A")
                        st.metric("📊 vs Distrito", f"{row['vs_distrito_avg']:+.1f}%" if pd.notna(row['vs_distrito_avg']) else "N/A")
                        st.metric("⏱️ Días en Mercado", f"{row['days_on_market']:.0f}" if pd.notna(row['days_on_market']) else "N/A")
                    with rc3:
                        st.metric("📍 Distrito", row['distrito'])
                        st.metric("🏘️ Barrio", row['barrio'])
                        st.metric("👤 Vendedor", row['seller_type'])
                        st.metric("🌅 Orientación", row['orientation'] if pd.notna(row['orientation']) else "N/A")
                    st.markdown(f"[🔗 Ver en Idealista]({row['url']})")
        else:
            st.warning("No hay propiedades activas para analizar")

    with analytics_tab3:
        st.markdown("### Gangas: Propiedades con Mejor Precio/m²")
        st.info("Propiedades con precio/m² **15% o más por debajo** del promedio de su distrito.")

        bargains = identify_bargains(df[df['status'] == 'active'], threshold=-15.0)

        if not bargains.empty:
            st.success(f"✨ Encontradas {len(bargains)} gangas potenciales")

            bargains_display = bargains[[
                'title', 'price', 'price_per_sqm', 'vs_distrito_avg',
                'distrito', 'barrio', 'rooms', 'size_sqm', 'quality_score', 'url',
            ]].copy()
            bargains_display.columns = [
                'Título', 'Precio', '€/m²', '% vs Distrito',
                'Distrito', 'Barrio', 'Hab.', 'm²', 'Score', 'Link',
            ]
            bargains_display['Precio'] = bargains['price']
            bargains_display['€/m²'] = bargains['price_per_sqm']
            bargains_display['% vs Distrito'] = bargains['vs_distrito_avg']
            bargains_display['Score'] = bargains['quality_score']
            bargains_display['m²'] = bargains['size_sqm']
            bargains_display['Link'] = bargains['url']

            st.dataframe(
                bargains_display, hide_index=True, use_container_width=True, height=400,
                column_config={
                    "Precio": st.column_config.NumberColumn("Precio", format="€%d", min_value=0),
                    "€/m²": st.column_config.NumberColumn("€/m²", format="€%d"),
                    "% vs Distrito": st.column_config.NumberColumn("% vs Distrito", format="%.1f%%"),
                    "Score": st.column_config.ProgressColumn("Score", format="%d", min_value=0, max_value=100),
                    "m²": st.column_config.NumberColumn("m²", format="%d m²"),
                    "Link": st.column_config.LinkColumn("Idealista", display_text="🔗 Ver"),
                },
            )
            csv = bargains.to_csv(index=False)
            st.download_button(
                label="📥 Descargar Gangas (CSV)", data=csv,
                file_name=f"gangas_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
            )
        else:
            st.warning("No se encontraron gangas con el criterio actual (15% por debajo del promedio)")

    # =========================================================================
    # Time-to-sale by district
    # =========================================================================
    st.markdown("#### ⏱️ Tiempo Medio de Venta por Distrito")

    df_all = load_data(status="all", distritos=None, min_price=None, max_price=None, seller_type="All")
    sold_df = df_all[
        (df_all['status'] == 'sold_removed') & (df_all['distrito'].notna())
    ].copy()
    sold_df = sold_df[sold_df['first_seen_date'] > '2026-01-14'].copy()

    if not sold_df.empty and len(sold_df) > 10:
        sold_df['days_on_market_calc'] = sold_df.apply(
            lambda row: (
                pd.to_datetime(row['last_seen_date']) - pd.to_datetime(row['first_seen_date'])
            ).days if pd.notna(row['last_seen_date']) and pd.notna(row['first_seen_date']) else 0,
            axis=1,
        )
        time_to_sale = sold_df.groupby('distrito').agg(
            {'days_on_market_calc': ['mean', 'median', 'count']}
        ).reset_index()
        time_to_sale.columns = ['distrito', 'media_dias', 'mediana_dias', 'cantidad']
        time_to_sale = time_to_sale[time_to_sale['cantidad'] >= 3]
        time_to_sale = time_to_sale.sort_values('mediana_dias', ascending=True)

        if not time_to_sale.empty:
            fig_time = go.Figure()
            fig_time.add_trace(go.Bar(
                name='Tiempo Medio', x=time_to_sale['media_dias'], y=time_to_sale['distrito'],
                orientation='h', marker_color='#e74c3c',
                text=time_to_sale['media_dias'].round(1), textposition='outside',
            ))
            fig_time.add_trace(go.Bar(
                name='Tiempo Mediano', x=time_to_sale['mediana_dias'], y=time_to_sale['distrito'],
                orientation='h', marker_color='#3498db',
                text=time_to_sale['mediana_dias'].round(1), textposition='outside',
            ))
            fig_time.update_layout(
                title='Días en Mercado hasta Venta (Propiedades Vendidas)',
                xaxis_title='Días', yaxis_title='Distrito', barmode='group', height=600,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            st.plotly_chart(fig_time, use_container_width=True)
            avg_time = sold_df['days_on_market_calc'].mean()
            median_time = sold_df['days_on_market_calc'].median()
            st.caption(
                f"📊 Tiempo medio global: {avg_time:.1f} días | "
                f"Tiempo mediano: {median_time:.1f} días | "
                f"Total vendidas: {len(sold_df):,}"
            )
        else:
            st.info("ℹ️ No hay suficientes datos de ventas por distrito aún.")
    else:
        st.info("ℹ️ Aún no hay suficientes propiedades vendidas para calcular el tiempo medio de venta.")

    # =========================================================================
    # Price-decline zones
    # =========================================================================
    st.markdown("#### 📉 Zonas con Precios en Descenso")

    try:
        price_trends = get_price_trends_by_zone(zone_type='distrito', min_properties=50)

        if price_trends and len(price_trends) > 0:
            trends_df = pd.DataFrame(price_trends)
            decreasing_df = trends_df[trends_df['price_change_pct'] < 0].copy()

            if not decreasing_df.empty:
                decreasing_df = decreasing_df.sort_values('price_change_pct', ascending=True)
                fig_trends2 = go.Figure()
                fig_trends2.add_trace(go.Bar(
                    x=decreasing_df['price_change_pct'], y=decreasing_df['zone'],
                    orientation='h', marker_color='#e74c3c',
                    text=decreasing_df['price_change_pct'].apply(lambda x: f'{x:.1f}%'),
                    textposition='outside',
                    hovertemplate='<b>%{y}</b><br>Cambio: %{x:.2f}%<br><extra></extra>',
                ))
                fig_trends2.update_layout(
                    title='Distritos con Mayor Descenso de Precios',
                    xaxis_title='Cambio de Precio (%)', yaxis_title='Distrito',
                    height=max(400, len(decreasing_df) * 40), showlegend=False,
                )
                st.plotly_chart(fig_trends2, use_container_width=True)

                st.markdown("**📊 Detalle de Cambios de Precio/m²**")
                display_trends = decreasing_df.copy()
                display_trends['Precio Inicial/m²'] = display_trends['first_avg_price']
                display_trends['Precio Actual/m²'] = display_trends['last_avg_price']
                display_trends['Cambio €/m²'] = display_trends['price_change']
                display_trends['Cambio %'] = display_trends['price_change_pct']
                display_trends['Propiedades'] = display_trends['property_count']

                st.dataframe(
                    display_trends[['zone', 'Precio Inicial/m²', 'Precio Actual/m²', 'Cambio €/m²', 'Cambio %', 'Propiedades']].rename(columns={'zone': 'Distrito'}),
                    hide_index=True, use_container_width=True,
                    column_config={
                        "Precio Inicial/m²": st.column_config.NumberColumn("Precio Inicial/m²", format="€%d"),
                        "Precio Actual/m²": st.column_config.NumberColumn("Precio Actual/m²", format="€%d"),
                        "Cambio €/m²": st.column_config.NumberColumn("Cambio €/m²", format="€%d"),
                        "Cambio %": st.column_config.NumberColumn("Cambio %", format="%.2f%%"),
                    },
                )
                avg_decrease = decreasing_df['price_change_pct'].mean()
                max_decrease = decreasing_df['price_change_pct'].min()
                st.caption(f"📉 Descenso promedio: {avg_decrease:.2f}% | Máximo descenso: {max_decrease:.2f}%")
            else:
                st.success("✅ ¡Buenas noticias! No hay distritos con descenso de precios significativo.")
                increasing_df = trends_df[trends_df['price_change_pct'] > 0].copy()
                if not increasing_df.empty:
                    increasing_df = increasing_df.sort_values('price_change_pct', ascending=False).head(5)
                    st.markdown("**📈 Top 5 Distritos con Mayor Aumento de Precios:**")
                    for _, row in increasing_df.iterrows():
                        st.write(f"- **{row['zone']}**: +{row['price_change_pct']:.2f}% (€{row['price_change']:,.0f})")
        else:
            st.info("ℹ️ No hay suficientes datos para calcular tendencias de precios.")
    except Exception as e:
        st.error(f"⚠️ Error al calcular tendencias de precios: {str(e)}")

    # Days on market (from filtered df, for sold props)
    sold_df2 = df[(df['status'] == 'sold_removed') & (df['distrito'].notna())].copy()
    if not sold_df2.empty and len(sold_df2) > 10:
        st.markdown("#### Tiempo Medio en Mercado por Distrito (Propiedades Vendidas)")
        market_time = sold_df2.groupby('distrito')['days_on_market'].mean().reset_index()
        market_time = market_time.sort_values('days_on_market', ascending=True)

        fig_market = px.bar(
            market_time, x='days_on_market', y='distrito', orientation='h',
            title='Días Promedio en Mercado hasta Venta',
            labels={'days_on_market': 'Días', 'distrito': 'Distrito'},
            color='days_on_market', color_continuous_scale='RdYlGn_r',
        )
        fig_market.update_layout(showlegend=False, height=600)
        st.plotly_chart(fig_market, use_container_width=True)

    # =========================================================================
    # Price history section
    # =========================================================================
    st.markdown("---")
    st.subheader("💰 Histórico de Precios")

    from analytics import (
        get_price_drops_dataframe,
        get_property_evolution_dataframe,
        get_desperate_sellers_dataframe,
        get_price_history_summary,
    )

    try:
        history_summary = get_price_history_summary()
        hc = st.columns(4)
        hc[0].metric("Propiedades Rastreadas", f"{history_summary['total_records']:,}")
        hc[1].metric("Con Cambios de Precio", f"{history_summary['properties_with_changes']:,}")
        hc[2].metric(
            "Bajadas de Precio", f"{history_summary['price_drops']:,}",
            f"{history_summary['avg_drop_percent']:.1f}% promedio", delta_color="inverse",
        )
        hc[3].metric(
            "Subidas de Precio", f"{history_summary['price_increases']:,}",
            f"+{history_summary['avg_increase_percent']:.1f}% promedio",
        )
    except Exception:
        st.info("📊 El histórico de precios se irá poblando conforme el scraper detecte cambios.")

    st.markdown("---")

    price_tab1, price_tab2, price_tab3 = st.tabs([
        "📉 Bajadas Recientes",
        "📊 Evolución por Propiedad",
        "🎯 Vendedores Desesperados",
    ])

    with price_tab1:
        st.markdown("### Bajadas de Precio Recientes")
        col1, col2 = st.columns(2)
        with col1:
            days_filter = st.selectbox(
                "Período", options=[7, 14, 30],
                format_func=lambda x: f"Últimos {x} días", key="price_drops_days",
            )
        with col2:
            min_drop = st.slider("Bajada Mínima (%)", 1.0, 30.0, 5.0, 1.0, key="price_drops_min")

        drops_df = get_price_drops_dataframe(days=days_filter, min_drop_percent=min_drop)

        if not drops_df.empty:
            st.success(f"✅ Encontradas {len(drops_df)} propiedades con bajadas de precio")
            dc = st.columns(3)
            dc[0].metric("Bajada Promedio", f"{abs(drops_df['change_percent'].mean()):.1f}%")
            dc[1].metric("Mayor Bajada", f"{abs(drops_df['change_percent'].min()):.1f}%")
            dc[2].metric("Ahorro Total", f"€{abs(drops_df['change_amount'].sum()):,.0f}")
            st.markdown("---")
            st.markdown("#### 📋 Propiedades con Bajadas de Precio")

            for idx, row in drops_df.iterrows():
                col1, col2, col3, col4, col5, col6, col7, col8 = st.columns([3, 1.2, 1.2, 1, 0.8, 0.8, 0.8, 0.8])
                with col1:
                    lbl = row['title'][:60] + "..." if len(row['title']) > 60 else row['title']
                    st.markdown(f"[{lbl}]({row['url']})")
                    st.caption(f"📍 {row['distrito']} - {row['barrio']}")
                with col2:
                    old_sqm = row['old_price'] / row['size_sqm'] if pd.notna(row['size_sqm']) and row['size_sqm'] > 0 else 0
                    st.markdown("<small>Precio Ant/m²</small>", unsafe_allow_html=True)
                    st.markdown(f"**€{old_sqm:,.0f}**" if old_sqm > 0 else "N/A")
                with col3:
                    new_sqm = row['new_price'] / row['size_sqm'] if pd.notna(row['size_sqm']) and row['size_sqm'] > 0 else 0
                    chg_sqm = old_sqm - new_sqm if old_sqm > 0 and new_sqm > 0 else 0
                    st.markdown("<small>Precio Act/m²</small>", unsafe_allow_html=True)
                    st.markdown(f"**€{new_sqm:,.0f}**" if new_sqm > 0 else "N/A")
                    if chg_sqm > 0:
                        st.markdown(f"<small style='color:red;'>↓ €{abs(chg_sqm):,.0f}</small>", unsafe_allow_html=True)
                with col4:
                    st.markdown("<small>Precio Total</small>", unsafe_allow_html=True)
                    st.markdown(f"**€{row['new_price']:,.0f}**")
                with col5:
                    st.markdown("<small>Bajada</small>", unsafe_allow_html=True)
                    st.markdown(f"**{abs(row['change_percent']):.1f}%**")
                with col6:
                    st.markdown("<small>Hab.</small>", unsafe_allow_html=True)
                    st.markdown(f"**{int(row['rooms'])}**" if pd.notna(row['rooms']) else "N/A")
                with col7:
                    st.markdown("<small>m²</small>", unsafe_allow_html=True)
                    st.markdown(f"**{row['size_sqm']:.0f}**" if pd.notna(row['size_sqm']) else "N/A")
                with col8:
                    st.markdown("<small>Planta</small>", unsafe_allow_html=True)
                    st.markdown(f"**{row['floor']}**" if pd.notna(row['floor']) and row['floor'] else "N/A")
                st.markdown("---")

            csv = drops_df.to_csv(index=False)
            st.download_button(
                "📥 Descargar Bajadas (CSV)", data=csv,
                file_name=f"bajadas_precio_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
            )
        else:
            st.info(f"No se encontraron bajadas de precio ≥{min_drop}% en los últimos {days_filter} días.")

    with price_tab2:
        st.markdown("### Evolución de Precio por Propiedad")

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT DISTINCT l.listing_id, l.title, l.distrito, l.price
                FROM listings l
                JOIN price_history ph ON l.listing_id = ph.listing_id
                WHERE l.status = 'active' AND ph.change_amount IS NOT NULL
                ORDER BY l.title LIMIT 100
            """)
            properties_with_changes = cursor.fetchall()

        if properties_with_changes:
            property_options = {
                f"{title[:50]}... ({distrito}) - €{price:,.0f}": listing_id
                for listing_id, title, distrito, price in properties_with_changes
            }
            selected_label = st.selectbox(
                "Selecciona una propiedad", options=list(property_options.keys()),
                key="property_evolution_selector",
            )
            selected_listing_id = property_options[selected_label]
            evo_df = get_property_evolution_dataframe(selected_listing_id)

            if not evo_df.empty:
                stats = get_property_price_stats(selected_listing_id)
                ec = st.columns(4)
                ec[0].metric("Precio Inicial", f"€{stats['initial_price']:,.0f}")
                ec[1].metric("Precio Actual", f"€{stats['current_price']:,.0f}")
                chg_color = "inverse" if stats['total_change'] < 0 else "normal"
                ec[2].metric(
                    "Cambio Total", f"€{abs(stats['total_change']):,.0f}",
                    f"{stats['total_change_pct']:+.1f}%", delta_color=chg_color,
                )
                ec[3].metric("Cambios de Precio", f"{stats['num_changes']}")
                st.markdown("---")

                fig_evo2 = go.Figure()
                fig_evo2.add_trace(go.Scatter(
                    x=evo_df['date_recorded'], y=evo_df['price'],
                    mode='lines+markers', name='Precio',
                    line=dict(color='#3498db', width=3), marker=dict(size=10),
                    text=[f"€{p:,.0f}" for p in evo_df['price']],
                    hovertemplate='<b>%{x}</b><br>Precio: %{text}<extra></extra>',
                ))
                for _, row in evo_df.iterrows():
                    if pd.notna(row['change_amount']) and row['change_amount'] != 0:
                        color = '#e74c3c' if row['change_amount'] < 0 else '#2ecc71'
                        symbol = '▼' if row['change_amount'] < 0 else '▲'
                        fig_evo2.add_annotation(
                            x=row['date_recorded'], y=row['price'],
                            text=f"{symbol} {abs(row['change_percent']):.1f}%",
                            showarrow=True, arrowhead=2, arrowcolor=color,
                            font=dict(color=color, size=10),
                            bgcolor='white', bordercolor=color, borderwidth=1,
                        )
                fig_evo2.update_layout(
                    title="Evolución del Precio", xaxis_title="Fecha",
                    yaxis_title="Precio (€)", hovermode='x unified', height=500,
                )
                st.plotly_chart(fig_evo2, use_container_width=True)

                st.markdown("#### Historial Detallado")
                hist_disp = evo_df[['date_recorded', 'price', 'change_amount', 'change_percent']].copy()
                hist_disp.columns = ['Fecha', 'Precio', 'Cambio (€)', 'Cambio (%)']
                st.dataframe(
                    hist_disp, hide_index=True, use_container_width=True,
                    column_config={
                        "Precio": st.column_config.NumberColumn("Precio", format="€%d", min_value=0),
                        "Cambio (€)": st.column_config.NumberColumn("Cambio (€)", format="€%d"),
                        "Cambio (%)": st.column_config.NumberColumn("Cambio (%)", format="%.1f%%"),
                    },
                )
        else:
            st.info("📊 Aún no hay propiedades con cambios de precio registrados.")

    with price_tab3:
        st.markdown("### Vendedores Desesperados (Múltiples Bajadas)")
        col1, col2 = st.columns(2)
        with col1:
            min_drops_filter = st.slider("Mínimo de Bajadas", 2, 5, 2, 1, key="desperate_min_drops")
        with col2:
            min_total_drop = st.slider("Bajada Total Mínima (%)", 5.0, 30.0, 10.0, 5.0, key="desperate_min_total")

        desperate_df = get_desperate_sellers_dataframe(
            min_drops=min_drops_filter, min_total_drop_pct=min_total_drop
        )

        if not desperate_df.empty:
            st.success(f"✅ Encontradas {len(desperate_df)} propiedades con múltiples bajadas")
            st.markdown("#### Top Oportunidades (por Score de Urgencia)")

            disp_df = desperate_df[[
                'title', 'distrito', 'barrio', 'initial_price', 'current_price',
                'total_drop', 'total_drop_pct', 'num_drops', 'urgency_score', 'rooms', 'size_sqm',
            ]].head(20).copy()
            disp_df.columns = [
                'Título', 'Distrito', 'Barrio', 'Precio Inicial', 'Precio Actual',
                'Bajada Total (€)', 'Bajada Total (%)', 'Nº Bajadas', 'Score Urgencia', 'Hab.', 'm²',
            ]
            st.dataframe(
                disp_df, hide_index=True, use_container_width=True, height=500,
                column_config={
                    "Precio Inicial": st.column_config.NumberColumn("Precio Inicial", format="€%d", min_value=0),
                    "Precio Actual": st.column_config.NumberColumn("Precio Actual", format="€%d", min_value=0),
                    "Bajada Total (€)": st.column_config.NumberColumn("Bajada Total (€)", format="€%d"),
                    "Bajada Total (%)": st.column_config.NumberColumn("Bajada Total (%)", format="%.1f%%"),
                    "Score Urgencia": st.column_config.ProgressColumn("Score Urgencia", format="%d", min_value=0, max_value=100),
                    "m²": st.column_config.NumberColumn("m²", format="%d m²"),
                },
            )
            csv = desperate_df.to_csv(index=False)
            st.download_button(
                "📥 Descargar Vendedores Desesperados (CSV)", data=csv,
                file_name=f"vendedores_desesperados_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
            )
        else:
            st.info(f"No se encontraron propiedades con ≥{min_drops_filter} bajadas y ≥{min_total_drop}% de bajada total.")

    # =========================================================================
    # Chollos by barrio
    # =========================================================================
    st.markdown("---")
    st.markdown("#### 💎 Chollos por Barrio")
    st.caption("Propiedades con precio/m² significativamente inferior a la media del barrio")

    all_active = load_data(status="active", distritos=None, min_price=None, max_price=None, seller_type="All")
    chollos_df = all_active[
        (all_active['price'] > 0) & (all_active['size_sqm'] > 0) & (all_active['barrio'].notna())
    ].copy()

    if not chollos_df.empty and len(chollos_df) > 20:
        chollos_df['price_per_sqm'] = chollos_df['price'] / chollos_df['size_sqm']
        barrio_stats2 = chollos_df.groupby('barrio').agg(
            {'price_per_sqm': ['mean', 'std', 'count']}
        ).reset_index()
        barrio_stats2.columns = ['barrio', 'mean_price_sqm', 'std_price_sqm', 'count']
        barrio_stats2 = barrio_stats2[barrio_stats2['count'] >= 5]
        chollos_df = chollos_df.merge(barrio_stats2[['barrio', 'mean_price_sqm', 'std_price_sqm']], on='barrio', how='left')
        chollos_df['z_score'] = (chollos_df['price_per_sqm'] - chollos_df['mean_price_sqm']) / chollos_df['std_price_sqm']
        chollos = chollos_df[chollos_df['z_score'] < -1.5].copy().sort_values('z_score')

        if not chollos.empty:
            st.success(f"🎯 Encontrados {len(chollos)} chollos potenciales")
            top_chollos = chollos.head(20)

            display_chollos = pd.DataFrame({
                'Título': top_chollos['title'],
                'Barrio': top_chollos['barrio'],
                'Precio': top_chollos['price'],
                'Precio/m²': top_chollos['price_per_sqm'],
                'Media Barrio': top_chollos['mean_price_sqm'],
                'Descuento': (top_chollos['mean_price_sqm'] - top_chollos['price_per_sqm']) / top_chollos['mean_price_sqm'],
                'Habitaciones': top_chollos['rooms'].fillna(0).astype(int),
                'm²': top_chollos['size_sqm'],
                'URL': top_chollos['url'],
            })
            st.dataframe(
                display_chollos, hide_index=True, use_container_width=True,
                column_config={
                    "Precio": st.column_config.NumberColumn("Precio", format="€%d", min_value=0),
                    "Precio/m²": st.column_config.NumberColumn("Precio/m²", format="€%d"),
                    "Media Barrio": st.column_config.NumberColumn("Media Barrio", format="€%d"),
                    "Descuento": st.column_config.NumberColumn("Descuento", format="%.1f%%"),
                    "m²": st.column_config.NumberColumn("m²", format="%d m²"),
                    "URL": st.column_config.LinkColumn("Enlace", display_text="Ver oferta"),
                },
            )
            chollos_by_barrio = chollos.groupby('barrio').size().reset_index(name='Chollos')
            chollos_by_barrio = chollos_by_barrio.sort_values('Chollos', ascending=False).head(10)

            cc1, cc2 = st.columns(2)
            with cc1:
                st.dataframe(chollos_by_barrio, hide_index=True, use_container_width=True)
            with cc2:
                fig_ch = px.bar(
                    chollos_by_barrio, x='Chollos', y='barrio', orientation='h',
                    title='Top 10 Barrios con Más Chollos',
                    labels={'Chollos': 'Número de Chollos', 'barrio': 'Barrio'},
                )
                fig_ch.update_layout(showlegend=False, height=400)
                st.plotly_chart(fig_ch, use_container_width=True)

            st.caption("💡 **Criterio**: Propiedades con precio/m² al menos 1.5 σ por debajo de la media del barrio")
        else:
            st.info("ℹ️ No se encontraron chollos significativos en este momento")
    else:
        st.info("ℹ️ No hay suficientes datos para detectar chollos (mínimo 20 propiedades activas)")

    # =========================================================================
    # Data table
    # =========================================================================
    st.markdown("---")
    st.subheader("📋 Datos Detallados")

    display_columns = ['title', 'distrito', 'barrio', 'price', 'rooms',
                       'size_sqm', 'price_per_sqm', 'seller_type', 'description', 'status']
    display_columns = [c for c in display_columns if c in df.columns]
    display_df2 = df[display_columns].copy()

    if 'price' in display_df2.columns:
        display_df2['price'] = display_df2['price'].apply(
            lambda x: f"€{x:,.0f}" if pd.notna(x) and x > 0 else "N/A"
        )
    if 'price_per_sqm' in display_df2.columns:
        display_df2['price_per_sqm'] = display_df2['price_per_sqm'].apply(
            lambda x: f"€{x:,.0f}" if pd.notna(x) else "N/A"
        )
    if 'description' in display_df2.columns:
        display_df2['description'] = display_df2['description'].apply(
            lambda x: x[:150] + '...' if pd.notna(x) and len(str(x)) > 150 else (x if pd.notna(x) else "N/A")
        )

    st.dataframe(display_df2, hide_index=True, use_container_width=True, height=400)
