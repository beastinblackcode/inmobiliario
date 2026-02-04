"""
Streamlit dashboard for Madrid Real Estate Tracker.
Visualizes property listings with interactive filters and analytics.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

from database import (
    get_listings,
    get_database_stats,
    get_sold_last_n_days,
    get_price_trends_by_zone,
    download_database_from_cloud,
    is_streamlit_cloud
)


# Page configuration
st.set_page_config(
    page_title="Madrid Real Estate Tracker",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better aesthetics
st.markdown("""
    <style>
    .main {
        padding: 0rem 1rem;
    }
    .stMetric {
        background-color: #f0f2f6;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    h1 {
        color: #1f77b4;
        padding-bottom: 20px;
    }
    h2 {
        color: #2c3e50;
        padding-top: 20px;
    }
    </style>
""", unsafe_allow_html=True)


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_data(status, distritos, min_price, max_price, seller_type):
    """Load and cache listing data with filters."""
    listings = get_listings(
        status=status,
        distrito=distritos if distritos else None,
        min_price=min_price,
        max_price=max_price,
        seller_type=seller_type
    )
    return pd.DataFrame(listings)


def check_password():
    """Returns True if user entered correct password."""
    def password_entered():
        username = st.session_state.get("username", "")
        password = st.session_state.get("password", "")
        
        # Check credentials from secrets
        if "auth" in st.secrets and "users" in st.secrets["auth"]:
            # Multi-user mode: check if username exists and password matches
            users = st.secrets["auth"]["users"]
            if username in users and users[username] == password:
                st.session_state["password_correct"] = True
                st.session_state["current_user"] = username
                # Clear credentials from session state
                if "username" in st.session_state:
                    del st.session_state["username"]
                if "password" in st.session_state:
                    del st.session_state["password"]
            else:
                st.session_state["password_correct"] = False
        elif "auth" in st.secrets:
            # Legacy single-user mode (backward compatibility)
            if (username == st.secrets["auth"].get("username", "") and 
                password == st.secrets["auth"].get("password", "")):
                st.session_state["password_correct"] = True
                st.session_state["current_user"] = username
                # Clear credentials from session state
                if "username" in st.session_state:
                    del st.session_state["username"]
                if "password" in st.session_state:
                    del st.session_state["password"]
            else:
                st.session_state["password_correct"] = False
        else:
            st.session_state["password_correct"] = False

    # Check if password is already validated
    if "password_correct" not in st.session_state:
        # First run, show login form
        st.markdown("## 🔐 Acceso al Dashboard")
        st.markdown("Por favor, introduce tus credenciales para acceder.")
        st.text_input("Usuario", key="username", autocomplete="username")
        st.text_input("Contraseña", type="password", key="password", autocomplete="current-password")
        st.button("Iniciar Sesión", on_click=password_entered, type="primary")
        return False
    elif not st.session_state["password_correct"]:
        # Password incorrect, show error
        st.markdown("## 🔐 Acceso al Dashboard")
        st.text_input("Usuario", key="username", autocomplete="username")
        st.text_input("Contraseña", type="password", key="password", autocomplete="current-password")
        st.button("Iniciar Sesión", on_click=password_entered, type="primary")
        st.error("😕 Usuario o contraseña incorrectos")
        return False
    else:
        # Password correct
        return True


def calculate_days_on_market(row):

    """Calculate days between first and last seen dates."""
    try:
        first = datetime.fromisoformat(row['first_seen_date'])
        last = datetime.fromisoformat(row['last_seen_date'])
        return (last - first).days
    except:
        return 0


def main():
    # Authentication check
    if not check_password():
        st.stop()
    
    # Initialize database (download from cloud if needed)
    if not download_database_from_cloud():
        st.error("❌ No se pudo cargar la base de datos. Por favor, contacta al administrador.")
        st.stop()
    
    # Header
    st.title("🏠 Madrid Real Estate Tracker")
    st.markdown("**Monitorización diaria del mercado inmobiliario de Madrid**")
    
    # Sidebar filters
    st.sidebar.header("🔍 Filtros")
    
    # Status filter
    status_filter = st.sidebar.radio(
        "Estado",
        options=["active", "sold_removed", "all"],
        format_func=lambda x: {
            "active": "Activos",
            "sold_removed": "Vendidos/Retirados",
            "all": "Todos"
        }[x],
        index=0
    )
    
    status_value = None if status_filter == "all" else status_filter
    
    # District filter
    all_districts = [
        "Centro", "Arganzuela", "Retiro", "Salamanca", "Chamartín",
        "Tetuán", "Chamberí", "Fuencarral-El Pardo", "Moncloa-Aravaca",
        "Latina", "Carabanchel", "Usera", "Puente de Vallecas",
        "Moratalaz", "Ciudad Lineal", "Hortaleza", "Villaverde",
        "Villa de Vallecas", "Vicálvaro", "San Blas-Canillejas", "Barajas"
    ]
    
    selected_districts = st.sidebar.multiselect(
        "Distritos",
        options=all_districts,
        default=[]
    )
    
    # Price range filter
    st.sidebar.subheader("Rango de Precio")
    col1, col2 = st.sidebar.columns(2)
    with col1:
        min_price = st.number_input("Mín (€)", min_value=0, value=0, step=10000)
    with col2:
        max_price = st.number_input("Máx (€)", min_value=0, value=2000000, step=10000)
    
    # Seller type filter
    seller_type = st.sidebar.selectbox(
        "Tipo de Vendedor",
        options=["All", "Particular", "Agencia"]
    )
    
    # Deployment info
    st.sidebar.markdown("---")
    if is_streamlit_cloud():
        st.sidebar.caption("☁️ Deployed on Streamlit Cloud")
    else:
        st.sidebar.caption("💻 Running locally")
    
    # Show logged-in user
    if "current_user" in st.session_state:
        st.sidebar.caption(f"👤 Usuario: {st.session_state['current_user']}")

    
    # Load data
    with st.spinner("Cargando datos..."):
        df = load_data(
            status=status_value,
            distritos=selected_districts,
            min_price=min_price if min_price > 0 else None,
            max_price=max_price if max_price < 2000000 else None,
            seller_type=seller_type
        )
    
    if df.empty:
        st.warning("⚠️ No hay datos disponibles. Ejecuta el scraper primero: `python scraper.py`")
        return
    
    # Calculate derived metrics
    df['price_per_sqm'] = df.apply(
        lambda row: row['price'] / row['size_sqm'] if row['size_sqm'] and row['size_sqm'] > 0 else None,
        axis=1
    )
    df['days_on_market'] = df.apply(calculate_days_on_market, axis=1)
    
    # KPI Section
    st.markdown("---")
    st.subheader("📊 Métricas Principales")
    
    # First row: Mean metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        avg_price = df[df['price'] > 0]['price'].mean() if len(df[df['price'] > 0]) > 0 else 0
        st.metric(
            label="Precio Medio",
            value=f"€{avg_price:,.0f}",
            delta=None
        )
    
    with col2:
        avg_price_sqm = df[df['price_per_sqm'].notna()]['price_per_sqm'].mean() if len(df[df['price_per_sqm'].notna()]) > 0 else 0
        st.metric(
            label="Precio Medio por m²",
            value=f"€{avg_price_sqm:,.0f}",
            delta=None
        )
    
    with col3:
        active_count = len(df[df['status'] == 'active']) if 'status' in df.columns else len(df)
        st.metric(
            label="Inmuebles Activos",
            value=f"{active_count:,}",
            delta=None
        )
    
    with col4:
        sold_30_days = get_sold_last_n_days(30)
        st.metric(
            label="Vendidos (30 días)",
            value=f"{sold_30_days:,}",
            delta=None
        )
    
    # Second row: Median metrics (more robust to outliers)
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        median_price = df[df['price'] > 0]['price'].median() if len(df[df['price'] > 0]) > 0 else 0
        st.metric(
            label="Precio Mediano",
            value=f"€{median_price:,.0f}",
            delta=None,
            help="Mediana: menos sensible a propiedades de lujo"
        )
    
    with col2:
        median_price_sqm = df[df['price_per_sqm'].notna()]['price_per_sqm'].median() if len(df[df['price_per_sqm'].notna()]) > 0 else 0
        st.metric(
            label="Precio Mediano por m²",
            value=f"€{median_price_sqm:,.0f}",
            delta=None,
            help="Mediana: menos sensible a outliers"
        )
    
    with col3:
        # Calculate percentile 25 and 75 for price range
        if len(df[df['price'] > 0]) > 0:
            p25 = df[df['price'] > 0]['price'].quantile(0.25)
            p75 = df[df['price'] > 0]['price'].quantile(0.75)
            st.metric(
                label="Rango Intercuartil",
                value=f"€{p25:,.0f} - €{p75:,.0f}",
                delta=None,
                help="50% central de los precios"
            )
    
    with col4:
        # Total properties in database
        total_props = len(df)
        st.metric(
            label="Total Propiedades",
            value=f"{total_props:,}",
            delta=None
        )
    
    # Top Barrios Section
    st.markdown("---")
    st.subheader("🏘️ Top Barrios por Precio Medio")
    
    # Filter valid barrios
    barrio_df = df[(df['barrio'].notna()) & (df['price'] > 0)].copy()
    
    if not barrio_df.empty:
        barrio_stats = barrio_df.groupby('barrio').agg({
            'price': ['mean', 'median'],
            'listing_id': 'count'
        }).reset_index()
        barrio_stats.columns = ['Barrio', 'Precio Medio', 'Precio Mediano', 'Cantidad']
        barrio_stats = barrio_stats[barrio_stats['Cantidad'] >= 3]  # At least 3 listings
        barrio_stats = barrio_stats.sort_values('Precio Mediano', ascending=False)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**🔝 Top 10 Más Caros (por Mediana)**")
            top_expensive = barrio_stats.head(10).copy()
            top_expensive['Precio Medio'] = top_expensive['Precio Medio'].apply(lambda x: f"€{x:,.0f}")
            top_expensive['Precio Mediano'] = top_expensive['Precio Mediano'].apply(lambda x: f"€{x:,.0f}")
            st.dataframe(
                top_expensive,
                hide_index=True,
                use_container_width=True
            )
        
        with col2:
            st.markdown("**💰 Top 10 Más Baratos (por Mediana)**")
            top_cheap = barrio_stats.tail(10).copy()
            top_cheap['Precio Medio'] = top_cheap['Precio Medio'].apply(lambda x: f"€{x:,.0f}")
            top_cheap['Precio Mediano'] = top_cheap['Precio Mediano'].apply(lambda x: f"€{x:,.0f}")
            st.dataframe(
                top_cheap,
                hide_index=True,
                use_container_width=True
            )
    
    # Visualizations
    st.markdown("---")
    
    # Interactive Map Section
    st.subheader("🗺️ Mapa Interactivo de Propiedades")
    
    # Add coordinates to dataframe
    from coordinates import get_barrio_coordinates
    df['latitude'] = df.apply(
        lambda row: get_barrio_coordinates(row['distrito'], row['barrio'])[0] if pd.notna(row['distrito']) and pd.notna(row['barrio']) else None,
        axis=1
    )
    df['longitude'] = df.apply(
        lambda row: get_barrio_coordinates(row['distrito'], row['barrio'])[1] if pd.notna(row['distrito']) and pd.notna(row['barrio']) else None,
        axis=1
    )
    
    # Map controls
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown("**Visualiza la distribución geográfica de propiedades con mapa de calor de precios**")
    with col2:
        map_limit = st.selectbox(
            "Mostrar",
            options=[100, 500, 1000, "Todos"],
            index=1,
            help="Limitar número de propiedades para mejor rendimiento"
        )
    
    # Limit data for performance
    map_df = df.copy()
    if map_limit != "Todos":
        map_df = map_df.head(map_limit)
    
    # Create and display map
    from map_view import create_property_map
    from streamlit_folium import st_folium
    
    with st.spinner("Cargando mapa..."):
        property_map = create_property_map(map_df)
        st_folium(property_map, width=1200, height=600, returned_objects=[])
    
    # Map legend
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown("🟢 **< 300k€**")
    with col2:
        st.markdown("🔵 **300k-500k€**")
    with col3:
        st.markdown("🟠 **500k-800k€**")
    with col4:
        st.markdown("🔴 **> 800k€**")
    
    st.caption(f"📍 Mostrando {len(map_df):,} de {len(df):,} propiedades | 🔥 Mapa de calor muestra intensidad de precios")
    
    st.markdown("---")

    
    # Seller Type Distribution
    st.markdown("#### Distribución por Tipo de Vendedor")
    seller_df = df[df['seller_type'].notna()].copy()
    
    if not seller_df.empty:
        seller_counts = seller_df['seller_type'].value_counts().reset_index()
        seller_counts.columns = ['Tipo', 'Cantidad']
        
        # Calculate percentages
        total = seller_counts['Cantidad'].sum()
        seller_counts['Porcentaje'] = (seller_counts['Cantidad'] / total * 100).round(1)
        
        fig_pie = px.pie(
            seller_counts,
            values='Cantidad',
            names='Tipo',
            title='Particulares vs Inmobiliarias',
            color_discrete_sequence=['#2ecc71', '#3498db', '#e74c3c']
        )
        fig_pie.update_traces(
            textposition='inside',
            textinfo='percent+label',
            hovertemplate='<b>%{label}</b><br>Cantidad: %{value:,}<br>Porcentaje: %{percent}<extra></extra>'
        )
        fig_pie.update_layout(
            showlegend=True,
            height=400
        )
        st.plotly_chart(fig_pie, use_container_width=True)
        
        # Show detailed stats
        col1, col2 = st.columns(2)
        for idx, row in seller_counts.iterrows():
            with col1 if idx % 2 == 0 else col2:
                st.metric(
                    label=row['Tipo'],
                    value=f"{row['Cantidad']:,} propiedades",
                    delta=f"{row['Porcentaje']}%"
                )
    
    # Price by District - Mean vs Median Comparison
    st.markdown("#### Precio por Distrito: Media vs Mediana")
    distrito_df = df[(df['distrito'].notna()) & (df['price'] > 0)].copy()
    
    if not distrito_df.empty:
        distrito_stats = distrito_df.groupby('distrito')['price'].agg(['mean', 'median']).reset_index()
        distrito_stats.columns = ['distrito', 'Media', 'Mediana']
        distrito_stats = distrito_stats.sort_values('Mediana', ascending=True)
        
        # Create grouped bar chart
        fig_bar = go.Figure()
        
        fig_bar.add_trace(go.Bar(
            name='Precio Medio',
            x=distrito_stats['Media'],
            y=distrito_stats['distrito'],
            orientation='h',
            marker_color='#1f77b4'
        ))
        
        fig_bar.add_trace(go.Bar(
            name='Precio Mediano',
            x=distrito_stats['Mediana'],
            y=distrito_stats['distrito'],
            orientation='h',
            marker_color='#ff7f0e'
        ))
        
        fig_bar.update_layout(
            title='Comparación Precio Medio vs Mediano por Distrito',
            xaxis_title='Precio (€)',
            yaxis_title='Distrito',
            barmode='group',
            xaxis_tickformat=',.0f',
            height=600,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        st.plotly_chart(fig_bar, use_container_width=True)
    
    # Price per m² Evolution
    st.markdown("---")
    st.markdown("#### 📈 Evolución del Precio por m²")
    
    # Filters for evolution chart
    col_period, col_distrito = st.columns([1, 2])
    
    with col_period:
        time_period = st.radio(
            "Período",
            ["Últimos 7 días", "Últimos 30 días", "Todo el período"],
            horizontal=True
        )
    
    with col_distrito:
        distrito_filter = st.selectbox(
            "Distrito",
            ["Todos"] + sorted(df[df['distrito'].notna()]['distrito'].unique().tolist())
        )
    
    # Import analytics function at the beginning
    from analytics import get_price_per_sqm_evolution
    
    # Filter data based on selection
    evolution_df = df[df['status'] == 'active'].copy()
    
    # Apply time filter
    if time_period == "Últimos 7 días":
        cutoff_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        evolution_df = evolution_df[evolution_df['first_seen_date'] >= cutoff_date]
        period_code = 'D'  # Daily
    elif time_period == "Últimos 30 días":
        cutoff_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        evolution_df = evolution_df[evolution_df['first_seen_date'] >= cutoff_date]
        period_code = 'D'  # Daily
    else:
        period_code = 'D'  # Daily for all data
    
    # Get evolution data
    evolution_data = get_price_per_sqm_evolution(
        evolution_df, 
        period=period_code,
        distrito=distrito_filter if distrito_filter != "Todos" else None
    )
    
    if not evolution_data.empty and len(evolution_data) > 1:
        # Calculate trend metrics
        current_avg = evolution_data['avg_price_sqm'].iloc[-1]
        
        # 7-day change
        if len(evolution_data) >= 7:
            week_ago_avg = evolution_data['avg_price_sqm'].iloc[-7]
            change_7d_pct = ((current_avg - week_ago_avg) / week_ago_avg) * 100
            change_7d_eur = current_avg - week_ago_avg
        else:
            change_7d_pct = 0
            change_7d_eur = 0
        
        # 30-day change (if available)
        if len(evolution_data) >= 30:
            month_ago_avg = evolution_data['avg_price_sqm'].iloc[-30]
            change_30d_pct = ((current_avg - month_ago_avg) / month_ago_avg) * 100
            change_30d_eur = current_avg - month_ago_avg
        else:
            change_30d_pct = 0
            change_30d_eur = 0
        
        # Determine trend
        if change_7d_pct > 2:
            trend_icon = "↗️"
            trend_text = "Subiendo"
            trend_color = "#e74c3c"
        elif change_7d_pct < -2:
            trend_icon = "↘️"
            trend_text = "Bajando"
            trend_color = "#27ae60"
        else:
            trend_icon = "→"
            trend_text = "Estable"
            trend_color = "#95a5a6"
        
        # Display metrics
        metric_cols = st.columns(4)
        
        with metric_cols[0]:
            st.metric(
                "Precio Actual (€/m²)",
                f"{current_avg:,.0f} €",
                help="Precio promedio por m² actual"
            )
        
        with metric_cols[1]:
            st.metric(
                "Cambio 7 días",
                f"{change_7d_eur:+,.0f} €",
                f"{change_7d_pct:+.1f}%"
            )
        
        with metric_cols[2]:
            if change_30d_pct != 0:
                st.metric(
                    "Cambio 30 días",
                    f"{change_30d_eur:+,.0f} €",
                    f"{change_30d_pct:+.1f}%"
                )
            else:
                st.metric(
                    "Cambio 30 días",
                    "N/A",
                    help="No hay suficientes datos"
                )
        
        with metric_cols[3]:
            st.metric(
                "Tendencia",
                f"{trend_icon} {trend_text}",
                help="Basado en cambio de 7 días"
            )
        
        # Create line chart
        fig_evolution = go.Figure()
        
        fig_evolution.add_trace(go.Scatter(
            x=evolution_data['date'],
            y=evolution_data['avg_price_sqm'],
            mode='lines+markers',
            name='Precio Medio/m²',
            line=dict(color='#3498db', width=3),
            marker=dict(size=6),
            hovertemplate='<b>%{x|%d/%m/%Y}</b><br>Precio: %{y:,.0f} €/m²<extra></extra>'
        ))
        
        fig_evolution.add_trace(go.Scatter(
            x=evolution_data['date'],
            y=evolution_data['median_price_sqm'],
            mode='lines+markers',
            name='Precio Mediano/m²',
            line=dict(color='#e67e22', width=2, dash='dash'),
            marker=dict(size=4),
            hovertemplate='<b>%{x|%d/%m/%Y}</b><br>Precio: %{y:,.0f} €/m²<extra></extra>'
        ))
        
        # Update layout
        distrito_title = f" - {distrito_filter}" if distrito_filter != "Todos" else ""
        fig_evolution.update_layout(
            title=f'Evolución del Precio por m²{distrito_title}',
            xaxis_title='Fecha',
            yaxis_title='Precio (€/m²)',
            hovermode='x unified',
            height=450,
            showlegend=True,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            xaxis=dict(
                showgrid=True,
                gridcolor='rgba(128, 128, 128, 0.2)'
            ),
            yaxis=dict(
                showgrid=True,
                gridcolor='rgba(128, 128, 128, 0.2)',
                tickformat=',.0f'
            )
        )
        
        st.plotly_chart(fig_evolution, use_container_width=True)
        
        # Additional info
        st.info(f"📊 Mostrando datos de {len(evolution_data)} días con un total de {evolution_data['count'].sum():,.0f} propiedades analizadas")
    else:
        st.warning("No hay suficientes datos para mostrar la evolución temporal. Se necesitan al menos 2 días de datos.")
    
    # Advanced Analytics Section
    st.markdown("---")
    st.subheader("📊 Análisis Avanzado")
    
    # Import analytics module
    from analytics import (
        get_velocity_metrics,
        get_price_trends,
        rank_opportunities,
        identify_bargains,
        get_new_vs_sold_trends
    )
    
    # Create tabs for different analytics
    analytics_tab1, analytics_tab2, analytics_tab3 = st.tabs([
        "📈 Tendencias Temporales",
        "🎯 Mejores Oportunidades",
        "💎 Gangas (Precio/m²)"
    ])
    
    with analytics_tab1:
        st.markdown("### Métricas de Velocidad del Mercado")
        
        # Calculate velocity metrics
        velocity = get_velocity_metrics(df)
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Días en Mercado (Media)",
                f"{velocity['avg_days_on_market']:.0f}",
                help="Promedio de días que las propiedades están activas"
            )
        
        with col2:
            st.metric(
                "Días en Mercado (Mediana)",
                f"{velocity['median_days_on_market']:.0f}",
                help="Mediana de días en mercado (menos afectada por outliers)"
            )
        
        with col3:
            st.metric(
                "Nuevos (7 días)",
                f"{velocity['new_last_7_days']:,}",
                help="Propiedades nuevas en los últimos 7 días"
            )
        
        with col4:
            st.metric(
                "Vendidos (7 días)",
                f"{velocity['sold_last_7_days']:,}",
                help="Propiedades vendidas en los últimos 7 días"
            )
        
        st.markdown("---")
        
        # New vs Sold Trends
        st.markdown("### Nuevos vs Vendidos (Últimos 30 días)")
        
        trends_data = get_new_vs_sold_trends(df, days=30)
        
        if trends_data['dates']:
            # go is already imported at top of file
            
            fig_trends = go.Figure()

            
            fig_trends.add_trace(go.Scatter(
                x=trends_data['dates'],
                y=trends_data['new'],
                name='Nuevos',
                mode='lines+markers',
                line=dict(color='#2ecc71', width=2),
                marker=dict(size=6)
            ))
            
            fig_trends.add_trace(go.Scatter(
                x=trends_data['dates'],
                y=trends_data['sold'],
                name='Vendidos',
                mode='lines+markers',
                line=dict(color='#e74c3c', width=2),
                marker=dict(size=6)
            ))
            
            fig_trends.update_layout(
                xaxis_title='Fecha',
                yaxis_title='Cantidad',
                hovermode='x unified',
                height=400
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
        
        # Rank opportunities
        df_ranked = rank_opportunities(df[df['status'] == 'active'])
        
        if not df_ranked.empty:
            top_opportunities = df_ranked.head(20)
            
            # Display as cards
            for idx, row in top_opportunities.iterrows():
                score = row['quality_score']
                
                # Determine score color
                if score >= 80:
                    score_color = "🟢"
                    score_label = "Excelente"
                elif score >= 70:
                    score_color = "🔵"
                    score_label = "Muy Bueno"
                elif score >= 60:
                    score_color = "🟡"
                    score_label = "Bueno"
                else:
                    score_color = "🟠"
                    score_label = "Regular"
                
                with st.expander(f"{score_color} **{score:.0f}/100** ({score_label}) - {row['title'][:70]}..."):
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.metric("💰 Precio", f"€{row['price']:,}")
                        st.metric("📐 Tamaño", f"{row['size_sqm']:.0f} m²" if pd.notna(row['size_sqm']) else "N/A")
                        st.metric("🛏️ Habitaciones", int(row['rooms']) if pd.notna(row['rooms']) else "N/A")
                    
                    with col2:
                        st.metric("💵 €/m²", f"€{row['price_per_sqm']:,.0f}" if pd.notna(row['price_per_sqm']) else "N/A")
                        st.metric("📊 vs Distrito", f"{row['vs_distrito_avg']:+.1f}%" if pd.notna(row['vs_distrito_avg']) else "N/A")
                        st.metric("⏱️ Días en Mercado", f"{row['days_on_market']:.0f}" if pd.notna(row['days_on_market']) else "N/A")
                    
                    with col3:
                        st.metric("📍 Distrito", row['distrito'])
                        st.metric("🏘️ Barrio", row['barrio'])
                        st.metric("👤 Vendedor", row['seller_type'])
                        st.metric("🌅 Orientación", row['orientation'] if pd.notna(row['orientation']) else "N/A")
                    
                    st.markdown(f"[🔗 Ver en Idealista]({row['url']})")
        else:
            st.warning("No hay propiedades activas para analizar")
    
    with analytics_tab3:
        st.markdown("### Gangas: Propiedades con Mejor Precio/m²")
        
        st.info("""
        Propiedades con precio/m² **15% o más por debajo** del promedio de su distrito.
        Estas pueden representar buenas oportunidades de compra.
        """)
        
        # Identify bargains
        bargains = identify_bargains(df[df['status'] == 'active'], threshold=-15.0)
        
        if not bargains.empty:
            st.success(f"✨ Encontradas {len(bargains)} gangas potenciales")
            
            # Display table
            bargains_display = bargains[[
                'title', 'price', 'price_per_sqm', 'vs_distrito_avg', 
                'distrito', 'barrio', 'rooms', 'size_sqm', 'quality_score'
            ]].copy()
            
            bargains_display.columns = [
                'Título', 'Precio', '€/m²', '% vs Distrito', 
                'Distrito', 'Barrio', 'Hab.', 'm²', 'Score'
            ]
            
            # Format columns
            bargains_display['Precio'] = bargains_display['Precio'].apply(lambda x: f"€{x:,.0f}")
            bargains_display['€/m²'] = bargains_display['€/m²'].apply(lambda x: f"€{x:,.0f}" if pd.notna(x) else "N/A")
            bargains_display['% vs Distrito'] = bargains_display['% vs Distrito'].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "N/A")
            bargains_display['Score'] = bargains_display['Score'].apply(lambda x: f"{x:.0f}")
            bargains_display['m²'] = bargains_display['m²'].apply(lambda x: f"{x:.0f}" if pd.notna(x) else "N/A")
            
            st.dataframe(
                bargains_display,
                hide_index=True,
                use_container_width=True,
                height=400
            )
            
            # Download button
            csv = bargains.to_csv(index=False)
            st.download_button(
                label="📥 Descargar Gangas (CSV)",
                data=csv,
                file_name=f"gangas_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
        else:
            st.warning("No se encontraron gangas con el criterio actual (15% por debajo del promedio)")
    
    # Time to Sale Analysis (Metric 1)
    st.markdown("#### ⏱️ Tiempo Medio de Venta por Distrito")
    sold_df = df[(df['status'] == 'sold_removed') & (df['distrito'].notna())].copy()
    
    # Filter out initial historical data (properties that were already gone on first scrape)
    # Only include properties first seen AFTER 2026-01-14 (real sales during tracking period)
    sold_df = sold_df[sold_df['first_seen_date'] > '2026-01-14'].copy()
    
    if not sold_df.empty and len(sold_df) > 10:
        # Calculate days on market for sold properties
        sold_df['days_on_market_calc'] = sold_df.apply(
            lambda row: (
                pd.to_datetime(row['last_seen_date']) - pd.to_datetime(row['first_seen_date'])
            ).days if pd.notna(row['last_seen_date']) and pd.notna(row['first_seen_date']) else 0,
            axis=1
        )
        
        # Group by district
        time_to_sale = sold_df.groupby('distrito').agg({
            'days_on_market_calc': ['mean', 'median', 'count']
        }).reset_index()
        time_to_sale.columns = ['distrito', 'media_dias', 'mediana_dias', 'cantidad']
        
        # Filter districts with at least 3 sales
        time_to_sale = time_to_sale[time_to_sale['cantidad'] >= 3]
        time_to_sale = time_to_sale.sort_values('mediana_dias', ascending=True)
        
        if not time_to_sale.empty:
            fig_time = go.Figure()
            
            fig_time.add_trace(go.Bar(
                name='Tiempo Medio',
                x=time_to_sale['media_dias'],
                y=time_to_sale['distrito'],
                orientation='h',
                marker_color='#e74c3c',
                text=time_to_sale['media_dias'].round(1),
                textposition='outside'
            ))
            
            fig_time.add_trace(go.Bar(
                name='Tiempo Mediano',
                x=time_to_sale['mediana_dias'],
                y=time_to_sale['distrito'],
                orientation='h',
                marker_color='#3498db',
                text=time_to_sale['mediana_dias'].round(1),
                textposition='outside'
            ))
            
            fig_time.update_layout(
                title='Días en Mercado hasta Venta (Propiedades Vendidas)',
                xaxis_title='Días',
                yaxis_title='Distrito',
                barmode='group',
                height=600,
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1
                )
            )
            st.plotly_chart(fig_time, use_container_width=True)
            
            # Show summary stats
            avg_time = sold_df['days_on_market_calc'].mean()
            median_time = sold_df['days_on_market_calc'].median()
            st.caption(f"📊 Tiempo medio global: {avg_time:.1f} días | Tiempo mediano: {median_time:.1f} días | Total vendidas: {len(sold_df):,}")
        else:
            st.info("ℹ️ No hay suficientes datos de ventas por distrito aún. Necesitas al menos 3 ventas por distrito.")
    else:
        st.info("ℹ️ Aún no hay suficientes propiedades vendidas para calcular el tiempo medio de venta. Esta métrica mejorará con más días de datos.")
    
    # Price Trend Analysis (Metric 2)
    st.markdown("#### 📉 Zonas con Precios en Descenso")
    
    try:
        # Get price trends by distrito
        price_trends = get_price_trends_by_zone(zone_type='distrito', min_properties=50)
        
        if price_trends and len(price_trends) > 0:
            # Convert to DataFrame
            trends_df = pd.DataFrame(price_trends)
            
            # Filter only zones with price decreases
            decreasing_df = trends_df[trends_df['price_change_pct'] < 0].copy()
            
            if not decreasing_df.empty:
                # Sort by price change percentage (most negative first)
                decreasing_df = decreasing_df.sort_values('price_change_pct', ascending=True)
                
                # Create visualization
                fig_trends = go.Figure()
                
                fig_trends.add_trace(go.Bar(
                    x=decreasing_df['price_change_pct'],
                    y=decreasing_df['zone'],
                    orientation='h',
                    marker_color='#e74c3c',
                    text=decreasing_df['price_change_pct'].apply(lambda x: f'{x:.1f}%'),
                    textposition='outside',
                    hovertemplate='<b>%{y}</b><br>' +
                                  'Cambio: %{x:.2f}%<br>' +
                                  '<extra></extra>'
                ))
                
                fig_trends.update_layout(
                    title='Distritos con Mayor Descenso de Precios',
                    xaxis_title='Cambio de Precio (%)',
                    yaxis_title='Distrito',
                    height=max(400, len(decreasing_df) * 40),
                    showlegend=False
                )
                
                st.plotly_chart(fig_trends, use_container_width=True)
                
                # Detailed table
                st.markdown("**📊 Detalle de Cambios de Precio**")
                
                display_trends = decreasing_df.copy()
                display_trends['Precio Inicial'] = display_trends['first_avg_price'].apply(lambda x: f"€{x:,.0f}")
                display_trends['Precio Actual'] = display_trends['last_avg_price'].apply(lambda x: f"€{x:,.0f}")
                display_trends['Cambio €'] = display_trends['price_change'].apply(lambda x: f"€{x:,.0f}")
                display_trends['Cambio %'] = display_trends['price_change_pct'].apply(lambda x: f"{x:.2f}%")
                display_trends['Propiedades'] = display_trends['property_count']
                
                st.dataframe(
                    display_trends[['zone', 'Precio Inicial', 'Precio Actual', 'Cambio €', 'Cambio %', 'Propiedades']].rename(columns={'zone': 'Distrito'}),
                    hide_index=True,
                    use_container_width=True
                )
                
                # Summary stats
                avg_decrease = decreasing_df['price_change_pct'].mean()
                max_decrease = decreasing_df['price_change_pct'].min()
                st.caption(f"📉 Descenso promedio: {avg_decrease:.2f}% | Máximo descenso: {max_decrease:.2f}%")
                
            else:
                st.success("✅ ¡Buenas noticias! No hay distritos con descenso de precios significativo en este período.")
                
                # Show zones with price increases
                increasing_df = trends_df[trends_df['price_change_pct'] > 0].copy()
                if not increasing_df.empty:
                    increasing_df = increasing_df.sort_values('price_change_pct', ascending=False).head(5)
                    st.markdown("**📈 Top 5 Distritos con Mayor Aumento de Precios:**")
                    for _, row in increasing_df.iterrows():
                        st.write(f"- **{row['zone']}**: +{row['price_change_pct']:.2f}% (€{row['price_change']:,.0f})")
        else:
            st.info("ℹ️ No hay suficientes datos para calcular tendencias de precios. Necesitas al menos 50 propiedades por distrito con datos históricos.")
            
    except Exception as e:
        st.error(f"⚠️ Error al calcular tendencias de precios: {str(e)}")
        st.info("ℹ️ Esta métrica requiere datos históricos de múltiples días. Asegúrate de ejecutar el scraper diariamente.")
    
    # Days on Market by District (for sold properties)
    sold_df = df[(df['status'] == 'sold_removed') & (df['distrito'].notna())].copy()
    
    if not sold_df.empty and len(sold_df) > 10:
        st.markdown("#### Tiempo Medio en Mercado por Distrito (Propiedades Vendidas)")
        
        market_time = sold_df.groupby('distrito')['days_on_market'].mean().reset_index()
        market_time = market_time.sort_values('days_on_market', ascending=True)
        
        fig_market = px.bar(
            market_time,
            x='days_on_market',
            y='distrito',
            orientation='h',
            title='Días Promedio en Mercado hasta Venta',
            labels={'days_on_market': 'Días', 'distrito': 'Distrito'},
            color='days_on_market',
            color_continuous_scale='RdYlGn_r'
        )
        fig_market.update_layout(
            showlegend=False,
            height=600
        )
        st.plotly_chart(fig_market, use_container_width=True)
    
    # Price Evolution by District
    st.markdown("---")
    
    # ============================================================================
    # PRICE HISTORY SECTIONS
    # ============================================================================
    
    
    # ============================================================================
    # PROPERTY LOOKUP SECTION
    # ============================================================================
    
    st.markdown("---")
    st.subheader("🔍 Buscar Propiedad")
    
    st.markdown("""
    Introduce la URL de Idealista o el ID del piso para ver su histórico de precios completo.
    """)
    
    # Import database function
    from database import get_listing_by_url, get_property_price_stats
    from analytics import get_property_evolution_dataframe
    
    # Search input
    col1, col2 = st.columns([4, 1])
    
    with col1:
        search_input = st.text_input(
            "URL o ID",
            placeholder="https://www.idealista.com/inmueble/110506346/ o 110506346",
            label_visibility="collapsed",
            key="property_search_input"
        )
    
    with col2:
        search_button = st.button("🔍 Buscar", use_container_width=True, type="primary")
    
    # Process search
    if search_button and search_input:
        with st.spinner("Buscando propiedad..."):
            listing = get_listing_by_url(search_input)
            
            if listing:
                st.success(f"✅ Propiedad encontrada: {listing['listing_id']}")
                
                # Property Card
                st.markdown("### 📍 Detalles de la Propiedad")
                
                # Status badge
                status_emoji = "✅" if listing['status'] == 'active' else "❌"
                status_text = "Activo" if listing['status'] == 'active' else "Vendido/Retirado"
                status_color = "green" if listing['status'] == 'active' else "red"
                
                st.markdown(f"""
                <div style="background-color: #f0f2f6; padding: 20px; border-radius: 10px; margin-bottom: 20px;">
                    <h4 style="margin-top: 0;">{listing['title']}</h4>
                    <p style="color: #666; margin: 5px 0;">
                        📍 {listing['distrito']} - {listing['barrio']}
                    </p>
                    <p style="font-size: 24px; color: #1f77b4; margin: 10px 0;">
                        💶 {listing['price']:,}€
                    </p>
                    <p style="margin: 5px 0;">
                        📐 {listing['size_sqm']:.0f if listing['size_sqm'] else 'N/A'} m² • 
                        🛏️ {listing['rooms'] if listing['rooms'] else 'N/A'} hab • 
                        🏢 Piso {listing['floor'] if listing['floor'] else 'N/A'}
                    </p>
                    <p style="margin: 10px 0;">
                        <span style="background-color: {status_color}; color: white; padding: 5px 10px; border-radius: 5px;">
                            {status_emoji} {status_text}
                        </span>
                    </p>
                    <p style="margin: 10px 0;">
                        <a href="{listing['url']}" target="_blank" style="color: #1f77b4; text-decoration: none;">
                            🔗 Ver en Idealista →
                        </a>
                    </p>
                </div>
                """, unsafe_allow_html=True)
                
                # Get price history
                evolution_df = get_property_evolution_dataframe(listing['listing_id'])
                
                if not evolution_df.empty:
                    # Get statistics
                    stats = get_property_price_stats(listing['listing_id'])
                    
                    # Statistics Panel
                    st.markdown("### 📊 Estadísticas de Precio")
                    
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.metric(
                            "Precio Inicial",
                            f"€{stats['initial_price']:,}",
                            help="Primer precio registrado"
                        )
                    
                    with col2:
                        st.metric(
                            "Precio Actual",
                            f"€{stats['current_price']:,}",
                            help="Último precio registrado"
                        )
                    
                    with col3:
                        change_color = "inverse" if stats['total_change'] < 0 else "normal"
                        st.metric(
                            "Cambio Total",
                            f"€{abs(stats['total_change']):,}",
                            f"{stats['total_change_pct']:+.1f}%",
                            delta_color=change_color,
                            help="Diferencia entre precio inicial y actual"
                        )
                    
                    with col4:
                        st.metric(
                            "Cambios de Precio",
                            f"{stats['num_changes']}",
                            help="Número de veces que cambió el precio"
                        )
                    
                    # Additional stats
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.metric(
                            "Primera Vista",
                            listing['first_seen_date'],
                            help="Primera vez que se detectó esta propiedad"
                        )
                    
                    with col2:
                        st.metric(
                            "Última Vista",
                            listing['last_seen_date'],
                            help="Última vez que se vio activa"
                        )
                    
                    with col3:
                        if stats['avg_days_between_changes']:
                            st.metric(
                                "Días entre Cambios",
                                f"{stats['avg_days_between_changes']:.0f}",
                                help="Promedio de días entre cambios de precio"
                            )
                    
                    st.markdown("---")
                    
                    # Price Evolution Chart
                    st.markdown("### 📈 Evolución del Precio")
                    
                    fig_evolution = go.Figure()
                    
                    fig_evolution.add_trace(go.Scatter(
                        x=evolution_df['date_recorded'],
                        y=evolution_df['price'],
                        mode='lines+markers',
                        name='Precio',
                        line=dict(color='#3498db', width=3),
                        marker=dict(size=12),
                        text=[f"€{p:,}" for p in evolution_df['price']],
                        hovertemplate='<b>%{x}</b><br>Precio: %{text}<extra></extra>'
                    ))
                    
                    # Add annotations for price changes
                    for idx, row in evolution_df.iterrows():
                        if pd.notna(row['change_amount']) and row['change_amount'] != 0:
                            color = '#e74c3c' if row['change_amount'] < 0 else '#2ecc71'
                            symbol = '▼' if row['change_amount'] < 0 else '▲'
                            
                            fig_evolution.add_annotation(
                                x=row['date_recorded'],
                                y=row['price'],
                                text=f"{symbol} {abs(row['change_percent']):.1f}%",
                                showarrow=True,
                                arrowhead=2,
                                arrowcolor=color,
                                font=dict(color=color, size=12, family="Arial Black"),
                                bgcolor='white',
                                bordercolor=color,
                                borderwidth=2,
                                borderpad=4
                            )
                    
                    fig_evolution.update_layout(
                        title=f"Histórico de Precios - {listing['title'][:50]}...",
                        xaxis_title="Fecha",
                        yaxis_title="Precio (€)",
                        hovermode='x unified',
                        height=500,
                        showlegend=False
                    )
                    
                    st.plotly_chart(fig_evolution, use_container_width=True)
                    
                    # Detailed History Table
                    st.markdown("### 📋 Historial Detallado")
                    
                    history_display = evolution_df[['date_recorded', 'price', 'change_amount', 'change_percent']].copy()
                    history_display.columns = ['Fecha', 'Precio', 'Cambio (€)', 'Cambio (%)']
                    history_display['Precio'] = history_display['Precio'].apply(lambda x: f"€{x:,}")
                    history_display['Cambio (€)'] = history_display['Cambio (€)'].apply(
                        lambda x: f"{x:+,.0f}€" if pd.notna(x) else "Inicial"
                    )
                    history_display['Cambio (%)'] = history_display['Cambio (%)'].apply(
                        lambda x: f"{x:+.1f}%" if pd.notna(x) else "-"
                    )
                    
                    st.dataframe(
                        history_display,
                        hide_index=True,
                        use_container_width=True,
                        height=300
                    )
                    
                    # Download button
                    csv = evolution_df.to_csv(index=False)
                    st.download_button(
                        label="📥 Descargar Histórico (CSV)",
                        data=csv,
                        file_name=f"historico_{listing['listing_id']}_{datetime.now().strftime('%Y%m%d')}.csv",
                        mime="text/csv"
                    )
                    
                else:
                    st.info("📊 Esta propiedad aún no tiene cambios de precio registrados.")
                    st.caption("El histórico se irá poblando conforme el scraper detecte cambios.")
                
            else:
                st.error("❌ Propiedad no encontrada")
                st.info("""
                **Posibles razones:**
                - El ID o URL no es válido
                - La propiedad no ha sido scrapeada aún
                - El formato de la URL es incorrecto
                
                **Formatos válidos:**
                - URL completa: `https://www.idealista.com/inmueble/110506346/`
                - Solo ID: `110506346`
                """)
    
    elif search_button and not search_input:
        st.warning("⚠️ Por favor, introduce una URL o ID para buscar.")
    st.markdown("---")
    st.subheader("💰 Histórico de Precios")
    
    # Import price history analytics
    from analytics import (
        get_price_drops_dataframe,
        get_property_evolution_dataframe,
        get_desperate_sellers_dataframe,
        get_price_history_summary
    )
    
    # Get summary stats
    try:
        history_summary = get_price_history_summary()
        
        # Display summary metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Propiedades Rastreadas",
                f"{history_summary['total_records']:,}",
                help="Total de registros en histórico de precios"
            )
        
        with col2:
            st.metric(
                "Con Cambios de Precio",
                f"{history_summary['properties_with_changes']:,}",
                help="Propiedades que han cambiado de precio"
            )
        
        with col3:
            st.metric(
                "Bajadas de Precio",
                f"{history_summary['price_drops']:,}",
                f"{history_summary['avg_drop_percent']:.1f}% promedio",
                delta_color="inverse",
                help="Total de bajadas de precio detectadas"
            )
        
        with col4:
            st.metric(
                "Subidas de Precio",
                f"{history_summary['price_increases']:,}",
                f"+{history_summary['avg_increase_percent']:.1f}% promedio",
                help="Total de subidas de precio detectadas"
            )
    except Exception as e:
        st.info("📊 El histórico de precios se irá poblando conforme el scraper detecte cambios.")
    
    st.markdown("---")
    
    # Create tabs for price history sections
    price_tab1, price_tab2, price_tab3 = st.tabs([
        "📉 Bajadas Recientes",
        "📊 Evolución por Propiedad",
        "🎯 Vendedores Desesperados"
    ])
    
    # TAB 1: Recent Price Drops
    with price_tab1:
        st.markdown("### Bajadas de Precio Recientes")
        
        col1, col2 = st.columns(2)
        
        with col1:
            days_filter = st.selectbox(
                "Período",
                options=[7, 14, 30],
                format_func=lambda x: f"Últimos {x} días",
                key="price_drops_days"
            )
        
        with col2:
            min_drop = st.slider(
                "Bajada Mínima (%)",
                min_value=1.0,
                max_value=30.0,
                value=5.0,
                step=1.0,
                key="price_drops_min"
            )
        
        # Get price drops
        drops_df = get_price_drops_dataframe(days=days_filter, min_drop_percent=min_drop)
        
        if not drops_df.empty:
            st.success(f"✅ Encontradas {len(drops_df)} propiedades con bajadas de precio")
            
            # Summary metrics
            col1, col2, col3 = st.columns(3)
            
            with col1:
                avg_drop = abs(drops_df['change_percent'].mean())
                st.metric("Bajada Promedio", f"{avg_drop:.1f}%")
            
            with col2:
                max_drop = abs(drops_df['change_percent'].min())
                st.metric("Mayor Bajada", f"{max_drop:.1f}%")
            
            with col3:
                total_savings = abs(drops_df['change_amount'].sum())
                st.metric("Ahorro Total", f"€{total_savings:,.0f}")
            
            st.markdown("---")
            
            # Display table
            display_df = drops_df[[
                'title', 'distrito', 'barrio', 'old_price', 'new_price', 
                'change_amount', 'change_percent', 'date_recorded', 'rooms', 'size_sqm'
            ]].copy()
            
            display_df.columns = [
                'Título', 'Distrito', 'Barrio', 'Precio Anterior', 'Precio Actual',
                'Bajada (€)', 'Bajada (%)', 'Fecha', 'Hab.', 'm²'
            ]
            
            # Format columns
            display_df['Precio Anterior'] = display_df['Precio Anterior'].apply(lambda x: f"€{x:,.0f}")
            display_df['Precio Actual'] = display_df['Precio Actual'].apply(lambda x: f"€{x:,.0f}")
            display_df['Bajada (€)'] = display_df['Bajada (€)'].apply(lambda x: f"€{abs(x):,.0f}")
            display_df['Bajada (%)'] = display_df['Bajada (%)'].apply(lambda x: f"{abs(x):.1f}%")
            display_df['m²'] = display_df['m²'].apply(lambda x: f"{x:.0f}" if pd.notna(x) else "N/A")
            
            st.dataframe(
                display_df,
                hide_index=True,
                use_container_width=True,
                height=400
            )
            
            # Download button
            csv = drops_df.to_csv(index=False)
            st.download_button(
                label="📥 Descargar Bajadas (CSV)",
                data=csv,
                file_name=f"bajadas_precio_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
        else:
            st.info(f"No se encontraron bajadas de precio ≥{min_drop}% en los últimos {days_filter} días.")
            st.caption("💡 Tip: Reduce el porcentaje mínimo o aumenta el período de tiempo.")
    
    # TAB 2: Property Evolution
    with price_tab2:
        st.markdown("### Evolución de Precio por Propiedad")
        
        # Get list of properties with price changes
        from database import get_connection
        
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT DISTINCT l.listing_id, l.title, l.distrito, l.price
                FROM listings l
                JOIN price_history ph ON l.listing_id = ph.listing_id
                WHERE l.status = 'active'
                AND ph.change_amount IS NOT NULL
                ORDER BY l.title
                LIMIT 100
            """)
            properties_with_changes = cursor.fetchall()
        
        if properties_with_changes:
            # Create property selector
            property_options = {
                f"{title[:50]}... ({distrito}) - €{price:,.0f}": listing_id
                for listing_id, title, distrito, price in properties_with_changes
            }
            
            selected_property_label = st.selectbox(
                "Selecciona una propiedad",
                options=list(property_options.keys()),
                key="property_evolution_selector"
            )
            
            selected_listing_id = property_options[selected_property_label]
            
            # Get evolution data
            evolution_df = get_property_evolution_dataframe(selected_listing_id)
            
            if not evolution_df.empty:
                # Get property stats
                from database import get_property_price_stats
                stats = get_property_price_stats(selected_listing_id)
                
                # Display stats
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Precio Inicial", f"€{stats['initial_price']:,.0f}")
                
                with col2:
                    st.metric("Precio Actual", f"€{stats['current_price']:,.0f}")
                
                with col3:
                    change_color = "inverse" if stats['total_change'] < 0 else "normal"
                    st.metric(
                        "Cambio Total",
                        f"€{abs(stats['total_change']):,.0f}",
                        f"{stats['total_change_pct']:+.1f}%",
                        delta_color=change_color
                    )
                
                with col4:
                    st.metric("Cambios de Precio", f"{stats['num_changes']}")
                
                st.markdown("---")
                
                # Create evolution chart
                fig_evolution = go.Figure()
                
                fig_evolution.add_trace(go.Scatter(
                    x=evolution_df['date_recorded'],
                    y=evolution_df['price'],
                    mode='lines+markers',
                    name='Precio',
                    line=dict(color='#3498db', width=3),
                    marker=dict(size=10),
                    text=[f"€{p:,.0f}" for p in evolution_df['price']],
                    hovertemplate='<b>%{x}</b><br>Precio: %{text}<extra></extra>'
                ))
                
                # Add annotations for price changes
                for idx, row in evolution_df.iterrows():
                    if pd.notna(row['change_amount']) and row['change_amount'] != 0:
                        color = '#e74c3c' if row['change_amount'] < 0 else '#2ecc71'
                        symbol = '▼' if row['change_amount'] < 0 else '▲'
                        
                        fig_evolution.add_annotation(
                            x=row['date_recorded'],
                            y=row['price'],
                            text=f"{symbol} {abs(row['change_percent']):.1f}%",
                            showarrow=True,
                            arrowhead=2,
                            arrowcolor=color,
                            font=dict(color=color, size=10),
                            bgcolor='white',
                            bordercolor=color,
                            borderwidth=1
                        )
                
                fig_evolution.update_layout(
                    title="Evolución del Precio",
                    xaxis_title="Fecha",
                    yaxis_title="Precio (€)",
                    hovermode='x unified',
                    height=500
                )
                
                st.plotly_chart(fig_evolution, use_container_width=True)
                
                # Show detailed history table
                st.markdown("#### Historial Detallado")
                
                history_display = evolution_df[['date_recorded', 'price', 'change_amount', 'change_percent']].copy()
                history_display.columns = ['Fecha', 'Precio', 'Cambio (€)', 'Cambio (%)']
                history_display['Precio'] = history_display['Precio'].apply(lambda x: f"€{x:,.0f}")
                history_display['Cambio (€)'] = history_display['Cambio (€)'].apply(
                    lambda x: f"{x:+,.0f}€" if pd.notna(x) else "Inicial"
                )
                history_display['Cambio (%)'] = history_display['Cambio (%)'].apply(
                    lambda x: f"{x:+.1f}%" if pd.notna(x) else "-"
                )
                
                st.dataframe(
                    history_display,
                    hide_index=True,
                    use_container_width=True
                )
        else:
            st.info("📊 Aún no hay propiedades con cambios de precio registrados.")
            st.caption("El histórico se irá poblando conforme el scraper detecte cambios.")
    
    # TAB 3: Desperate Sellers
    with price_tab3:
        st.markdown("### Vendedores Desesperados (Múltiples Bajadas)")
        
        col1, col2 = st.columns(2)
        
        with col1:
            min_drops_filter = st.slider(
                "Mínimo de Bajadas",
                min_value=2,
                max_value=5,
                value=2,
                step=1,
                key="desperate_min_drops"
            )
        
        with col2:
            min_total_drop_filter = st.slider(
                "Bajada Total Mínima (%)",
                min_value=5.0,
                max_value=30.0,
                value=10.0,
                step=5.0,
                key="desperate_min_total"
            )
        
        # Get desperate sellers
        desperate_df = get_desperate_sellers_dataframe(
            min_drops=min_drops_filter,
            min_total_drop_pct=min_total_drop_filter
        )
        
        if not desperate_df.empty:
            st.success(f"✅ Encontradas {len(desperate_df)} propiedades con múltiples bajadas")
            
            st.markdown("#### Top Oportunidades (por Score de Urgencia)")
            
            # Display table
            display_df = desperate_df[[
                'title', 'distrito', 'barrio', 'initial_price', 'current_price',
                'total_drop', 'total_drop_pct', 'num_drops', 'urgency_score',
                'rooms', 'size_sqm'
            ]].head(20).copy()
            
            display_df.columns = [
                'Título', 'Distrito', 'Barrio', 'Precio Inicial', 'Precio Actual',
                'Bajada Total (€)', 'Bajada Total (%)', 'Nº Bajadas', 'Score Urgencia',
                'Hab.', 'm²'
            ]
            
            # Format columns
            display_df['Precio Inicial'] = display_df['Precio Inicial'].apply(lambda x: f"€{x:,.0f}")
            display_df['Precio Actual'] = display_df['Precio Actual'].apply(lambda x: f"€{x:,.0f}")
            display_df['Bajada Total (€)'] = display_df['Bajada Total (€)'].apply(lambda x: f"€{abs(x):,.0f}")
            display_df['Bajada Total (%)'] = display_df['Bajada Total (%)'].apply(lambda x: f"{abs(x):.1f}%")
            display_df['m²'] = display_df['m²'].apply(lambda x: f"{x:.0f}" if pd.notna(x) else "N/A")
            
            st.dataframe(
                display_df,
                hide_index=True,
                use_container_width=True,
                height=500
            )
            
            st.caption("💡 **Score de Urgencia** = Bajada Total (%) × Número de Bajadas")
            st.caption("Un score alto indica un vendedor muy motivado para cerrar la venta.")
            
            # Download button
            csv = desperate_df.to_csv(index=False)
            st.download_button(
                label="📥 Descargar Vendedores Desesperados (CSV)",
                data=csv,
                file_name=f"vendedores_desesperados_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
        else:
            st.info(f"No se encontraron propiedades con ≥{min_drops_filter} bajadas y ≥{min_total_drop_filter}% de bajada total.")
            st.caption("💡 Tip: Reduce los filtros para ver más resultados.")
    st.markdown("#### 📈 Evolución de Precios por Distrito")
    
    if 'first_seen_date' in df.columns:
        # Prepare data for price evolution
        df['date'] = pd.to_datetime(df['first_seen_date'], errors='coerce')
        evolution_df = df[(df['date'].notna()) & (df['price'] > 0) & (df['distrito'].notna())].copy()
        
        if not evolution_df.empty and len(evolution_df) > 50:
            # Calculate daily average price by distrito
            daily_prices = evolution_df.groupby(['date', 'distrito'])['price'].mean().reset_index()
            daily_prices = daily_prices.sort_values('date')
            
            # Get top distritos by number of properties
            top_distritos = evolution_df['distrito'].value_counts().head(8).index.tolist()
            daily_prices_filtered = daily_prices[daily_prices['distrito'].isin(top_distritos)]
            
            if not daily_prices_filtered.empty:
                fig_evolution = px.line(
                    daily_prices_filtered,
                    x='date',
                    y='price',
                    color='distrito',
                    title='Evolución del Precio Medio por Distrito (Top 8)',
                    labels={'date': 'Fecha', 'price': 'Precio Medio (€)', 'distrito': 'Distrito'},
                    markers=True
                )
                fig_evolution.update_layout(
                    yaxis_tickformat=',.0f',
                    height=500,
                    hovermode='x unified',
                    legend=dict(
                        orientation="v",
                        yanchor="top",
                        y=1,
                        xanchor="left",
                        x=1.02
                    )
                )
                st.plotly_chart(fig_evolution, use_container_width=True)
                
                # Show summary stats
                latest_date = daily_prices_filtered['date'].max()
                earliest_date = daily_prices_filtered['date'].min()
                days_tracked = (latest_date - earliest_date).days + 1
                st.caption(f"📊 Periodo analizado: {earliest_date.strftime('%Y-%m-%d')} a {latest_date.strftime('%Y-%m-%d')} ({days_tracked} días)")
            else:
                st.info("ℹ️ No hay suficientes datos para mostrar la evolución de precios por distrito")
        else:
            st.info("ℹ️ Necesitas más días de datos para ver la evolución temporal (mínimo 50 propiedades)")
    
    # Chollos Detection
    st.markdown("---")
    st.markdown("#### 💎 Chollos por Barrio")
    st.caption("Propiedades con precio/m² significativamente inferior a la media del barrio")
    
    # Calculate chollos
    chollos_df = df[(df['status'] == 'active') & (df['price'] > 0) & (df['size_sqm'] > 0) & (df['barrio'].notna())].copy()
    
    if not chollos_df.empty and len(chollos_df) > 20:
        # Calculate price per sqm
        chollos_df['price_per_sqm'] = chollos_df['price'] / chollos_df['size_sqm']
        
        # Calculate statistics by barrio
        barrio_stats = chollos_df.groupby('barrio').agg({
            'price_per_sqm': ['mean', 'std', 'count']
        }).reset_index()
        barrio_stats.columns = ['barrio', 'mean_price_sqm', 'std_price_sqm', 'count']
        
        # Only consider barrios with at least 5 properties
        barrio_stats = barrio_stats[barrio_stats['count'] >= 5]
        
        # Merge back with original data
        chollos_df = chollos_df.merge(barrio_stats[['barrio', 'mean_price_sqm', 'std_price_sqm']], on='barrio', how='left')
        
        # Calculate z-score (how many standard deviations below the mean)
        chollos_df['z_score'] = (chollos_df['price_per_sqm'] - chollos_df['mean_price_sqm']) / chollos_df['std_price_sqm']
        
        # Define chollos as properties with z-score < -1.5 (1.5 std deviations below mean)
        chollos = chollos_df[chollos_df['z_score'] < -1.5].copy()
        chollos = chollos.sort_values('z_score')
        
        if not chollos.empty:
            st.success(f"🎯 Encontrados {len(chollos)} chollos potenciales")
            
            # Show top chollos
            st.markdown("**Top 20 Mejores Chollos**")
            
            top_chollos = chollos.head(20)
            
            # Create display dataframe
            display_chollos = pd.DataFrame({
                'Título': top_chollos['title'],
                'Barrio': top_chollos['barrio'],
                'Precio': top_chollos['price'].apply(lambda x: f"€{x:,.0f}"),
                'Precio/m²': top_chollos['price_per_sqm'].apply(lambda x: f"€{x:,.0f}"),
                'Media Barrio': top_chollos['mean_price_sqm'].apply(lambda x: f"€{x:,.0f}"),
                'Descuento': top_chollos.apply(
                    lambda row: f"{((row['mean_price_sqm'] - row['price_per_sqm']) / row['mean_price_sqm'] * 100):.1f}%",
                    axis=1
                ),
                'Habitaciones': top_chollos['rooms'].fillna(0).astype(int),
                'm²': top_chollos['size_sqm'].apply(lambda x: f"{x:.0f}"),
                'URL': top_chollos['url']
            })
            
            st.dataframe(
                display_chollos,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "URL": st.column_config.LinkColumn("Ver Anuncio")
                }
            )
            
            # Chollos by barrio summary
            st.markdown("**Chollos por Barrio**")
            chollos_by_barrio = chollos.groupby('barrio').size().reset_index(name='Chollos')
            chollos_by_barrio = chollos_by_barrio.sort_values('Chollos', ascending=False).head(10)
            
            col1, col2 = st.columns(2)
            with col1:
                st.dataframe(
                    chollos_by_barrio,
                    hide_index=True,
                    use_container_width=True
                )
            
            with col2:
                fig_chollos = px.bar(
                    chollos_by_barrio,
                    x='Chollos',
                    y='barrio',
                    orientation='h',
                    title='Top 10 Barrios con Más Chollos',
                    labels={'Chollos': 'Número de Chollos', 'barrio': 'Barrio'}
                )
                fig_chollos.update_layout(showlegend=False, height=400)
                st.plotly_chart(fig_chollos, use_container_width=True)
            
            st.caption("💡 **Criterio**: Propiedades con precio/m² al menos 1.5 desviaciones estándar por debajo de la media del barrio")
        else:
            st.info("ℹ️ No se encontraron chollos significativos en este momento")
    else:
        st.info("ℹ️ No hay suficientes datos para detectar chollos (mínimo 20 propiedades activas)")

    
    # Data Table
    st.markdown("---")
    st.subheader("📋 Datos Detallados")
    
    # Select columns to display
    display_columns = [
        'title', 'distrito', 'barrio', 'price', 'rooms', 
        'size_sqm', 'price_per_sqm', 'seller_type', 'description', 'status'
    ]
    display_columns = [col for col in display_columns if col in df.columns]
    
    display_df = df[display_columns].copy()
    
    # Format numeric columns
    if 'price' in display_df.columns:
        display_df['price'] = display_df['price'].apply(lambda x: f"€{x:,.0f}" if pd.notna(x) and x > 0 else "N/A")
    if 'price_per_sqm' in display_df.columns:
        display_df['price_per_sqm'] = display_df['price_per_sqm'].apply(lambda x: f"€{x:,.0f}" if pd.notna(x) else "N/A")
    
    # Truncate descriptions for better display
    if 'description' in display_df.columns:
        display_df['description'] = display_df['description'].apply(
            lambda x: x[:150] + '...' if pd.notna(x) and len(str(x)) > 150 else (x if pd.notna(x) else "N/A")
        )
    
    st.dataframe(
        display_df,
        hide_index=True,
        use_container_width=True,
        height=400
    )
    
    # Footer
    st.markdown("---")
    st.caption(f"📅 Última actualización: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    st.caption(f"📊 Total de registros mostrados: {len(df):,}")


if __name__ == "__main__":
    main()
