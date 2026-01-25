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
    st.subheader("📈 Análisis Visual")
    
    # Price Distribution Histogram
    st.markdown("#### Distribución de Precios")
    price_df = df[df['price'] > 0].copy()
    
    if not price_df.empty:
        # Calculate 95th percentile to filter outliers for better visualization
        p95 = price_df['price'].quantile(0.95)
        price_df_filtered = price_df[price_df['price'] <= p95]
        
        # Show info about filtered data
        outliers_count = len(price_df) - len(price_df_filtered)
        
        fig_hist = px.histogram(
            price_df_filtered,
            x='price',
            nbins=50,
            title=f'Distribución de Precios de Inmuebles (hasta €{p95:,.0f} - 95% de propiedades)',
            labels={'price': 'Precio (€)', 'count': 'Cantidad'},
            color_discrete_sequence=['#1f77b4']
        )
        fig_hist.update_layout(
            xaxis_tickformat=',.0f',
            showlegend=False,
            height=400
        )
        st.plotly_chart(fig_hist, use_container_width=True)
        
        # Show outliers info
        if outliers_count > 0:
            st.caption(f"ℹ️ Se excluyeron {outliers_count:,} propiedades de lujo (>{p95:,.0f}€) para mejorar la visualización")
    
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
    
    # Time to Sale Analysis (Metric 1)
    st.markdown("#### ⏱️ Tiempo Medio de Venta por Distrito")
    sold_df = df[(df['status'] == 'sold_removed') & (df['distrito'].notna())].copy()
    
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
