"""
Map visualization component for Madrid Real Estate Tracker.
Creates interactive maps with property markers and price heatmap.
"""

import folium
from folium.plugins import HeatMap, MarkerCluster
import pandas as pd
from typing import Optional
import streamlit as st


def create_property_map(
    listings_df: pd.DataFrame,
    center: tuple = (40.4168, -3.7038),
    zoom_start: int = 11
) -> folium.Map:
    """
    Create interactive map with property listings.
    
    Args:
        listings_df: DataFrame with listings (must have latitude, longitude columns)
        center: Map center coordinates (lat, lon)
        zoom_start: Initial zoom level
        
    Returns:
        Folium map object
    """
    # Create base map
    m = folium.Map(
        location=center,
        zoom_start=zoom_start,
        tiles='OpenStreetMap'
    )
    
    # Filter out listings without coordinates
    df_with_coords = listings_df[
        listings_df['latitude'].notna() & 
        listings_df['longitude'].notna()
    ].copy()
    
    if len(df_with_coords) == 0:
        return m
    
    # Add price heatmap layer
    if 'price' in df_with_coords.columns:
        heat_data = [
            [row['latitude'], row['longitude'], row['price'] / 1000000]  # Normalize price
            for _, row in df_with_coords.iterrows()
            if pd.notna(row['price']) and row['price'] > 0
        ]
        
        if heat_data:
            HeatMap(
                heat_data,
                name='Mapa de Calor de Precios',
                radius=15,
                blur=20,
                max_zoom=13,
                gradient={
                    0.0: 'blue',
                    0.3: 'cyan',
                    0.5: 'lime',
                    0.7: 'yellow',
                    1.0: 'red'
                }
            ).add_to(m)
    
    # Add marker cluster for properties
    marker_cluster = MarkerCluster(
        name='Propiedades',
        overlay=True,
        control=True
    ).add_to(m)
    
    # Add individual markers
    for idx, row in df_with_coords.iterrows():
        # Determine marker color based on price
        if pd.notna(row.get('price')):
            if row['price'] > 800000:
                color = 'red'
                icon_name = 'star'
            elif row['price'] > 500000:
                color = 'orange'
                icon_name = 'home'
            elif row['price'] > 300000:
                color = 'blue'
                icon_name = 'home'
            else:
                color = 'green'
                icon_name = 'home'
        else:
            color = 'gray'
            icon_name = 'question'
        
        # Create popup HTML
        popup_html = f"""
        <div style="font-family: Arial; width: 250px;">
            <h4 style="margin: 0 0 10px 0; color: #333;">{row.get('title', 'Sin título')[:60]}</h4>
            <hr style="margin: 5px 0;">
            <p style="margin: 5px 0;"><b>💰 Precio:</b> {row.get('price', 'N/A'):,}€</p>
            <p style="margin: 5px 0;"><b>📍 Ubicación:</b> {row.get('barrio', 'N/A')}, {row.get('distrito', 'N/A')}</p>
            <p style="margin: 5px 0;"><b>🛏️ Habitaciones:</b> {row.get('rooms', 'N/A')}</p>
            <p style="margin: 5px 0;"><b>📐 Superficie:</b> {row.get('size_sqm', 'N/A')} m²</p>
            <p style="margin: 5px 0;"><b>🏢 Vendedor:</b> {row.get('seller_type', 'N/A')}</p>
            <p style="margin: 5px 0;"><b>📅 Visto:</b> {row.get('last_seen_date', 'N/A')}</p>
            <hr style="margin: 5px 0;">
            <a href="{row.get('url', '#')}" target="_blank" style="color: #0066cc; text-decoration: none;">
                🔗 Ver en Idealista →
            </a>
        </div>
        """
        
        # Add marker to cluster
        folium.Marker(
            location=[row['latitude'], row['longitude']],
            popup=folium.Popup(popup_html, max_width=300),
            tooltip=f"{row.get('price', 'N/A'):,}€ - {row.get('barrio', 'N/A')}",
            icon=folium.Icon(
                color=color,
                icon=icon_name,
                prefix='fa'
            )
        ).add_to(marker_cluster)
    
    # Add layer control
    folium.LayerControl().add_to(m)
    
    return m


def create_distrito_map(
    listings_df: pd.DataFrame,
    distrito_stats: pd.DataFrame
) -> folium.Map:
    """
    Create map showing distrito-level statistics.
    
    Args:
        listings_df: DataFrame with listings
        distrito_stats: DataFrame with distrito statistics
        
    Returns:
        Folium map object
    """
    # Create base map
    m = folium.Map(
        location=(40.4168, -3.7038),
        zoom_start=11,
        tiles='OpenStreetMap'
    )
    
    # Add distrito markers with statistics
    for _, distrito_row in distrito_stats.iterrows():
        distrito = distrito_row['distrito']
        
        # Get representative coordinates (first listing in distrito)
        distrito_listings = listings_df[listings_df['distrito'] == distrito]
        if len(distrito_listings) == 0:
            continue
            
        lat = distrito_listings['latitude'].mean()
        lon = distrito_listings['longitude'].mean()
        
        if pd.isna(lat) or pd.isna(lon):
            continue
        
        # Create popup with distrito stats
        popup_html = f"""
        <div style="font-family: Arial; width: 200px;">
            <h3 style="margin: 0 0 10px 0; color: #333;">{distrito}</h3>
            <hr style="margin: 5px 0;">
            <p style="margin: 5px 0;"><b>📊 Total:</b> {distrito_row.get('count', 0)}</p>
            <p style="margin: 5px 0;"><b>💰 Precio medio:</b> {distrito_row.get('avg_price', 0):,.0f}€</p>
            <p style="margin: 5px 0;"><b>📐 m² medio:</b> {distrito_row.get('avg_size', 0):.0f} m²</p>
            <p style="margin: 5px 0;"><b>💵 €/m²:</b> {distrito_row.get('price_per_sqm', 0):,.0f}€</p>
        </div>
        """
        
        # Determine circle color based on price
        avg_price = distrito_row.get('avg_price', 0)
        if avg_price > 600000:
            color = 'red'
        elif avg_price > 400000:
            color = 'orange'
        elif avg_price > 250000:
            color = 'blue'
        else:
            color = 'green'
        
        # Add circle marker
        folium.CircleMarker(
            location=[lat, lon],
            radius=distrito_row.get('count', 0) / 50,  # Size based on count
            popup=folium.Popup(popup_html, max_width=250),
            tooltip=distrito,
            color=color,
            fill=True,
            fillColor=color,
            fillOpacity=0.6
        ).add_to(m)
    
    return m
