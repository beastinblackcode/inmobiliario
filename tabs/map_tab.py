"""
Map tab — interactive property map with heat layer.

Entry point: render_map_tab(df)
"""

import streamlit as st
import pandas as pd


def render_map_tab(df: pd.DataFrame) -> None:
    """Render all content for the 🗺️ Mapa tab."""

    from coordinates import get_barrio_coordinates

    st.subheader("🗺️ Mapa Interactivo de Propiedades")

    # Only process active properties for the map
    map_source_df = df[df["status"] == "active"].copy()

    map_source_df["latitude"] = map_source_df.apply(
        lambda row: get_barrio_coordinates(row["distrito"], row["barrio"])[0]
        if pd.notna(row["distrito"]) and pd.notna(row["barrio"])
        else None,
        axis=1,
    )
    map_source_df["longitude"] = map_source_df.apply(
        lambda row: get_barrio_coordinates(row["distrito"], row["barrio"])[1]
        if pd.notna(row["distrito"]) and pd.notna(row["barrio"])
        else None,
        axis=1,
    )

    # Count properties with coordinates
    props_with_coords = map_source_df[
        map_source_df["latitude"].notna() & map_source_df["longitude"].notna()
    ]

    if len(props_with_coords) == 0:
        st.warning(
            "⚠️ No hay propiedades con coordenadas disponibles para mostrar en el mapa."
        )
        st.info(
            "💡 Las coordenadas se asignan por barrio. Verifica que las propiedades tengan distrito y barrio asignados."
        )
        return

    # Map controls
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown(
            "**Visualiza la distribución geográfica de propiedades con mapa de calor de precios**"
        )
    with col2:
        map_limit = st.selectbox(
            "Mostrar",
            options=[100, 500, 1000, "Todos"],
            index=1,
            help="Limitar número de propiedades para mejor rendimiento",
        )

    # Limit data for performance
    map_df = props_with_coords.copy()
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

    st.caption(
        f"📍 Mostrando {len(map_df):,} de {len(props_with_coords):,} propiedades "
        "| 🔥 Mapa de calor muestra intensidad de precios"
    )
