"""
Search tab — personalised property search with price tracking.

Entry point: render_search_tab()
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go


def render_search_tab() -> None:
    """Render all content for the 🔍 Mis Búsquedas tab."""

    st.subheader("🔍 Mis Búsquedas Personalizadas")
    st.info(
        "Mostrando inmuebles activos entre 250k-450k €, ≥40m², sin bajos, "
        "con ascensor (zona centro/norte/este)."
    )

    # -------------------------------------------------------------------------
    # Load data independently from sidebar filters
    # -------------------------------------------------------------------------
    from database import get_listings as get_listings_personal

    personal_raw = get_listings_personal(
        status="active",
        min_price=250000,
        max_price=450000,
    )
    personal_df = pd.DataFrame(personal_raw)

    if personal_df.empty:
        st.warning("No se encontraron inmuebles en el rango de precio 250k-450k €.")
        return

    # Calculate price_per_sqm
    personal_df["price_per_sqm"] = personal_df.apply(
        lambda row: row["price"] / row["size_sqm"]
        if row.get("size_sqm") and row["size_sqm"] > 0
        else None,
        axis=1,
    )

    # Filter: Size >= 40 m²
    personal_df = personal_df[personal_df["size_sqm"].fillna(0) >= 40]

    # Filter: Floor — not "Bajo"
    personal_df["floor"] = personal_df["floor"].fillna("").astype(str)
    personal_df = personal_df[
        ~personal_df["floor"].str.contains("bajo", case=False, na=False)
    ]

    # Filter: Elevator — has "ascensor" but NOT "sin ascensor" / "no dispone"
    personal_df["description"] = personal_df["description"].fillna("").astype(str)
    has_elevator = personal_df["description"].str.contains(
        "ascensor", case=False, na=False
    )
    no_sin = ~personal_df["description"].str.contains(
        "sin ascensor", case=False, na=False
    )
    no_no_dispone = ~personal_df["description"].str.contains(
        "no dispone de ascensor", case=False, na=False
    )
    personal_df = personal_df[has_elevator & no_sin & no_no_dispone]

    # Filter: Location
    target_districts = [
        "Centro",
        "Chamberí",
        "Moratalaz",
        "Retiro",
        "Tetuán",
        "Arganzuela",
        "Chamartín",
        "Salamanca",
        "Hortaleza",
    ]
    target_barrios = ["Argüelles"]
    personal_df = personal_df[
        (personal_df["distrito"].isin(target_districts))
        | (personal_df["barrio"].isin(target_barrios))
    ]

    if personal_df.empty:
        st.warning("No se encontraron inmuebles que coincidan con todos los criterios.")
        return

    st.success(f"Se encontraron **{len(personal_df)}** inmuebles.")

    # Summary metrics
    p_col1, p_col2, p_col3 = st.columns(3)
    with p_col1:
        st.metric("Precio Medio", f"€{personal_df['price'].mean():,.0f}")
    with p_col2:
        avg_sqm = personal_df["price_per_sqm"].dropna().mean()
        st.metric(
            "Precio Medio/m²",
            f"€{avg_sqm:,.0f}" if pd.notna(avg_sqm) else "N/A",
        )
    with p_col3:
        st.metric("Tamaño Medio", f"{personal_df['size_sqm'].mean():.0f} m²")

    # Results table
    display_cols = [
        "listing_id",
        "title",
        "price",
        "distrito",
        "barrio",
        "size_sqm",
        "floor",
        "url",
    ]
    st.dataframe(
        personal_df[display_cols],
        use_container_width=True,
        column_config={
            "price": st.column_config.NumberColumn("Precio", format="€%d"),
            "size_sqm": st.column_config.NumberColumn("Tamaño", format="%d m²"),
            "url": st.column_config.LinkColumn("Enlace"),
        },
        hide_index=True,
    )

    # -------------------------------------------------------------------------
    # Price evolution
    # -------------------------------------------------------------------------
    st.markdown("### 📉 Seguimiento de Precios")

    from database import get_price_history_for_listings

    listing_ids = personal_df["listing_id"].tolist()

    try:
        price_histories = get_price_history_for_listings(listing_ids)
        if price_histories:
            ph_df = pd.DataFrame(price_histories)
            ph_df["date"] = pd.to_datetime(ph_df["date"])

            # Daily average price evolution
            daily_avg = (
                ph_df.groupby("date")
                .agg(avg_price=("new_price", "mean"), count=("listing_id", "nunique"))
                .reset_index()
                .sort_values("date")
            )

            if len(daily_avg) > 1:
                fig_personal_evol = go.Figure()
                fig_personal_evol.add_trace(
                    go.Scatter(
                        x=daily_avg["date"],
                        y=daily_avg["avg_price"],
                        mode="lines+markers",
                        name="Precio Medio",
                        line=dict(color="#8e44ad", width=3),
                        marker=dict(size=6),
                        hovertemplate="<b>%{x|%d/%m/%Y}</b><br>Precio: %{y:,.0f} €<extra></extra>",
                    )
                )
                fig_personal_evol.update_layout(
                    title="Evolución Precio Medio (Inmuebles Filtrados)",
                    xaxis_title="Fecha",
                    yaxis_title="Precio (€)",
                    hovermode="x unified",
                    height=400,
                )
                st.plotly_chart(fig_personal_evol, use_container_width=True)

            # Properties with price drops
            st.markdown("#### 📉 Bajadas de Precio Recientes")
            drops = ph_df[ph_df["price_change"] < 0].copy()
            if not drops.empty:
                drops = drops.sort_values("date", ascending=False).head(20)
                drops["price_change_fmt"] = drops["price_change"].apply(
                    lambda x: f"€{x:,.0f}"
                )
                drops["pct"] = drops.apply(
                    lambda r: f"{(r['price_change'] / (r['new_price'] - r['price_change'])) * 100:.1f}%"
                    if (r["new_price"] - r["price_change"]) != 0
                    else "N/A",
                    axis=1,
                )
                st.dataframe(
                    drops[
                        [
                            "listing_id",
                            "date",
                            "new_price",
                            "price_change_fmt",
                            "pct",
                        ]
                    ],
                    column_config={
                        "new_price": st.column_config.NumberColumn(
                            "Nuevo Precio", format="€%d"
                        ),
                        "price_change_fmt": "Cambio",
                        "pct": "% Cambio",
                        "date": "Fecha",
                    },
                    hide_index=True,
                )
            else:
                st.info("No hay bajadas de precio recientes para estos inmuebles.")
        else:
            st.info(
                "No hay datos de historial de precios disponibles para estos inmuebles."
            )
    except Exception:
        st.info("No hay datos de historial de precios disponibles.")
