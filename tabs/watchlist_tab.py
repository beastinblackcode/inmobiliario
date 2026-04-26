"""
Mi Watchlist — tracked properties with price evolution and drop alerts.

Entry point: render_watchlist_tab()

Shows:
  - Summary KPIs (total saved, active, price drops since added)
  - Per-property cards with price evolution sparkline
  - Full table with remove button
  - Price history chart for selected property
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime


def render_watchlist_tab():
    from database import (
        get_watchlist,
        remove_from_watchlist,
        add_to_watchlist,
        get_price_history,
        migrate_create_watchlist_table,
    )
    from nlp_analyzer import get_signals_for_listings, signals_to_badges

    # Ensure table exists on older DBs
    migrate_create_watchlist_table()

    entries = get_watchlist(include_sold=True)

    # Pre-fetch NLP signals for all watchlist entries
    wl_ids = [e["listing_id"] for e in entries]
    nlp_signals = get_signals_for_listings(wl_ids) if wl_ids else {}

    if not entries:
        st.info(
            "📭 Tu watchlist está vacía.  \n"
            "Guarda propiedades desde **🔍 Mis Búsquedas** con el botón ☆ Guardar."
        )
        return

    # ── KPI summary ───────────────────────────────────────────────────────────
    total     = len(entries)
    active    = sum(1 for e in entries if e["status"] == "active")
    dropped   = sum(1 for e in entries if (e.get("price_change") or 0) < 0)
    raised    = sum(1 for e in entries if (e.get("price_change") or 0) > 0)
    avg_delta = None
    deltas    = [e["price_change_pct"] for e in entries if e.get("price_change_pct") is not None]
    if deltas:
        avg_delta = sum(deltas) / len(deltas)

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("📋 Guardadas",    total)
    col2.metric("🟢 Activas",      active)
    col3.metric("📉 Bajaron",      dropped)
    col4.metric("📈 Subieron",     raised)
    if avg_delta is not None:
        delta_color = "inverse" if avg_delta > 0 else "normal"
        col5.metric("Δ Precio medio", f"{avg_delta:+.1f}%", delta_color=delta_color)

    st.markdown("---")

    # ── Per-property cards ────────────────────────────────────────────────────
    st.subheader("📌 Propiedades Guardadas")

    for entry in entries:
        lid          = entry["listing_id"]
        price_now    = entry.get("current_price")
        price_add    = entry.get("price_at_add")
        change       = entry.get("price_change")
        change_pct   = entry.get("price_change_pct")
        status       = entry.get("status", "unknown")
        barrio       = entry.get("barrio", "—")
        distrito     = entry.get("distrito", "—")
        rooms        = entry.get("rooms")
        sqm          = entry.get("size_sqm")
        drops        = entry.get("num_drops", 0)
        days_watched = entry.get("days_watched", 0)
        url          = entry.get("url", "#")
        note         = entry.get("note", "")

        # Card style based on status / price change
        if status != "active":
            card_border = "#888"
            status_badge = "🔴 Vendido/Retirado"
        elif change is not None and change < 0:
            card_border = "#2e9e52"
            status_badge = f"📉 Bajó {change_pct:+.1f}%"
        elif change is not None and change > 0:
            card_border = "#c0392b"
            status_badge = f"📈 Subió {change_pct:+.1f}%"
        else:
            card_border = "#4fc3f7"
            status_badge = "➡️ Sin cambio"

        with st.container(border=True):
            head_col, btn_col = st.columns([6, 1])

            with head_col:
                rooms_str = f"{int(rooms)} hab · " if rooms else ""
                sqm_str   = f"{sqm:.0f} m² · " if sqm else ""
                price_str = f"€{price_now:,}" if price_now else "—"
                add_str   = f"€{price_add:,}" if price_add else "—"
                nlp_str = signals_to_badges(nlp_signals.get(lid, {}))
                st.markdown(
                    f"**[{barrio}, {distrito}]({url})**  \n"
                    f"{rooms_str}{sqm_str}"
                    f"Precio actual: **{price_str}** · Al guardar: {add_str} · "
                    f"{status_badge} · {drops} bajadas · {days_watched}d vigilado"
                    + (f"  \n{nlp_str}" if nlp_str else "")
                )
                if note:
                    st.caption(f"📝 {note}")

            with btn_col:
                if st.button("🗑️ Quitar", key=f"rm_{lid}", use_container_width=True):
                    remove_from_watchlist(lid)
                    st.rerun()

            # Price history sparkline (collapsible)
            history = get_price_history(lid)
            if len(history) >= 2:
                with st.expander("📊 Ver evolución de precio"):
                    hdf = pd.DataFrame(history)
                    hdf["date"] = pd.to_datetime(hdf["date_recorded"])
                    hdf = hdf.sort_values("date")

                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=hdf["date"],
                        y=hdf["new_price"],
                        mode="lines+markers",
                        line=dict(color=card_border, width=2),
                        marker=dict(size=6),
                        hovertemplate="<b>%{x|%d/%m/%Y}</b><br>€%{y:,.0f}<extra></extra>",
                    ))
                    # Mark the price at add
                    if price_add:
                        fig.add_hline(
                            y=price_add,
                            line_dash="dot",
                            line_color="rgba(150,150,150,0.5)",
                            annotation_text="Precio al guardar",
                            annotation_position="bottom right",
                        )
                    fig.update_layout(
                        height=220,
                        margin=dict(l=10, r=10, t=10, b=30),
                        plot_bgcolor="rgba(0,0,0,0)",
                        paper_bgcolor="rgba(0,0,0,0)",
                        yaxis_title="€",
                        xaxis_title=None,
                    )
                    st.plotly_chart(fig, use_container_width=True)

    # ── Full table ────────────────────────────────────────────────────────────
    st.markdown("---")
    st.subheader("📋 Tabla Completa")

    df = pd.DataFrame(entries)
    if not df.empty:
        display_cols = {
            "barrio":           "Barrio",
            "distrito":         "Distrito",
            "current_price":    "Precio actual (€)",
            "price_at_add":     "Al guardar (€)",
            "price_change_pct": "Δ Precio (%)",
            "size_sqm":         "m²",
            "rooms":            "Hab.",
            "num_drops":        "Bajadas",
            "days_watched":     "Días vigilado",
            "status":           "Estado",
            "added_date":       "Guardado",
        }
        df_display = df[list(display_cols.keys())].copy()
        df_display.columns = list(display_cols.values())
        df_display["Δ Precio (%)"] = df_display["Δ Precio (%)"].apply(
            lambda x: f"{x:+.1f}%" if x is not None and str(x) != "nan" else "—"
        )
        df_display["Estado"] = df_display["Estado"].apply(
            lambda s: "🟢 Activo" if s == "active" else "🔴 Retirado"
        )
        st.dataframe(df_display, use_container_width=True, hide_index=True)
