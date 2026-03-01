"""
Tab: Detalle de Propiedad
Vista completa de un piso: metadata, score desglosado factor a factor,
histórico de precios y propiedades similares en el mismo barrio.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime

from database import get_connection, get_property_price_stats, get_notarial_prices
from analytics import (
    calculate_distrito_stats,
    calculate_barrio_stats,
    calculate_days_on_market,
    explain_score,
    estimate_fair_price,
)
from data_utils import load_data


def _get_listing_by_url(url: str) -> dict | None:
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """SELECT listing_id, title, url, price, distrito, barrio, rooms,
                      size_sqm, floor, orientation, seller_type, description,
                      first_seen_date, last_seen_date, status
               FROM listings WHERE url = ? LIMIT 1""",
            (url.strip(),),
        )
        row = cursor.fetchone()
    return dict(row) if row else None


def _get_price_history(listing_id: str) -> list:
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """SELECT date_recorded, price, change_amount, change_percent
               FROM price_history WHERE listing_id = ?
               ORDER BY date_recorded""",
            (listing_id,),
        )
        return [dict(r) for r in cursor.fetchall()]


def _get_similar(listing: dict, exclude_id: str, limit: int = 5) -> list:
    """Properties in same barrio, similar price range (±30%)."""
    with get_connection() as conn:
        cursor = conn.cursor()
        lo, hi = listing["price"] * 0.7, listing["price"] * 1.3
        cursor.execute(
            """SELECT listing_id, title, price, size_sqm, rooms, url
               FROM listings
               WHERE barrio = ? AND listing_id != ? AND status = 'active'
                 AND price BETWEEN ? AND ?
               ORDER BY ABS(price - ?) LIMIT ?""",
            (listing["barrio"], exclude_id, lo, hi, listing["price"], limit),
        )
        return [dict(r) for r in cursor.fetchall()]


def render_detail_tab() -> None:
    st.header("🔍 Detalle de Propiedad")
    st.caption("Pega la URL de Idealista para ver el análisis completo del piso.")

    # ── URL input ─────────────────────────────────────────────────────────────
    url_input = st.text_input(
        "URL del piso",
        placeholder="https://www.idealista.com/inmueble/12345678/",
        key="detail_url_input",
    )

    if not url_input or not url_input.strip():
        st.info("Introduce la URL del piso para ver su ficha completa.")
        return

    listing = _get_listing_by_url(url_input)

    if not listing:
        st.warning("No se encontró ningún piso con esa URL en la base de datos.")
        return

    history = _get_price_history(listing["listing_id"])

    # ── Header ────────────────────────────────────────────────────────────────
    st.markdown("---")
    col_title, col_link = st.columns([5, 1])
    with col_title:
        status_badge = "🟢 Activo" if listing["status"] == "active" else "🔴 Vendido/Retirado"
        st.subheader(listing["title"])
        st.caption(f"{status_badge} · {listing['distrito']} · {listing['barrio']}")
    with col_link:
        st.link_button("🔗 Ver en Idealista", listing["url"], use_container_width=True)

    # ── KPIs principales ──────────────────────────────────────────────────────
    price_sqm = listing["price"] / listing["size_sqm"] if listing.get("size_sqm") else None
    days = calculate_days_on_market({
        "first_seen_date": listing.get("first_seen_date"),
        "last_seen_date":  listing.get("last_seen_date"),
    })

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("💰 Precio", f"€{listing['price']:,}")
    k2.metric("📐 Tamaño", f"{listing['size_sqm']:.0f} m²" if listing.get("size_sqm") else "N/A")
    k3.metric("💵 €/m²", f"€{price_sqm:,.0f}" if price_sqm else "N/A")
    k4.metric("🛏️ Habitaciones", listing["rooms"] if listing.get("rooms") else "N/A")
    k5.metric("⏱️ Días en mercado", f"{days}")

    k6, k7, k8, k9, k10 = st.columns(5)
    k6.metric("🌅 Orientación", listing.get("orientation") or "N/A")
    k7.metric("🏢 Planta", listing.get("floor") or "N/A")
    k8.metric("👤 Vendedor", listing.get("seller_type") or "N/A")
    k9.metric("📅 Visto por primera vez", listing.get("first_seen_date", "N/A")[:10])
    k10.metric("🔄 Última actualización", listing.get("last_seen_date", "N/A")[:10])

    # ── Descripción ───────────────────────────────────────────────────────────
    if listing.get("description"):
        with st.expander("📄 Descripción del anuncio"):
            st.markdown(listing["description"])

    st.markdown("---")

    # ── Score desglosado ──────────────────────────────────────────────────────
    st.subheader("🎯 Score de Oportunidad")

    try:
        all_active = load_data(status="active", distritos=None, min_price=None,
                               max_price=None, seller_type="All")
        if "price_per_sqm" not in all_active.columns:
            all_active["price_per_sqm"] = all_active["price"] / all_active["size_sqm"]
        distrito_stats = calculate_distrito_stats(all_active)
        barrio_stats   = calculate_barrio_stats(all_active)

        # Build row dict with drop history
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """SELECT COUNT(*) AS num_drops, SUM(ABS(change_percent)) AS total_drop_pct
                   FROM price_history WHERE listing_id = ? AND change_amount < 0""",
                (listing["listing_id"],),
            )
            drop_row = dict(cursor.fetchone())

        score_row = {**listing,
                     "price_per_sqm": price_sqm,
                     "days_on_market": days,
                     "num_drops": drop_row.get("num_drops") or 0,
                     "total_drop_pct": drop_row.get("total_drop_pct") or 0}

        # Build notarial_stats for this distrito
        _notarial_raw = get_notarial_prices(distrito=listing.get("distrito"))
        _notarial_stats = {}
        if _notarial_raw:
            latest_not = max(_notarial_raw, key=lambda r: r["periodo"])
            _notarial_stats[listing.get("distrito")] = latest_not["precio_m2"]

        factors = explain_score(score_row, distrito_stats, barrio_stats, _notarial_stats)
        total_score = sum(f["points"] for f in factors)
        total_score = max(0, min(100, total_score))

        # Score badge
        if total_score >= 75:
            color, label = "#2ecc71", "Excelente oportunidad"
        elif total_score >= 55:
            color, label = "#3498db", "Buena oportunidad"
        elif total_score >= 35:
            color, label = "#f39c12", "Oportunidad moderada"
        else:
            color, label = "#e74c3c", "Sin ventaja destacada"

        sc1, sc2 = st.columns([1, 3])
        with sc1:
            st.markdown(
                f"""<div style='text-align:center;background:{color};border-radius:12px;
                    padding:20px;color:white;'>
                    <div style='font-size:48px;font-weight:900;'>{total_score}</div>
                    <div style='font-size:13px;margin-top:4px;'>{label}</div>
                    </div>""",
                unsafe_allow_html=True,
            )

        with sc2:
            for f in factors:
                pct = max(0, f["points"]) / f["max_points"] if f["max_points"] > 0 else 0
                bar_color = "#2ecc71" if f["points"] > 0 else ("#e74c3c" if f["points"] < 0 else "#ddd")
                pts_str = f"{f['points']:+d} / {f['max_points']}"
                st.markdown(
                    f"""<div style='margin-bottom:10px;'>
                        <div style='display:flex;justify-content:space-between;font-size:13px;'>
                          <span><b>{f['label']}</b> — {f['description']}</span>
                          <span style='color:{bar_color};font-weight:700;'>{pts_str}</span>
                        </div>
                        <div style='background:#eee;border-radius:4px;height:8px;margin-top:4px;'>
                          <div style='background:{bar_color};width:{pct*100:.0f}%;height:8px;border-radius:4px;'></div>
                        </div>
                    </div>""",
                    unsafe_allow_html=True,
                )

    except Exception as e:
        st.warning(f"No se pudo calcular el score: {e}")

    st.markdown("---")

    # ── Historial de precios ───────────────────────────────────────────────────
    st.subheader("📈 Histórico de Precios")

    if history:
        stats = get_property_price_stats(listing["listing_id"])
        if stats:
            h1, h2, h3, h4 = st.columns(4)
            h1.metric("Precio inicial", f"€{stats['initial_price']:,}")
            h2.metric("Precio actual",  f"€{stats['current_price']:,}")
            delta_color = "inverse" if stats["total_change"] < 0 else "normal"
            h3.metric("Variación total",
                      f"€{abs(stats['total_change']):,}",
                      f"{stats['total_change_pct']:+.1f}%",
                      delta_color=delta_color)
            h4.metric("Cambios registrados", stats["num_changes"])

        df_hist = pd.DataFrame(history)
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df_hist["date_recorded"], y=df_hist["price"],
            mode="lines+markers", name="Precio",
            line=dict(color="#3498db", width=3), marker=dict(size=10),
            text=[f"€{p:,.0f}" for p in df_hist["price"]],
            hovertemplate="<b>%{x}</b><br>Precio: %{text}<extra></extra>",
        ))
        for _, row in df_hist.iterrows():
            if pd.notna(row["change_amount"]) and row["change_amount"] != 0:
                color  = "#e74c3c" if row["change_amount"] < 0 else "#2ecc71"
                symbol = "▼" if row["change_amount"] < 0 else "▲"
                fig.add_annotation(
                    x=row["date_recorded"], y=row["price"],
                    text=f"{symbol} {abs(row['change_percent']):.1f}%",
                    showarrow=True, arrowhead=2, arrowcolor=color,
                    font=dict(color=color, size=10),
                    bgcolor="white", bordercolor=color, borderwidth=1,
                )
        fig.update_layout(
            xaxis_title="Fecha", yaxis_title="Precio (€)",
            hovermode="x unified", height=400,
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig, use_container_width=True)

        with st.expander("📋 Ver tabla de historial"):
            hist_disp = df_hist[["date_recorded", "price", "change_amount", "change_percent"]].copy()
            hist_disp.columns = ["Fecha", "Precio", "Cambio (€)", "Cambio (%)"]
            st.dataframe(
                hist_disp, hide_index=True, use_container_width=True,
                column_config={
                    "Precio":      st.column_config.NumberColumn("Precio", format="€%d"),
                    "Cambio (€)":  st.column_config.NumberColumn("Cambio (€)", format="€%d"),
                    "Cambio (%)":  st.column_config.NumberColumn("Cambio (%)", format="%.1f%%"),
                },
            )
    else:
        st.info("Este piso aún no tiene cambios de precio registrados.")

    st.markdown("---")

    # ── Valoración estimada ───────────────────────────────────────────────────
    st.subheader("💡 Valoración Estimada")

    try:
        valuation = estimate_fair_price(listing, all_active)

        if "error" in valuation:
            st.warning(valuation["error"])
        else:
            est   = valuation["estimated_price"]
            gap   = valuation["gap_pct"]
            conf  = valuation["confidence"]
            scope = valuation["scope"]
            nc    = valuation["num_comps"]

            conf_color = {"alta": "#2ecc71", "media": "#f39c12", "baja": "#e74c3c"}[conf]
            conf_icon  = {"alta": "🟢", "media": "🟡", "baja": "🔴"}[conf]

            # Main metrics row
            v1, v2, v3, v4 = st.columns(4)
            v1.metric("Precio listado",    f"€{listing['price']:,}")
            v2.metric("Precio estimado",   f"€{est:,}")
            delta_label = f"{gap:+.1f}% {'sobre' if gap > 0 else 'bajo'} valor"
            delta_color = "inverse" if gap > 0 else "normal"
            v3.metric("Diferencia", f"€{abs(listing['price'] - est):,}", delta_label, delta_color=delta_color)
            v4.metric("Confianza", f"{conf_icon} {conf.capitalize()}",
                      f"Basado en {nc} comparables del {scope}")

            # Gap verdict
            if gap > 10:
                st.error(f"⚠️ Este piso está **{gap:.1f}% por encima** del precio estimado. Margen de negociación elevado.")
            elif gap > 5:
                st.warning(f"📊 Precio ligeramente alto ({gap:.1f}% sobre el estimado). Hay margen de negociación.")
            elif gap < -10:
                st.success(f"🎯 ¡Oportunidad! El precio está **{abs(gap):.1f}% por debajo** del valor estimado.")
            elif gap < -5:
                st.success(f"✅ Buen precio ({abs(gap):.1f}% bajo el estimado).")
            else:
                st.info(f"⚖️ Precio en línea con el mercado (diferencia de {abs(gap):.1f}%).")

            # Adjustments breakdown
            if valuation["adjustments"]:
                with st.expander("🔧 Detalle de ajustes aplicados"):
                    st.markdown(
                        f"**Base:** €{valuation['base_sqm']:,}/m² "
                        f"(media ponderada de {nc} comparables del {scope})"
                    )
                    for adj in valuation["adjustments"]:
                        sign = "+" if adj["pct"] > 0 else ""
                        st.markdown(f"- {adj['label']} → **{sign}{adj['pct']*100:.0f}%**")
                    st.markdown(
                        f"**€/m² ajustado:** €{valuation['adjusted_sqm']:,}/m² · "
                        f"**Precio estimado:** €{valuation['adjusted_sqm']:,} × "
                        f"{listing['size_sqm']:.0f} m² = **€{est:,}**"
                    )

            # Comparable properties used
            if valuation["comp_listings"]:
                with st.expander(f"📋 Ver los {min(nc, 10)} comparables utilizados"):
                    for c in valuation["comp_listings"]:
                        sqm_c = c.get("price_per_sqm", 0)
                        st.markdown(
                            f"- **{c['title'][:60]}** · {c['barrio']} · "
                            f"€{c['price']:,} · {c['size_sqm']:.0f}m² · "
                            f"€{sqm_c:,.0f}/m² · [Ver]({c['url']})"
                        )
    except Exception as e:
        st.warning(f"No se pudo calcular la valoración: {e}")

    # ── Referencia notarial ───────────────────────────────────────────────────
    notarial_data = get_notarial_prices(distrito=listing.get("distrito"))
    if notarial_data and listing.get("size_sqm"):
        # Most recent year
        latest = max(notarial_data, key=lambda r: r["periodo"])
        notarial_sqm   = latest["precio_m2"]
        notarial_total = round(notarial_sqm * listing["size_sqm"])
        notarial_gap   = 100 * (listing["price"] - notarial_total) / notarial_total if notarial_total else None

        st.markdown("---")
        st.subheader("🏛️ Referencia Notarial")
        st.caption(
            f"Precio escriturado real en {listing['distrito']} en {latest['periodo']} "
            f"según el Portal del Notariado."
        )

        nr1, nr2, nr3 = st.columns(3)
        nr1.metric(
            f"€/m² notarial ({latest['periodo']})",
            f"€{notarial_sqm:,.0f}",
        )
        nr2.metric(
            "Precio estimado (notarial)",
            f"€{notarial_total:,}",
            help=f"€{notarial_sqm:,.0f}/m² × {listing['size_sqm']:.0f} m²",
        )
        if notarial_gap is not None:
            delta_color = "inverse" if notarial_gap > 0 else "normal"
            nr3.metric(
                "Diferencia vs precio listado",
                f"€{abs(listing['price'] - notarial_total):,}",
                f"{notarial_gap:+.1f}% vs notarial",
                delta_color=delta_color,
            )

        if notarial_gap is not None:
            if notarial_gap > 20:
                st.error(
                    f"⚠️ El precio listado está **{notarial_gap:.1f}% por encima** del precio "
                    f"escriturado real del distrito en {latest['periodo']}. Margen de negociación elevado."
                )
            elif notarial_gap > 5:
                st.warning(
                    f"📊 El precio listado supera en {notarial_gap:.1f}% el precio escriturado del distrito."
                )
            elif notarial_gap < -10:
                st.success(
                    f"🎯 El precio listado está {abs(notarial_gap):.1f}% **por debajo** del precio "
                    f"escriturado del distrito — potencial oportunidad."
                )
            else:
                st.info(
                    f"⚖️ Precio en línea con el precio escriturado notarial "
                    f"(diferencia de {notarial_gap:+.1f}%)."
                )

        # Show historical notarial series for context
        if len(notarial_data) > 1:
            import pandas as pd
            import plotly.graph_objects as go
            df_not = pd.DataFrame(notarial_data)
            fig_not = go.Figure()
            fig_not.add_trace(go.Scatter(
                x=df_not["periodo"], y=df_not["precio_m2"],
                mode="lines+markers", name="€/m² notarial",
                line=dict(color="#9b59b6", width=2), marker=dict(size=7),
                hovertemplate="<b>%{x}</b><br>€/m²: %{y:,.0f}<extra></extra>",
            ))
            # Current listing price/m² as reference line
            if listing.get("size_sqm"):
                fig_not.add_hline(
                    y=listing["price"] / listing["size_sqm"],
                    line_dash="dot", line_color="#e74c3c",
                    annotation_text=f"Este piso: €{listing['price']/listing['size_sqm']:,.0f}/m²",
                    annotation_position="top right",
                )
            fig_not.update_layout(
                height=280,
                xaxis=dict(title="Año", dtick=1),
                yaxis_title="€/m²",
                margin=dict(t=20, b=30, l=10, r=10),
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                showlegend=False,
            )
            st.plotly_chart(fig_not, use_container_width=True)

    st.markdown("---")

    # ── Similares en el mismo barrio ──────────────────────────────────────────
    st.subheader(f"🏘️ Similares en {listing['barrio']}")
    similares = _get_similar(listing, listing["listing_id"])

    if similares:
        for s in similares:
            sqm_s = s["price"] / s["size_sqm"] if s.get("size_sqm") else None
            with st.container(border=True):
                c1, c2, c3, c4 = st.columns([3, 1, 1, 1])
                c1.markdown(f"**{s['title'][:70]}**")
                c2.metric("Precio", f"€{s['price']:,}")
                c3.metric("€/m²", f"€{sqm_s:,.0f}" if sqm_s else "N/A")
                c4.link_button("Ver", s["url"])
    else:
        st.info("No hay pisos similares activos en el mismo barrio.")
