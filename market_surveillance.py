"""
Market Surveillance page for the Streamlit dashboard.
Displays market health indicators, macro data, and automatic diagnosis.
"""

import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime


def render_market_surveillance():
    """Main render function for the Market Surveillance page."""
    
    st.title("🛡️ Panel de Vigilancia del Mercado")
    st.markdown("**Análisis integral del mercado inmobiliario de Madrid — Indicadores internos + Macro**")
    
    # Fetch data with caching
    with st.spinner("📡 Obteniendo datos macro (INE, BCE)..."):
        macro = _fetch_macro_data()
    
    # Pass Euríbor to affordability index (fetched first so we can reuse it)
    euribor_rate = _fetch_macro_data().get("euribor", {}).get("current")

    with st.spinner("📊 Calculando indicadores internos..."):
        indicators = _fetch_internal_indicators(euribor_rate=euribor_rate)
    
    # Calculate market score
    from market_indicators import calculate_market_score, generate_diagnosis, get_market_alerts

    score = calculate_market_score(
        price_trend=indicators["price_trend"],
        sales_speed=indicators["sales_speed"],
        supply_demand=indicators["supply_demand"],
        inventory=indicators["inventory"],
        euribor=macro.get("euribor"),
        paro=macro.get("paro"),
        affordability=indicators.get("affordability"),
        price_drop_ratio=indicators.get("price_drop_ratio"),
        notarial_gap=indicators.get("notarial_gap"),
    )

    alerts = get_market_alerts(
        price_trend=indicators.get("price_trend"),
        sales_speed=indicators.get("sales_speed"),
        supply_demand=indicators.get("supply_demand"),
        inventory=indicators.get("inventory"),
        rotation=indicators.get("rotation"),
        affordability=indicators.get("affordability"),
        macro=macro,
        notarial_gap=indicators.get("notarial_gap"),
    )

    # ========================================================================
    # Section 1: Market Semaphore
    # ========================================================================
    _render_semaphore(score)

    # ========================================================================
    # Section 1b: Alerts Panel (only if there are alerts)
    # ========================================================================
    if alerts:
        _render_alerts(alerts)

    st.markdown("---")

    # ========================================================================
    # Section 2: Internal Indicators (KPIs)
    # ========================================================================
    _render_internal_kpis(indicators)
    
    st.markdown("---")
    
    # ========================================================================
    # Section 3: Macro Indicators (KPIs)
    # ========================================================================
    _render_macro_kpis(macro)
    
    st.markdown("---")
    
    # ========================================================================
    # Section 4: Automatic Diagnosis
    # ========================================================================
    _render_diagnosis(indicators, macro)
    
    st.markdown("---")
    
    # ========================================================================
    # Section 5: Temporal Charts
    # ========================================================================
    _render_charts(indicators, macro)
    
    # ========================================================================
    # Section 6: Score Breakdown
    # ========================================================================
    st.markdown("---")
    _render_score_breakdown(score)
    
    # Footer
    st.markdown("---")
    st.caption(
        f"🕐 Última actualización: {datetime.now().strftime('%d/%m/%Y %H:%M')} | "
        f"Fuentes: Idealista (scraping), INE, BCE"
    )


# ============================================================================
# Data fetching with Streamlit caching
# ============================================================================

@st.cache_data(ttl=86400, show_spinner=False)  # 24h cache
def _fetch_macro_data():
    from macro_data import get_all_macro_data
    return get_all_macro_data()


@st.cache_data(ttl=3600, show_spinner=False)  # 1h cache
def _fetch_internal_indicators(euribor_rate: float = None):
    from market_indicators import get_all_internal_indicators
    return get_all_internal_indicators(euribor_rate=euribor_rate)


# ============================================================================
# Section Renderers
# ============================================================================

def _render_alerts(alerts: list):
    """Render prominent alert panel below the semaphore."""

    critical = [a for a in alerts if a["level"] == "critical"]
    warnings = [a for a in alerts if a["level"] == "warning"]
    infos    = [a for a in alerts if a["level"] == "info"]

    st.markdown("### 🔔 Alertas del Mercado")

    if critical:
        for a in critical:
            st.error(f"**{a['emoji']} {a['title']}** — {a['detail']}")

    if warnings:
        for a in warnings:
            st.warning(f"**{a['emoji']} {a['title']}** — {a['detail']}")

    if infos:
        with st.expander(f"ℹ️ {len(infos)} nota(s) informativa(s)"):
            for a in infos:
                st.info(f"**{a['emoji']} {a['title']}** — {a['detail']}")

    st.markdown("")


def _render_semaphore(score: dict):
    """Render the market semaphore with big visual indicator."""
    
    # Color mapping
    colors = {
        "green": {"bg": "#1a5e1a", "text": "#4caf50", "glow": "rgba(76, 175, 80, 0.3)"},
        "yellow": {"bg": "#5e5e1a", "text": "#ffeb3b", "glow": "rgba(255, 235, 59, 0.3)"},
        "red": {"bg": "#5e1a1a", "text": "#f44336", "glow": "rgba(244, 67, 54, 0.3)"}
    }
    
    c = colors.get(score["color"], colors["yellow"])
    
    st.markdown(f"""
    <div style="
        background: linear-gradient(135deg, {c['bg']}, #1a1a2e);
        border: 2px solid {c['text']};
        border-radius: 16px;
        padding: 24px;
        text-align: center;
        box-shadow: 0 0 30px {c['glow']};
        margin-bottom: 20px;
    ">
        <div style="font-size: 64px; margin-bottom: 8px;">{score['emoji']}</div>
        <div style="font-size: 48px; font-weight: bold; color: {c['text']};">
            {score['score']:.0f}/100
        </div>
        <div style="font-size: 24px; color: {c['text']}; font-weight: 600; 
                    letter-spacing: 3px; margin-top: 4px;">
            {score['label']}
        </div>
        <div style="font-size: 14px; color: #aaa; margin-top: 12px;">
            {score['description']}
        </div>
    </div>
    """, unsafe_allow_html=True)


def _render_internal_kpis(indicators: dict):
    """Render internal market indicator KPIs."""
    
    st.subheader("📊 Indicadores Internos")
    st.caption("Calculados a partir de datos de scraping de Idealista")
    
    col1, col2, col3, col4 = st.columns(4)
    
    # 1. Price trend
    price = indicators["price_trend"]
    with col1:
        current = price.get("current")
        change_pct = price.get("change_pct", 0)
        if current:
            st.metric(
                label="💰 Precio Mediano",
                value=f"€{current:,.0f}",
                delta=f"{change_pct:+.1f}% semanal" if change_pct else None,
                help="Mediana de precios de propiedades nuevas esta semana"
            )
        else:
            st.metric(label="💰 Precio Mediano", value="Sin datos")
    
    # 2. Sales speed
    speed = indicators["sales_speed"]
    with col2:
        current = speed.get("current")
        change = speed.get("change")
        if current is not None:
            # Invert delta color: fewer days = good (green)
            delta_str = f"{change:+.0f} días" if change else None
            st.metric(
                label="⏱️ Velocidad Venta",
                value=f"{current:.0f} días",
                delta=delta_str,
                delta_color="inverse",
                help="Mediana de días en mercado de propiedades vendidas/retiradas"
            )
        else:
            st.metric(label="⏱️ Velocidad Venta", value="Sin datos")
    
    # 3. Supply/demand ratio
    sd = indicators["supply_demand"]
    with col3:
        current = sd.get("current")
        if current is not None and current < 50:  # Filter out infinity
            # Thresholds aligned with market_indicators.py:
            # <0.8 = excess demand (fast absorption) → green
            # 0.8–2.0 = balanced market → yellow
            # >2.0 = excess supply → red
            indicator = "🟢" if current < 0.8 else ("🟡" if current < 2.0 else "🔴")
            st.metric(
                label="⚖️ Ratio O/D",
                value=f"{current:.1f}x {indicator}",
                delta=f"{sd.get('change', 0):+.1f}" if sd.get("change") else None,
                delta_color="inverse",
                help="Nuevas publicaciones / Absorciones. >2.0 = exceso oferta, <0.8 = exceso demanda"
            )
        else:
            st.metric(label="⚖️ Ratio O/D", value="Sin datos")
    
    # 4. Inventory
    inv = indicators["inventory"]
    with col4:
        current = inv.get("current")
        change_pct = inv.get("change_pct")
        if current:
            st.metric(
                label="🏠 Inventario",
                value=f"{current:,}",
                delta=f"{change_pct:+.1f}%" if change_pct else None,
                help="Total de propiedades activas en el mercado"
            )
        else:
            st.metric(label="🏠 Inventario", value="Sin datos")
    
    # Second row
    col5, col6, col7, col8 = st.columns(4)
    
    # 5. Rotation rate (rolling)
    rotation = indicators["rotation"]
    with col5:
        current = rotation.get("current")
        change = rotation.get("change")
        window = rotation.get("window_weeks", 4)
        if current is not None:
            st.metric(
                label="🔄 Tasa Rotación",
                value=f"{current:.1f}%",
                delta=f"{change:+.1f}pp" if change is not None else None,
                help=(f"Ventas últimas {window} semanas / inventario medio activo. "
                      f"{rotation.get('sold_window', 0):,} vendidas de "
                      f"{rotation.get('active', 0):,} activas.")
            )

    # 6. Price dispersion
    disp = indicators["dispersion"]
    with col6:
        current = disp.get("current")
        if current is not None:
            indicator = "🔴" if current > 50 else ("🟡" if current > 30 else "🟢")
            st.metric(
                label="📐 Dispersión",
                value=f"{current:.0f}% {indicator}",
                help=(f"Diferencia media/mediana. "
                      f"Media: €{disp.get('mean_price', 0):,.0f} vs "
                      f"Mediana: €{disp.get('median_price', 0):,.0f}")
            )

    # 7. €/m²
    with col7:
        current_sqm = price.get("current_sqm")
        if current_sqm:
            st.metric(
                label="📏 €/m² Mediano",
                value=f"€{current_sqm:,.0f}",
                help="Precio mediano por metro cuadrado de nuevas propiedades"
            )
        else:
            st.metric(label="📏 €/m² Mediano", value="Sin datos")

    # 8. Affordability (monthly mortgage on median property)
    afford = indicators.get("affordability", {})
    with col8:
        monthly = afford.get("current")
        pti = afford.get("price_to_income")
        affordable = afford.get("affordable")
        rate_used = afford.get("rate_used")
        if monthly is not None:
            badge = "🟢" if affordable else "🔴"
            st.metric(
                label=f"🏠 Cuota Hipotecaria {badge}",
                value=f"€{monthly:,}/mes",
                help=(f"80% LTV, 25 años, Euríbor+spread ({rate_used:.2f}%). "
                      f"Ratio precio/ingreso: {pti:.1f}× ingresos anuales de referencia.")
            )
        else:
            st.metric(label="🏠 Cuota Hipotecaria", value="Sin datos")

    # Third row — new indicators
    col9, col10, col11, col12 = st.columns(4)

    # 9. Price drop ratio (seller stress)
    pdr = indicators.get("price_drop_ratio", {})
    with col9:
        drop_pct = pdr.get("current")
        drop_change = pdr.get("change")
        listings_drop = pdr.get("listings_with_drop", 0)
        avg_depth = pdr.get("avg_drop_pct")
        if drop_pct is not None:
            badge = "🟢" if drop_pct < 10 else ("🟡" if drop_pct < 25 else "🔴")
            depth_str = f" · bajada media {avg_depth:.1f}%" if avg_depth else ""
            st.metric(
                label=f"📉 Estrés Vendedor {badge}",
                value=f"{drop_pct:.1f}%",
                delta=f"{drop_change:+.1f}pp" if drop_change is not None else None,
                delta_color="inverse",
                help=(f"{listings_drop:,} propiedades activas con al menos una bajada "
                      f"en los últimos 30 días{depth_str}. "
                      f"<10% = poder vendedor · >30% = estrés generalizado.")
            )
        else:
            st.metric(label="📉 Estrés Vendedor", value="Sin datos")

    # 10. Affordability ratio (pmt / income %)
    with col10:
        if afford.get("current") and afford.get("reference_income_monthly"):
            pmt_ratio = afford["current"] / afford["reference_income_monthly"] * 100
            badge = "🟢" if pmt_ratio <= 30 else ("🟡" if pmt_ratio <= 40 else "🔴")
            st.metric(
                label=f"📊 Esfuerzo Hipotecario {badge}",
                value=f"{pmt_ratio:.0f}% ingresos",
                help=(f"Cuota mensual como % del ingreso de referencia "
                      f"(€{afford.get('reference_income_monthly', 0):,.0f}/mes). "
                      f"Umbral saludable: ≤33%.")
            )
        else:
            st.metric(label="📊 Esfuerzo Hipotecario", value="Sin datos")

    # 11. Rental yield (average gross yield across barrios)
    ry = indicators.get("rental_yield", {})
    with col11:
        avg_yield = ry.get("current")
        barrio_count = ry.get("barrio_count", 0)
        if avg_yield is not None:
            badge = "🟢" if avg_yield >= 5.0 else ("🟡" if avg_yield >= 3.5 else "🔴")
            st.metric(
                label=f"🏘️ Rentabilidad Alquiler {badge}",
                value=f"{avg_yield:.1f}%",
                help=(f"Rentabilidad bruta media (alquiler anual / precio venta). "
                      f"Calculada sobre {barrio_count} barrios con datos de alquiler. "
                      f"Referencia Madrid: 3.5–5 %.")
            )
        else:
            st.metric(
                label="🏘️ Rentabilidad Alquiler",
                value="Sin datos",
                help="Requiere datos de alquiler. Se actualizan con cada ejecución del scraper."
            )

    # 12. Notarial gap (overpricing vs real transaction prices)
    ng = indicators.get("notarial_gap", {})
    with col12:
        gap_val = ng.get("current")
        if gap_val is not None:
            yr = ng.get("notarial_year", "")
            badge = "🟢" if gap_val < 15 else ("🟡" if gap_val < 30 else "🔴")
            max_d = ng.get("max_distrito", "")
            st.metric(
                label=f"🏛️ Sobreprecio vs Notarial {badge}",
                value=f"{gap_val:+.1f}%",
                delta=f"Mayor en {max_d}" if max_d else None,
                delta_color="off",
                help=(f"Gap medio entre €/m² de Idealista y el precio escriturado real "
                      f"del Notariado {yr}. >30% = mercado muy tensionado.")
            )
        else:
            st.metric(
                label="🏛️ Sobreprecio vs Notarial",
                value="Sin datos",
                help="Requiere datos del Portal del Notariado importados en BD."
            )


def _render_macro_kpis(macro: dict):
    """Render macro economic indicator KPIs."""
    
    st.subheader("🏛️ Indicadores Macroeconómicos")
    st.caption("Fuentes: Banco Central Europeo (BCE), Instituto Nacional de Estadística (INE)")
    
    col1, col2, col3 = st.columns(3)
    
    # 1. Euribor
    euribor = macro.get("euribor", {})
    with col1:
        current = euribor.get("current")
        change = euribor.get("change")
        if current is not None:
            st.metric(
                label="🏦 Euríbor 12M",
                value=f"{current:.2f}%",
                delta=f"{change:+.3f} pp" if change else None,
                delta_color="inverse",
                help=f"Fuente: {euribor.get('source', 'BCE')} | {euribor.get('frequency', 'Mensual')}"
            )
        else:
            st.metric(label="🏦 Euríbor 12M", value="Sin datos")
    
    # 2. IPC
    ipc = macro.get("ipc", {})
    with col2:
        current = ipc.get("current")
        change = ipc.get("change")
        if current is not None:
            st.metric(
                label="📊 IPC (Var. Anual)",
                value=f"{current}%",
                delta=f"{change:+.1f} pp" if change else None,
                delta_color="inverse",
                help=f"Fuente: {ipc.get('source', 'INE')} | {ipc.get('frequency', 'Mensual')}"
            )
        else:
            st.metric(label="📊 IPC", value="Sin datos")
    
    # 3. IPV
    ipv = macro.get("ipv", {})
    with col3:
        current = ipv.get("current")
        if current is not None:
            st.metric(
                label="🏠 IPV Madrid (Var. Anual)",
                value=f"+{current}%" if current > 0 else f"{current}%",
                delta=f"{ipv.get('change', 0):+.1f} pp" if ipv.get("change") else None,
                help=f"Índice de Precios de Vivienda — Madrid | "
                     f"{ipv.get('source', 'INE')} | {ipv.get('frequency', 'Trimestral')}"
            )
        else:
            st.metric(label="🏠 IPV Madrid", value="Sin datos")
    
    col4, col5, col6 = st.columns(3)
    
    # 4. Compraventas
    compra = macro.get("compraventas", {})
    with col4:
        current = compra.get("current")
        change_pct = compra.get("change_pct")
        if current is not None:
            st.metric(
                label="📝 Compraventas Madrid",
                value=f"{current:,}",
                delta=f"{change_pct:+.1f}%" if change_pct else None,
                help=f"Nº de compraventas de vivienda registradas | "
                     f"{compra.get('source', 'INE')} | {compra.get('frequency', 'Mensual')}"
            )
        else:
            st.metric(label="📝 Compraventas", value="Sin datos")
    
    # 5. Paro
    paro = macro.get("paro", {})
    with col5:
        current = paro.get("current")
        change = paro.get("change")
        if current is not None:
            st.metric(
                label="👥 Tasa de Paro",
                value=f"{current:.1f}%",
                delta=f"{change:+.1f} pp" if change else None,
                delta_color="inverse",
                help=f"{paro.get('scope', 'Total Nacional')} | "
                     f"{paro.get('source', 'INE')} | {paro.get('frequency', 'Trimestral')}"
            )
        else:
            st.metric(label="👥 Paro", value="Sin datos")
    
    # 6. Hipotecas
    hipo = macro.get("hipotecas", {})
    with col6:
        current = hipo.get("current")
        change_pct = hipo.get("change_pct")
        if current is not None:
            st.metric(
                label="🏦 Hipotecas Madrid",
                value=f"{current:,}",
                delta=f"{change_pct:+.1f}%" if change_pct else None,
                help=f"Hipotecas de vivienda constituidas | "
                     f"{hipo.get('source', 'INE')} | {hipo.get('frequency', 'Mensual')}"
            )
        else:
            st.metric(label="🏦 Hipotecas", value="Sin datos")
    
    # Data freshness info
    with st.expander("ℹ️ Información sobre los datos"):
        sources = []
        for key, data in macro.items():
            last_date = data.get("series", [{}])[-1].get("date_str", "N/A") if data.get("series") else "N/A"
            error = data.get("error")
            status = "⚠️" if error else "✅"
            sources.append(f"| {status} {data.get('name', key)} | {data.get('source', 'N/A')} | {data.get('frequency', 'N/A')} | {last_date} |")
        
        st.markdown(
            "| Estado | Indicador | Fuente | Frecuencia | Último dato |\n"
            "|--------|-----------|--------|------------|------------|\n" +
            "\n".join(sources)
        )


def _render_diagnosis(indicators: dict, macro: dict):
    """Render the automatic diagnosis section."""

    st.subheader("🔮 Diagnóstico Automático")

    from market_indicators import generate_diagnosis

    diagnosis = generate_diagnosis(
        price_trend=indicators["price_trend"],
        sales_speed=indicators["sales_speed"],
        supply_demand=indicators["supply_demand"],
        inventory=indicators["inventory"],
        rotation=indicators["rotation"],
        dispersion=indicators["dispersion"],
        macro=macro
    )

    # Append affordability note if available
    afford = indicators.get("affordability", {})
    if afford.get("current"):
        monthly = afford["current"]
        pti = afford.get("price_to_income", "?")
        affordable = afford.get("affordable")
        aff_str = "asequible" if affordable else "**no asequible** (supera el 33% del ingreso bruto)"
        diagnosis += (
            f"\n\n💳 **Asequibilidad estimada**: cuota hipotecaria de referencia €{monthly:,}/mes "
            f"({aff_str}). Ratio precio/ingreso anual: {pti:.1f}×."
        )

    # Append breakpoint note if detected
    bp = indicators.get("price_trend", {}).get("breakpoint", {})
    if bp and bp.get("breakpoint"):
        before = bp.get("direction_before", "")
        after = bp.get("direction_after", "")
        week = bp.get("breakpoint_week", "")
        direction_label = {"up": "alcista", "down": "bajista", "stable": "lateral"}
        diagnosis += (
            f"\n\n🔄 **Cambio de tendencia detectado**: la tendencia pasó de "
            f"*{direction_label.get(before, before)}* a *{direction_label.get(after, after)}* "
            f"alrededor de la semana del {week}."
        )

    st.markdown(
        f'<div style="background: linear-gradient(135deg, #1a1a2e, #16213e); '
        f'border-left: 4px solid #4fc3f7; border-radius: 8px; padding: 20px; '
        f'margin: 10px 0; color: #e0e0e0;">'
        f'{diagnosis.replace(chr(10)+chr(10), "<br><br>")}'
        f'</div>',
        unsafe_allow_html=True
    )


def _render_charts(indicators: dict, macro: dict):
    """Render temporal comparison charts."""
    
    st.subheader("📈 Gráficos Temporales")
    
    tab1, tab2, tab3, tab4 = st.tabs([
        "💰 Precios vs Euríbor",
        "📦 Inventario vs Compraventas",
        "⏱️ Velocidad de Venta",
        "🗺️ Por Zona"
    ])

    # Chart 1: Prices vs Euribor
    with tab1:
        _chart_prices_vs_euribor(indicators, macro)

    # Chart 2: Inventory vs Compraventas
    with tab2:
        _chart_inventory_vs_compraventas(indicators, macro)

    # Chart 3: Sales speed evolution
    with tab3:
        _chart_sales_speed(indicators)

    # Chart 4: Segmentation by zone
    with tab4:
        _chart_zone_segmentation(indicators)


def _chart_prices_vs_euribor(indicators: dict, macro: dict):
    """Chart: Internal price median vs Euribor (dual Y axis)."""
    
    price_series = indicators["price_trend"].get("series", [])
    euribor_series = macro.get("euribor", {}).get("series", [])
    
    if not price_series:
        st.info("No hay suficientes datos de precios para mostrar este gráfico.")
        return
    
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # Price line
    fig.add_trace(
        go.Scatter(
            x=[p["week_start"] for p in price_series],
            y=[p["median_price"] for p in price_series],
            name="Precio Mediano",
            line=dict(color="#4fc3f7", width=3),
            mode="lines+markers",
            marker=dict(size=8)
        ),
        secondary_y=False
    )
    
    # €/m² line
    fig.add_trace(
        go.Scatter(
            x=[p["week_start"] for p in price_series],
            y=[p["median_price_sqm"] for p in price_series],
            name="€/m² Mediano",
            line=dict(color="#81c784", width=2, dash="dot"),
            mode="lines+markers",
            marker=dict(size=6),
            visible="legendonly"
        ),
        secondary_y=False
    )
    
    # Euribor line (secondary axis)
    if euribor_series:
        fig.add_trace(
            go.Scatter(
                x=[e["date_str"] for e in euribor_series],
                y=[e["value"] for e in euribor_series],
                name="Euríbor 12M",
                line=dict(color="#ff7043", width=2, dash="dash"),
                mode="lines+markers",
                marker=dict(size=6)
            ),
            secondary_y=True
        )
    
    fig.update_layout(
        title="Evolución Precio Mediano vs Euríbor",
        height=450,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)"
    )
    fig.update_xaxes(title_text="Fecha")
    fig.update_yaxes(title_text="Precio (€)", secondary_y=False, tickformat=",")
    fig.update_yaxes(title_text="Euríbor (%)", secondary_y=True)
    
    st.plotly_chart(fig, use_container_width=True)


def _chart_inventory_vs_compraventas(indicators: dict, macro: dict):
    """Chart: Active inventory vs official transactions."""
    
    inv_series = indicators["inventory"].get("series", [])
    compra_series = macro.get("compraventas", {}).get("series", [])
    
    if not inv_series:
        st.info("No hay suficientes datos de inventario para mostrar este gráfico.")
        return
    
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # Inventory (bar)
    fig.add_trace(
        go.Bar(
            x=[p["date"] for p in inv_series],
            y=[p["count"] for p in inv_series],
            name="Inventario Activo",
            marker_color="rgba(79, 195, 247, 0.6)",
            marker_line_color="#4fc3f7",
            marker_line_width=1
        ),
        secondary_y=False
    )
    
    # Compraventas (line, secondary)
    if compra_series:
        fig.add_trace(
            go.Scatter(
                x=[c["date_str"] for c in compra_series],
                y=[c["value"] for c in compra_series],
                name="Compraventas Madrid (INE)",
                line=dict(color="#ffb74d", width=3),
                mode="lines+markers",
                marker=dict(size=8)
            ),
            secondary_y=True
        )
    
    fig.update_layout(
        title="Inventario Activo vs Compraventas Oficiales (INE)",
        height=450,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)"
    )
    fig.update_xaxes(title_text="Fecha")
    fig.update_yaxes(title_text="Propiedades Activas", secondary_y=False, tickformat=",")
    fig.update_yaxes(title_text="Compraventas (INE)", secondary_y=True, tickformat=",")
    
    st.plotly_chart(fig, use_container_width=True)


def _chart_sales_speed(indicators: dict):
    """Chart: Sales speed evolution."""
    
    speed_series = indicators["sales_speed"].get("series", [])
    sd_series = indicators["supply_demand"].get("series", [])
    
    if not speed_series:
        st.info("No hay suficientes datos de ventas para mostrar este gráfico.")
        return
    
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # Median days on market
    fig.add_trace(
        go.Bar(
            x=[p["week_start"] for p in speed_series],
            y=[p["median_days"] for p in speed_series],
            name="Mediana días en mercado",
            marker_color="rgba(129, 199, 132, 0.7)",
            marker_line_color="#81c784",
            marker_line_width=1,
            text=[f"{p['sold_count']} ventas" for p in speed_series],
            textposition="outside"
        ),
        secondary_y=False
    )
    
    # Supply/demand ratio (line)
    if sd_series:
        fig.add_trace(
            go.Scatter(
                x=[p["week_start"] for p in sd_series],
                y=[min(p["ratio"], 10) for p in sd_series],  # Cap at 10 for readability
                name="Ratio O/D",
                line=dict(color="#ef5350", width=2, dash="dash"),
                mode="lines+markers",
                marker=dict(size=8)
            ),
            secondary_y=True
        )
    
    fig.update_layout(
        title="Velocidad de Venta y Ratio Oferta/Demanda",
        height=450,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)"
    )
    fig.update_xaxes(title_text="Semana")
    fig.update_yaxes(title_text="Días en mercado", secondary_y=False)
    fig.update_yaxes(title_text="Ratio O/D", secondary_y=True)
    
    st.plotly_chart(fig, use_container_width=True)


def _chart_zone_segmentation(indicators: dict):
    """Render price and sales speed segmentation by district/barrio."""

    from market_indicators import get_price_by_zone, get_sales_speed_by_zone

    zone_type = st.radio(
        "Nivel de detalle",
        options=["district", "barrio"],
        format_func=lambda x: "Distrito" if x == "district" else "Barrio",
        horizontal=True
    )

    col_a, col_b = st.columns(2)

    # Price by zone
    with col_a:
        data = get_price_by_zone(zone_type=zone_type, top_n=15)
        zones = data.get("zones", [])
        if zones:
            fig = go.Figure(go.Bar(
                y=[z["zone"] for z in zones],
                x=[z["median_price"] for z in zones],
                orientation="h",
                marker_color="rgba(79, 195, 247, 0.75)",
                marker_line_color="#4fc3f7",
                marker_line_width=1,
                text=[f"€{z['median_price']:,}" for z in zones],
                textposition="outside",
                customdata=[[z["count"], z["median_price_sqm"] or 0] for z in zones],
                hovertemplate=(
                    "<b>%{y}</b><br>"
                    "Mediana: €%{x:,.0f}<br>"
                    "€/m²: €%{customdata[1]:,.0f}<br>"
                    "Propiedades: %{customdata[0]}<extra></extra>"
                )
            ))
            fig.update_layout(
                title=f"Precio Mediano por {'Distrito' if zone_type == 'district' else 'Barrio'}",
                height=max(350, len(zones) * 30),
                xaxis_title="Precio Mediano (€)",
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=10, r=80, t=40, b=40)
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Sin datos de zona disponibles.")

    # Sales speed by zone
    with col_b:
        data = get_sales_speed_by_zone(zone_type=zone_type)
        zones = data.get("zones", [])
        if zones:
            # Color by speed: green = fast, red = slow
            max_days = max(z["median_days"] for z in zones) or 1
            colors = [
                f"rgba({int(255 * z['median_days'] / max_days)}, "
                f"{int(255 * (1 - z['median_days'] / max_days))}, 80, 0.75)"
                for z in zones
            ]
            fig = go.Figure(go.Bar(
                y=[z["zone"] for z in zones],
                x=[z["median_days"] for z in zones],
                orientation="h",
                marker_color=colors,
                text=[f"{z['median_days']:.0f}d" for z in zones],
                textposition="outside",
                customdata=[[z["count"]] for z in zones],
                hovertemplate=(
                    "<b>%{y}</b><br>"
                    "Mediana: %{x:.1f} días<br>"
                    "Ventas: %{customdata[0]}<extra></extra>"
                )
            ))
            fig.update_layout(
                title=f"Velocidad Venta por {'Distrito' if zone_type == 'district' else 'Barrio'}",
                height=max(350, len(zones) * 30),
                xaxis_title="Días en mercado (mediana)",
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=10, r=60, t=40, b=40)
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Sin datos de velocidad de venta por zona.")

    if data.get("error"):
        st.caption(f"ℹ️ {data['error']}")

    # ── Rental yield chart ──────────────────────────────────────────────────
    st.markdown("---")
    st.subheader("🏘️ Rentabilidad Bruta por Barrio (Top 20)")

    ry = indicators.get("rental_yield", {})
    all_yields = ry.get("all_yields", [])

    if all_yields:
        top20 = all_yields[:20]          # already sorted desc by yield_pct

        # Color gradient: green (high yield) → red (low yield)
        max_y = top20[0]["yield_pct"] or 1
        min_y = top20[-1]["yield_pct"] or 0

        def _yield_color(y):
            ratio = (y - min_y) / (max_y - min_y) if max_y != min_y else 0.5
            r = int(255 * (1 - ratio))
            g = int(200 * ratio)
            return f"rgba({r},{g},80,0.80)"

        colors = [_yield_color(z["yield_pct"]) for z in top20]
        labels_y = [f"{z['barrio']} ({z['distrito'][:4]})" for z in top20]

        fig_ry = go.Figure(go.Bar(
            y=labels_y,
            x=[z["yield_pct"] for z in top20],
            orientation="h",
            marker_color=colors,
            text=[f"{z['yield_pct']:.1f}%" for z in top20],
            textposition="outside",
            customdata=[[z["median_rent"], z["median_sale_price"], z["rental_listing_count"]]
                        for z in top20],
            hovertemplate=(
                "<b>%{y}</b><br>"
                "Rentabilidad: %{x:.2f}%<br>"
                "Alquiler mediano: €%{customdata[0]:,.0f}/mes<br>"
                "Venta mediana: €%{customdata[1]:,.0f}<br>"
                "Anuncios alquiler: %{customdata[2]}<extra></extra>"
            ),
        ))
        fig_ry.update_layout(
            height=max(400, len(top20) * 28),
            xaxis_title="Rentabilidad bruta (%)",
            xaxis=dict(range=[0, max_y * 1.15]),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=10, r=70, t=20, b=40),
        )
        st.plotly_chart(fig_ry, use_container_width=True)

        # Summary table (top 10)
        st.caption("Top 10 barrios por rentabilidad bruta")
        import pandas as pd
        df_ry = pd.DataFrame(top20[:10])[[
            "barrio", "distrito", "yield_pct", "median_rent", "median_sale_price",
            "rental_listing_count", "date_recorded"
        ]]
        df_ry.columns = [
            "Barrio", "Distrito", "Rent. Bruta (%)", "Alquiler Mediano (€/mes)",
            "Venta Mediana (€)", "Anuncios Alquiler", "Fecha Datos"
        ]
        st.dataframe(df_ry, use_container_width=True, hide_index=True)
    else:
        st.info(
            "📭 Sin datos de rentabilidad aún. "
            "Se calculan automáticamente con cada ejecución del scraper (python scraper.py)."
        )

    # ── Rental yield history chart ──────────────────────────────────────────
    st.markdown("---")
    st.subheader("📈 Evolución Semanal de Rentabilidad Bruta Media")

    from database import get_rental_yield_history
    import pandas as pd

    yield_history = get_rental_yield_history(weeks=12)

    if len(yield_history) >= 2:
        df_yh = pd.DataFrame(yield_history)
        df_yh["week_start"] = pd.to_datetime(df_yh["week_start"])

        # Determine color for trend arrow
        first_val = df_yh["avg_yield_pct"].iloc[0]
        last_val  = df_yh["avg_yield_pct"].iloc[-1]
        trend_delta = last_val - first_val
        trend_arrow = "📈" if trend_delta > 0.1 else ("📉" if trend_delta < -0.1 else "➡️")
        trend_color = "#1b7f3a" if trend_delta > 0.1 else ("#c0392b" if trend_delta < -0.1 else "#e69c1a")

        # Line chart
        fig_yh = go.Figure()
        fig_yh.add_trace(go.Scatter(
            x=df_yh["week_start"],
            y=df_yh["avg_yield_pct"],
            mode="lines+markers",
            name="Yield bruto medio",
            line=dict(color="#2e9e52", width=2.5),
            marker=dict(size=7, color="#2e9e52"),
            fill="tozeroy",
            fillcolor="rgba(46,158,82,0.08)",
            customdata=df_yh[["barrio_count"]].values,
            hovertemplate=(
                "<b>Semana %{x|%d %b %Y}</b><br>"
                "Yield bruto: %{y:.2f}%<br>"
                "Barrios con datos: %{customdata[0]}<extra></extra>"
            ),
        ))

        # Reference lines at 3.5% and 5%
        fig_yh.add_hline(
            y=5.0, line_dash="dot", line_color="rgba(27,127,58,0.5)",
            annotation_text="5% (alto)", annotation_position="bottom right"
        )
        fig_yh.add_hline(
            y=3.5, line_dash="dot", line_color="rgba(230,156,26,0.5)",
            annotation_text="3.5% (mínimo)", annotation_position="bottom right"
        )

        fig_yh.update_layout(
            height=320,
            yaxis_title="Rentabilidad bruta (%)",
            yaxis=dict(rangemode="tozero"),
            xaxis_title=None,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=10, r=20, t=20, b=40),
            legend=dict(orientation="h", y=1.05),
        )
        st.plotly_chart(fig_yh, use_container_width=True)

        # Summary caption
        st.caption(
            f"{trend_arrow} Rentabilidad actual: **{last_val:.2f}%** "
            f"({trend_delta:+.2f} pp vs hace 12 semanas) — "
            f"Media del período: {df_yh['avg_yield_pct'].mean():.2f}%"
        )
    elif len(yield_history) == 1:
        st.info(
            f"📊 Solo hay datos de una semana ({yield_history[0]['avg_yield_pct']:.2f}%). "
            "El gráfico de evolución se activa con ≥ 2 semanas de datos."
        )
    else:
        st.info(
            "📭 Sin histórico de rentabilidades todavía. "
            "Aparecerá aquí después de varias ejecuciones del scraper."
        )


def _render_score_breakdown(score: dict):
    """Render market score breakdown."""
    
    with st.expander("🔍 Desglose del Score de Mercado"):
        st.markdown("Cada componente contribuye al score total con un peso diferente:")
        
        components = score.get("components", {})
        weights = score.get("weights", {})
        
        labels = {
            "prices":        "💰 Precios",
            "speed":         "⏱️ Velocidad Venta",
            "supply_demand": "⚖️ Oferta/Demanda",
            "affordability": "🏠 Asequibilidad",
            "euribor":       "🏦 Euríbor + Tendencia",
            "price_drops":   "📉 Estrés Vendedor",
            "notarial_gap":  "🏛️ Sobreprecio vs Notarial",
            "employment":    "👥 Empleo",
        }

        # Progress bars for each component
        for key in ["prices", "speed", "supply_demand", "affordability", "euribor", "price_drops", "notarial_gap", "employment"]:
            component_score = components.get(key, 50)
            weight = weights.get(key, 0)
            weighted = component_score * weight
            
            col1, col2, col3 = st.columns([3, 1, 1])
            with col1:
                st.progress(component_score / 100, text=f"{labels.get(key, key)}: {component_score}/100")
            with col2:
                st.caption(f"Peso: {weight*100:.0f}%")
            with col3:
                st.caption(f"Aporte: {weighted:.1f}")
        
        st.markdown(f"**Score Total: {score['score']:.1f}/100**")
        
        # Gauge chart
        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=score['score'],
            domain={'x': [0, 1], 'y': [0, 1]},
            title={'text': "Score del Mercado"},
            gauge={
                'axis': {'range': [0, 100]},
                'bar': {'color': "#4fc3f7"},
                'steps': [
                    {'range': [0, 40], 'color': "rgba(244, 67, 54, 0.3)"},
                    {'range': [40, 75], 'color': "rgba(255, 235, 59, 0.3)"},
                    {'range': [75, 100], 'color': "rgba(76, 175, 80, 0.3)"}
                ],
                'threshold': {
                    'line': {'color': "white", 'width': 4},
                    'thickness': 0.75,
                    'value': score['score']
                }
            }
        ))
        
        fig.update_layout(
            height=300,
            paper_bgcolor="rgba(0,0,0,0)",
            font={'color': "#e0e0e0"}
        )
        
        st.plotly_chart(fig, use_container_width=True)
