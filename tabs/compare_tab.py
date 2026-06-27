"""⚖️ Comparador de propiedades — pon 2-4 pisos lado a lado.

Pensado para quien busca piso **para vivir** (no invertir): compara precio,
€/m², tamaño, habitaciones, planta, margen de negociación y posición frente a
la mediana del barrio, con una tabla lado a lado, un radar y un mini-mapa.

La lógica de puntuación (``compute_radar_scores``) y la tabla
(``build_comparison_table``) son funciones puras y testeables; el render de
Streamlit las consume.
"""

from __future__ import annotations

from typing import Dict, List, Optional

import pandas as pd
import streamlit as st

from coordinates import get_barrio_coordinates


# Radar axes, home-buyer oriented. Each entry: (label, listing-key, higher_better).
# higher_better=False means a lower raw value scores higher (cheaper = better).
_RADAR_AXES = [
    ("Precio",         "price",          False),
    ("€/m²",           "price_per_sqm",  False),
    ("Tamaño",         "size_sqm",       True),
    ("Habitaciones",   "rooms",          True),
    ("Margen negoc.",  "days_on_market", True),   # más días = más margen para el comprador
]

MAX_COMPARE = 4
MIN_COMPARE = 2


def _to_float(v) -> Optional[float]:
    try:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def compute_radar_scores(selected: List[Dict]) -> Dict[str, List[float]]:
    """
    Min-max normalise each radar axis to 0-100 across the *selected* set,
    honouring direction (cheaper price → higher score). Returns a dict keyed
    by axis label → list of scores (one per property, in order). When every
    property shares a value on an axis (or all are missing), that axis scores
    50 for all (neutral), so the radar stays readable.
    """
    scores: Dict[str, List[float]] = {}
    n = len(selected)
    for label, key, higher_better in _RADAR_AXES:
        raw = [_to_float(p.get(key)) for p in selected]
        present = [v for v in raw if v is not None]
        if not present or min(present) == max(present):
            scores[label] = [50.0] * n
            continue
        lo, hi = min(present), max(present)
        span = hi - lo
        axis_scores = []
        for v in raw:
            if v is None:
                axis_scores.append(50.0)  # neutral for missing
            else:
                pct = (v - lo) / span * 100.0
                axis_scores.append(round(pct if higher_better else 100.0 - pct, 1))
        scores[label] = axis_scores
    return scores


def build_comparison_table(
    selected: List[Dict], barrio_stats: Optional[Dict] = None
) -> pd.DataFrame:
    """
    Side-by-side table: attributes as rows, properties as columns. Includes a
    'vs mediana barrio' row when ``barrio_stats`` is available (the % the €/m²
    sits above/below the barrio's average — negative = cheaper than barrio).
    """
    barrio_stats = barrio_stats or {}

    def _label(p: Dict) -> str:
        barrio = p.get("barrio") or p.get("distrito") or "?"
        price = _to_float(p.get("price"))
        size = _to_float(p.get("size_sqm"))
        bits = [str(barrio)]
        if price:
            bits.append(f"€{price:,.0f}")
        if size:
            bits.append(f"{size:.0f}m²")
        return " · ".join(bits)

    def _vs_barrio(p: Dict) -> Optional[float]:
        sqm = _to_float(p.get("price_per_sqm"))
        barrio = p.get("barrio")
        st_ = barrio_stats.get(barrio) if barrio else None
        avg = st_.get("median_price_sqm") if st_ else None
        if sqm and avg and avg > 0:
            return round((sqm / avg - 1) * 100, 1)
        return None

    rows = {
        "Precio": [f"€{_to_float(p.get('price')):,.0f}" if _to_float(p.get('price')) else "—" for p in selected],
        "€/m²": [f"€{_to_float(p.get('price_per_sqm')):,.0f}" if _to_float(p.get('price_per_sqm')) else "—" for p in selected],
        "vs mediana barrio": [f"{v:+.1f}%" if (v := _vs_barrio(p)) is not None else "—" for p in selected],
        "Tamaño": [f"{_to_float(p.get('size_sqm')):.0f} m²" if _to_float(p.get('size_sqm')) else "—" for p in selected],
        "Habitaciones": [int(_to_float(p.get('rooms'))) if _to_float(p.get('rooms')) else "—" for p in selected],
        "Planta": [p.get("floor") or "—" for p in selected],
        "Distrito": [p.get("distrito") or "—" for p in selected],
        "Barrio": [p.get("barrio") or "—" for p in selected],
        "Días en mercado": [int(_to_float(p.get('days_on_market'))) if _to_float(p.get('days_on_market')) is not None else "—" for p in selected],
        "Vendedor": [p.get("seller_type") or "—" for p in selected],
        "Enlace": [p.get("url") or "—" for p in selected],
    }
    return pd.DataFrame(rows, index=[_label(p) for p in selected]).T


def _render_radar(selected: List[Dict]) -> None:
    import plotly.graph_objects as go

    scores = compute_radar_scores(selected)
    axes = list(scores.keys())
    fig = go.Figure()
    for i, p in enumerate(selected):
        vals = [scores[a][i] for a in axes]
        name = f"{p.get('barrio') or p.get('distrito') or '?'} · €{_to_float(p.get('price')) or 0:,.0f}"
        fig.add_trace(go.Scatterpolar(
            r=vals + [vals[0]],           # close the polygon
            theta=axes + [axes[0]],
            fill="toself",
            name=name,
        ))
    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
        showlegend=True,
        height=450,
        margin=dict(l=40, r=40, t=30, b=30),
    )
    st.plotly_chart(fig, use_container_width=True)
    st.caption(
        "Radar normalizado entre los pisos seleccionados (0-100). Más afuera = "
        "mejor para el comprador: más barato, más grande, más habitaciones, más "
        "margen de negociación. No es calidad absoluta, es comparación relativa."
    )


def _render_map(selected: List[Dict]) -> None:
    pts = []
    for p in selected:
        lat, lon = get_barrio_coordinates(p.get("distrito"), p.get("barrio"))
        pts.append({"lat": lat, "lon": lon})
    if pts:
        st.map(pd.DataFrame(pts), use_container_width=True)
        st.caption("Ubicación aproximada (centroide del barrio).")


def render_compare_tab(df: pd.DataFrame) -> None:
    st.header("⚖️ Comparador de Propiedades")
    st.caption("Selecciona entre 2 y 4 pisos para verlos lado a lado.")

    if df is None or df.empty:
        st.info("No hay propiedades que cumplan los filtros. Ajusta el sidebar.")
        return

    df = df.copy()
    df["listing_id"] = df["listing_id"].astype(str)

    def _option_label(lid: str) -> str:
        r = df[df["listing_id"] == lid].iloc[0]
        barrio = r.get("barrio") or r.get("distrito") or "?"
        price = _to_float(r.get("price"))
        size = _to_float(r.get("size_sqm"))
        parts = [str(barrio)]
        if price:
            parts.append(f"€{price:,.0f}")
        if size:
            parts.append(f"{size:.0f}m²")
        return " · ".join(parts)

    # Optional: preload the watchlist as candidates.
    candidate_ids = list(df["listing_id"])
    if st.checkbox("Comparar solo mis seguimientos", key="cmp_only_watchlist"):
        try:
            from database import get_watchlist_ids
            wl = {str(x) for x in get_watchlist_ids()}
            candidate_ids = [i for i in candidate_ids if i in wl]
            if not candidate_ids:
                st.info("No tienes seguimientos dentro de los filtros actuales.")
                return
        except Exception:
            st.warning("No se pudo cargar la lista de seguimientos.")

    selected_ids = st.multiselect(
        "Pisos a comparar (máx. 4)",
        options=candidate_ids,
        format_func=_option_label,
        max_selections=MAX_COMPARE,
        key="cmp_selected",
    )

    if len(selected_ids) < MIN_COMPARE:
        st.info(f"Selecciona al menos {MIN_COMPARE} pisos para comparar.")
        return

    selected = [df[df["listing_id"] == i].iloc[0].to_dict() for i in selected_ids]

    try:
        from database import get_barrio_price_stats
        barrio_stats = get_barrio_price_stats()
    except Exception:
        barrio_stats = {}

    st.subheader("📋 Tabla comparativa")
    table = build_comparison_table(selected, barrio_stats)
    st.dataframe(table, use_container_width=True)

    col_radar, col_map = st.columns([3, 2])
    with col_radar:
        st.subheader("🕸️ Radar comparativo")
        _render_radar(selected)
    with col_map:
        st.subheader("🗺️ Ubicación")
        _render_map(selected)
