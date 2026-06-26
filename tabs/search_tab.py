"""
Search tab — personalised property search with configurable filters,
fair-price indicator and opportunity score.

Entry point: render_search_tab()

Opportunity score (0-100):
  40 % — Price vs barrio median €/m²   (below median = better)
  30 % — Days on market                (longer = more room to negotiate)
  30 % — Number of price drops         (more drops = motivated seller)
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime


# ── Opportunity score helpers ─────────────────────────────────────────────────

def _price_score(vs_barrio_pct: float) -> float:
    pct = max(-20.0, min(20.0, vs_barrio_pct))
    return round(((-pct + 20) / 40) * 100)

def _days_score(days: int) -> float:
    if days < 30:  return 10
    if days < 60:  return 30
    if days < 90:  return 55
    if days < 180: return 75
    return 95

def _drops_score(drops: int) -> float:
    if drops == 0: return 0
    if drops == 1: return 40
    if drops == 2: return 70
    return 100

def compute_opportunity_score(vs_barrio_pct: float, days: int, drops: int) -> int:
    return int(round(
        0.40 * _price_score(vs_barrio_pct) +
        0.30 * _days_score(days) +
        0.30 * _drops_score(drops)
    ))

def score_badge(score: int) -> str:
    if score >= 70: return f"🟢 {score}"
    if score >= 40: return f"🟡 {score}"
    return f"🔴 {score}"


# ── Main render ───────────────────────────────────────────────────────────────

@st.fragment
def render_search_tab() -> None:
    """Render all content for the 🔍 Mis Búsquedas tab.

    Wrapped in @st.fragment so that interactions with the search filters
    only re-render this section, not the entire page.
    """

    st.subheader("🔍 Mis Búsquedas Personalizadas")

    ALL_DISTRICTS = [
        "Arganzuela", "Barajas", "Carabanchel", "Centro", "Chamartín",
        "Chamberí", "Ciudad Lineal", "Fuencarral-El Pardo", "Hortaleza",
        "Latina", "Moncloa-Aravaca", "Moratalaz", "Puente de Vallecas",
        "Retiro", "Salamanca", "San Blas-Canillejas", "Tetuán", "Usera",
        "Vicálvaro", "Villa de Vallecas", "Villaverde",
    ]

    AMENITY_OPTIONS = {
        "has_terraza":            "🌅 Terraza",
        "has_balcon":             "🪟 Balcón",
        "has_garaje":             "🚗 Garaje",
        "has_trastero":           "📦 Trastero",
        "has_piscina":            "🏊 Piscina",
        "has_ascensor":           "🛗 Ascensor",
        "has_portero":            "🧑‍💼 Portero",
        "has_aire_acondicionado": "🌬️ Aire acond.",
        "has_calefaccion":        "🔥 Calefacción",
        "has_armarios":           "🚪 Armarios emp.",
        "has_near_metro":         "🚇 Cerca metro",
        "has_near_parque":        "🌳 Cerca parque",
        "has_near_colegio":       "🏫 Cerca colegio",
        "has_near_hospital":      "🏥 Cerca hospital",
    }

    # ── Configurable filters ──────────────────────────────────────────────────
    with st.expander("⚙️ Filtros de Búsqueda", expanded=True):
        fc1, fc2 = st.columns(2)

        with fc1:
            st.markdown("**💰 Precio**")
            pc1, pc2 = st.columns(2)
            with pc1:
                min_price = st.number_input(
                    "Mínimo (€)", min_value=0, max_value=5_000_000,
                    value=250_000, step=10_000, format="%d",
                )
            with pc2:
                max_price = st.number_input(
                    "Máximo (€)", min_value=0, max_value=5_000_000,
                    value=450_000, step=10_000, format="%d",
                )

            st.markdown("**📐 Superficie**")
            min_sqm = st.slider("Superficie mínima (m²)", 0, 300, 40, step=5)

            st.markdown("**🛏️ Habitaciones**")
            min_rooms = st.slider("Habitaciones mínimas", 0, 10, 0, step=1,
                                  help="0 = sin filtro")

        with fc2:
            st.markdown("**📍 Distritos**")
            selected_districts = st.multiselect(
                "Distritos (vacío = todos)",
                options=ALL_DISTRICTS,
                default=["Centro", "Chamberí", "Moratalaz", "Retiro",
                         "Tetuán", "Arganzuela", "Chamartín", "Salamanca",
                         "Hortaleza"],
            )

            st.markdown("**🏢 Vendedor**")
            seller_filter = st.selectbox(
                "Tipo de vendedor",
                options=["Todos", "Particular", "Agencia"],
            )

            st.markdown("**🚪 Planta y extras**")
            exclude_bajo = st.checkbox("Excluir Bajos", value=True)
            require_elevator = st.checkbox("Requiere ascensor", value=True)

        sort_by = st.selectbox(
            "🔢 Ordenar por",
            options=["score_oportunidad", "price", "price_per_sqm",
                     "vs_barrio_pct", "dias_mercado", "bajadas"],
            format_func=lambda x: {
                "score_oportunidad": "🎯 Score Oportunidad (mejor primero)",
                "price":             "💰 Precio (menor primero)",
                "price_per_sqm":     "📏 €/m² (menor primero)",
                "vs_barrio_pct":     "📊 vs Barrio % (más barato primero)",
                "dias_mercado":      "⏱️ Días en mercado (más antiguo primero)",
                "bajadas":           "📉 Nº bajadas (más bajadas primero)",
            }[x],
        )

        st.markdown("---")
        st.markdown(
            "**🏷️ Características requeridas** "
            "<span style='color:#94a3b8;font-size:12px;'>"
            "el piso debe tener TODAS las seleccionadas</span>",
            unsafe_allow_html=True,
        )
        am_col1, am_col2 = st.columns([4, 1])
        with am_col1:
            required_amenities = st.multiselect(
                "Amenities",
                options=list(AMENITY_OPTIONS.keys()),
                format_func=lambda k: AMENITY_OPTIONS[k],
                default=[],
                label_visibility="collapsed",
                placeholder="Sin filtro — muestra todos",
                help=(
                    "Filtra pisos que tengan TODAS las características seleccionadas. "
                    "Datos extraídos automáticamente de las descripciones (NLP). "
                    "Pisos sin descripción analizada quedan excluidos si se aplica algún filtro."
                ),
            )
        with am_col2:
            min_year = st.number_input(
                "📅 Año constr. mín.",
                min_value=0, max_value=2025, value=0, step=5, format="%d",
                help="0 = sin filtro. Solo aplica a pisos con año de construcción detectado en la descripción.",
            )

    # ── Load & filter ─────────────────────────────────────────────────────────
    from database import (
        get_listings as get_listings_db,
        get_barrio_price_stats,
        get_drop_counts_for_listings,
    )

    raw = get_listings_db(
        status="active",
        distrito=selected_districts if selected_districts else None,
        min_price=min_price if min_price > 0 else None,
        max_price=max_price if max_price < 5_000_000 else None,
        seller_type=seller_filter if seller_filter != "Todos" else None,
    )
    df = pd.DataFrame(raw)

    if df.empty:
        st.warning("No se encontraron inmuebles con los filtros actuales.")
        return

    # Derived columns — use SQL-computed column if available
    if "price_per_sqm" not in df.columns:
        df["price_per_sqm"] = df.apply(
            lambda r: r["price"] / r["size_sqm"]
            if r.get("size_sqm") and r["size_sqm"] > 0 else None,
            axis=1,
        )
    df["floor"]       = df["floor"].fillna("").astype(str)
    df["description"] = df["description"].fillna("").astype(str)
    df["rooms"]       = pd.to_numeric(df["rooms"], errors="coerce")
    df["size_sqm"]    = pd.to_numeric(df["size_sqm"], errors="coerce")

    # Apply optional filters
    if min_sqm > 0:
        df = df[df["size_sqm"].fillna(0) >= min_sqm]

    if min_rooms > 0:
        df = df[df["rooms"].fillna(0) >= min_rooms]

    if exclude_bajo:
        df = df[~df["floor"].str.contains("bajo", case=False, na=False)]

    if require_elevator:
        has_lift  = df["description"].str.contains("ascensor", case=False, na=False)
        not_sin   = ~df["description"].str.contains("sin ascensor", case=False, na=False)
        not_no    = ~df["description"].str.contains("no dispone de ascensor", case=False, na=False)
        df = df[has_lift & not_sin & not_no]

    # ── Amenity filter (listing_amenities table + text fallback) ──────────────
    if required_amenities or min_year > 0:
        try:
            from nlp_analyzer import get_amenities_for_listings, extract_amenities
            _amenities_cache = get_amenities_for_listings(df["listing_id"].tolist())
        except Exception:
            _amenities_cache = {}

        def _get_amenities(row):
            lid = row["listing_id"]
            amen = _amenities_cache.get(lid)
            if amen is None and row.get("description"):
                try:
                    from nlp_analyzer import extract_amenities
                    amen = extract_amenities(row["description"])
                except Exception:
                    amen = {}
            return amen or {}

        def _passes_amenity_filter(row):
            amen = _get_amenities(row)
            if required_amenities and not all(amen.get(k, False) for k in required_amenities):
                return False
            if min_year > 0:
                cy = amen.get("construction_year")
                if cy is None or cy < min_year:
                    return False
            return True

        df = df[df.apply(_passes_amenity_filter, axis=1)]

    if df.empty:
        st.warning("No hay resultados con los filtros aplicados. Prueba a ampliar el rango.")
        return

    # ── Enrich with fair-price and opportunity data ───────────────────────────
    today = datetime.now().date()
    barrio_stats = get_barrio_price_stats(min_listings=5)

    def _vs_barrio(row):
        b, ppsqm = row.get("barrio"), row.get("price_per_sqm")
        if not b or not ppsqm or b not in barrio_stats:
            return None
        median = barrio_stats[b]["median_price_sqm"]
        return round((ppsqm - median) / median * 100, 1) if median else None

    df["vs_barrio_pct"]     = df.apply(_vs_barrio, axis=1)
    df["barrio_median_sqm"] = df["barrio"].map(
        lambda b: barrio_stats.get(b, {}).get("median_price_sqm")
    )

    def _days(row):
        fsd = row.get("first_seen_date")
        if not fsd:
            return None
        try:
            return (today - datetime.strptime(str(fsd), "%Y-%m-%d").date()).days
        except Exception:
            return None

    df["dias_mercado"] = df.apply(_days, axis=1)

    listing_ids = df["listing_id"].tolist()
    drop_counts = get_drop_counts_for_listings(listing_ids)
    df["bajadas"] = df["listing_id"].map(drop_counts).fillna(0).astype(int)

    # ── NLP signals ───────────────────────────────────────────────────────────
    try:
        from nlp_analyzer import get_signals_for_listings, signals_to_badges
        nlp_signals = get_signals_for_listings(listing_ids)
    except Exception:
        nlp_signals = {}

    df["nlp_bonus"]  = df["listing_id"].map(
        lambda lid: nlp_signals.get(lid, {}).get("nlp_bonus", 0)
    )
    df["nlp_badges"] = df["listing_id"].map(
        lambda lid: signals_to_badges(nlp_signals.get(lid, {})) if nlp_signals else ""
    )

    # ── Amenity badges ────────────────────────────────────────────────────────
    try:
        from nlp_analyzer import get_amenities_for_listings, amenities_to_badges
        # Reuse the already-built cache if the amenity filter ran; otherwise load now
        _am_map = (
            _amenities_cache                                    # pylint: disable=used-before-assignment
            if (required_amenities or min_year > 0)
            else get_amenities_for_listings(listing_ids)
        )
        df["amenity_badges"] = df["listing_id"].map(
            lambda lid: " ".join(amenities_to_badges(_am_map.get(lid, {})))
        )
    except Exception:
        df["amenity_badges"] = ""

    def _score(row):
        vs, days, drops = row["vs_barrio_pct"], row["dias_mercado"], row["bajadas"]
        if vs is None or days is None:
            return None
        base = compute_opportunity_score(vs, days, drops)
        bonus = int(row.get("nlp_bonus", 0))
        return min(100, base + bonus)

    df["score_oportunidad"] = df.apply(_score, axis=1)

    # Sort
    ascending = sort_by in ("price", "price_per_sqm", "vs_barrio_pct")
    df = df.sort_values(sort_by, ascending=ascending, na_position="last")

    # ── Summary metrics ───────────────────────────────────────────────────────
    st.success(f"**{len(df)}** inmuebles encontrados.")

    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.metric("Precio Medio", f"€{df['price'].mean():,.0f}")
    with m2:
        avg_sqm = df["price_per_sqm"].dropna().mean()
        st.metric("€/m² Medio", f"€{avg_sqm:,.0f}" if pd.notna(avg_sqm) else "N/A")
    with m3:
        st.metric("Tamaño Medio", f"{df['size_sqm'].mean():.0f} m²")
    with m4:
        top = df["score_oportunidad"].dropna()
        st.metric("Mejor Score", f"{int(top.iloc[0])}/100" if not top.empty else "N/A",
                  help="Score de oportunidad del primer resultado")

    # ── Score legend ──────────────────────────────────────────────────────────
    with st.expander("ℹ️ Cómo se calcula el Score de Oportunidad"):
        st.markdown("""
| Componente | Peso | Lógica |
|---|---|---|
| 💰 Precio vs barrio | 40% | Cuánto más barato que la mediana €/m² del barrio |
| ⏱️ Días en mercado | 30% | Más tiempo → mayor poder de negociación |
| 📉 Bajadas de precio | 30% | Más bajadas → vendedor más motivado |

🟢 ≥70 — Oportunidad clara · 🟡 40–69 — Interesante · 🔴 <40 — Precio de mercado o caro
        """)

    # ── Results table ─────────────────────────────────────────────────────────
    display_df = df[[
        "listing_id", "title", "price", "price_per_sqm",
        "barrio_median_sqm", "vs_barrio_pct",
        "distrito", "barrio", "size_sqm", "rooms",
        "dias_mercado", "bajadas", "score_oportunidad",
        "amenity_badges", "nlp_badges", "floor", "seller_type", "url",
    ]].copy()

    display_df["score_oportunidad"] = display_df["score_oportunidad"].apply(
        lambda s: score_badge(int(s)) if pd.notna(s) else "—"
    )
    display_df["vs_barrio_pct"] = display_df["vs_barrio_pct"].apply(
        lambda v: f"{v:+.1f}%" if pd.notna(v) else "—"
    )

    st.dataframe(
        display_df,
        use_container_width=True,
        column_config={
            "listing_id":        st.column_config.TextColumn("ID"),
            "title":             st.column_config.TextColumn("Título"),
            "price":             st.column_config.NumberColumn("Precio", format="€%d"),
            "price_per_sqm":     st.column_config.NumberColumn("€/m²", format="€%.0f"),
            "barrio_median_sqm": st.column_config.NumberColumn("Mediana barrio", format="€%.0f"),
            "vs_barrio_pct":     st.column_config.TextColumn("vs Barrio",
                                     help="% sobre/bajo la mediana €/m² del barrio"),
            "distrito":          st.column_config.TextColumn("Distrito"),
            "barrio":            st.column_config.TextColumn("Barrio"),
            "size_sqm":          st.column_config.NumberColumn("m²", format="%d m²"),
            "rooms":             st.column_config.NumberColumn("Hab.", format="%d"),
            "dias_mercado":      st.column_config.NumberColumn("Días"),
            "bajadas":           st.column_config.NumberColumn("Bajadas"),
            "score_oportunidad": st.column_config.TextColumn("🎯 Score",
                                     help="Score de oportunidad 0-100"),
            "floor":             st.column_config.TextColumn("Planta"),
            "seller_type":       st.column_config.TextColumn("Vendedor"),
            "amenity_badges":     st.column_config.TextColumn("🏷️ Amenities",
                                     help="Características detectadas en la descripción (NLP)"),
            "nlp_badges":        st.column_config.TextColumn("🔍 Señales",
                                     help="Señales del vendedor detectadas en la descripción"),
            "url":               st.column_config.LinkColumn("Enlace"),
        },
        hide_index=True,
    )

    # ── Descargar resultados (CSV / Excel) ────────────────────────────────────
    export_df = df[[
        "listing_id", "title", "price", "price_per_sqm",
        "barrio_median_sqm", "vs_barrio_pct", "distrito", "barrio",
        "size_sqm", "rooms", "dias_mercado", "bajadas", "score_oportunidad",
        "floor", "seller_type", "url",
    ]].rename(columns={
        "listing_id": "ID", "title": "Título", "price": "Precio (€)",
        "price_per_sqm": "€/m²", "barrio_median_sqm": "Mediana barrio €/m²",
        "vs_barrio_pct": "vs Barrio %", "distrito": "Distrito", "barrio": "Barrio",
        "size_sqm": "m²", "rooms": "Habitaciones", "dias_mercado": "Días en mercado",
        "bajadas": "Bajadas", "score_oportunidad": "Score Oportunidad",
        "floor": "Planta", "seller_type": "Vendedor", "url": "Enlace",
    })

    ts = datetime.now().strftime("%Y%m%d")
    dl1, dl2 = st.columns(2)
    with dl1:
        st.download_button(
            "⬇️ Descargar CSV",
            data=export_df.to_csv(index=False).encode("utf-8-sig"),
            file_name=f"busqueda_madrid_{ts}.csv",
            mime="text/csv",
            use_container_width=True,
            help="Descarga los inmuebles filtrados (todos, no solo los visibles).",
        )
    with dl2:
        try:
            import io
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                export_df.to_excel(writer, index=False, sheet_name="Búsqueda")
            st.download_button(
                "⬇️ Descargar Excel",
                data=buf.getvalue(),
                file_name=f"busqueda_madrid_{ts}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
        except Exception:
            st.caption("Exporta en Excel no disponible (falta openpyxl).")

    # ── Top 5 oportunidades ───────────────────────────────────────────────────
    from database import add_to_watchlist, remove_from_watchlist, get_watchlist_ids
    watchlist_ids = get_watchlist_ids()

    top5 = df[df["score_oportunidad"].notna()].head(5)
    if not top5.empty:
        st.markdown("### 🏆 Top 5 Oportunidades")
        for _, row in top5.iterrows():
            score = int(row["score_oportunidad"])
            badge = "🟢" if score >= 70 else ("🟡" if score >= 40 else "🔴")
            vs    = f"{row['vs_barrio_pct']:+.1f}%" if pd.notna(row["vs_barrio_pct"]) else "—"
            days  = int(row["dias_mercado"]) if pd.notna(row["dias_mercado"]) else "—"
            rooms_str = f"{int(row['rooms'])} hab · " if pd.notna(row.get("rooms")) else ""
            lid   = row["listing_id"]
            saved = lid in watchlist_ids

            nlp_str = row.get("nlp_badges", "")

            col_a, col_b, col_c = st.columns([4, 1, 1])
            with col_a:
                st.markdown(
                    f"**{badge} [{row['title'][:65]}]({row['url']})**  \n"
                    f"€{row['price']:,} · {row['barrio']}, {row['distrito']} · "
                    f"{rooms_str}{row['size_sqm']:.0f} m² · "
                    f"{vs} vs barrio · {days} días · {int(row['bajadas'])} bajadas"
                    + (f"  \n{nlp_str}" if nlp_str else "")
                )
            with col_b:
                st.metric("Score", f"{score}/100")
            with col_c:
                btn_label = "⭐ Guardado" if saved else "☆ Guardar"
                btn_type  = "secondary" if saved else "primary"
                if st.button(btn_label, key=f"wl_top_{lid}", type=btn_type, use_container_width=True):
                    if saved:
                        remove_from_watchlist(lid)
                    else:
                        add_to_watchlist(lid)
                    st.rerun()

    # ── Price evolution chart ─────────────────────────────────────────────────
    st.markdown("### 📉 Seguimiento de Precios")

    from database import get_price_history_for_listings

    try:
        price_histories = get_price_history_for_listings(listing_ids)
        if price_histories:
            ph_df = pd.DataFrame(price_histories)
            ph_df["date"] = pd.to_datetime(ph_df["date"])

            daily_avg = (
                ph_df.groupby("date")
                .agg(avg_price=("new_price", "mean"), count=("listing_id", "nunique"))
                .reset_index()
                .sort_values("date")
            )

            if len(daily_avg) > 1:
                fig_evol = go.Figure()
                fig_evol.add_trace(go.Scatter(
                    x=daily_avg["date"], y=daily_avg["avg_price"],
                    mode="lines+markers", name="Precio Medio",
                    line=dict(color="#8e44ad", width=3), marker=dict(size=6),
                    hovertemplate="<b>%{x|%d/%m/%Y}</b><br>Precio: %{y:,.0f} €<extra></extra>",
                ))
                fig_evol.update_layout(
                    title="Evolución Precio Medio (resultados filtrados)",
                    xaxis_title="Fecha", yaxis_title="Precio (€)",
                    hovermode="x unified", height=380,
                )
                st.plotly_chart(fig_evol, use_container_width=True)

            # Bajadas recientes
            st.markdown("#### 📉 Bajadas de Precio Recientes")
            drops_df = ph_df[ph_df["price_change"] < 0].copy()
            if not drops_df.empty:
                drops_df = drops_df.sort_values("date", ascending=False).head(20)
                drops_df["price_change_fmt"] = drops_df["price_change"].apply(
                    lambda x: f"€{x:,.0f}"
                )
                drops_df["pct"] = drops_df.apply(
                    lambda r: f"{(r['price_change'] / (r['new_price'] - r['price_change'])) * 100:.1f}%"
                    if (r["new_price"] - r["price_change"]) != 0 else "N/A",
                    axis=1,
                )
                st.dataframe(
                    drops_df[["listing_id", "date", "new_price", "price_change_fmt", "pct"]],
                    column_config={
                        "new_price":        st.column_config.NumberColumn("Nuevo Precio", format="€%d"),
                        "price_change_fmt": "Cambio",
                        "pct":              "% Cambio",
                        "date":             "Fecha",
                    },
                    hide_index=True,
                )
            else:
                st.info("No hay bajadas de precio recientes para estos inmuebles.")
        else:
            st.info("No hay historial de precios disponible para los resultados actuales.")
    except Exception:
        st.info("No hay historial de precios disponible.")
