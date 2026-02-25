"""
Search tab — personalised property search with price tracking,
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
    """
    Score based on how cheap a property is vs its barrio median price/m².
    vs_barrio_pct < 0 → below median (good for buyer).
    """
    # Linear mapping: -20 % or better → 100 pts, +20 % or worse → 0 pts
    pct = max(-20.0, min(20.0, vs_barrio_pct))
    return round(((-pct + 20) / 40) * 100)


def _days_score(days: int) -> float:
    """Score based on days on market. Longer = more negotiating power."""
    if days < 30:   return 10
    if days < 60:   return 30
    if days < 90:   return 55
    if days < 180:  return 75
    return 95


def _drops_score(drops: int) -> float:
    """Score based on number of price drops. More drops = motivated seller."""
    if drops == 0: return 0
    if drops == 1: return 40
    if drops == 2: return 70
    return 100


def compute_opportunity_score(vs_barrio_pct: float, days: int, drops: int) -> int:
    """Weighted 0-100 opportunity score."""
    score = (
        0.40 * _price_score(vs_barrio_pct) +
        0.30 * _days_score(days) +
        0.30 * _drops_score(drops)
    )
    return int(round(score))


def score_badge(score: int) -> str:
    if score >= 70: return f"🟢 {score}"
    if score >= 40: return f"🟡 {score}"
    return f"🔴 {score}"


# ── Main render ───────────────────────────────────────────────────────────────

def render_search_tab() -> None:
    """Render all content for the 🔍 Mis Búsquedas tab."""

    st.subheader("🔍 Mis Búsquedas Personalizadas")
    st.info(
        "Mostrando inmuebles activos entre 250k-450k €, ≥40m², sin bajos, "
        "con ascensor (zona centro/norte/este)."
    )

    # ── Load & filter ─────────────────────────────────────────────────────────
    from database import (
        get_listings as get_listings_personal,
        get_barrio_price_stats,
        get_drop_counts_for_listings,
    )

    personal_raw = get_listings_personal(
        status="active",
        min_price=250000,
        max_price=450000,
    )
    personal_df = pd.DataFrame(personal_raw)

    if personal_df.empty:
        st.warning("No se encontraron inmuebles en el rango de precio 250k-450k €.")
        return

    # price_per_sqm
    personal_df["price_per_sqm"] = personal_df.apply(
        lambda row: row["price"] / row["size_sqm"]
        if row.get("size_sqm") and row["size_sqm"] > 0 else None,
        axis=1,
    )

    # Size >= 40 m²
    personal_df = personal_df[personal_df["size_sqm"].fillna(0) >= 40]

    # Floor — not "Bajo"
    personal_df["floor"] = personal_df["floor"].fillna("").astype(str)
    personal_df = personal_df[
        ~personal_df["floor"].str.contains("bajo", case=False, na=False)
    ]

    # Elevator
    personal_df["description"] = personal_df["description"].fillna("").astype(str)
    has_elevator   = personal_df["description"].str.contains("ascensor", case=False, na=False)
    no_sin         = ~personal_df["description"].str.contains("sin ascensor", case=False, na=False)
    no_no_dispone  = ~personal_df["description"].str.contains("no dispone de ascensor", case=False, na=False)
    personal_df    = personal_df[has_elevator & no_sin & no_no_dispone]

    # Location
    target_districts = [
        "Centro", "Chamberí", "Moratalaz", "Retiro",
        "Tetuán", "Arganzuela", "Chamartín", "Salamanca", "Hortaleza",
    ]
    target_barrios = ["Argüelles"]
    personal_df = personal_df[
        (personal_df["distrito"].isin(target_districts))
        | (personal_df["barrio"].isin(target_barrios))
    ]

    if personal_df.empty:
        st.warning("No se encontraron inmuebles que coincidan con todos los criterios.")
        return

    # ── Enrich with fair-price and opportunity data ───────────────────────────
    today = datetime.now().date()

    # 1. Barrio median price/m²
    barrio_stats = get_barrio_price_stats(min_listings=5)

    def _vs_barrio(row):
        barrio   = row.get("barrio")
        ppsqm    = row.get("price_per_sqm")
        if not barrio or not ppsqm or barrio not in barrio_stats:
            return None
        median = barrio_stats[barrio]["median_price_sqm"]
        if not median:
            return None
        return round((ppsqm - median) / median * 100, 1)

    personal_df["vs_barrio_pct"] = personal_df.apply(_vs_barrio, axis=1)
    personal_df["barrio_median_sqm"] = personal_df["barrio"].map(
        lambda b: barrio_stats.get(b, {}).get("median_price_sqm")
    )

    # 2. Days on market
    def _days(row):
        fsd = row.get("first_seen_date")
        if not fsd:
            return None
        try:
            return (today - datetime.strptime(str(fsd), "%Y-%m-%d").date()).days
        except Exception:
            return None

    personal_df["dias_mercado"] = personal_df.apply(_days, axis=1)

    # 3. Price drop count
    listing_ids = personal_df["listing_id"].tolist()
    drop_counts = get_drop_counts_for_listings(listing_ids)
    personal_df["bajadas"] = personal_df["listing_id"].map(drop_counts).fillna(0).astype(int)

    # 4. Opportunity score
    def _score(row):
        vs    = row["vs_barrio_pct"]
        days  = row["dias_mercado"]
        drops = row["bajadas"]
        if vs is None or days is None:
            return None
        return compute_opportunity_score(vs, days, drops)

    personal_df["score_oportunidad"] = personal_df.apply(_score, axis=1)

    # Sort by score descending (None goes last)
    personal_df = personal_df.sort_values(
        "score_oportunidad", ascending=False, na_position="last"
    )

    # ── Summary metrics ───────────────────────────────────────────────────────
    st.success(f"Se encontraron **{len(personal_df)}** inmuebles.")

    p_col1, p_col2, p_col3, p_col4 = st.columns(4)
    with p_col1:
        st.metric("Precio Medio", f"€{personal_df['price'].mean():,.0f}")
    with p_col2:
        avg_sqm = personal_df["price_per_sqm"].dropna().mean()
        st.metric("Precio Medio/m²", f"€{avg_sqm:,.0f}" if pd.notna(avg_sqm) else "N/A")
    with p_col3:
        st.metric("Tamaño Medio", f"{personal_df['size_sqm'].mean():.0f} m²")
    with p_col4:
        top_score = personal_df["score_oportunidad"].dropna()
        st.metric(
            "Mejor Score",
            f"{int(top_score.iloc[0])}/100" if not top_score.empty else "N/A",
            help="Score de oportunidad del mejor inmueble filtrado"
        )

    # ── Score legend ──────────────────────────────────────────────────────────
    with st.expander("ℹ️ Cómo se calcula el Score de Oportunidad"):
        st.markdown("""
El **Score de Oportunidad (0-100)** combina tres señales:

| Componente | Peso | Lógica |
|---|---|---|
| 💰 Precio vs barrio | 40% | Cuánto más barato que la mediana €/m² del barrio |
| ⏱️ Días en mercado | 30% | Más tiempo → mayor poder de negociación |
| 📉 Bajadas de precio | 30% | Más bajadas → vendedor más motivado |

🟢 ≥70 — Oportunidad clara · 🟡 40–69 — Interesante · 🔴 <40 — Precio de mercado o caro
        """)

    # ── Results table ─────────────────────────────────────────────────────────
    display_df = personal_df[[
        "listing_id", "title", "price", "price_per_sqm",
        "barrio_median_sqm", "vs_barrio_pct",
        "distrito", "barrio", "size_sqm", "rooms",
        "dias_mercado", "bajadas", "score_oportunidad",
        "floor", "url",
    ]].copy()

    # Format score badge
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
            "barrio_median_sqm": st.column_config.NumberColumn("Mediana barrio €/m²", format="€%.0f"),
            "vs_barrio_pct":     st.column_config.TextColumn("vs Barrio", help="% sobre/bajo la mediana €/m² del barrio"),
            "distrito":          st.column_config.TextColumn("Distrito"),
            "barrio":            st.column_config.TextColumn("Barrio"),
            "size_sqm":          st.column_config.NumberColumn("m²", format="%d m²"),
            "rooms":             st.column_config.NumberColumn("Hab.", format="%d"),
            "dias_mercado":      st.column_config.NumberColumn("Días mercado"),
            "bajadas":           st.column_config.NumberColumn("Bajadas precio"),
            "score_oportunidad": st.column_config.TextColumn("🎯 Score", help="Score de oportunidad 0-100"),
            "floor":             st.column_config.TextColumn("Planta"),
            "url":               st.column_config.LinkColumn("Enlace"),
        },
        hide_index=True,
    )

    # ── Top opportunities highlight ───────────────────────────────────────────
    top = personal_df[personal_df["score_oportunidad"].notna()].head(5)
    if not top.empty:
        st.markdown("### 🏆 Top 5 Oportunidades")
        for _, row in top.iterrows():
            score = int(row["score_oportunidad"]) if pd.notna(row["score_oportunidad"]) else 0
            badge = "🟢" if score >= 70 else ("🟡" if score >= 40 else "🔴")
            vs    = f"{row['vs_barrio_pct']:+.1f}%" if pd.notna(row["vs_barrio_pct"]) else "—"
            days  = int(row["dias_mercado"]) if pd.notna(row["dias_mercado"]) else "—"
            drops = int(row["bajadas"])
            col_a, col_b = st.columns([4, 1])
            with col_a:
                st.markdown(
                    f"**{badge} [{row['title'][:60]}]({row['url']})**  \n"
                    f"€{row['price']:,} · {row['barrio']}, {row['distrito']} · "
                    f"{row['size_sqm']:.0f} m² · {vs} vs barrio · "
                    f"{days} días · {drops} bajadas"
                )
            with col_b:
                st.metric("Score", f"{score}/100")

    # ── Price evolution ───────────────────────────────────────────────────────
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
                    x=daily_avg["date"],
                    y=daily_avg["avg_price"],
                    mode="lines+markers",
                    name="Precio Medio",
                    line=dict(color="#8e44ad", width=3),
                    marker=dict(size=6),
                    hovertemplate="<b>%{x|%d/%m/%Y}</b><br>Precio: %{y:,.0f} €<extra></extra>",
                ))
                fig_evol.update_layout(
                    title="Evolución Precio Medio (Inmuebles Filtrados)",
                    xaxis_title="Fecha", yaxis_title="Precio (€)",
                    hovermode="x unified", height=400,
                )
                st.plotly_chart(fig_evol, use_container_width=True)

            # Properties with price drops
            st.markdown("#### 📉 Bajadas de Precio Recientes")
            drops_df = ph_df[ph_df["price_change"] < 0].copy()
            if not drops_df.empty:
                drops_df = drops_df.sort_values("date", ascending=False).head(20)
                drops_df["price_change_fmt"] = drops_df["price_change"].apply(lambda x: f"€{x:,.0f}")
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
            st.info("No hay datos de historial de precios disponibles para estos inmuebles.")
    except Exception:
        st.info("No hay datos de historial de precios disponibles.")
