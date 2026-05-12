"""
Tab: Detalle de Propiedad
Vista completa de un piso: metadata, score desglosado factor a factor,
histórico de precios y propiedades similares en el mismo barrio.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import date, datetime
from typing import Any


def _iso_date(v: Any, default: str = "N/A") -> str:
    """Normalise a date column to ``'YYYY-MM-DD'`` regardless of backend.

    SQLite stores dates as TEXT (``'YYYY-MM-DD'`` or ``'YYYY-MM-DD HH:MM:SS'``)
    and pre-cutover code freely sliced them with ``[:10]``.  Postgres
    returns native ``datetime.date`` / ``datetime.datetime`` via
    psycopg, which break ``len()`` and slicing.  This helper hides
    the difference so callers don't have to ``isinstance``-check
    every place.
    """
    if v is None or v == "":
        return default
    if isinstance(v, (date, datetime)):
        return v.isoformat()[:10]
    s = str(v)
    return s[:10] if len(s) >= 10 else s

from database import (
    get_connection, get_property_price_stats,
    get_notarial_prices, get_price_trend_by_district,
)
from analytics import (
    calculate_distrito_stats,
    calculate_barrio_stats,
    calculate_days_on_market,
    calculate_negotiability_score,
    negotiability_label,
    explain_score,
    estimate_fair_price,
)
from data_utils import load_data
from property_history import get_property_history, PropertyHistory
from offer_engine import suggest_offer, OfferSuggestion


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


def _get_listing_by_id(listing_id: str) -> dict | None:
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """SELECT listing_id, title, url, price, distrito, barrio, rooms,
                      size_sqm, floor, orientation, seller_type, description,
                      first_seen_date, last_seen_date, status
               FROM listings WHERE listing_id = ? LIMIT 1""",
            (listing_id.strip(),),
        )
        row = cursor.fetchone()
    return dict(row) if row else None


def _search_listings(query: str, limit: int = 8) -> list[dict]:
    """
    Full-text search across title, barrio, distrito and listing_id.
    Returns up to `limit` results ordered by relevance (active first, then by
    how well the query matches the title).
    """
    q = query.strip()
    if not q:
        return []

    # If query looks like a listing ID (all digits) search by ID directly
    if q.isdigit():
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """SELECT listing_id, title, url, price, distrito, barrio,
                          rooms, size_sqm, floor, orientation, seller_type,
                          description, first_seen_date, last_seen_date, status
                   FROM listings
                   WHERE listing_id LIKE ?
                   ORDER BY status = 'active' DESC, listing_id
                   LIMIT ?""",
                (f"%{q}%", limit),
            )
            return [dict(r) for r in cursor.fetchall()]

    # If query looks like a URL, fall back to URL search
    if q.startswith("http"):
        result = _get_listing_by_url(q)
        return [result] if result else []

    # Free-text search: match against title, barrio, distrito
    pattern = f"%{q}%"
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """SELECT listing_id, title, url, price, distrito, barrio,
                      rooms, size_sqm, floor, orientation, seller_type,
                      description, first_seen_date, last_seen_date, status
               FROM listings
               WHERE title LIKE ?
                  OR barrio LIKE ?
                  OR distrito LIKE ?
                  OR listing_id LIKE ?
               ORDER BY
                   status = 'active' DESC,
                   title LIKE ? DESC,
                   last_seen_date DESC
               LIMIT ?""",
            (pattern, pattern, pattern, pattern, f"{q}%", limit),
        )
        return [dict(r) for r in cursor.fetchall()]


def _format_result_label(r: dict) -> str:
    """Human-readable label for a search result used in the selectbox."""
    status_icon = "🟢" if r["status"] == "active" else "🔴"
    price = f"€{r['price']:,}" if r.get("price") else "—"
    size  = f"{int(r['size_sqm'])}m²" if r.get("size_sqm") else "—"
    rooms = f"{r['rooms']}hab" if r.get("rooms") else "—"
    return (
        f"{status_icon} {r['title'][:55]}  ·  {r['barrio']}  ·  "
        f"{price}  ·  {size}  ·  {rooms}"
    )


def _get_price_history(listing_id: str) -> list:
    """Raw price_history entries (only price changes are stored)."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """SELECT date_recorded, price, change_amount, change_percent
               FROM price_history WHERE listing_id = ?
               ORDER BY date_recorded""",
            (listing_id,),
        )
        return [dict(r) for r in cursor.fetchall()]


def _build_chart_series(history: list, listing: dict) -> tuple[list, str]:
    """
    Always produce a usable price series for the chart, regardless of how
    many entries are stored in price_history.

    Strategy
    --------
    • Start point: first stored entry, or `first_seen_date` + current price
      if the price history is empty (bug-tolerant: ~54 % of legacy listings
      lack the initial insert).
    • End point: `last_seen_date` at the listing's current price — extends
      the line so the user can see how long the listing has held its price.
    • Inflection points: any actual changes from price_history sit between.

    Returns
    -------
    (series, kind):
        series — list of {date, price, change_amount, change_percent}
        kind   — "flat" | "single_change" | "multi_change"
    """
    # Backend-agnostic normalisation: SQLite returns strings, Postgres
    # returns ``datetime.date``; ``_iso_date`` collapses both to
    # ``'YYYY-MM-DD'`` so the downstream chart + Plotly handling works
    # without ``isinstance``-checking everywhere.
    first_seen = _iso_date(listing.get("first_seen_date"), default="")
    last_seen  = _iso_date(listing.get("last_seen_date"),  default="")
    cur_price  = listing.get("price")

    series: list = []

    # Anchor: start point
    if history:
        first_entry = dict(history[0])
        first_entry["date_recorded"] = _iso_date(first_entry.get("date_recorded"), default="")
        series.append(first_entry)
        for h in history[1:]:
            entry = dict(h)
            entry["date_recorded"] = _iso_date(entry.get("date_recorded"), default="")
            series.append(entry)
    elif first_seen and cur_price:
        # Synthesise initial point from listing metadata
        series.append({
            "date_recorded":  first_seen,
            "price":          cur_price,
            "change_amount":  None,
            "change_percent": None,
        })

    # Anchor: end point at last_seen (extends a flat line)
    if last_seen and cur_price and series:
        last_in_series = series[-1]
        # Only add the trailing point if the existing last entry isn't already at last_seen
        if last_in_series["date_recorded"] != last_seen:
            series.append({
                "date_recorded":  last_seen,
                "price":          cur_price,
                "change_amount":  0,
                "change_percent": 0.0,
            })

    # Classify the trajectory
    real_changes = sum(
        1 for h in history if h.get("change_amount") not in (None, 0)
    )
    if real_changes == 0:
        kind = "flat"
    elif real_changes == 1:
        kind = "single_change"
    else:
        kind = "multi_change"

    return series, kind


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


@st.fragment
def _render_property_banner(ph: PropertyHistory) -> None:
    """Banner shown right under the header when the listing has been
    republished.  Surfaces the three buyer-actionable lifetime stats:
    republication count, total days on market across all listings, and
    the net price change from the *original* asking price to the
    current one.

    Hidden for singletons — there's nothing useful to say.
    """
    if ph.republication_count == 0:
        return

    # Cumulative change: negative is good news for the buyer.
    delta_eur = ph.cumulative_change_eur
    delta_pct = ph.cumulative_change_pct
    if delta_eur is not None and delta_eur < 0:
        delta_str = f"<span style='color:#a7f3d0;'>↓ €{abs(delta_eur):,} ({delta_pct:+.1f}%)</span>"
    elif delta_eur is not None and delta_eur > 0:
        delta_str = f"<span style='color:#fda4af;'>↑ €{delta_eur:,} ({delta_pct:+.1f}%)</span>"
    else:
        delta_str = "<span style='color:#cbd5e1;'>sin cambio neto</span>"

    # Visual weight scales with republication count — 2 listings is
    # informative, 5 listings is a strong leverage signal.
    if ph.republication_count >= 3:
        bg, accent = "#7c2d12", "#fdba74"   # deep amber: strong signal
    else:
        bg, accent = "#1e293b", "#94a3b8"   # neutral slate

    st.markdown(
        f"""<div style='background:{bg};border-left:4px solid {accent};
            padding:14px 18px;border-radius:8px;margin:12px 0;color:#e2e8f0;'>
            <div style='font-size:15px;font-weight:600;'>
              🔄 Esta propiedad ha sido republicada
              <span style='color:{accent};'>{ph.republication_count} vez{'es' if ph.republication_count > 1 else ''}</span>
            </div>
            <div style='font-size:13px;margin-top:6px;'>
              <b>{ph.listing_count}</b> anuncios en total ·
              <b>{ph.total_days_on_market}</b> días acumulados en mercado ·
              precio inicial → actual: {delta_str}
            </div>
        </div>""",
        unsafe_allow_html=True,
    )


def _render_property_timeline(ph: PropertyHistory, current_listing_id: str) -> None:
    """Expandable "Historial de la propiedad" section listing every
    ``listing_id`` that ever pointed at this property, chronologically.

    For each listing we show the date range, vendor type, opening price
    → final price (with drop info), days on market, and a link back to
    Idealista.  The currently-displayed listing is highlighted.

    Hidden for singletons.
    """
    if ph.republication_count == 0:
        return

    with st.expander(f"🕰️ Historial completo de esta propiedad ({ph.listing_count} anuncios)"):
        st.caption(
            "Mismo piso publicado bajo distintos `listing_id`. Te interesa "
            "como comprador para entender la verdadera trayectoria de precio "
            "y tiempo en mercado — datos que cada anuncio individual oculta."
        )

        rows = []
        for i, l in enumerate(ph.listings, start=1):
            is_current = l.listing_id == current_listing_id

            # Format date range
            f = l.first_seen_date.isoformat() if l.first_seen_date else "—"
            lt = l.last_seen_date.isoformat()  if l.last_seen_date  else "—"
            date_range = f"{f} → {lt}"

            # Price column: initial → final + drop info if changed
            if l.initial_price and l.final_price and l.initial_price != l.final_price:
                delta = l.final_price - l.initial_price
                price_str = (
                    f"€{l.initial_price:,} → €{l.final_price:,}  "
                    f"({delta:+,})"
                )
            elif l.final_price:
                price_str = f"€{l.final_price:,}"
            else:
                price_str = "—"

            status_icon = "🟢" if l.status == "active" else "🔴"
            current_marker = "👉 " if is_current else "   "

            rows.append({
                "#":       current_marker + str(i),
                "Periodo": date_range,
                "Días":    l.days_on_market,
                "Vendedor": l.seller_type or "—",
                "Precio":  price_str,
                "Bajadas": l.n_drops if l.n_drops > 0 else "",
                "Estado":  f"{status_icon} {l.status}",
                "URL":     l.url or "",
            })

        df = pd.DataFrame(rows)
        st.dataframe(
            df,
            hide_index=True,
            use_container_width=True,
            column_config={
                "URL": st.column_config.LinkColumn("URL", display_text="Ver anuncio"),
            },
        )

        # Footnote: cross-listing aggregate so the reader doesn't have
        # to sum mentally.
        total_drops = sum(l.n_drops for l in ph.listings)
        total_drop_eur = sum(l.accumulated_drop_eur for l in ph.listings)
        st.markdown(
            f"**Total acumulado:** {total_drops} bajada{'s' if total_drops != 1 else ''} "
            f"sumando €{abs(total_drop_eur):,} a lo largo de todos los anuncios."
        )


def _render_offer_suggestion(offer: OfferSuggestion) -> None:
    """Render the suggested offer section.

    Layout: a big "range card" on the left with low/mid/high in €,
    plus a savings vs asking strip; on the right, a compact factor
    breakdown so the buyer can see *why* the discount is what it is.
    """
    asking = offer.asking_price
    saved_eur = asking - offer.suggested_mid
    saved_pct = offer.discount_vs_asking_pct

    # Choose accent colour by signal strength.
    if saved_pct >= 8:
        accent = "#16a34a"   # green: meaningful savings
        verdict = "Margen claro para ofertar por debajo"
    elif saved_pct >= 3:
        accent = "#0891b2"   # cyan: moderate
        verdict = "Margen moderado vs precio pedido"
    elif saved_pct > 0:
        accent = "#94a3b8"   # slate: small
        verdict = "Margen pequeño — propiedad bien tasada"
    else:
        accent = "#dc2626"   # red: no margin or asking below fair
        verdict = "Sin margen — precio igual o inferior al fair value"

    fair_v_str = f"€{offer.fair_value:,}"
    fair_method_label = {
        "barrio_comps":   "comparables del barrio",
        "distrito_comps": "comparables del distrito",
        "barrio_median":  "mediana del barrio",
        "notarial":       "anchor notarial",
    }.get(offer.fair_value_method, offer.fair_value_method)
    confidence_emoji = {"high": "🟢", "medium": "🟡", "low": "🔴"}.get(offer.fair_confidence, "")

    above_fair_suffix = " · pedido por encima del fair" if offer.is_above_fair_value else ""

    # ``st.html`` renders raw HTML without going through the markdown
    # parser, which would otherwise treat our indented closing
    # ``</div>`` lines as a fenced code block when an inline f-string
    # conditional collapses to an empty line.  The whole block is
    # pre-built as a flat string for the same reason.
    left, right = st.columns([1, 1])

    with left:
        st.html(
            f"<div style=\"background:#0f172a;border-radius:12px;padding:24px;"
            f"color:#e2e8f0;border-left:6px solid {accent};\">"
            f"<div style=\"font-size:13px;color:#94a3b8;letter-spacing:0.5px;"
            f"text-transform:uppercase;\">Rango de oferta sugerido</div>"
            f"<div style=\"font-size:36px;font-weight:900;margin-top:8px;color:white;\">"
            f"€{offer.suggested_low:,} <span style=\"color:#64748b;font-size:24px;\">—</span> €{offer.suggested_high:,}"
            f"</div>"
            f"<div style=\"font-size:14px;margin-top:4px;color:#cbd5e1;\">"
            f"Punto medio: <b style=\"color:white;\">€{offer.suggested_mid:,}</b></div>"
            f"<div style=\"margin-top:16px;padding:12px;background:#1e293b;border-radius:8px;\">"
            f"<div style=\"font-size:13px;color:#94a3b8;\">vs precio pedido (€{asking:,}):</div>"
            f"<div style=\"font-size:22px;font-weight:700;color:{accent};margin-top:2px;\">"
            f"−€{saved_eur:,} ({saved_pct:+.1f}%)</div>"
            f"<div style=\"font-size:12px;color:#cbd5e1;margin-top:4px;\">{verdict}</div>"
            f"</div>"
            f"<div style=\"font-size:12px;color:#64748b;margin-top:12px;\">"
            f"Fair value: {fair_v_str} ({fair_method_label}) {confidence_emoji}{above_fair_suffix}"
            f"</div>"
            f"</div>"
        )

    with right:
        if not offer.factors:
            st.info(
                "Sin factores de descuento detectados.  La oferta sugerida "
                "iguala al fair value (compra a precio de mercado).  Esto es "
                "típico de pisos recién publicados o sin señales de presión."
            )
            return

        st.html(
            "<div style=\"font-size:13px;color:#94a3b8;letter-spacing:0.5px;"
            "text-transform:uppercase;margin-bottom:10px;\">Factores aplicados</div>"
        )
        for f in offer.factors:
            pct_abs   = abs(f.discount_pct)
            bar_width = min(100, pct_abs * 15)
            st.html(
                f"<div style=\"margin-bottom:10px;\">"
                f"<div style=\"display:flex;justify-content:space-between;font-size:13px;\">"
                f"<span><b>{f.label}</b> — {f.why}</span>"
                f"<span style=\"color:{accent};font-weight:700;white-space:nowrap;margin-left:8px;\">"
                f"{f.discount_pct:+.1f}%</span>"
                f"</div>"
                f"<div style=\"background:#1e293b;border-radius:4px;height:6px;margin-top:4px;\">"
                f"<div style=\"background:{accent};width:{bar_width:.0f}%;height:6px;border-radius:4px;\"></div>"
                f"</div>"
                f"</div>"
            )

        st.caption(
            f"Descuento total: {offer.total_discount_pct:+.1f}% sobre el fair value. "
            "Mete la oferta en el extremo bajo si tienes señales fuertes; usa el alto "
            "como tope de cierre."
        )


def render_detail_tab() -> None:
    st.header("🔍 Detalle de Propiedad")

    # ── Buscador inteligente ───────────────────────────────────────────────────
    # Soporta: texto libre (título, barrio, distrito), ID numérico, URL completa
    query = st.text_input(
        "Buscar piso",
        placeholder="Escribe título, barrio, ID o pega la URL de Idealista…",
        key="detail_url_input",
    )

    listing = None  # se resolverá a continuación

    if not query or not query.strip():
        st.info("💡 Busca por nombre del anuncio, barrio, ID numérico o pega directamente la URL de Idealista.")
        return

    q = query.strip()

    # ── Resolución directa: URL o ID exacto ───────────────────────────────────
    if q.startswith("http"):
        listing = _get_listing_by_url(q)
        if not listing:
            st.warning("No se encontró ningún piso con esa URL en la base de datos.")
            return
    elif q.isdigit() and len(q) >= 7:
        listing = _get_listing_by_id(q)
        if not listing:
            st.warning(f"No se encontró ningún piso con el ID {q}.")
            return
    else:
        # ── Búsqueda por texto: mostrar resultados como selector ───────────────
        results = _search_listings(q)

        if not results:
            st.warning(f"Sin resultados para «{q}». Prueba con el barrio, parte del título o el ID del anuncio.")
            return

        if len(results) == 1:
            # Un único resultado → cargarlo directamente
            listing = results[0]
        else:
            # Varios resultados → selectbox para elegir
            labels = [_format_result_label(r) for r in results]
            labels_with_placeholder = ["— Selecciona un anuncio —"] + labels

            selected_label = st.selectbox(
                f"{len(results)} resultados encontrados",
                options=labels_with_placeholder,
                key="detail_search_select",
            )

            if selected_label == "— Selecciona un anuncio —":
                return

            selected_idx = labels.index(selected_label)
            listing = results[selected_idx]

    if not listing:
        st.warning("No se pudo cargar el piso seleccionado.")
        return

    history = _get_price_history(listing["listing_id"])

    # Property-level history — None when fingerprints haven't been
    # computed yet, or this listing was scraped after the last
    # fingerprint job.  Both downstream renders no-op safely on None.
    try:
        property_history = get_property_history(listing["listing_id"])
    except Exception:
        property_history = None

    # ── Header ────────────────────────────────────────────────────────────────
    st.markdown("---")
    col_title, col_link = st.columns([5, 1])
    with col_title:
        status_badge = "🟢 Activo" if listing["status"] == "active" else "🔴 Vendido/Retirado"
        st.subheader(listing["title"])
        st.caption(f"{status_badge} · {listing['distrito']} · {listing['barrio']}")
    with col_link:
        st.link_button("🔗 Ver en Idealista", listing["url"], use_container_width=True)

    if property_history is not None:
        _render_property_banner(property_history)

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
    k9.metric("📅 Visto por primera vez", _iso_date(listing.get("first_seen_date")))
    k10.metric("🔄 Última actualización",  _iso_date(listing.get("last_seen_date")))

    # ── Características y entorno (NLP) ───────────────────────────────────────
    try:
        from nlp_analyzer import (
            get_amenities_for_listings, extract_amenities, amenities_to_badges,
        )
        amenities = get_amenities_for_listings([listing["listing_id"]]).get(
            listing["listing_id"]
        )
        # Fallback: compute on the fly for listings not yet in the table
        if amenities is None and listing.get("description"):
            amenities = extract_amenities(listing["description"])

        if amenities:
            badges = amenities_to_badges(amenities)
            year = amenities.get("construction_year")
            if badges or year:
                st.markdown("**🏷️ Características detectadas en la descripción**")
                cols = st.columns([1, 4])
                with cols[0]:
                    if year:
                        st.metric("📅 Año construcción", str(year))
                with cols[1]:
                    if badges:
                        # Render badges as a compact pill row
                        st.markdown(
                            " ".join(
                                f"<span style='background:#1e293b;color:#e2e8f0;"
                                f"padding:4px 10px;border-radius:14px;margin-right:6px;"
                                f"display:inline-block;margin-bottom:6px;font-size:13px;'>"
                                f"{b}</span>"
                                for b in badges
                            ),
                            unsafe_allow_html=True,
                        )
                    else:
                        st.caption("Sin amenities específicos detectados.")
                st.caption(
                    f"Detectadas {amenities['amenities_count']} características automáticamente "
                    "del texto del anuncio (regex + diccionarios)."
                )
    except Exception as e:
        st.caption(f"⚠️ Análisis de características no disponible: {e}")

    # ── Descripción ───────────────────────────────────────────────────────────
    if listing.get("description"):
        with st.expander("📄 Descripción del anuncio"):
            st.markdown(listing["description"])

    # ── Historial de la propiedad (cross-listing) ──────────────────────────
    if property_history is not None:
        _render_property_timeline(property_history, listing["listing_id"])

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

    # ── Score de Negociabilidad ──────────────────────────────────────────────
    # Reuses the same score_row, distrito_stats and barrio_stats computed
    # above for the quality score — no extra queries.
    st.markdown("---")
    st.subheader("🤝 Score de Negociabilidad")
    st.caption(
        "Estima cuánto margen tienes para ofertar **por debajo** del precio "
        "publicado. Complementa el score de calidad: una propiedad de calidad "
        "alta y negociabilidad alta es donde se cierra la oportunidad real."
    )

    try:
        n_score = calculate_negotiability_score(score_row, distrito_stats, barrio_stats)
        n_badge, n_label = negotiability_label(n_score)

        if n_score >= 70:
            n_color = "#2ecc71"
        elif n_score >= 45:
            n_color = "#3498db"
        elif n_score >= 20:
            n_color = "#f39c12"
        else:
            n_color = "#95a5a6"

        # Component breakdown (mirrors the calculate_negotiability_score weights)
        dom = score_row.get("days_on_market", 0) or 0
        if dom >= 120:   days_pts = 35
        elif dom >= 90:  days_pts = 28
        elif dom >= 60:  days_pts = 20
        elif dom >= 30:  days_pts = 10
        elif dom >= 14:  days_pts = 5
        else:            days_pts = 0

        nd = score_row.get("num_drops", 0) or 0
        td = abs(score_row.get("total_drop_pct", 0) or 0)
        drops_pts = (18 if nd >= 3 else 12 if nd == 2 else 6 if nd == 1 else 0)
        drops_pts += (12 if td >= 15 else 8 if td >= 8 else 4 if td >= 4 else 0)
        drops_pts = min(30, drops_pts)

        gap_pct = score_row.get("vs_distrito_avg")
        if gap_pct is None and price_sqm and listing.get("distrito") in distrito_stats:
            avg = distrito_stats[listing["distrito"]].get("avg_price_sqm", 0)
            gap_pct = (price_sqm / avg - 1) * 100 if avg > 0 else 0
        gap_pct = gap_pct or 0
        if gap_pct >= 20:   gap_pts = 20
        elif gap_pct >= 10: gap_pts = 12
        elif gap_pct >= 5:  gap_pts = 6
        elif gap_pct >= 0:  gap_pts = 2
        else:               gap_pts = 0

        seller = score_row.get("seller_type", "")
        if seller == "Particular":
            seller_pts = 15
        elif seller in ("Profesional", "Agencia"):
            seller_pts = 4
        else:
            seller_pts = 8

        n_factors = [
            {
                "label": "Días en mercado",
                "description": f"{dom:.0f} días" + (
                    " — vendedor probablemente cansado" if dom >= 90
                    else " — todavía fresco" if dom < 30 else ""
                ),
                "points": days_pts, "max_points": 35,
            },
            {
                "label": "Historial de bajadas",
                "description": f"{nd} bajadas · {td:.1f}% acumulado" if nd > 0
                              else "sin bajadas registradas",
                "points": drops_pts, "max_points": 30,
            },
            {
                "label": "Sobreprecio vs distrito",
                "description": f"{gap_pct:+.1f}%" + (
                    " — claramente por encima" if gap_pct >= 10
                    else " — a precio de mercado" if gap_pct >= -5
                    else " — ya por debajo, poco margen"
                ),
                "points": gap_pts, "max_points": 20,
            },
            {
                "label": "Tipo de vendedor",
                "description": (
                    f"{seller} — más flexible" if seller == "Particular"
                    else f"{seller} — menos flexible" if seller in ("Profesional", "Agencia")
                    else "Desconocido"
                ),
                "points": seller_pts, "max_points": 15,
            },
        ]

        nc1, nc2 = st.columns([1, 3])
        with nc1:
            st.markdown(
                f"""<div style='text-align:center;background:{n_color};border-radius:12px;
                    padding:20px;color:white;'>
                    <div style='font-size:48px;font-weight:900;'>{n_score:.0f}</div>
                    <div style='font-size:13px;margin-top:4px;'>{n_badge} {n_label}</div>
                    </div>""",
                unsafe_allow_html=True,
            )
        with nc2:
            for f in n_factors:
                pct = f["points"] / f["max_points"] if f["max_points"] > 0 else 0
                bar_color = "#3498db" if f["points"] > 0 else "#ddd"
                st.markdown(
                    f"""<div style='margin-bottom:10px;'>
                        <div style='display:flex;justify-content:space-between;font-size:13px;'>
                          <span><b>{f['label']}</b> — {f['description']}</span>
                          <span style='color:{bar_color};font-weight:700;'>{f['points']:.0f} / {f['max_points']}</span>
                        </div>
                        <div style='background:#eee;border-radius:4px;height:8px;margin-top:4px;'>
                          <div style='background:{bar_color};width:{pct*100:.0f}%;height:8px;border-radius:4px;'></div>
                        </div>
                    </div>""",
                    unsafe_allow_html=True,
                )

    except Exception as e:
        st.warning(f"No se pudo calcular el score de negociabilidad: {e}")

    # ── Sugerencia de oferta ──────────────────────────────────────────────────
    # The synthesis of every signal above: fair value (comparables +
    # notarial + trend) discounted by buyer-leverage factors (days on
    # market, drops, seller type, NLP signals, republications).  One
    # actionable number — what to put on the table.
    st.markdown("---")
    st.subheader("💸 Sugerencia de Oferta")
    st.caption(
        "Rango de precio realista para presentar al vendedor.  Combina el "
        "fair value (comparables del barrio + anchor notarial) con los "
        "signals que indican margen de negociación.  La oferta sugerida "
        "**nunca excede el fair value** — no se trata de pagar más."
    )

    try:
        from analytics import estimate_fair_price

        # Reuse all_active (already loaded for the quality score) so we
        # don't repeat the heavy listings query.
        notarial_sqm_for_distrito = None
        if _notarial_raw:
            _latest = max(_notarial_raw, key=lambda r: r["periodo"])
            notarial_sqm_for_distrito = _latest["precio_m2"]

        fair = estimate_fair_price(
            listing             = listing,
            all_active_df       = all_active,
            notarial_sqm        = notarial_sqm_for_distrito,
            district_trend_pct  = None,            # optional; skip for now
        )

        if "error" in fair:
            st.info(f"No se puede calcular sugerencia: {fair['error']}")
        else:
            fair_value = int(fair.get("trend_adjusted_price") or fair.get("estimated_price") or 0)
            confidence = fair.get("confidence", "medium")
            method     = "barrio_comps" if (fair.get("num_comps") or 0) >= 5 else "distrito_comps"

            # NLP signals — already loaded earlier via ``amenities`` but
            # those are physical (terraza/garaje), not the leverage signals
            # (negociable/urgencia).  Load the right table.
            nlp_signals = None
            try:
                from nlp_analyzer import get_signals_for_listings
                nlp_signals = get_signals_for_listings([listing["listing_id"]]).get(
                    listing["listing_id"]
                )
            except Exception:
                pass

            offer = suggest_offer(
                listing           = score_row,
                fair_value        = fair_value,
                fair_confidence   = confidence,
                fair_value_method = method,
                property_history  = property_history,
                nlp_signals       = nlp_signals,
            )
            _render_offer_suggestion(offer)

    except Exception as e:
        st.warning(f"No se pudo calcular la sugerencia de oferta: {e}")

    st.markdown("---")

    # ── Historial de precios ───────────────────────────────────────────────────
    st.subheader("📈 Histórico de Precios")

    series, kind = _build_chart_series(history, listing)

    # KPIs (always shown when we have any anchor data)
    if series:
        initial_price = series[0]["price"]
        current_price = listing["price"]
        total_change  = current_price - initial_price
        total_pct     = (total_change / initial_price * 100) if initial_price else 0
        real_changes  = sum(1 for h in history if h.get("change_amount") not in (None, 0))

        h1, h2, h3, h4 = st.columns(4)
        h1.metric("Precio inicial", f"€{initial_price:,}")
        h2.metric("Precio actual",  f"€{current_price:,}")
        delta_color = "inverse" if total_change < 0 else "normal"
        h3.metric(
            "Variación total",
            f"€{abs(total_change):,}",
            f"{total_pct:+.1f}%" if initial_price else "—",
            delta_color=delta_color,
        )
        h4.metric("Cambios registrados", real_changes)

        # Status banner reflecting the trajectory kind
        if kind == "flat":
            st.caption(
                f"➡️ Precio sin cambios desde **{series[0]['date_recorded']}** "
                f"(visto por última vez **{_iso_date(listing.get('last_seen_date'), default='')}**)."
            )
        elif kind == "single_change":
            st.caption("📉 Una bajada/subida registrada — la línea conecta los puntos clave.")
        else:
            st.caption(f"🔄 {real_changes} cambios de precio registrados.")

        # Build the chart
        df_hist = pd.DataFrame(series)
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df_hist["date_recorded"], y=df_hist["price"],
            mode="lines+markers", name="Precio",
            line=dict(color="#3498db", width=3),
            marker=dict(size=10),
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
        st.info(
            "No hay datos suficientes para mostrar el histórico de este piso "
            "(faltan first_seen_date o precio)."
        )

    st.markdown("---")

    # ── Pre-load notarial + trend data for valuation ──────────────────────────
    _notarial_raw   = get_notarial_prices(distrito=listing.get("distrito"))
    _not_sqm        = None
    _not_latest_yr  = None
    if _notarial_raw:
        _not_latest     = max(_notarial_raw, key=lambda r: r["periodo"])
        _not_sqm        = _not_latest["precio_m2"]
        _not_latest_yr  = _not_latest["periodo"]

    # District price trend over last 8 weeks
    _district_trend_pct = None
    try:
        trend_rows = [
            r for r in get_price_trend_by_district(weeks=8)
            if r["distrito"] == listing.get("distrito")
        ]
        if len(trend_rows) >= 3:
            first_sqm = trend_rows[0]["avg_sqm"]
            last_sqm  = trend_rows[-1]["avg_sqm"]
            if first_sqm:
                _district_trend_pct = (last_sqm - first_sqm) / first_sqm * 100
    except Exception:
        pass

    # ── Valoración estimada ───────────────────────────────────────────────────
    st.subheader("💡 Valoración Estimada")

    try:
        valuation = estimate_fair_price(
            listing, all_active,
            notarial_sqm=_not_sqm,
            district_trend_pct=_district_trend_pct,
        )

        if "error" in valuation:
            st.warning(valuation["error"])
        else:
            est   = valuation["estimated_price"]
            gap   = valuation["gap_pct"]
            conf  = valuation["confidence"]
            scope = valuation["scope"]
            nc    = valuation["num_comps"]
            not_p = valuation.get("notarial_price")
            not_g = valuation.get("notarial_gap_pct")
            trend = valuation.get("district_trend_pct")
            trend_adj = valuation.get("trend_adjusted_price")

            conf_icon = {"alta": "🟢", "media": "🟡", "baja": "🔴"}[conf]

            # ── Trend warning banner ──────────────────────────────────────────
            if valuation.get("trend_warning") and trend is not None:
                st.warning(
                    f"📉 **Tendencia bajista en {listing.get('distrito')}**: los precios han caído "
                    f"**{trend:.1f}%** en las últimas 8 semanas. El precio estimado puede estar inflado "
                    f"respecto a la situación actual del mercado."
                )

            # ── Main metrics: oferta + transacción estimada ───────────────────
            if not_p:
                v1, v2, v3, v4 = st.columns(4)
                v1.metric("Precio listado", f"€{listing['price']:,}")
                v2.metric(
                    "Est. oferta (comparables)",
                    f"€{est:,}",
                    f"{gap:+.1f}% vs listado",
                    delta_color="inverse" if gap > 0 else "normal",
                    help=f"Media ponderada de {nc} comparables en Idealista ({scope}), con ajustes por características.",
                )
                not_delta_color = "inverse" if not_g and not_g > 0 else "normal"
                v3.metric(
                    f"Est. transacción (notarial {_not_latest_yr})",
                    f"€{not_p:,}",
                    f"{not_g:+.1f}% vs listado" if not_g is not None else None,
                    delta_color=not_delta_color,
                    help=f"Precio notarial escriturado ({_not_latest_yr}) en {listing.get('distrito')} "
                         f"ajustado por características (planta, orientación, tamaño). "
                         "Refleja lo que realmente se escritura, no lo que se pide.",
                )
                v4.metric(
                    "Confianza", f"{conf_icon} {conf.capitalize()}",
                    f"{nc} comparables · {scope}",
                )
            else:
                v1, v2, v3, v4 = st.columns(4)
                v1.metric("Precio listado",  f"€{listing['price']:,}")
                v2.metric("Precio estimado", f"€{est:,}")
                delta_label = f"{gap:+.1f}% {'sobre' if gap > 0 else 'bajo'} valor"
                v3.metric("Diferencia", f"€{abs(listing['price'] - est):,}", delta_label,
                          delta_color="inverse" if gap > 0 else "normal")
                v4.metric("Confianza", f"{conf_icon} {conf.capitalize()}",
                          f"Basado en {nc} comparables del {scope}")

            # ── Trend-adjusted price note ─────────────────────────────────────
            if trend_adj and trend is not None:
                st.caption(
                    f"💡 Aplicando la tendencia actual ({trend:+.1f}%), el precio estimado ajustado sería **€{trend_adj:,}**."
                )

            # ── Gap verdict (vs comparables) ──────────────────────────────────
            if gap > 10:
                st.error(f"⚠️ Precio de oferta **{gap:.1f}% por encima** del estimado de comparables. Margen de negociación elevado.")
            elif gap > 5:
                st.warning(f"📊 Precio ligeramente alto ({gap:.1f}% sobre el estimado de comparables).")
            elif gap < -10:
                st.success(f"🎯 ¡Oportunidad! Precio **{abs(gap):.1f}% por debajo** del estimado de comparables.")
            elif gap < -5:
                st.success(f"✅ Buen precio ({abs(gap):.1f}% bajo el estimado de comparables).")
            else:
                st.info(f"⚖️ Precio en línea con el mercado de oferta (diferencia de {abs(gap):.1f}%).")

            # ── Adjustments breakdown ─────────────────────────────────────────
            if valuation["adjustments"]:
                with st.expander("🔧 Detalle de ajustes aplicados"):
                    st.markdown(
                        f"**Base comparables:** €{valuation['base_sqm']:,}/m² "
                        f"(media ponderada de {nc} comparables del {scope})"
                    )
                    if _not_sqm:
                        st.markdown(f"**Base notarial {_not_latest_yr}:** €{round(_not_sqm):,}/m²")
                    st.markdown("**Ajustes por características:**")
                    for adj in valuation["adjustments"]:
                        sign = "+" if adj["pct"] > 0 else ""
                        st.markdown(f"- {adj['label']} → **{sign}{adj['pct']*100:.0f}%**")
                    st.markdown(
                        f"**€/m² ajustado (comparables):** €{valuation['adjusted_sqm']:,}/m² · "
                        f"**Precio estimado:** €{valuation['adjusted_sqm']:,} × "
                        f"{listing['size_sqm']:.0f} m² = **€{est:,}**"
                    )

            # ── Comparable properties used ────────────────────────────────────
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

    # ── Evolución notarial histórica (sección compacta) ──────────────────────
    if _notarial_raw and len(_notarial_raw) > 1 and listing.get("size_sqm"):
        st.markdown("---")
        st.subheader("🏛️ Evolución del Precio Real Escriturado")
        st.caption(
            f"Precios escriturados reales en {listing.get('distrito')} desde 2014 "
            f"(Portal del Notariado). La línea roja muestra el €/m² de este piso."
        )
        df_not = pd.DataFrame(_notarial_raw)
        fig_not = go.Figure()
        fig_not.add_trace(go.Scatter(
            x=df_not["periodo"], y=df_not["precio_m2"],
            mode="lines+markers", name="€/m² notarial",
            line=dict(color="#9b59b6", width=2), marker=dict(size=7),
            hovertemplate="<b>%{x}</b><br>€/m²: %{y:,.0f}<extra></extra>",
        ))
        listing_sqm = listing["price"] / listing["size_sqm"]
        fig_not.add_hline(
            y=listing_sqm,
            line_dash="dot", line_color="#e74c3c",
            annotation_text=f"Este piso: €{listing_sqm:,.0f}/m²",
            annotation_position="top right",
        )
        fig_not.update_layout(
            height=260,
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
