"""
Mi Zona — personalised buyer landing page.

Lets the user freeze their hunting criteria (barrios, price ceiling,
size floor, room range) once, and surfaces three actionable lists for
every visit:

  * 🏆 Top oportunidades — every active listing matching the criteria,
    ranked by ``offer_engine.suggest_offer`` margin vs asking price.
    This is what the rest of the dashboard could only approximate via
    ``quality_score`` × ``negotiability_score``; here it's the
    explicit "you should offer X, asking is Y" computation done
    per-listing.

  * 🔄 Republicaciones — listings in the user's barrios where the
    property-fingerprint tables flagged a re-publication (same flat,
    different listing_id).  Strong fatigue signal.

  * 📉 Bajadas recientes — listings in the user's barrios whose price
    dropped in the last 7 days.  Surfaces moving targets before they
    catch attention elsewhere.

Persistence
-----------
Criteria are stored as a small JSON next to ``.streamlit/`` so they
survive Streamlit Cloud reboots and aren't entangled with session
state.  One file per username (falls back to ``default`` when no
auth context, useful for local dev).  Not in git — config files in
``.streamlit/`` are already ``.gitignore``-d.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import streamlit as st


CONFIG_DIR  = Path(".streamlit")
CONFIG_FILE_TEMPLATE = "mi_zona_{user}.json"

# Storage key under ``user_preferences``.  Single key per user — the
# whole criteria dict travels together.
_PREF_KEY = "mi_zona_criteria"

DEFAULT_CRITERIA: dict[str, Any] = {
    "barrios":    [],
    "max_price":  450_000,
    "min_size":   60,
    "min_rooms":  2,
    "max_rooms":  4,
    "seller_any": True,                   # False → only Particular
}


# ──────────────────────────────────────────────────────────────────────
# Persistence
# ──────────────────────────────────────────────────────────────────────


def _current_user() -> str:
    """Pull the logged-in username from session state, default to ``default``."""
    return re.sub(r"[^A-Za-z0-9_-]", "_", str(st.session_state.get("user", "default")))


def _config_path() -> Path:
    return CONFIG_DIR / CONFIG_FILE_TEMPLATE.format(user=_current_user())


def _load_criteria() -> dict:
    """Load Mi Zona criteria from the user_preferences store.

    Delegates to ``user_preferences.get_user_pref`` which tries
    Postgres first and falls back to a local JSON file when the DB
    is unavailable (SQLite local dev, fresh deploy without the new
    Alembic migration applied yet).  Either way, missing keys are
    backfilled with ``DEFAULT_CRITERIA`` so adding a new criterion
    in a future version is always backwards-safe for existing rows.
    """
    from user_preferences import get_user_pref
    data = get_user_pref(_current_user(), _PREF_KEY)
    if not data:
        # Legacy path: the previous version stored a per-user JSON
        # file at ``.streamlit/mi_zona_<user>.json``.  Try to recover
        # so a user who upgrades doesn't lose their criteria.
        legacy = _config_path()
        if legacy.exists():
            try:
                with legacy.open("r", encoding="utf-8") as f:
                    data = json.load(f)
            except (OSError, json.JSONDecodeError):
                data = None
    merged = DEFAULT_CRITERIA.copy()
    merged.update(data or {})
    return merged


def _save_criteria(criteria: dict) -> None:
    """Persist Mi Zona criteria via ``user_preferences.set_user_pref``."""
    from user_preferences import set_user_pref
    set_user_pref(_current_user(), _PREF_KEY, criteria)


# ──────────────────────────────────────────────────────────────────────
# Filtering + ranking
# ──────────────────────────────────────────────────────────────────────


def _apply_criteria(df: pd.DataFrame, c: dict) -> pd.DataFrame:
    """Filter ``df`` (active listings) down to ones matching the criteria.

    Tolerant to missing columns / nulls — the dashboard's ``load_data``
    always returns the same shape but the dataclass may grow over time.
    """
    out = df.copy()
    if c.get("barrios"):
        out = out[out["barrio"].isin(c["barrios"])]
    if c.get("max_price"):
        out = out[out["price"] <= c["max_price"]]
    if c.get("min_size"):
        out = out[out["size_sqm"].fillna(0) >= c["min_size"]]
    if c.get("min_rooms") is not None:
        out = out[out["rooms"].fillna(0) >= c["min_rooms"]]
    if c.get("max_rooms") is not None:
        out = out[out["rooms"].fillna(99) <= c["max_rooms"]]
    if not c.get("seller_any", True):
        out = out[out["seller_type"] == "Particular"]
    return out


def _compute_offers(
    candidates_df: pd.DataFrame,
    all_active_df: pd.DataFrame,
    notarial_by_distrito: dict[str, float],
) -> list[dict]:
    """For each candidate, run estimate_fair_price + suggest_offer.

    Heavy-ish (1 call per candidate) but bounded by the candidate
    count (after filtering it's typically 50-300).  Cached by the
    caller via ``@st.cache_data`` to avoid recomputing on every UI
    rerun.
    """
    from analytics import estimate_fair_price
    from offer_engine import suggest_offer

    results: list[dict] = []
    for _, row in candidates_df.iterrows():
        listing = row.to_dict()
        # Need num_drops / total_drop_pct / days_on_market for the
        # offer engine factors.  ``load_data`` already provides
        # ``days_on_market``; the drop counts come from a separate
        # query that's expensive at scale, so we approximate here as
        # zero when missing.  TODO: enrich the load_data projection
        # to carry drop stats so the offer engine sees them.
        listing.setdefault("num_drops",      0)
        listing.setdefault("total_drop_pct", 0.0)

        notarial = notarial_by_distrito.get(listing.get("distrito"))
        try:
            fair = estimate_fair_price(
                listing             = listing,
                all_active_df       = all_active_df,
                notarial_sqm        = notarial,
                district_trend_pct  = None,
            )
            if "error" in fair:
                continue
            fair_value = int(fair.get("trend_adjusted_price") or fair.get("estimated_price") or 0)
            confidence = fair.get("confidence", "medium")
            method     = "barrio_comps" if (fair.get("num_comps") or 0) >= 5 else "distrito_comps"
            offer = suggest_offer(
                listing           = listing,
                fair_value        = fair_value,
                fair_confidence   = confidence,
                fair_value_method = method,
            )
        except Exception:
            continue

        results.append({
            "listing_id":   listing.get("listing_id"),
            "title":        listing.get("title"),
            "url":          listing.get("url"),
            "distrito":     listing.get("distrito"),
            "barrio":       listing.get("barrio"),
            "price":        listing.get("price"),
            "size_sqm":     listing.get("size_sqm"),
            "rooms":        listing.get("rooms"),
            "seller_type":  listing.get("seller_type"),
            "days":         int(listing.get("days_on_market") or 0),
            "fair_value":   offer.fair_value,
            "suggested_mid":  offer.suggested_mid,
            "suggested_low":  offer.suggested_low,
            "suggested_high": offer.suggested_high,
            "margin_pct":   offer.discount_vs_asking_pct,
            "factors":      [(f.label, f.discount_pct) for f in offer.factors],
        })
    return results


# Caching the ranking step.  Keyed on the criteria tuple plus a hash
# of the input DataFrame; if either changes we recompute.
@st.cache_data(ttl=300, show_spinner="Calculando sugerencias de oferta…")
def _ranked_offers(
    criteria:             tuple,
    notarial_by_distrito: dict,
    df_active:            pd.DataFrame,
) -> pd.DataFrame:
    # Recover dict from tuple-of-pairs (st.cache_data needs hashable args).
    c = dict(criteria)
    cand = _apply_criteria(df_active, c)
    if cand.empty:
        return pd.DataFrame()
    rows = _compute_offers(cand, df_active, notarial_by_distrito)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows).sort_values("margin_pct", ascending=False)
    return df


# ──────────────────────────────────────────────────────────────────────
# Render
# ──────────────────────────────────────────────────────────────────────


def _render_criteria_form(criteria: dict, barrios_universe: list[str]) -> None:
    """Inline form (inside a Streamlit expander) for editing criteria."""
    has_criteria = bool(criteria.get("barrios"))
    with st.expander("⚙️ Criterios", expanded=not has_criteria):
        with st.form("mi_zona_criteria"):
            picked_barrios = st.multiselect(
                "Barrios a vigilar",
                options=barrios_universe,
                default=[b for b in criteria.get("barrios", []) if b in barrios_universe],
                help="Selecciona uno o más barrios donde estarías dispuesto a comprar.",
            )

            c1, c2, c3 = st.columns(3)
            with c1:
                max_price = st.number_input(
                    "Precio máximo (€)",
                    min_value=50_000, max_value=5_000_000,
                    value=int(criteria.get("max_price", 450_000)),
                    step=10_000,
                )
            with c2:
                min_size = st.number_input(
                    "Tamaño mínimo (m²)",
                    min_value=20, max_value=500,
                    value=int(criteria.get("min_size", 60)),
                    step=5,
                )
            with c3:
                seller_any = st.selectbox(
                    "Vendedor",
                    options=["Cualquiera", "Solo particular"],
                    index=0 if criteria.get("seller_any", True) else 1,
                )

            c4, c5 = st.columns(2)
            with c4:
                min_rooms = st.number_input(
                    "Habitaciones mín.",
                    min_value=0, max_value=10,
                    value=int(criteria.get("min_rooms", 2)),
                    step=1,
                )
            with c5:
                max_rooms = st.number_input(
                    "Habitaciones máx.",
                    min_value=0, max_value=10,
                    value=int(criteria.get("max_rooms", 4)),
                    step=1,
                )

            submitted = st.form_submit_button("💾 Guardar criterios", type="primary")
            if submitted:
                new_criteria = {
                    "barrios":    picked_barrios,
                    "max_price":  int(max_price),
                    "min_size":   int(min_size),
                    "min_rooms":  int(min_rooms),
                    "max_rooms":  int(max_rooms),
                    "seller_any": seller_any == "Cualquiera",
                }
                _save_criteria(new_criteria)
                # Clear the per-criteria ranking cache so the new
                # results show up immediately.
                _ranked_offers.clear()
                st.success("✅ Criterios guardados.")
                st.rerun()


def _render_summary_chip(criteria: dict, n_matches: int) -> None:
    """Compact one-line summary of the active criteria + match count."""
    if not criteria.get("barrios"):
        st.info("Aún no has definido barrios. Abre **Criterios** para empezar.")
        return
    seller_s = "particular" if not criteria["seller_any"] else "cualquier vendedor"
    st.caption(
        f"📍 **{len(criteria['barrios'])} barrios** · "
        f"≤ €{criteria['max_price']:,} · "
        f"≥ {criteria['min_size']} m² · "
        f"{criteria['min_rooms']}–{criteria['max_rooms']} habitaciones · "
        f"{seller_s} · "
        f"**{n_matches} listings activos** en el universo"
    )


def _render_offer_card(row: dict) -> None:
    """One ranked-opportunity card with the offer engine output inline."""
    margin   = row["margin_pct"]
    asking   = row["price"]
    mid      = row["suggested_mid"]

    if margin >= 8:
        accent = "#16a34a"
    elif margin >= 3:
        accent = "#0891b2"
    elif margin > 0:
        accent = "#94a3b8"
    else:
        accent = "#dc2626"

    title  = (row["title"] or "—")[:80]
    seller = row["seller_type"] or "—"
    factor_chips = (
        " · ".join(f"{lbl} {pct:+.1f}%" for lbl, pct in (row["factors"] or [])[:4])
        or "sin factores aplicados"
    )

    st.html(
        f"<div style=\"background:#0f172a;border-radius:10px;padding:14px 18px;"
        f"margin-bottom:10px;color:#e2e8f0;border-left:4px solid {accent};\">"
        f"<div style=\"display:flex;justify-content:space-between;align-items:baseline;\">"
        f"<div style=\"font-weight:600;font-size:15px;\">{title}</div>"
        f"<div style=\"font-size:13px;color:#94a3b8;\">{row['barrio']} · {row['distrito']}</div>"
        f"</div>"
        f"<div style=\"display:flex;justify-content:space-between;margin-top:8px;font-size:14px;\">"
        f"<span>"
        f"<b>€{asking:,}</b> · {row['size_sqm']:.0f} m² · {int(row['rooms']) if row['rooms'] else '—'}h · "
        f"{seller} · DOM {row['days']}d"
        f"</span>"
        f"<span style=\"color:{accent};font-weight:700;\">"
        f"Oferta sugerida €{mid:,} (−{margin:.1f}%)"
        f"</span>"
        f"</div>"
        f"<div style=\"font-size:12px;color:#cbd5e1;margin-top:6px;\">"
        f"<a href=\"{row['url']}\" target=\"_blank\" style=\"color:#7dd3fc;\">Ver en Idealista</a> · "
        f"{factor_chips}"
        f"</div>"
        f"</div>"
    )


def render_mi_zona_tab(df: pd.DataFrame) -> None:
    """Entry point — receives the same filtered DataFrame as the rest of pages."""
    st.header("🎯 Mi Zona")
    st.caption(
        "Tu landing personalizada — top oportunidades en tus barrios, "
        "ranking por **margen de oferta** del motor (no por calidad/negociabilidad "
        "por separado).  Define los criterios una vez y vuelve a revisar cada visita."
    )

    if df.empty:
        st.warning("⚠️ No hay datos activos para analizar.")
        return

    active_df = df[df["status"] == "active"].copy()
    if "price_per_sqm" not in active_df.columns:
        active_df["price_per_sqm"] = active_df["price"] / active_df["size_sqm"]

    # Universe of barrios from current data — keeps the multiselect
    # honest (a barrio with zero stock won't show as an option).
    barrios_universe = sorted(active_df["barrio"].dropna().unique().tolist())

    criteria = _load_criteria()
    _render_criteria_form(criteria, barrios_universe)

    # Notarial €/m² per distrito for the offer engine's fair value
    # (matches the same source used in the detail tab).
    try:
        from database import get_notarial_prices
        notarial_rows = get_notarial_prices() or []
        notarial_by_distrito: dict[str, float] = {}
        for r in notarial_rows:
            d   = r["distrito"]
            per = r.get("periodo", 0) or 0
            if d not in notarial_by_distrito or per > notarial_by_distrito[d][0]:
                notarial_by_distrito[d] = (per, float(r["precio_m2"]))
        notarial_by_distrito = {d: v[1] for d, v in notarial_by_distrito.items()}
    except Exception:
        notarial_by_distrito = {}

    # Filter to matching universe so the summary chip is accurate.
    matching = _apply_criteria(active_df, criteria)
    _render_summary_chip(criteria, len(matching))

    if not criteria.get("barrios"):
        return

    if matching.empty:
        st.warning(
            "Ningún listing activo encaja con tus criterios. "
            "Relaja precio máximo, baja el tamaño mínimo o añade más barrios."
        )
        return

    # ── Section 1: Top oportunidades ────────────────────────────────
    st.markdown("---")
    st.subheader("🏆 Top oportunidades")
    st.caption(
        "Ordenado por margen entre el precio pedido y la oferta sugerida por "
        "el motor.  Mayor % = más espacio para ofertar por debajo."
    )

    criteria_key = tuple(sorted(criteria.items(), key=lambda kv: kv[0]))
    ranked = _ranked_offers(criteria_key, notarial_by_distrito, active_df)

    if ranked.empty:
        st.info("Sin datos suficientes para calcular ofertas en este conjunto.")
    else:
        n_show = min(10, len(ranked))
        for _, row in ranked.head(n_show).iterrows():
            _render_offer_card(row.to_dict())
        st.caption(f"Mostrando top {n_show} de {len(ranked)} matches con oferta calculable.")

    # ── Section 2: Republicaciones en tus barrios ───────────────────
    st.markdown("---")
    st.subheader("🔄 Republicaciones en tus barrios")
    st.caption(
        "Propiedades cuya fingerprint matchea con anuncios anteriores — "
        "señal de fatiga del vendedor."
    )

    try:
        from property_history import get_republication_counts
        rep_counts = get_republication_counts(matching["listing_id"].tolist())
        republished = [
            {"lid": lid, "n": n}
            for lid, n in rep_counts.items() if n > 0
        ]
        republished.sort(key=lambda r: r["n"], reverse=True)
    except Exception as e:
        st.caption(f"⚠️ No se pudo cargar republicaciones: {e}")
        republished = []

    if not republished:
        st.info("Ninguno de tus matches está marcado como republicado en este momento.")
    else:
        for r in republished[:8]:
            row = matching[matching["listing_id"] == r["lid"]].iloc[0]
            st.markdown(
                f"- 🔄 **{r['n']}x** · "
                f"[{(row['title'] or '—')[:70]}]({row['url']}) · "
                f"€{int(row['price']):,} · {row['barrio']} · "
                f"{int(row['size_sqm']) if pd.notna(row['size_sqm']) else '—'} m² · "
                f"{row['seller_type']}"
            )
        if len(republished) > 8:
            st.caption(f"…{len(republished) - 8} republicaciones más.")

    # ── Section 3: Bajadas recientes ────────────────────────────────
    st.markdown("---")
    st.subheader("📉 Bajadas recientes (últimos 7 días)")
    st.caption(
        "Anuncios en tus barrios cuyo precio cayó esta semana. "
        "Movimiento en frío — antes de que se quemen del todo."
    )

    try:
        from database import get_recent_price_drops
        all_drops = get_recent_price_drops(days=7, min_drop_percent=1.0) or []
        my_barrios = set(criteria["barrios"])
        my_drops = [d for d in all_drops if d.get("barrio") in my_barrios]
        my_drops.sort(key=lambda d: d.get("change_percent", 0))   # most negative first
    except Exception as e:
        st.caption(f"⚠️ No se pudo cargar bajadas: {e}")
        my_drops = []

    if not my_drops:
        st.info("Sin bajadas registradas en tus barrios esta semana.")
    else:
        for d in my_drops[:10]:
            st.markdown(
                f"- 📉 **{d.get('change_percent', 0):+.1f}%** "
                f"(€{abs(int(d.get('change_amount', 0))):,}) · "
                f"[{(d.get('title') or '—')[:70]}]({d.get('url')}) · "
                f"ahora €{int(d.get('new_price', 0)):,} · "
                f"{d.get('barrio')} · "
                f"hace {d.get('date_recorded')}"
            )
