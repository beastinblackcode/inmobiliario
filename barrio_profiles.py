"""
Barrio profiles — enriched per-barrio analytics for the public dashboard.

Builds one rich profile per barrio combining:
  - KPIs (already in market_snapshots / get_barrio_summary)
  - Top opportunities (reuses analytics.rank_opportunities)
  - Distribution (price buckets, rooms, amenities %, construction year)
  - Neighbours (same district + closest by €/m²)
  - Verdict (deterministic rule on top of KPIs vs distrito/madrid)

Output is a dict keyed by barrio name, ready for JSON serialisation.

Entry point: build_all_barrio_profiles()
"""

from __future__ import annotations

import statistics
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd


# ────────────────────────────────────────────────────────────────────────────
# Verdict rules (deterministic, no LLM)
# ────────────────────────────────────────────────────────────────────────────

_TENSION_HIGH   = 1.15   # +15% vs distrito → tensionado
_TENSION_LOW    = 0.85   # -15% vs distrito → infravalorado
_PCT_DROPS_HIGH = 18.0   # >18% pisos con bajadas → bajista
_DAYS_FAST      = 0.80   # días < 80% media → mercado caliente
_DAYS_SLOW      = 1.20   # días > 120% media → mercado lento


def compute_verdict(
    kpis: Dict[str, Any],
    distrito_avg_sqm: Optional[float],
    madrid_avg_sqm: Optional[float],
    madrid_avg_days: Optional[float],
) -> Dict[str, str]:
    """Return {label, emoji, summary, recommendation} for a barrio."""

    ppsqm     = kpis.get("price_per_sqm")
    days      = kpis.get("avg_days_market")
    pct_drops = kpis.get("pct_with_drops") or 0
    active    = kpis.get("active_count") or 0

    if not ppsqm or active < 5:
        return {
            "label":          "Datos insuficientes",
            "emoji":          "⚪",
            "summary":        "Pocos pisos activos para emitir un veredicto fiable.",
            "recommendation": "Consulta otros barrios cercanos.",
        }

    # ── Base: tensión vs distrito ────────────────────────────────────
    ratio_distrito = (ppsqm / distrito_avg_sqm) if distrito_avg_sqm else 1.0

    if ratio_distrito >= _TENSION_HIGH:
        base_label, base_emoji = "Mercado tensionado", "🟡"
    elif ratio_distrito <= _TENSION_LOW:
        base_label, base_emoji = "Infravalorado vs distrito", "🟢"
    else:
        base_label, base_emoji = "Equilibrado", "🔵"

    # ── Modificador: dinámica del mercado ────────────────────────────
    days_ratio = (days / madrid_avg_days) if (days and madrid_avg_days) else 1.0

    if pct_drops >= _PCT_DROPS_HIGH and days_ratio >= _DAYS_SLOW:
        modifier       = "punto de inflexión bajista"
        recommendation = "Esperar 2-3 meses puede mejorar tus condiciones de negociación."
        emoji          = "🟠"
    elif days_ratio <= _DAYS_FAST and pct_drops < 10:
        modifier       = "mercado caliente"
        recommendation = "Si encuentras un piso con score alto, actuar rápido."
        emoji          = "🔴" if "Infravalorado" not in base_label else "🟢"
    else:
        modifier       = None
        recommendation = (
            "Buen momento si encuentras un piso con score alto y vendedor flexible."
            if "Infravalorado" in base_label else
            "Espera a encontrar un piso con margen claro vs la mediana."
        )
        emoji = base_emoji

    # ── Summary text ─────────────────────────────────────────────────
    parts = []
    if madrid_avg_sqm and ppsqm:
        diff_madrid = (ppsqm / madrid_avg_sqm - 1) * 100
        if abs(diff_madrid) >= 3:
            sign = "por encima" if diff_madrid > 0 else "por debajo"
            parts.append(f"€/m² {abs(diff_madrid):.0f}% {sign} de la media de Madrid")
    if distrito_avg_sqm and ppsqm and abs(ppsqm / distrito_avg_sqm - 1) >= 0.05:
        diff_d = (ppsqm / distrito_avg_sqm - 1) * 100
        sign   = "más caro" if diff_d > 0 else "más barato"
        parts.append(f"{abs(diff_d):.0f}% {sign} que la media del distrito")
    if pct_drops >= 15:
        parts.append(f"{pct_drops:.0f}% de pisos con bajadas")
    if days and madrid_avg_days:
        if days_ratio >= _DAYS_SLOW:
            parts.append(f"días en mercado {(days_ratio-1)*100:.0f}% por encima de la media")
        elif days_ratio <= _DAYS_FAST:
            parts.append(f"días en mercado {(1-days_ratio)*100:.0f}% por debajo de la media")

    summary = ". ".join(parts).capitalize() if parts else \
              "Mercado en línea con la media de Madrid."
    label   = f"{base_label} · {modifier}" if modifier else base_label

    return {
        "label":          label,
        "emoji":          emoji,
        "summary":        summary,
        "recommendation": recommendation,
    }


# ────────────────────────────────────────────────────────────────────────────
# Top opportunities per barrio
# ────────────────────────────────────────────────────────────────────────────

def get_top_opportunities_for_barrio(
    barrio: str,
    df_ranked: pd.DataFrame,
    n: int = 5,
) -> List[Dict]:
    """Return the top N opportunity rows from df_ranked for a given barrio."""
    if df_ranked.empty or "barrio" not in df_ranked.columns:
        return []
    sub = df_ranked[df_ranked["barrio"] == barrio].head(n)
    out = []
    for _, row in sub.iterrows():
        out.append({
            "listing_id":          row.get("listing_id"),
            "title":               (row.get("title") or "")[:120],
            "price":               int(row["price"]) if pd.notna(row.get("price")) else None,
            "price_per_sqm":       round(row["price_per_sqm"]) if pd.notna(row.get("price_per_sqm")) else None,
            "size_sqm":            int(row["size_sqm"]) if pd.notna(row.get("size_sqm")) else None,
            "rooms":               int(row["rooms"]) if pd.notna(row.get("rooms")) else None,
            "vs_distrito_pct":     round(row["vs_distrito_avg"], 1) if pd.notna(row.get("vs_distrito_avg")) else None,
            "days_on_market":      int(row["days_on_market"]) if pd.notna(row.get("days_on_market")) else None,
            "num_drops":           int(row.get("num_drops") or 0),
            "quality_score":       round(row["quality_score"]) if pd.notna(row.get("quality_score")) else None,
            "negotiability_score": round(row["negotiability_score"]) if pd.notna(row.get("negotiability_score")) else None,
            "url":                 row.get("url"),
        })
    return out


# ────────────────────────────────────────────────────────────────────────────
# Distribution
# ────────────────────────────────────────────────────────────────────────────

_PRICE_BUCKETS = [
    ("0-200k",   0,          200_000),
    ("200-300k", 200_000,    300_000),
    ("300-400k", 300_000,    400_000),
    ("400-500k", 400_000,    500_000),
    ("500-700k", 500_000,    700_000),
    ("700k-1M",  700_000,  1_000_000),
    (">1M",    1_000_000, float("inf")),
]


def get_distribution_for_barrio(
    barrio_listings: pd.DataFrame,
    amenities_map: Dict[str, Dict],
) -> Dict:
    """Compute price/rooms/amenities/year distribution for a barrio's listings."""
    if barrio_listings.empty:
        return {
            "price_buckets":         [],
            "rooms_distribution":    {},
            "amenities_pct":         {},
            "amenities_sample_size": 0,
            "median_construction_year": None,
            "sample_size":           0,
        }

    n = len(barrio_listings)

    # ── Price buckets ────────────────────────────────────────────────
    buckets = []
    for label, low, high in _PRICE_BUCKETS:
        c = int(((barrio_listings["price"] >= low) & (barrio_listings["price"] < high)).sum())
        buckets.append({"range": label, "count": c})

    # ── Rooms distribution ───────────────────────────────────────────
    rooms_clean   = barrio_listings["rooms"].dropna().astype(int)
    rooms_dist: Dict[str, int] = {"1": 0, "2": 0, "3": 0, "4+": 0}
    for r in rooms_clean:
        if r <= 1:   rooms_dist["1"]  += 1
        elif r == 2: rooms_dist["2"]  += 1
        elif r == 3: rooms_dist["3"]  += 1
        else:        rooms_dist["4+"] += 1

    # ── Amenity percentages ──────────────────────────────────────────
    amenity_keys = [
        "has_terraza", "has_balcon", "has_garaje", "has_trastero",
        "has_piscina", "has_ascensor", "has_portero",
        "has_aire_acondicionado", "has_calefaccion", "has_armarios",
        "has_near_metro", "has_near_parque", "has_near_colegio", "has_near_hospital",
    ]
    amen_counts = {k: 0 for k in amenity_keys}
    amen_listings = 0
    construction_years: List[int] = []

    for lid in barrio_listings["listing_id"]:
        amen = amenities_map.get(lid)
        if not amen:
            continue
        amen_listings += 1
        for k in amenity_keys:
            if amen.get(k):
                amen_counts[k] += 1
        cy = amen.get("construction_year")
        if cy and 1800 < cy < 2030:
            construction_years.append(int(cy))

    amenities_pct = (
        {k: round(v / amen_listings * 100, 1) for k, v in amen_counts.items()}
        if amen_listings >= 3 else {}
    )
    median_year = int(statistics.median(construction_years)) if construction_years else None

    return {
        "price_buckets":            buckets,
        "rooms_distribution":       rooms_dist,
        "amenities_pct":            amenities_pct,
        "amenities_sample_size":    amen_listings,
        "median_construction_year": median_year,
        "sample_size":              n,
    }


# ────────────────────────────────────────────────────────────────────────────
# Neighbours
# ────────────────────────────────────────────────────────────────────────────

def get_neighbor_barrios(
    target_barrio: str,
    target_distrito: str,
    target_ppsqm: Optional[float],
    all_summaries: List[Dict],
    n: int = 5,
) -> List[Dict]:
    """Return up to N neighbour barrios (same distrito first, then nearest by €/m²)."""
    if not all_summaries:
        return []

    same_distrito = [
        s for s in all_summaries
        if s.get("distrito") == target_distrito
        and s.get("barrio") != target_barrio
        and s.get("price_per_sqm")
    ]
    same_distrito.sort(
        key=lambda s: abs((s["price_per_sqm"] or 0) - (target_ppsqm or 0))
        if target_ppsqm else 0
    )
    picked = same_distrito[:n]

    # Fill remainder with nearest across Madrid
    if len(picked) < n and target_ppsqm:
        already = {s["barrio"] for s in picked} | {target_barrio}
        rest = [
            s for s in all_summaries
            if s.get("barrio") not in already and s.get("price_per_sqm")
        ]
        rest.sort(key=lambda s: abs(s["price_per_sqm"] - target_ppsqm))
        picked.extend(rest[: n - len(picked)])

    out = []
    for s in picked:
        ppsqm = s.get("price_per_sqm")
        diff  = ((ppsqm / target_ppsqm - 1) * 100) if (ppsqm and target_ppsqm) else None
        out.append({
            "barrio":        s["barrio"],
            "distrito":      s.get("distrito"),
            "price_per_sqm": round(ppsqm) if ppsqm else None,
            "median_price":  round(s["median_price"]) if s.get("median_price") else None,
            "active_count":  s.get("active_count"),
            "diff_pct":      round(diff, 1) if diff is not None else None,
        })
    return out


# ────────────────────────────────────────────────────────────────────────────
# Drop stats per barrio (single SQL pass)
# ────────────────────────────────────────────────────────────────────────────

def compute_drop_stats_per_barrio() -> Dict[str, Dict[str, float]]:
    """Return {barrio: {pct_with_drops, avg_drops}} from price_history."""
    from database import get_connection
    out: Dict[str, Dict[str, float]] = {}
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT barrio, COUNT(*) AS active
                FROM listings
                WHERE status='active' AND barrio IS NOT NULL AND barrio != ''
                GROUP BY barrio
            """)
            active_per = {row[0]: row[1] for row in cur.fetchall()}

            cur.execute("""
                SELECT l.barrio,
                       COUNT(DISTINCT l.listing_id) AS with_drops,
                       AVG(d.n_drops)               AS avg_drops
                FROM listings l
                JOIN (
                    SELECT listing_id, COUNT(*) AS n_drops
                    FROM price_history
                    WHERE change_amount < 0
                    GROUP BY listing_id
                ) d ON d.listing_id = l.listing_id
                WHERE l.status='active'
                  AND l.barrio IS NOT NULL AND l.barrio != ''
                GROUP BY l.barrio
            """)
            drop_per = {row[0]: (row[1], row[2]) for row in cur.fetchall()}

        for barrio, active in active_per.items():
            with_drops, avg_drops = drop_per.get(barrio, (0, 0.0))
            out[barrio] = {
                "pct_with_drops": round(with_drops / active * 100, 1) if active else 0.0,
                "avg_drops":      round(avg_drops, 2) if avg_drops else 0.0,
            }
    except Exception as exc:
        print(f"⚠️  compute_drop_stats_per_barrio failed: {exc}")
    return out


# ────────────────────────────────────────────────────────────────────────────
# Main orchestrator
# ────────────────────────────────────────────────────────────────────────────

def build_all_barrio_profiles() -> Dict[str, Any]:
    """
    Build one rich profile per barrio. Returns:

    {
      "metadata":        {...},
      "madrid_baseline": {...},
      "profiles":        { barrio_name: {...}, ... }
    }
    """
    print("📊 Building barrio profiles...")
    from database import get_listings, get_barrio_summary
    from analytics import rank_opportunities
    from nlp_analyzer import get_amenities_for_listings

    # 1. All active listings (single query) ─────────────────────────
    raw_active = get_listings(status="active")
    df_all     = pd.DataFrame(raw_active)
    if df_all.empty:
        print("⚠️  No active listings")
        return {"metadata": {}, "madrid_baseline": {}, "profiles": {}}

    df_all["price_per_sqm"] = df_all.apply(
        lambda r: r["price"] / r["size_sqm"]
        if pd.notna(r.get("size_sqm")) and r.get("size_sqm") > 0 else None,
        axis=1,
    )
    # Filter unrealistic values (same thresholds as rank_opportunities)
    df_all = df_all[
        df_all["price_per_sqm"].notna() &
        (df_all["price_per_sqm"] >= 1_500) &
        (df_all["price_per_sqm"] <= 25_000)
    ]

    # 2. Rank opportunities once ─────────────────────────────────────
    print(f"  → ranking opportunities ({len(df_all)} listings)…")
    df_ranked = rank_opportunities(df_all)

    # 3. Amenities (single batch) ────────────────────────────────────
    print("  → loading amenities…")
    try:
        amenities_map = get_amenities_for_listings(df_all["listing_id"].tolist())
    except Exception as exc:
        print(f"⚠️  amenities load failed: {exc}")
        amenities_map = {}

    # 4. Drop stats per barrio ───────────────────────────────────────
    print("  → computing drop stats…")
    drop_stats = compute_drop_stats_per_barrio()

    # 5. Madrid + distrito baselines ─────────────────────────────────
    distrito_ppsqm: Dict[str, float] = (
        df_all.dropna(subset=["distrito"])
        .groupby("distrito")["price_per_sqm"]
        .median()
        .to_dict()
    )
    madrid_ppsqm = float(df_all["price_per_sqm"].median())

    today = datetime.now().date()
    def _days(row):
        fsd = row.get("first_seen_date")
        if not fsd: return None
        try:
            return (today - datetime.strptime(str(fsd)[:10], "%Y-%m-%d").date()).days
        except Exception:
            return None
    df_all["_days"]      = df_all.apply(_days, axis=1)
    madrid_avg_days      = float(df_all["_days"].dropna().median())

    # 6. Barrio stats computed directly from filtered df_all ──────────
    # (avoids get_barrio_summary AVG which is skewed by outlier chalets)
    barrios_unique = sorted(df_all["barrio"].dropna().unique().tolist())
    print(f"  → computing barrio stats for {len(barrios_unique)} barrios…")

    grp = df_all.groupby("barrio")

    def _median_days(series):
        vals = series.dropna()
        return float(vals.median()) if len(vals) else None

    barrio_stats_df = grp.agg(
        active_count  =("listing_id",    "count"),
        median_price  =("price",         "median"),
        price_per_sqm =("price_per_sqm", "median"),
        avg_size_sqm  =("size_sqm",      "mean"),
        avg_rooms     =("rooms",         "mean"),
    ).reset_index()

    # Days on market (already computed as _days column)
    days_by_barrio = df_all.groupby("barrio")["_days"].median().to_dict()
    # Distrito: take mode per barrio
    distrito_by_barrio = df_all.groupby("barrio")["distrito"].agg(
        lambda x: x.mode().iloc[0] if len(x.mode()) else ""
    ).to_dict()

    summary_map: Dict[str, Dict] = {}
    for _, row in barrio_stats_df.iterrows():
        b = row["barrio"]
        summary_map[b] = {
            "barrio":        b,
            "distrito":      distrito_by_barrio.get(b, ""),
            "active_count":  int(row["active_count"]),
            "median_price":  float(row["median_price"])  if pd.notna(row["median_price"])  else None,
            "median_price_sqm": float(row["price_per_sqm"]) if pd.notna(row["price_per_sqm"]) else None,
            "avg_size_sqm":  float(row["avg_size_sqm"])  if pd.notna(row["avg_size_sqm"])  else None,
            "avg_rooms":     float(row["avg_rooms"])     if pd.notna(row["avg_rooms"])     else None,
            "avg_days_market": days_by_barrio.get(b),
        }

    # Lightweight list for neighbour lookup
    all_for_neighbours = [
        {
            "barrio":        s["barrio"],
            "distrito":      s["distrito"],
            "active_count":  s["active_count"],
            "price_per_sqm": round(s["median_price_sqm"]) if s.get("median_price_sqm") else None,
            "median_price":  round(s["median_price"])     if s.get("median_price")     else None,
        }
        for s in summary_map.values()
    ]

    # 7. Build one profile per barrio ────────────────────────────────
    profiles: Dict[str, Any] = {}
    for barrio in barrios_unique:
        s = summary_map.get(barrio)
        if not s or not s.get("active_count"):
            continue

        distrito         = s.get("distrito") or ""
        ppsqm            = round(s["median_price_sqm"]) if s.get("median_price_sqm") else None
        days             = round(s["avg_days_market"])  if s.get("avg_days_market")  else None
        ds               = drop_stats.get(barrio, {})
        distrito_avg_sqm = distrito_ppsqm.get(distrito)

        kpis = {
            "active_count":    s.get("active_count"),
            "median_price":    round(s["median_price"]) if s.get("median_price") else None,
            "price_per_sqm":   ppsqm,
            "avg_size_sqm":    round(s["avg_size_sqm"], 1) if s.get("avg_size_sqm") else None,
            "avg_rooms":       round(s["avg_rooms"], 1)    if s.get("avg_rooms")    else None,
            "avg_days_market": days,
            "vs_distrito_pct": round((ppsqm / distrito_avg_sqm - 1) * 100, 1)
                               if (ppsqm and distrito_avg_sqm) else None,
            "vs_madrid_pct":   round((ppsqm / madrid_ppsqm     - 1) * 100, 1)
                               if (ppsqm and madrid_ppsqm)    else None,
            "pct_with_drops":  ds.get("pct_with_drops"),
            "avg_drops":       ds.get("avg_drops"),
        }

        verdict       = compute_verdict(kpis, distrito_avg_sqm, madrid_ppsqm, madrid_avg_days)
        top_opps      = get_top_opportunities_for_barrio(barrio, df_ranked, n=5)
        barrio_df     = df_all[df_all["barrio"] == barrio]
        distribution  = get_distribution_for_barrio(barrio_df, amenities_map)
        neighbours    = get_neighbor_barrios(barrio, distrito, ppsqm, all_for_neighbours, n=5)

        profiles[barrio] = {
            "barrio":            barrio,
            "distrito":          distrito,
            "kpis":              kpis,
            "verdict":           verdict,
            "top_opportunities": top_opps,
            "distribution":      distribution,
            "neighbours":        neighbours,
        }

    print(f"✅ Built {len(profiles)} barrio profiles")
    return {
        "metadata": {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "version":      "1.0",
            "barrio_count": len(profiles),
        },
        "madrid_baseline": {
            "median_price_per_sqm":  round(madrid_ppsqm),
            "median_days_on_market": round(madrid_avg_days),
        },
        "profiles": profiles,
    }
