"""
Export aggregated public metrics from the private database.

Generates a JSON file with market-level KPIs suitable for the
public-facing Market Thermometer dashboard. NO individual listing
data is included — only aggregated statistics.

Usage:
    python export_public_metrics.py                   # writes to stdout
    python export_public_metrics.py -o metrics.json   # writes to file
    python export_public_metrics.py --push             # writes + git push
"""

import json
import os
import sys
import traceback
from datetime import datetime
from typing import Dict, Any, Optional

# ---------------------------------------------------------------------------
# Ensure we can import sibling modules (database, market_indicators, etc.)
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def _safe(fn, *args, **kwargs):
    """Call *fn* and return its result; on failure return None and log."""
    try:
        return fn(*args, **kwargs)
    except Exception as exc:
        print(f"⚠️  {fn.__name__} failed: {exc}")
        return None


# ===================================================================
# Indicator loaders
# ===================================================================

def _load_internal_indicators(euribor_rate: Optional[float] = None) -> Dict:
    from market_indicators import get_all_internal_indicators
    return _safe(get_all_internal_indicators, euribor_rate=euribor_rate) or {}


def _load_market_score(indicators: Dict, euribor: Dict, paro: Dict) -> Dict:
    from market_indicators import calculate_market_score
    return _safe(
        calculate_market_score,
        price_trend=indicators.get("price_trend", {}),
        sales_speed=indicators.get("sales_speed", {}),
        supply_demand=indicators.get("supply_demand", {}),
        inventory=indicators.get("inventory", {}),
        euribor=euribor,
        paro=paro,
        affordability=indicators.get("affordability"),
        price_drop_ratio=indicators.get("price_drop_ratio"),
        notarial_gap=indicators.get("notarial_gap"),
    ) or {}


def _load_market_alerts(indicators: Dict, macro: Dict) -> list:
    from market_indicators import get_market_alerts
    alerts = _safe(
        get_market_alerts,
        price_trend=indicators.get("price_trend", {}),
        sales_speed=indicators.get("sales_speed", {}),
        supply_demand=indicators.get("supply_demand", {}),
        inventory=indicators.get("inventory", {}),
        rotation=indicators.get("rotation", {}),
        affordability=indicators.get("affordability", {}),
        macro=macro,
    )
    return alerts or []


def _load_macro() -> Dict:
    from macro_data import get_all_macro_data
    return _safe(get_all_macro_data) or {}


def _load_zone_prices() -> list:
    """Direct query for zone prices — avoids column name mismatch in get_price_by_zone."""
    import sqlite3, statistics
    from collections import defaultdict
    from database import get_connection
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT distrito, price, size_sqm
                FROM listings
                WHERE price > 0 AND status = 'active'
                  AND distrito IS NOT NULL AND distrito != ''
            """)
            rows = cur.fetchall()
        zone_prices: dict = defaultdict(list)
        zone_sqm: dict = defaultdict(list)
        for distrito, price, sqm in rows:
            zone_prices[distrito].append(price)
            if sqm and sqm > 0:
                zone_sqm[distrito].append(price / sqm)
        result = []
        for z, prices in zone_prices.items():
            if len(prices) < 3:
                continue
            sqm_list = zone_sqm.get(z, [])
            result.append({
                "zone": z,
                "count": len(prices),
                "median_price": round(statistics.median(prices)),
                "median_price_sqm": round(statistics.median(sqm_list)) if sqm_list else None,
            })
        result.sort(key=lambda x: x["median_price"], reverse=True)
        return result
    except Exception as exc:
        print(f"⚠️  _load_zone_prices failed: {exc}")
        return []


def _load_zone_speed() -> list:
    """Direct query for days-on-market by district."""
    import statistics
    from collections import defaultdict
    from database import get_connection
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT distrito,
                       julianday(COALESCE(last_seen_date, date('now'))) - julianday(first_seen_date) AS days
                FROM listings
                WHERE status = 'sold_removed'
                  AND distrito IS NOT NULL AND distrito != ''
                  AND first_seen_date IS NOT NULL
            """)
            rows = cur.fetchall()
        zone_days: dict = defaultdict(list)
        for distrito, days in rows:
            if days is not None and days >= 0:
                zone_days[distrito].append(days)
        result = []
        for z, days_list in zone_days.items():
            if len(days_list) < 2:
                continue
            result.append({
                "zone": z,
                "median_days": round(statistics.median(days_list), 1),
                "sales_count": len(days_list),
            })
        return result
    except Exception as exc:
        print(f"⚠️  _load_zone_speed failed: {exc}")
        return []


def _load_rental_yields() -> list:
    from database import get_rental_yields
    return _safe(get_rental_yields) or []


def _load_price_trend_by_district() -> list:
    from database import get_price_trend_by_district
    return _safe(get_price_trend_by_district, 12) or []


def _load_market_trend() -> list:
    from database import get_market_summary_trend
    return _safe(get_market_summary_trend) or []


def _load_notarial_gap() -> list:
    from database import get_notarial_gap_by_district
    return _safe(get_notarial_gap_by_district) or []


def _load_db_stats() -> Dict:
    from database import get_database_stats
    return _safe(get_database_stats) or {}


def _load_price_drop_stats() -> Dict:
    from database import get_price_drop_stats
    return _safe(get_price_drop_stats) or {}


def _build_aged_stock_alert(threshold_warning: float = 0.35,
                             threshold_critical: float = 0.50,
                             min_days: int = 90) -> Optional[Dict]:
    """
    Returns an alert dict if a significant % of active listings have been
    on the market for more than *min_days* days — a sign of weak real demand.

    Returns None if the data is insufficient or below the threshold.
    """
    from database import get_connection
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
                SELECT
                    COUNT(*) AS total_active,
                    SUM(CASE
                        WHEN julianday('now') - julianday(first_seen_date) > {min_days}
                        THEN 1 ELSE 0
                    END) AS aged_count
                FROM listings
                WHERE status = 'active'
                  AND first_seen_date IS NOT NULL
            """)
            row = cur.fetchone()
        if not row:
            return None
        total, aged = row
        if not total or total < 50:          # not enough data
            return None
        aged = aged or 0
        ratio = aged / total
        if ratio < threshold_warning:
            return None

        pct = round(ratio * 100, 1)
        level = "critical" if ratio >= threshold_critical else "warning"
        return {
            "level": level,
            "metric": "aged_stock",
            "title": f"Stock envejecido: {pct}% lleva +{min_days} días",
            "detail": (
                f"{aged:,} de {total:,} pisos activos llevan más de {min_days} días "
                f"en el mercado ({pct}%). Esto sugiere que la demanda real es más débil "
                f"de lo que indica el volumen de oferta."
            ),
        }
    except Exception as exc:
        print(f"⚠️  _build_aged_stock_alert failed: {exc}")
        return None


# ===================================================================
# Sanitise helpers  (strip series / internal data that might leak
# individual listing info)
# ===================================================================

def _sanitise_indicator(ind: Dict) -> Dict:
    """Keep only aggregate fields from an indicator dict."""
    safe_keys = {
        "name", "current", "previous", "change", "change_pct",
        "trend", "unit", "description",
        # affordability extras
        "monthly_payment", "annual_cost", "price_to_income",
        "affordable", "median_salary",
        # rental yield extras
        "avg_yield", "top_barrios",
        # notarial gap
        "avg_gap_pct", "max_gap_district", "min_gap_district",
        # price drop ratio
        "drop_ratio", "total_active", "with_drops",
        # rotation
        "rate",
        # series (already aggregated weekly)
        "series", "breakpoint",
    }
    return {k: v for k, v in ind.items() if k in safe_keys}


def _sanitise_alert(alert: Dict) -> Dict:
    return {
        "level": alert.get("level", "info"),
        "title": alert.get("title", ""),
        "message": alert.get("detail") or alert.get("message", ""),
    }

# Métricas cuyo ratio es poco fiable por limitaciones del scraper
_UNRELIABLE_METRICS = {"supply_demand"}


# ===================================================================
# Build the public metrics JSON
# ===================================================================

def build_public_metrics() -> Dict[str, Any]:
    """
    Orchestrate all data loading and return the public JSON dict.
    """
    print("📊 Generating public metrics...")

    # 1. Macro data (needed for euribor rate → affordability)
    macro = _load_macro()
    euribor = macro.get("euribor", {})
    paro = macro.get("paro", {})
    euribor_rate = euribor.get("current") if euribor else None

    # 2. Internal indicators
    indicators = _load_internal_indicators(euribor_rate)

    # 3. Market score
    score = _load_market_score(indicators, euribor, paro)

    # 4. Alerts (base)
    alerts_raw = _load_market_alerts(indicators, macro)

    # 4b. Custom alerts (not in market_indicators.py)
    aged_alert = _build_aged_stock_alert()
    if aged_alert:
        alerts_raw = alerts_raw + [aged_alert]

    # 5. Zone-level data
    zone_prices = _load_zone_prices()
    zone_speed = _load_zone_speed()
    rental_yields = _load_rental_yields()

    # 6. Trends
    district_trend = _load_price_trend_by_district()
    market_trend = _load_market_trend()

    # 7. Extra aggregates
    notarial_gap = _load_notarial_gap()
    db_stats = _load_db_stats()
    drop_stats = _load_price_drop_stats()

    # ----- Merge zone prices + speed into one list -----
    speed_map = {z.get("zone"): z for z in zone_speed}
    zones_merged = []
    for zp in zone_prices:
        name = zp.get("zone", "")
        entry = {
            "name": name,
            "median_price": zp.get("median_price"),
            "price_per_sqm": zp.get("median_price_sqm"),
            "active_count": zp.get("count"),
        }
        sp = speed_map.get(name, {})
        entry["days_to_sell"] = sp.get("median_days")
        zones_merged.append(entry)

    # ----- Rental yields (top barrios) -----
    yields_clean = []
    for ry in rental_yields[:15]:
        yields_clean.append({
            "barrio": ry.get("barrio", ""),
            "distrito": ry.get("distrito", ""),
            "gross_yield": ry.get("yield_pct"),
            "rent_median": ry.get("median_rent"),
            "sale_price_sqm": ry.get("median_sale_sqm"),
        })

    # ----- Build final JSON -----
    metrics: Dict[str, Any] = {
        "metadata": {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "version": "1.0",
            "source": "Termómetro Inmobiliario Madrid",
        },
        "market_score": {
            "score": score.get("score"),
            "label": score.get("label", ""),
            "emoji": score.get("emoji", ""),
            "description": score.get("description", ""),
            "trend": score.get("trend", "stable"),
        },
        "indicators": {
            k: _sanitise_indicator(v)
            for k, v in indicators.items()
        },
        "macro": {
            k: {
                "name": v.get("name", k),
                "current": v.get("current"),
                "previous": v.get("previous"),
                "change": v.get("change"),
                "trend": v.get("trend"),
                "unit": v.get("unit", ""),
            }
            for k, v in macro.items()
            if isinstance(v, dict)
        },
        "zones": zones_merged,
        "rental_yields": yields_clean,
        "trends": {
            "market": market_trend,
            "by_district": district_trend,
        },
        "notarial_gap": notarial_gap,
        "price_drop_stats": drop_stats,
        "db_stats": db_stats,
        "alerts": [
            _sanitise_alert(a) for a in alerts_raw
            if a.get("metric") not in _UNRELIABLE_METRICS
        ][:10],
    }

    print(f"✅ Public metrics generated — {len(json.dumps(metrics)) / 1024:.1f} KB")
    return metrics


# ===================================================================
# CLI
# ===================================================================

def export_metrics(output_path: Optional[str] = None) -> bool:
    """Generate and optionally write the public metrics JSON."""
    try:
        metrics = build_public_metrics()
        pretty = json.dumps(metrics, ensure_ascii=False, indent=2, default=str)

        if output_path:
            os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(pretty)
            print(f"📁 Written to {output_path}")
        else:
            print(pretty)

        return True
    except Exception as exc:
        print(f"❌ Export failed: {exc}")
        traceback.print_exc()
        return False


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Export public market metrics")
    parser.add_argument("-o", "--output", help="Output JSON file path")
    parser.add_argument("--push", action="store_true",
                        help="Git commit+push after writing (requires -o)")
    args = parser.parse_args()

    ok = export_metrics(args.output)

    if ok and args.push and args.output:
        import subprocess
        repo_dir = os.path.dirname(os.path.abspath(args.output))
        print(f"🚀 Pushing to git in {repo_dir}...")
        subprocess.run(["git", "-C", repo_dir, "add", os.path.basename(args.output)])
        subprocess.run([
            "git", "-C", repo_dir, "commit", "-m",
            f"Update metrics {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        ])
        subprocess.run(["git", "-C", repo_dir, "push", "origin", "main"])

    sys.exit(0 if ok else 1)
