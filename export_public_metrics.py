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


def _load_market_score(indicators: Dict, euribor: Dict, paro: Dict,
                       afiliados_ss: Dict = None) -> Dict:
    from market_indicators import calculate_market_score
    return _safe(
        calculate_market_score,
        price_trend=indicators.get("price_trend", {}),
        sales_speed=indicators.get("sales_speed", {}),
        supply_demand=indicators.get("supply_demand", {}),
        inventory=indicators.get("inventory", {}),
        euribor=euribor,
        paro=paro,
        afiliados_ss=afiliados_ss,
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


# All 139 barrios tracked by the scraper, grouped by district
_ALL_BARRIOS: list = [
    # Arganzuela
    ("Arganzuela", "Acacias"), ("Arganzuela", "Chopera"), ("Arganzuela", "Delicias"),
    ("Arganzuela", "Imperial"), ("Arganzuela", "Legazpi"), ("Arganzuela", "Palos de la Frontera"),
    # Barajas
    ("Barajas", "Aeropuerto"), ("Barajas", "Alameda de Osuna"),
    ("Barajas", "Campo de las Naciones"), ("Barajas", "Casco Histórico de Barajas"), ("Barajas", "Timón"),
    # Carabanchel
    ("Carabanchel", "Abrantes"), ("Carabanchel", "Buena Vista"), ("Carabanchel", "Comillas"),
    ("Carabanchel", "Opañel"), ("Carabanchel", "PAU de Carabanchel"), ("Carabanchel", "Puerta Bonita"),
    ("Carabanchel", "San Isidro"), ("Carabanchel", "Vista Alegre"),
    # Centro
    ("Centro", "Chueca-Justicia"), ("Centro", "Huertas-Cortes"), ("Centro", "Lavapiés-Embajadores"),
    ("Centro", "Malasaña-Universidad"), ("Centro", "Palacio"), ("Centro", "Sol"),
    # Chamartín
    ("Chamartín", "Bernabéu-Hispanoamérica"), ("Chamartín", "Castilla"), ("Chamartín", "Ciudad Jardín"),
    ("Chamartín", "El Viso"), ("Chamartín", "Nueva España"), ("Chamartín", "Prosperidad"),
    # Chamberí
    ("Chamberí", "Almagro"), ("Chamberí", "Arapiles"), ("Chamberí", "Gaztambide"),
    ("Chamberí", "Nuevos Ministerios-Ríos Rosas"), ("Chamberí", "Trafalgar"), ("Chamberí", "Vallehermoso"),
    # Ciudad Lineal
    ("Ciudad Lineal", "Atalaya"), ("Ciudad Lineal", "Colina"), ("Ciudad Lineal", "Concepción"),
    ("Ciudad Lineal", "Costillares"), ("Ciudad Lineal", "Pueblo Nuevo"), ("Ciudad Lineal", "Quintana"),
    ("Ciudad Lineal", "San Juan Bautista"), ("Ciudad Lineal", "San Pascual"), ("Ciudad Lineal", "Ventas"),
    # Fuencarral-El Pardo
    ("Fuencarral-El Pardo", "Arroyo del Fresno"), ("Fuencarral-El Pardo", "El Pardo"),
    ("Fuencarral-El Pardo", "Fuentelarreina"), ("Fuencarral-El Pardo", "La Paz"),
    ("Fuencarral-El Pardo", "Las Tablas"), ("Fuencarral-El Pardo", "Mirasierra"),
    ("Fuencarral-El Pardo", "Montecarmelo"), ("Fuencarral-El Pardo", "Peñagrande"),
    ("Fuencarral-El Pardo", "Pilar"), ("Fuencarral-El Pardo", "Tres Olivos-Valverde"),
    # Hortaleza
    ("Hortaleza", "Apóstol Santiago"), ("Hortaleza", "Canillas"), ("Hortaleza", "Conde Orgaz-Piovera"),
    ("Hortaleza", "Palomas"), ("Hortaleza", "Pinar del Rey"), ("Hortaleza", "Sanchinarro"),
    ("Hortaleza", "Valdebebas-Valdefuentes"), ("Hortaleza", "Virgen del Cortijo-Manoteras"),
    # Latina
    ("Latina", "Águilas"), ("Latina", "Aluche"), ("Latina", "Campamento"),
    ("Latina", "Cuatro Vientos"), ("Latina", "Los Cármenes"), ("Latina", "Lucero"),
    ("Latina", "Puerta del Ángel"),
    # Moncloa-Aravaca
    ("Moncloa-Aravaca", "Aravaca"), ("Moncloa-Aravaca", "Argüelles"), ("Moncloa-Aravaca", "Casa de Campo"),
    ("Moncloa-Aravaca", "Ciudad Universitaria"), ("Moncloa-Aravaca", "El Plantío"),
    ("Moncloa-Aravaca", "Valdemarín"), ("Moncloa-Aravaca", "Valdezarza"),
    # Moratalaz
    ("Moratalaz", "Fontarrón"), ("Moratalaz", "Horcajo"), ("Moratalaz", "Marroquina"),
    ("Moratalaz", "Media Legua"), ("Moratalaz", "Pavones"), ("Moratalaz", "Vinateros"),
    # Puente de Vallecas
    ("Puente de Vallecas", "Entrevías"), ("Puente de Vallecas", "Numancia"),
    ("Puente de Vallecas", "Palomeras Bajas"), ("Puente de Vallecas", "Palomeras Sureste"),
    ("Puente de Vallecas", "Portazgo"), ("Puente de Vallecas", "San Diego"),
    # Retiro
    ("Retiro", "Adelfas"), ("Retiro", "Estrella"), ("Retiro", "Ibiza"),
    ("Retiro", "Jerónimos"), ("Retiro", "Niño Jesús"), ("Retiro", "Pacífico"),
    # Salamanca
    ("Salamanca", "Castellana"), ("Salamanca", "Fuente del Berro"), ("Salamanca", "Goya"),
    ("Salamanca", "Guindalera"), ("Salamanca", "Lista"), ("Salamanca", "Recoletos"),
    # San Blas-Canillejas
    ("San Blas-Canillejas", "Amposta"), ("San Blas-Canillejas", "Arcos"),
    ("San Blas-Canillejas", "Canillejas"), ("San Blas-Canillejas", "Hellín"),
    ("San Blas-Canillejas", "Rejas"), ("San Blas-Canillejas", "Rosas"),
    ("San Blas-Canillejas", "Salvador"), ("San Blas-Canillejas", "Simancas"),
    # Tetuán
    ("Tetuán", "Bellas Vistas"), ("Tetuán", "Berruguete"), ("Tetuán", "Cuatro Caminos"),
    ("Tetuán", "Cuzco-Castillejos"), ("Tetuán", "Valdeacederas"), ("Tetuán", "Ventilla-Almenara"),
    # Usera
    ("Usera", "12 de Octubre-Orcasur"), ("Usera", "Almendrales"), ("Usera", "Moscardó"),
    ("Usera", "Orcasitas"), ("Usera", "Pradolongo"), ("Usera", "San Fermín"), ("Usera", "Zofío"),
    # Vicálvaro
    ("Vicálvaro", "Ambroz"), ("Vicálvaro", "Casco Histórico de Vicálvaro"),
    ("Vicálvaro", "El Cañaveral"), ("Vicálvaro", "Los Ahijones"), ("Vicálvaro", "Los Berrocales"),
    ("Vicálvaro", "Los Cerros"), ("Vicálvaro", "Valdebernardo-Valderrivas"),
    # Villa de Vallecas
    ("Villa de Vallecas", "Casco Histórico de Vallecas"),
    ("Villa de Vallecas", "Ensanche de Vallecas-La Gavia"),
    ("Villa de Vallecas", "Santa Eugenia"), ("Villa de Vallecas", "Valdecarros"),
    # Villaverde
    ("Villaverde", "Butarque"), ("Villaverde", "Los Ángeles"), ("Villaverde", "Los Rosales"),
    ("Villaverde", "San Cristóbal"), ("Villaverde", "Villaverde Alto"),
]


def _load_barrio_data(rental_yields_raw: list) -> list:
    """
    Build a summary entry for each of the 139 tracked barrios.
    Merges KPIs from get_barrio_summary() with rental yield if available.
    Barrios with fewer than 5 active listings are still included but with null values.
    """
    from database import get_barrio_summary
    barrio_names = [b for _, b in _ALL_BARRIOS]
    distrito_map = {b: d for d, b in _ALL_BARRIOS}

    summaries = _safe(get_barrio_summary, barrio_names) or []
    summary_map = {s["barrio"]: s for s in summaries}

    # Build yield lookup by barrio name
    yield_map = {ry.get("barrio", ""): ry for ry in rental_yields_raw}

    result = []
    for distrito, barrio in _ALL_BARRIOS:
        s = summary_map.get(barrio, {})
        ry = yield_map.get(barrio, {})
        entry = {
            "barrio": barrio,
            "distrito": s.get("distrito") or distrito,
            "active_count": s.get("active_count"),
            "median_price": round(s["median_price"]) if s.get("median_price") else None,
            "price_per_sqm": round(s["median_price_sqm"]) if s.get("median_price_sqm") else None,
            "avg_size_sqm": round(s["avg_size_sqm"], 1) if s.get("avg_size_sqm") else None,
            "avg_rooms": round(s["avg_rooms"], 1) if s.get("avg_rooms") else None,
            "avg_days_market": round(s["avg_days_market"]) if s.get("avg_days_market") else None,
            "gross_yield": ry.get("yield_pct"),
            "rent_median": ry.get("median_rent"),
        }
        result.append(entry)
    return result


def _load_barrio_trends(barrios_with_data: list) -> list:
    """
    Return weekly price evolution for barrios that have active listings.
    Uses last_seen_date (same approach as market/district trends).
    """
    from database import get_price_evolution_by_barrio
    if not barrios_with_data:
        return []
    return _safe(get_price_evolution_by_barrio, barrios_with_data, 12) or []


def _load_valuation_model() -> Dict:
    """
    Compute valuation coefficients from the listings data.

    Returns a dict with:
    - barrio_baselines: per-barrio median €/m², count, std, avg_rooms, avg_size
    - district_baselines: per-district fallback
    - adjustments: global coefficients for floor, elevator, exterior
    - model_stats: training samples, date
    """
    import statistics
    from collections import defaultdict
    from database import get_connection

    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT distrito, barrio, price, size_sqm, rooms, floor, orientation
                FROM listings
                WHERE status IN ('active', 'sold_removed')
                  AND price > 10000
                  AND size_sqm > 10
                  AND distrito IS NOT NULL
                  AND barrio IS NOT NULL
            """)
            rows = cur.fetchall()

        if not rows:
            return {}

        # ── Parse floor & orientation ─────────────────
        import re

        def parse_floor(floor_str):
            if not floor_str:
                return 0.0, None, None
            f = str(floor_str).lower()
            has_lift = True if "con ascensor" in f else (False if "sin ascensor" in f else None)
            is_ext = None  # extracted from orientation
            level = 0.0
            if "bajo" in f or "sótano" in f:
                level = 0.0
            elif "entreplanta" in f:
                level = 0.5
            elif "ático" in f:
                level = 10.0
            else:
                m = re.search(r"planta (\d+)", f)
                if m:
                    level = float(m.group(1))
            return level, has_lift, is_ext

        # ── Gather per-barrio stats ────────────────────
        barrio_data = defaultdict(lambda: {
            "prices_sqm": [], "sizes": [], "rooms_list": [],
            "floors": [], "with_lift": 0, "without_lift": 0,
            "exterior": 0, "interior": 0, "distrito": "",
        })

        for distrito, barrio, price, size_sqm, rooms, floor, orient in rows:
            psqm = price / size_sqm
            bd = barrio_data[barrio]
            bd["distrito"] = distrito
            bd["prices_sqm"].append(psqm)
            bd["sizes"].append(size_sqm)
            if rooms:
                bd["rooms_list"].append(rooms)
            level, has_lift, _ = parse_floor(floor)
            bd["floors"].append(level)
            if has_lift is True:
                bd["with_lift"] += 1
            elif has_lift is False:
                bd["without_lift"] += 1
            if orient:
                o = str(orient).lower()
                if o == "exterior":
                    bd["exterior"] += 1
                elif o == "interior":
                    bd["interior"] += 1

        # ── Build barrio baselines ─────────────────────
        barrio_baselines = {}
        all_prices_sqm = []

        for barrio, bd in barrio_data.items():
            plist = bd["prices_sqm"]
            if len(plist) < 3:
                continue
            med = statistics.median(plist)
            # Filter extreme outliers (>3x or <0.33x median)
            filtered = [p for p in plist if 0.33 * med < p < 3 * med]
            if len(filtered) < 3:
                filtered = plist

            med_clean = statistics.median(filtered)
            std = statistics.stdev(filtered) if len(filtered) > 1 else med_clean * 0.15
            all_prices_sqm.extend(filtered)

            barrio_baselines[barrio] = {
                "distrito": bd["distrito"],
                "median_sqm": round(med_clean),
                "std_sqm": round(std),
                "count": len(filtered),
                "avg_size": round(statistics.mean(bd["sizes"]), 1),
                "avg_rooms": round(statistics.mean(bd["rooms_list"]), 1) if bd["rooms_list"] else None,
                "avg_floor": round(statistics.mean(bd["floors"]), 1) if bd["floors"] else None,
            }

        # ── District baselines (fallback) ──────────────
        district_data = defaultdict(list)
        for barrio, bl in barrio_baselines.items():
            district_data[bl["distrito"]].append(bl["median_sqm"])

        district_baselines = {}
        for dist, prices in district_data.items():
            district_baselines[dist] = {
                "median_sqm": round(statistics.median(prices)),
                "count": sum(
                    barrio_baselines[b]["count"]
                    for b, bl in barrio_baselines.items()
                    if bl["distrito"] == dist
                ),
            }

        # ── Global adjustment coefficients ─────────────
        # Elevator premium: compare with_lift vs without_lift listings
        lift_prices = []
        no_lift_prices = []
        ext_prices = []
        int_prices = []

        for _, bd in barrio_data.items():
            # We need individual listing data for this...
            pass

        # Recompute from raw rows for elevator & exterior premium
        lift_sqm = []
        no_lift_sqm = []
        ext_sqm = []
        int_sqm = []
        floor_price_pairs = []  # (floor_level, €/m²)

        for _, _, price, size_sqm, _, floor, orient in rows:
            psqm = price / size_sqm
            level, has_lift, _ = parse_floor(floor)
            if has_lift is True:
                lift_sqm.append(psqm)
            elif has_lift is False:
                no_lift_sqm.append(psqm)
            if orient:
                o = str(orient).lower()
                if o == "exterior":
                    ext_sqm.append(psqm)
                elif o == "interior":
                    int_sqm.append(psqm)
            floor_price_pairs.append((level, psqm))

        # Calculate premiums — CLAMPED to realistic ranges to avoid
        # confounding bias (e.g. lifts correlate with expensive zones).
        # Market studies for Madrid put these premiums at:
        #   elevator: +3% to +8%
        #   exterior: +2% to +6%
        #   floor:    +0.5% to +2.5% per level
        #   room:     +1% to +3% per extra room
        lift_premium = 0.05  # default 5%
        if lift_sqm and no_lift_sqm:
            med_lift = statistics.median(lift_sqm)
            med_no_lift = statistics.median(no_lift_sqm)
            if med_no_lift > 0:
                raw = (med_lift - med_no_lift) / med_no_lift
                lift_premium = max(0.03, min(0.08, raw))  # clamp 3-8%

        ext_premium = 0.03  # default 3%
        if ext_sqm and int_sqm:
            med_ext = statistics.median(ext_sqm)
            med_int = statistics.median(int_sqm)
            if med_int > 0:
                raw = (med_ext - med_int) / med_int
                ext_premium = max(0.02, min(0.06, raw))  # clamp 2-6%

        floor_premium = 0.015  # default 1.5% per floor
        if floor_price_pairs:
            high_floor = [p for f, p in floor_price_pairs if f >= 3]
            low_floor = [p for f, p in floor_price_pairs if f <= 1]
            if high_floor and low_floor:
                med_high = statistics.median(high_floor)
                med_low = statistics.median(low_floor)
                if med_low > 0:
                    avg_diff = (med_high - med_low) / med_low
                    floor_premium = round(avg_diff / 4, 4)
                    floor_premium = max(0.005, min(0.025, floor_premium))  # clamp 0.5-2.5%

        room_premium = 0.02  # default 2% per room

        # Madrid global median
        madrid_median_sqm = round(statistics.median(all_prices_sqm)) if all_prices_sqm else 0

        return {
            "barrio_baselines": barrio_baselines,
            "district_baselines": district_baselines,
            "adjustments": {
                "elevator_premium": round(lift_premium, 4),
                "exterior_premium": round(ext_premium, 4),
                "floor_premium_per_level": round(floor_premium, 4),
                "room_premium_per_unit": round(room_premium, 4),
                "terrace_premium": 0.06,
                "garage_premium": 0.04,
                "condition_reformado": 0.12,
                "condition_obra_nueva": 0.18,
                "energy_A": 0.05,
                "energy_B": 0.03,
                "energy_C": 0.01,
                "energy_D": 0.0,
                "energy_E": -0.02,
                "energy_F": -0.04,
                "energy_G": -0.06,
            },
            "madrid_median_sqm": madrid_median_sqm,
            "training_samples": len(rows),
            "training_date": datetime.utcnow().isoformat() + "Z",
        }
    except Exception as exc:
        print(f"⚠️  _load_valuation_model failed: {exc}")
        traceback.print_exc()
        return {}


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
    afiliados_ss = macro.get("afiliados_ss", {})
    euribor_rate = euribor.get("current") if euribor else None

    # 2. Internal indicators
    indicators = _load_internal_indicators(euribor_rate)

    # 3. Market score
    score = _load_market_score(indicators, euribor, paro, afiliados_ss)

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

    # 8. Barrio-level data (Fase 2)
    barrios_data = _load_barrio_data(rental_yields)
    barrios_with_data = [b["barrio"] for b in barrios_data if b.get("active_count")]
    barrio_trends = _load_barrio_trends(barrios_with_data)

    # 9. Valuation model coefficients (Fase 5)
    valuation_model = _load_valuation_model()

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
                "change_pct": v.get("change_pct"),
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
        "barrios": barrios_data,
        "barrio_trends": barrio_trends,
        "price_drop_stats": drop_stats,
        "db_stats": db_stats,
        "alerts": [
            _sanitise_alert(a) for a in alerts_raw
            if a.get("metric") not in _UNRELIABLE_METRICS
        ][:10],
        "valuation_model": valuation_model,
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
