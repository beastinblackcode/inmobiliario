"""
Internal market indicators module for Market Surveillance.
Calculates market health metrics from scraped property data.
"""

import sqlite3
import statistics
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from database import get_connection


# ============================================================================
# Trend Breakpoint Detection Helper
# ============================================================================

def _detect_trend_breakpoint(series: list, value_key: str = "median_price") -> Dict:
    """
    Detect whether a sustained trend has recently reversed.

    Uses a simple 3-point moving average comparison:
      - Splits the series into first-half and second-half.
      - If the direction of the average slope flips, a breakpoint is flagged.

    Returns:
        Dict with 'breakpoint' (bool), 'direction_before', 'direction_after',
        'breakpoint_week' (week_start of the inflection point).
    """
    result = {
        "breakpoint": False,
        "direction_before": None,
        "direction_after": None,
        "breakpoint_week": None
    }

    values = [pt.get(value_key) for pt in series if pt.get(value_key) is not None]
    if len(values) < 4:
        return result

    # Smooth with 3-point moving average
    def slope(segment):
        if len(segment) < 2:
            return 0
        return segment[-1] - segment[0]

    mid = len(values) // 2
    first_slope = slope(values[:mid])
    second_slope = slope(values[mid:])

    def direction(s):
        if s > 0:
            return "up"
        elif s < 0:
            return "down"
        return "stable"

    dir_before = direction(first_slope)
    dir_after = direction(second_slope)

    if dir_before != dir_after and dir_before != "stable" and dir_after != "stable":
        result["breakpoint"] = True
        result["direction_before"] = dir_before
        result["direction_after"] = dir_after
        # Approximate the inflection week
        if mid < len(series):
            result["breakpoint_week"] = series[mid].get("week_start")

    result["direction_before"] = dir_before
    result["direction_after"] = dir_after
    return result


# ============================================================================
# Weekly Price Evolution
# ============================================================================

def get_weekly_price_evolution(weeks: int = 8) -> Dict:
    """
    Calculate median price evolution of active properties week by week.
    
    Returns:
        Dict with 'series' (weekly data), 'current', 'change_pct', 'trend'
    """
    result = {
        "name": "Precio Mediano",
        "unit": "€",
        "series": [],
        "current": None,
        "previous": None,
        "change": None,
        "change_pct": None,
        "trend": "stable"
    }
    
    with get_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT DISTINCT strftime('%Y-%W', first_seen_date) as week_num,
                   MIN(first_seen_date) as week_start
            FROM listings
            WHERE first_seen_date IS NOT NULL
            GROUP BY strftime('%Y-%W', first_seen_date)
            ORDER BY first_seen_date DESC
            LIMIT ?
        """, (weeks,))
        
        week_info = cursor.fetchall()
        week_info.reverse()  # Chronological order
        
        for week_num, week_start in week_info:
            if not week_num:
                continue
                
            cursor.execute("""
                SELECT price FROM listings
                WHERE price > 0
                AND strftime('%Y-%W', first_seen_date) = ?
            """, (week_num,))
            
            prices = [row[0] for row in cursor.fetchall()]
            
            if len(prices) >= 10:
                median_price = statistics.median(prices)
                
                # Also get €/m²
                cursor.execute("""
                    SELECT price / size_sqm FROM listings
                    WHERE price > 0 AND size_sqm > 0
                    AND strftime('%Y-%W', first_seen_date) = ?
                """, (week_num,))
                prices_sqm = [row[0] for row in cursor.fetchall()]
                median_sqm = statistics.median(prices_sqm) if prices_sqm else 0
                
                result["series"].append({
                    "week": week_num,
                    "week_start": week_start,
                    "median_price": round(median_price),
                    "median_price_sqm": round(median_sqm),
                    "count": len(prices)
                })
        
        if len(result["series"]) >= 2:
            # Compare last 2 weeks (skip baseline if it's the first)
            current = result["series"][-1]
            previous = result["series"][-2]

            result["current"] = current["median_price"]
            result["current_sqm"] = current["median_price_sqm"]
            result["previous"] = previous["median_price"]
            result["change"] = result["current"] - result["previous"]
            result["change_pct"] = round(
                (result["change"] / result["previous"] * 100) if result["previous"] > 0 else 0, 2
            )

            if result["change_pct"] < -1:
                result["trend"] = "down"
            elif result["change_pct"] > 1:
                result["trend"] = "up"
            else:
                result["trend"] = "stable"

            # Breakpoint detection over the full window
            result["breakpoint"] = _detect_trend_breakpoint(
                result["series"], value_key="median_price"
            )

    return result


# ============================================================================
# Sales Speed (Time to Sale)
# ============================================================================

def get_weekly_sales_speed(weeks: int = 8) -> Dict:
    """
    Calculate median days on market for sold properties by week.
    """
    result = {
        "name": "Velocidad de Venta",
        "unit": "días",
        "series": [],
        "current": None,
        "previous": None,
        "change": None,
        "trend": "stable"
    }
    
    with get_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT DISTINCT strftime('%Y-%W', last_seen_date) as week_num,
                   MIN(last_seen_date) as week_start
            FROM listings
            WHERE status = 'sold_removed'
            AND last_seen_date IS NOT NULL
            GROUP BY strftime('%Y-%W', last_seen_date)
            ORDER BY last_seen_date DESC
            LIMIT ?
        """, (weeks,))
        
        week_info = cursor.fetchall()
        week_info.reverse()
        
        for week_num, week_start in week_info:
            if not week_num:
                continue
                
            cursor.execute("""
                SELECT julianday(last_seen_date) - julianday(first_seen_date) as days
                FROM listings
                WHERE status = 'sold_removed'
                AND first_seen_date IS NOT NULL
                AND last_seen_date IS NOT NULL
                AND strftime('%Y-%W', last_seen_date) = ?
                AND julianday(last_seen_date) - julianday(first_seen_date) >= 0
            """, (week_num,))
            
            days = [row[0] for row in cursor.fetchall() if row[0] is not None]
            
            if len(days) >= 5:
                median_days = statistics.median(days)
                avg_days = statistics.mean(days)
                
                result["series"].append({
                    "week": week_num,
                    "week_start": week_start,
                    "median_days": round(median_days, 1),
                    "avg_days": round(avg_days, 1),
                    "sold_count": len(days)
                })
        
        if len(result["series"]) >= 2:
            current = result["series"][-1]
            previous = result["series"][-2]
            
            result["current"] = current["median_days"]
            result["previous"] = previous["median_days"]
            result["change"] = round(result["current"] - result["previous"], 1)
            
            # Faster sales = positive market signal
            if result["change"] < -1:
                result["trend"] = "up"  # Faster = market accelerating
            elif result["change"] > 1:
                result["trend"] = "down"  # Slower = market decelerating
            else:
                result["trend"] = "stable"
    
    return result


# ============================================================================
# Supply/Demand Ratio
# ============================================================================

def get_supply_demand_ratio(weeks: int = 8) -> Dict:
    """
    Calculate ratio of new listings to sold/removed per week.
    Ratio > 1.5 = excess supply, < 0.8 = excess demand
    """
    result = {
        "name": "Ratio Oferta/Demanda",
        "unit": "ratio",
        "series": [],
        "current": None,
        "previous": None,
        "change": None,
        "trend": "stable"
    }
    
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # Get all weeks with data
        cursor.execute("""
            SELECT DISTINCT strftime('%Y-%W', first_seen_date) as week_num,
                   MIN(first_seen_date) as week_start
            FROM listings
            WHERE first_seen_date IS NOT NULL
            GROUP BY strftime('%Y-%W', first_seen_date)
            ORDER BY first_seen_date DESC
            LIMIT ?
        """, (weeks,))
        
        week_info = cursor.fetchall()
        week_info.reverse()
        
        # Skip first week (baseline)
        for week_num, week_start in week_info[1:]:
            if not week_num:
                continue
            
            # New listings this week
            cursor.execute("""
                SELECT COUNT(*) FROM listings
                WHERE strftime('%Y-%W', first_seen_date) = ?
            """, (week_num,))
            new_count = cursor.fetchone()[0]
            
            # Sold/removed this week
            cursor.execute("""
                SELECT COUNT(*) FROM listings
                WHERE status = 'sold_removed'
                AND strftime('%Y-%W', last_seen_date) = ?
            """, (week_num,))
            sold_count = cursor.fetchone()[0]
            
            # Cap: if no sales treat as strong excess supply (10.0 = ≥10× more new than sold)
            MAX_RATIO = 10.0
            if sold_count > 0:
                ratio = round(min(new_count / sold_count, MAX_RATIO), 2)
            elif new_count > 0:
                ratio = MAX_RATIO  # No absorption at all
            else:
                ratio = 1.0  # No activity — neutral

            result["series"].append({
                "week": week_num,
                "week_start": week_start,
                "new_count": new_count,
                "sold_count": sold_count,
                "ratio": ratio,
                "capped": sold_count == 0 and new_count > 0
            })
        
        if len(result["series"]) >= 2:
            current = result["series"][-1]
            previous = result["series"][-2]
            
            result["current"] = current["ratio"]
            result["previous"] = previous["ratio"]
            result["change"] = round(result["current"] - result["previous"], 2)
            
            # High ratio = more supply than demand
            if result["current"] > 2.0:
                result["trend"] = "up"  # Excess supply
            elif result["current"] < 0.8:
                result["trend"] = "down"  # Excess demand (fast absorption)
            else:
                result["trend"] = "stable"
    
    return result


# ============================================================================
# Inventory Evolution
# ============================================================================

def get_inventory_evolution(weeks: int = 8) -> Dict:
    """
    Track active inventory (stock) over time.
    """
    result = {
        "name": "Inventario Activo",
        "unit": "propiedades",
        "series": [],
        "current": None,
        "previous": None,
        "change": None,
        "change_pct": None,
        "trend": "stable"
    }
    
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # Get dates to sample (Mondays)
        cursor.execute("""
            SELECT DISTINCT last_seen_date FROM listings
            WHERE last_seen_date IS NOT NULL
            ORDER BY last_seen_date
        """)
        all_dates = [r[0] for r in cursor.fetchall()]
        
        # Sample weekly (Mondays + last date)
        sampled = []
        for d in all_dates:
            try:
                dt = datetime.strptime(d, '%Y-%m-%d')
                if dt.weekday() == 0:  # Monday
                    sampled.append(d)
            except:
                pass
        
        # Always include last date
        if all_dates and (not sampled or sampled[-1] != all_dates[-1]):
            sampled.append(all_dates[-1])
        
        # Limit to last N weeks
        sampled = sampled[-weeks:]
        
        for d in sampled:
            cursor.execute("""
                SELECT COUNT(*) FROM listings
                WHERE first_seen_date <= ?
                AND (last_seen_date >= ? OR status = 'active')
            """, (d, d))
            count = cursor.fetchone()[0]
            
            result["series"].append({
                "date": d,
                "count": count
            })
        
        if len(result["series"]) >= 2:
            result["current"] = result["series"][-1]["count"]
            result["previous"] = result["series"][-2]["count"]
            result["change"] = result["current"] - result["previous"]
            result["change_pct"] = round(
                (result["change"] / result["previous"] * 100) if result["previous"] > 0 else 0, 1
            )
            
            if result["change_pct"] < -3:
                result["trend"] = "down"
            elif result["change_pct"] > 3:
                result["trend"] = "up"
            else:
                result["trend"] = "stable"
    
    return result


# ============================================================================
# Rotation Rate
# ============================================================================

def get_rotation_rate(weeks: int = 4) -> Dict:
    """
    Calculate rolling rotation rate: sales in last N weeks / avg active inventory.

    This is a true market-velocity metric (avoids the ever-growing denominator
    of the old lifetime-cumulative version).

    Returns:
        Dict with 'current' (%) for the latest window and 'series' for charting.
    """
    result = {
        "name": "Tasa de Rotación",
        "unit": "%",
        "series": [],
        "current": None,
        "previous": None,
        "change": None,
        "active": None,
        "sold_window": None,
        "trend": "stable",
        "window_weeks": weeks
    }

    with get_connection() as conn:
        cursor = conn.cursor()

        # Current active inventory (snapshot)
        cursor.execute("SELECT COUNT(*) FROM listings WHERE status = 'active'")
        active = cursor.fetchone()[0]
        result["active"] = active

        # Last scraping date as anchor
        cursor.execute("""
            SELECT MAX(last_seen_date) FROM listings
            WHERE last_seen_date IS NOT NULL
        """)
        row = cursor.fetchone()
        if not row or not row[0]:
            return result

        anchor_str = row[0]
        try:
            anchor = datetime.strptime(anchor_str, '%Y-%m-%d')
        except ValueError:
            return result

        # Build rolling 1-week buckets going back `weeks` weeks
        buckets = []
        for i in range(weeks):
            bucket_end = anchor - timedelta(weeks=i)
            bucket_start = bucket_end - timedelta(weeks=1)
            buckets.append((bucket_start.strftime('%Y-%m-%d'),
                            bucket_end.strftime('%Y-%m-%d')))

        buckets.reverse()  # chronological

        for start, end in buckets:
            # Sales that disappeared in this window
            cursor.execute("""
                SELECT COUNT(*) FROM listings
                WHERE status = 'sold_removed'
                AND last_seen_date > ?
                AND last_seen_date <= ?
            """, (start, end))
            sold_week = cursor.fetchone()[0]

            # Active inventory at the end of the window
            cursor.execute("""
                SELECT COUNT(*) FROM listings
                WHERE first_seen_date <= ?
                AND (last_seen_date >= ? OR status = 'active')
            """, (end, end))
            active_week = cursor.fetchone()[0]

            rate = round(sold_week / active_week * 100, 1) if active_week > 0 else 0
            result["series"].append({
                "week_start": start,
                "week_end": end,
                "sold": sold_week,
                "active": active_week,
                "rate": rate
            })

        # Summary metrics from all buckets in window
        total_sold = sum(b["sold"] for b in result["series"])
        avg_active = (sum(b["active"] for b in result["series"]) / len(result["series"])
                      if result["series"] else 0)

        result["sold_window"] = total_sold
        result["current"] = round(total_sold / avg_active * 100, 1) if avg_active > 0 else 0

        # Trend: compare first half vs second half of window
        if len(result["series"]) >= 4:
            mid = len(result["series"]) // 2
            first_half = result["series"][:mid]
            second_half = result["series"][mid:]
            rate_first = (sum(b["sold"] for b in first_half) /
                          max(sum(b["active"] for b in first_half) / len(first_half), 1) * 100)
            rate_second = (sum(b["sold"] for b in second_half) /
                           max(sum(b["active"] for b in second_half) / len(second_half), 1) * 100)
            result["previous"] = round(rate_first, 1)
            result["change"] = round(result["current"] - result["previous"], 1)
            if result["change"] > 2:
                result["trend"] = "up"
            elif result["change"] < -2:
                result["trend"] = "down"

    return result


# ============================================================================
# Affordability Index
# ============================================================================

def get_affordability_index(euribor_rate: float = None) -> Dict:
    """
    Estimate housing affordability as monthly mortgage payment on the median property.

    Assumptions (configurable):
    - Loan-to-value:  80 % of median price
    - Term:           25 years (300 monthly payments)
    - Rate:           Euríbor 12m + 1 % spread (variable mortgage reference)
    - If Euríbor is unavailable, falls back to 3.5 % as conservative estimate.

    Returns:
        Dict with 'monthly_payment', 'annual_cost', 'price_to_income_ratio'
        (using Spanish median household income ~€30k/year as reference),
        'affordable' (bool), and 'trend'.
    """
    SPREAD = 1.0            # bank spread above Euríbor (%)
    LTV = 0.80              # loan-to-value ratio
    TERM_YEARS = 25
    REFERENCE_INCOME = 30_000  # approx. Spanish median household gross income (€/year)

    result = {
        "name": "Índice de Asequibilidad",
        "unit": "€/mes",
        "current": None,           # monthly payment
        "annual_cost": None,
        "median_price": None,
        "loan_amount": None,
        "rate_used": None,
        "price_to_income": None,   # years of gross income to buy
        "affordable": None,        # True if monthly < 33% gross monthly income
        "trend": "stable"
    }

    with get_connection() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            SELECT price FROM listings
            WHERE price > 0 AND status = 'active'
        """)
        prices = [row[0] for row in cursor.fetchall()]

    if not prices:
        return result

    median_price = statistics.median(prices)
    result["median_price"] = round(median_price)

    # Effective annual interest rate
    rate_annual = (euribor_rate or 3.5) + SPREAD
    result["rate_used"] = round(rate_annual, 2)

    # Monthly mortgage payment — standard annuity formula
    loan = median_price * LTV
    result["loan_amount"] = round(loan)
    n = TERM_YEARS * 12
    r = rate_annual / 100 / 12  # monthly rate

    if r > 0:
        monthly = loan * (r * (1 + r) ** n) / ((1 + r) ** n - 1)
    else:
        monthly = loan / n  # zero-interest edge case

    result["current"] = round(monthly)
    result["annual_cost"] = round(monthly * 12)

    # Price-to-income (gross years of income needed)
    result["price_to_income"] = round(median_price / REFERENCE_INCOME, 1)

    # Affordability threshold: monthly payment ≤ 33 % of gross monthly income
    gross_monthly = REFERENCE_INCOME / 12
    result["affordable"] = monthly <= gross_monthly * 0.33

    # Trend: compare payment threshold
    ratio = monthly / gross_monthly
    if ratio > 0.40:
        result["trend"] = "down"   # Very unaffordable
    elif ratio > 0.33:
        result["trend"] = "stable"  # Borderline
    else:
        result["trend"] = "up"     # Affordable

    return result


# ============================================================================
# Price Dispersion
# ============================================================================

def get_price_dispersion() -> Dict:
    """
    Calculate price dispersion: difference between mean and median.
    High dispersion = polarized market.
    """
    result = {
        "name": "Dispersión de Precios",
        "unit": "%",
        "current": None,
        "mean_price": None,
        "median_price": None,
        "trend": "stable"
    }
    
    with get_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT price FROM listings
            WHERE price > 0 AND status = 'active'
        """)
        prices = [row[0] for row in cursor.fetchall()]
        
        if prices:
            mean_price = statistics.mean(prices)
            median_price = statistics.median(prices)
            
            # Dispersion = how much mean exceeds median (as %)
            dispersion = round((mean_price - median_price) / median_price * 100, 1) if median_price > 0 else 0
            
            result["current"] = dispersion
            result["mean_price"] = round(mean_price)
            result["median_price"] = round(median_price)
            
            if dispersion > 50:
                result["trend"] = "up"  # Very polarized
            elif dispersion < 20:
                result["trend"] = "down"  # Homogeneous
            else:
                result["trend"] = "stable"
    
    return result


# ============================================================================
# Alert System
# ============================================================================

def get_market_alerts(
    price_trend: Dict = None,
    sales_speed: Dict = None,
    supply_demand: Dict = None,
    inventory: Dict = None,
    rotation: Dict = None,
    affordability: Dict = None,
    macro: Dict = None
) -> list:
    """
    Detect significant market changes and return a list of alert dicts.

    Each alert has:
        - 'level':   'critical' | 'warning' | 'info'
        - 'emoji':   visual indicator
        - 'title':   short headline
        - 'detail':  human-readable explanation
        - 'metric':  which indicator triggered it

    Callers can filter by level and display in the dashboard.
    """
    alerts = []

    def add(level, emoji, title, detail, metric):
        alerts.append({
            "level": level,
            "emoji": emoji,
            "title": title,
            "detail": detail,
            "metric": metric
        })

    # ── Price alerts ──────────────────────────────────────────────────────────
    if price_trend:
        chg = price_trend.get("change_pct") or 0
        if chg >= 5:
            add("critical", "🚨", "Subida brusca de precios",
                f"Los precios subieron un {chg:+.1f}% en la última semana.",
                "price_trend")
        elif chg <= -5:
            add("critical", "🚨", "Caída brusca de precios",
                f"Los precios bajaron un {chg:+.1f}% en la última semana.",
                "price_trend")
        elif chg >= 2:
            add("warning", "⚠️", "Precios al alza",
                f"Subida del {chg:+.1f}% respecto a la semana anterior.",
                "price_trend")
        elif chg <= -2:
            add("warning", "⚠️", "Precios a la baja",
                f"Bajada del {chg:+.1f}% respecto a la semana anterior.",
                "price_trend")

        # Trend breakpoint
        bp = price_trend.get("breakpoint", {})
        if bp and bp.get("breakpoint"):
            before = bp.get("direction_before", "")
            after = bp.get("direction_after", "")
            week = bp.get("breakpoint_week", "")
            add("warning", "🔄", "Cambio de tendencia de precios",
                f"La tendencia pasó de '{before}' a '{after}' alrededor de {week}.",
                "price_trend")

    # ── Sales speed alerts ────────────────────────────────────────────────────
    if sales_speed:
        speed = sales_speed.get("current")
        chg = sales_speed.get("change") or 0
        if speed is not None:
            if speed <= 2:
                add("info", "⚡", "Mercado muy activo",
                    f"Mediana de venta: {speed:.0f} días. Alta presión de demanda.",
                    "sales_speed")
            if chg >= 7:
                add("warning", "🐌", "Ventas ralentizándose",
                    f"El tiempo en mercado aumentó {chg:+.0f} días esta semana.",
                    "sales_speed")
            elif chg <= -7:
                add("warning", "🏃", "Aceleración de ventas",
                    f"El tiempo en mercado bajó {chg:+.0f} días esta semana.",
                    "sales_speed")

    # ── Supply / demand alerts ────────────────────────────────────────────────
    if supply_demand:
        ratio = supply_demand.get("current")
        if ratio is not None:
            if ratio >= 5:
                add("critical", "🚨", "Exceso severo de oferta",
                    f"Ratio O/D = {ratio:.1f}×. Entran {ratio:.0f}× más propiedades de las que salen.",
                    "supply_demand")
            elif ratio >= 3:
                add("warning", "⚠️", "Exceso de oferta",
                    f"Ratio O/D = {ratio:.1f}×. El inventario está creciendo rápido.",
                    "supply_demand")
            elif ratio <= 0.5:
                add("warning", "🔥", "Demanda muy superior a oferta",
                    f"Ratio O/D = {ratio:.1f}×. El stock se absorbe rápidamente.",
                    "supply_demand")

    # ── Inventory alerts ──────────────────────────────────────────────────────
    if inventory:
        chg_pct = inventory.get("change_pct") or 0
        current = inventory.get("current")
        if chg_pct >= 10:
            add("warning", "📦", "Inventario creciendo rápido",
                f"El stock activo creció un {chg_pct:+.1f}% — ahora {current:,} propiedades.",
                "inventory")
        elif chg_pct <= -10:
            add("warning", "📉", "Inventario cayendo rápido",
                f"El stock activo cayó un {chg_pct:+.1f}% — ahora {current:,} propiedades.",
                "inventory")

    # ── Rotation rate alerts ──────────────────────────────────────────────────
    if rotation:
        rate = rotation.get("current")
        chg = rotation.get("change") or 0
        if rate is not None:
            if rate >= 20:
                add("info", "🔥", "Rotación muy alta",
                    f"El {rate:.1f}% del inventario se vendió en las últimas {rotation.get('window_weeks', 4)} semanas.",
                    "rotation")
            elif rate < 2:
                add("warning", "⚠️", "Rotación muy baja",
                    f"Solo el {rate:.1f}% del inventario se vendió en las últimas {rotation.get('window_weeks', 4)} semanas.",
                    "rotation")
            if chg <= -5:
                add("warning", "📉", "Tasa de rotación bajando",
                    f"La rotación bajó {chg:+.1f}pp respecto al periodo anterior.",
                    "rotation")

    # ── Affordability alerts ──────────────────────────────────────────────────
    if affordability:
        pti = affordability.get("price_to_income")
        monthly = affordability.get("current")
        affordable = affordability.get("affordable")
        if affordable is False:
            add("warning", "🏠", "Vivienda poco asequible",
                f"Cuota estimada: €{monthly:,}/mes — supera el 33% del ingreso bruto de referencia. "
                f"Ratio precio/ingreso: {pti:.1f}× ingresos anuales.",
                "affordability")
        if pti and pti >= 10:
            add("critical", "🚨", "Ratio precio/ingreso extremo",
                f"El precio mediano equivale a {pti:.1f} años de ingreso bruto de referencia.",
                "affordability")

    # ── Macro alerts ──────────────────────────────────────────────────────────
    if macro:
        euribor = macro.get("euribor", {})
        euribor_val = euribor.get("current")
        euribor_trend = euribor.get("trend")
        if euribor_val and euribor_val >= 4.0:
            add("critical", "📈", "Euríbor muy elevado",
                f"Euríbor 12M en {euribor_val:.2f}% — encarece significativamente las hipotecas.",
                "euribor")
        elif euribor_trend == "down" and euribor_val:
            add("info", "📉", "Euríbor bajando",
                f"Euríbor 12M en {euribor_val:.2f}% con tendencia bajista. Buena señal para hipotecas.",
                "euribor")

    # Sort: critical first, then warning, then info
    order = {"critical": 0, "warning": 1, "info": 2}
    alerts.sort(key=lambda a: order.get(a["level"], 9))

    return alerts


# ============================================================================
# Price Drop Ratio
# ============================================================================

def get_price_drop_ratio(window_days: int = 30) -> Dict:
    """
    Return the percentage of currently-active listings that have had at least
    one price reduction in the last *window_days* days.

    A high ratio (> 30 %) signals widespread seller stress and typically
    precedes a broader price correction.  A low ratio (< 10 %) indicates
    sellers have pricing power.

    Returns:
        {
            "name": str,
            "current": float,          # % of active listings with a drop
            "unit": "%",
            "trend": "up"|"down"|"stable",
            "change": float,           # pp change vs previous window
            "listings_with_drop": int,
            "total_active": int,
            "avg_drop_pct": float,     # average depth of the drops (%)
            "window_days": int,
        }
    """
    result: Dict = {
        "name": "Ratio Bajadas Precio",
        "unit": "%",
        "current": None,
        "trend": "stable",
        "change": 0.0,
        "listings_with_drop": 0,
        "total_active": 0,
        "avg_drop_pct": None,
        "window_days": window_days,
        "error": None,
    }

    try:
        with get_connection() as conn:
            cursor = conn.cursor()

            # Total active listings right now
            cursor.execute(
                "SELECT COUNT(*) FROM listings WHERE status = 'active'"
            )
            total_active = cursor.fetchone()[0]
            result["total_active"] = total_active

            if total_active == 0:
                result["error"] = "No hay propiedades activas"
                return result

            # Active listings that had at least one price drop in the window
            cursor.execute(
                """
                SELECT
                    COUNT(DISTINCT ph.listing_id)                          AS listings_with_drop,
                    AVG(CAST(ph.price_change AS REAL)
                        / CAST(ph.new_price - ph.price_change AS REAL)
                        * 100)                                              AS avg_drop_pct
                FROM price_history ph
                INNER JOIN listings l ON l.listing_id = ph.listing_id
                WHERE l.status = 'active'
                  AND ph.price_change < 0
                  AND ph.date >= date('now', ?)
                """,
                (f"-{window_days} days",),
            )
            row = cursor.fetchone()
            listings_with_drop = row[0] or 0
            avg_drop_pct = row[1]

            current_ratio = round(listings_with_drop / total_active * 100, 1)
            result["listings_with_drop"] = listings_with_drop
            result["current"] = current_ratio
            result["avg_drop_pct"] = (
                round(abs(avg_drop_pct), 1) if avg_drop_pct is not None else None
            )

            # Previous window (same length, shifted back) for trend
            cursor.execute(
                """
                SELECT COUNT(DISTINCT ph.listing_id)
                FROM price_history ph
                INNER JOIN listings l ON l.listing_id = ph.listing_id
                WHERE l.status = 'active'
                  AND ph.price_change < 0
                  AND ph.date >= date('now', ?)
                  AND ph.date <  date('now', ?)
                """,
                (f"-{window_days * 2} days", f"-{window_days} days"),
            )
            prev_with_drop = cursor.fetchone()[0] or 0
            prev_ratio = round(prev_with_drop / total_active * 100, 1)
            change = round(current_ratio - prev_ratio, 1)
            result["change"] = change

            if change > 1:
                result["trend"] = "up"    # more drops = worsening
            elif change < -1:
                result["trend"] = "down"  # fewer drops = improving
            else:
                result["trend"] = "stable"

    except Exception as exc:
        result["error"] = str(exc)

    return result


# ============================================================================
# Market Health Score
# ============================================================================

def calculate_market_score(
    price_trend: Dict,
    sales_speed: Dict,
    supply_demand: Dict,
    inventory: Dict,
    euribor: Dict = None,
    paro: Dict = None,
    affordability: Dict = None,
    price_drop_ratio: Dict = None,
) -> Dict:
    """
    Calculate composite market health score (0-100).

    Score interpretation:
    - 75-100: 🟢 Bullish  (mercado alcista — demanda sólida, vendedores con poder)
    - 40-74:  🟡 Neutral  (mercado en transición — señales mixtas)
    - 0-39:   🔴 Bearish  (mercado bajista — demanda débil, vendedores bajo presión)

    Weights (revised):
    - Price trend       : 25 %   ← bajado del 30 %
    - Sales speed       : 20 %   ← bajado del 25 %
    - Supply/demand     : 15 %   ← bajado del 20 %
    - Affordability     : 15 %   ← NUEVO  (accesibilidad hipotecaria)
    - Euríbor + trend   : 10 %   ← bajado del 15 %
    - Price drop ratio  : 10 %   ← NUEVO  (estrés vendedor)
    - Employment        :  5 %   ← bajado del 10 %
    """
    scores: Dict = {}

    # ------------------------------------------------------------------
    # 1. Price trend (25 %) — rising prices → bullish
    # ------------------------------------------------------------------
    price_pct = price_trend.get("change_pct", 0) or 0
    if price_pct > 5:
        scores["prices"] = 90
    elif price_pct > 2:
        scores["prices"] = 75
    elif price_pct > 0:
        scores["prices"] = 60
    elif price_pct > -2:
        scores["prices"] = 45
    elif price_pct > -5:
        scores["prices"] = 30
    else:
        scores["prices"] = 15

    # ------------------------------------------------------------------
    # 2. Sales speed (20 %) — fewer days to sell → bullish
    # ------------------------------------------------------------------
    current_speed = sales_speed.get("current")
    if current_speed is not None:
        if current_speed <= 2:
            scores["speed"] = 90
        elif current_speed <= 5:
            scores["speed"] = 75
        elif current_speed <= 10:
            scores["speed"] = 55
        elif current_speed <= 20:
            scores["speed"] = 35
        else:
            scores["speed"] = 15
    else:
        scores["speed"] = 50

    # ------------------------------------------------------------------
    # 3. Supply/demand ratio (15 %) — low ratio → scarce supply → bullish
    # ------------------------------------------------------------------
    sd_ratio = supply_demand.get("current")
    if sd_ratio is not None:
        if sd_ratio < 0.5:
            scores["supply_demand"] = 90
        elif sd_ratio < 1.0:
            scores["supply_demand"] = 75
        elif sd_ratio < 2.0:
            scores["supply_demand"] = 55
        elif sd_ratio < 4.0:
            scores["supply_demand"] = 35
        else:
            scores["supply_demand"] = 15
    else:
        scores["supply_demand"] = 50

    # ------------------------------------------------------------------
    # 4. Affordability (15 %) — NEW
    #    Based on monthly mortgage payment as % of reference income.
    #    Lower payment % → easier access → bullish demand.
    # ------------------------------------------------------------------
    if affordability and affordability.get("monthly_payment") and affordability.get("reference_income_monthly"):
        monthly_pmt = affordability["monthly_payment"]
        ref_income   = affordability["reference_income_monthly"]
        pmt_ratio    = monthly_pmt / ref_income * 100  # % of income
        if pmt_ratio <= 25:
            scores["affordability"] = 90   # very accessible
        elif pmt_ratio <= 30:
            scores["affordability"] = 75
        elif pmt_ratio <= 35:
            scores["affordability"] = 55
        elif pmt_ratio <= 45:
            scores["affordability"] = 35
        else:
            scores["affordability"] = 15   # very stretched
    else:
        # Fallback: use boolean affordable flag if available
        if affordability and affordability.get("affordable") is not None:
            scores["affordability"] = 70 if affordability["affordable"] else 30
        else:
            scores["affordability"] = 50

    # ------------------------------------------------------------------
    # 5. Euríbor + trend (10 %) — lower AND falling → bullish
    #    Trend bonus/malus: ±5 pts if clearly rising or falling.
    # ------------------------------------------------------------------
    if euribor and euribor.get("current"):
        euribor_val = euribor["current"]
        if euribor_val < 2.0:
            base = 85
        elif euribor_val < 2.5:
            base = 70
        elif euribor_val < 3.0:
            base = 55
        elif euribor_val < 4.0:
            base = 35
        else:
            base = 15
        # Apply trend adjustment
        euribor_trend = euribor.get("trend", "stable")
        if euribor_trend == "down":
            base = min(base + 5, 95)    # falling rates → bullish bonus
        elif euribor_trend == "up":
            base = max(base - 5, 5)     # rising rates → bearish penalty
        scores["euribor"] = base
    else:
        scores["euribor"] = 50

    # ------------------------------------------------------------------
    # 6. Price drop ratio (10 %) — NEW
    #    % of active listings with at least one price cut in 30 days.
    #    High ratio → widespread seller stress → bearish.
    # ------------------------------------------------------------------
    if price_drop_ratio and price_drop_ratio.get("current") is not None:
        drop_pct = price_drop_ratio["current"]
        if drop_pct < 5:
            scores["price_drops"] = 90   # very few sellers discounting
        elif drop_pct < 10:
            scores["price_drops"] = 75
        elif drop_pct < 20:
            scores["price_drops"] = 55
        elif drop_pct < 30:
            scores["price_drops"] = 35
        else:
            scores["price_drops"] = 15   # >30% sellers cutting → stress
    else:
        scores["price_drops"] = 50

    # ------------------------------------------------------------------
    # 7. Employment (5 %) — lower unemployment → bullish
    # ------------------------------------------------------------------
    if paro and paro.get("current"):
        paro_val = paro["current"]
        if paro_val < 8:
            scores["employment"] = 85
        elif paro_val < 10:
            scores["employment"] = 70
        elif paro_val < 13:
            scores["employment"] = 50
        elif paro_val < 18:
            scores["employment"] = 30
        else:
            scores["employment"] = 15
    else:
        scores["employment"] = 50

    # ------------------------------------------------------------------
    # Weighted total
    # ------------------------------------------------------------------
    weights = {
        "prices":        0.25,
        "speed":         0.20,
        "supply_demand": 0.15,
        "affordability": 0.15,
        "euribor":       0.10,
        "price_drops":   0.10,
        "employment":    0.05,
    }

    total_score = sum(scores[k] * weights[k] for k in weights)
    total_score = round(total_score, 1)
    
    # Determine color
    if total_score >= 75:
        color = "green"
        label = "ALCISTA"
        emoji = "🟢"
        description = "El mercado muestra señales alcistas con demanda sólida."
    elif total_score >= 40:
        color = "yellow"
        label = "EN TRANSICIÓN"
        emoji = "🟡"
        description = "El mercado muestra señales mixtas, en fase de transición."
    else:
        color = "red"
        label = "BAJISTA"
        emoji = "🔴"
        description = "El mercado muestra señales de debilitamiento."
    
    return {
        "score": total_score,
        "color": color,
        "label": label,
        "emoji": emoji,
        "description": description,
        "components": scores,
        "weights": weights
    }


# ============================================================================
# Automatic Diagnosis
# ============================================================================

def generate_diagnosis(
    price_trend: Dict,
    sales_speed: Dict,
    supply_demand: Dict,
    inventory: Dict,
    rotation: Dict,
    dispersion: Dict,
    macro: Dict = None
) -> str:
    """
    Generate automatic market diagnosis text based on indicators.
    """
    paragraphs = []
    
    # Price analysis
    price_pct = price_trend.get("change_pct", 0) or 0
    current_price = price_trend.get("current")
    if current_price:
        if price_pct > 2:
            paragraphs.append(
                f"📈 **Los precios están subiendo** un {price_pct:+.1f}% semanal. "
                f"El precio mediano actual es de €{current_price:,.0f}."
            )
        elif price_pct < -2:
            paragraphs.append(
                f"📉 **Los precios están bajando** un {price_pct:+.1f}% semanal. "
                f"El precio mediano actual es de €{current_price:,.0f}."
            )
        else:
            paragraphs.append(
                f"➡️ **Los precios se mantienen estables** ({price_pct:+.1f}% semanal). "
                f"El precio mediano actual es de €{current_price:,.0f}."
            )
    
    # Sales speed
    speed = sales_speed.get("current")
    if speed is not None:
        if speed <= 3:
            paragraphs.append(
                f"⚡ **Las ventas son muy rápidas**: las propiedades se venden en una mediana de "
                f"{speed:.0f} días, indicando alta demanda."
            )
        elif speed <= 7:
            paragraphs.append(
                f"🏃 **Ritmo de ventas moderado**: mediana de {speed:.0f} días en mercado."
            )
        else:
            paragraphs.append(
                f"🐌 **Las ventas se están ralentizando**: mediana de {speed:.0f} días en mercado, "
                f"sugiriendo menor presión de demanda."
            )
    
    # Supply/demand
    sd = supply_demand.get("current")
    if sd is not None:
        if sd > 3:
            paragraphs.append(
                f"⚠️ **Exceso de oferta**: entran {sd:.1f}x más propiedades de las que se venden. "
                f"Esto podría presionar los precios a la baja."
            )
        elif sd > 1.5:
            paragraphs.append(
                f"📊 **Oferta creciente**: ratio O/D de {sd:.1f}x. El inventario tiende a crecer."
            )
        elif sd < 0.8:
            paragraphs.append(
                f"🔥 **Demanda supera oferta**: ratio O/D de {sd:.1f}x. "
                f"Se venden más de las que entran."
            )
        else:
            paragraphs.append(
                f"⚖️ **Mercado equilibrado**: ratio oferta/demanda de {sd:.1f}x."
            )
    
    # Inventory
    inv_change = inventory.get("change_pct")
    inv_current = inventory.get("current")
    if inv_current and inv_change:
        if inv_change > 5:
            paragraphs.append(
                f"📦 **El inventario crece significativamente** ({inv_change:+.1f}%), "
                f"con {inv_current:,} propiedades activas. Señal de mercado que se relaja."
            )
        elif inv_change < -5:
            paragraphs.append(
                f"📦 **El inventario se reduce** ({inv_change:+.1f}%), "
                f"con {inv_current:,} propiedades activas. Señal de mercado competitivo."
            )
    
    # Macro context
    if macro:
        macro_notes = []
        
        euribor = macro.get("euribor", {})
        if euribor.get("current"):
            val = euribor["current"]
            trend = euribor.get("trend", "stable")
            if trend == "down":
                macro_notes.append(f"Euríbor bajando ({val:.2f}%), favoreciendo la financiación")
            elif trend == "up":
                macro_notes.append(f"Euríbor subiendo ({val:.2f}%), encareciendo hipotecas")
            else:
                macro_notes.append(f"Euríbor estable en {val:.2f}%")
        
        ipv = macro.get("ipv", {})
        if ipv.get("current"):
            macro_notes.append(f"precios oficiales (IPV Madrid) subiendo un {ipv['current']}% interanual")
        
        if macro_notes:
            paragraphs.append(
                f"🏛️ **Contexto macro**: {'; '.join(macro_notes)}."
            )
    
    # Dispersion
    disp = dispersion.get("current")
    if disp and disp > 40:
        paragraphs.append(
            f"🎯 **Mercado polarizado**: la diferencia entre precio medio y mediano es del "
            f"{disp:.0f}%, indicando alta disparidad entre segmentos de lujo y el resto."
        )
    
    return "\n\n".join(paragraphs)


# ============================================================================
# District / Barrio Segmentation
# ============================================================================

def get_price_by_zone(zone_type: str = "district", top_n: int = 10) -> Dict:
    """
    Break down median price and €/m² by district or barrio.

    Args:
        zone_type: 'district' or 'barrio'  (maps to DB column)
        top_n:     how many zones to return (sorted by median price desc)

    Returns:
        Dict with 'zones' (list of dicts), 'zone_type', 'total_zones'
    """
    col = "district" if zone_type == "district" else "barrio"

    result = {
        "name": f"Precio por {'Distrito' if zone_type == 'district' else 'Barrio'}",
        "zone_type": zone_type,
        "zones": [],
        "total_zones": 0
    }

    with get_connection() as conn:
        cursor = conn.cursor()

        # Check column exists
        cursor.execute("PRAGMA table_info(listings)")
        cols = {row[1] for row in cursor.fetchall()}
        if col not in cols:
            result["error"] = f"Column '{col}' not found in listings table"
            return result

        cursor.execute(f"""
            SELECT
                {col}                                    AS zone,
                COUNT(*)                                 AS count,
                AVG(price)                               AS avg_price,
                price                                    -- for median below
            FROM listings
            WHERE price > 0
              AND status = 'active'
              AND {col} IS NOT NULL
              AND {col} != ''
            GROUP BY {col}
            ORDER BY AVG(price) DESC
        """)
        # SQLite doesn't have MEDIAN, so we fetch individual rows grouped
        # and compute it in Python
        pass

        # Efficient: get all active listings with zone + price
        cursor.execute(f"""
            SELECT {col}, price, size_sqm
            FROM listings
            WHERE price > 0
              AND status = 'active'
              AND {col} IS NOT NULL
              AND {col} != ''
        """)
        rows = cursor.fetchall()

    # Group in Python for median accuracy
    from collections import defaultdict
    zone_prices: dict = defaultdict(list)
    zone_sqm: dict = defaultdict(list)

    for zone, price, sqm in rows:
        if zone:
            zone_prices[zone].append(price)
            if sqm and sqm > 0:
                zone_sqm[zone].append(price / sqm)

    result["total_zones"] = len(zone_prices)

    # Build sorted list
    zone_data = []
    for zone, prices in zone_prices.items():
        if len(prices) < 3:  # Skip zones with very few listings
            continue
        med_price = statistics.median(prices)
        sqm_list = zone_sqm.get(zone, [])
        med_sqm = statistics.median(sqm_list) if sqm_list else None
        zone_data.append({
            "zone": zone,
            "count": len(prices),
            "median_price": round(med_price),
            "median_price_sqm": round(med_sqm) if med_sqm else None,
            "min_price": round(min(prices)),
            "max_price": round(max(prices))
        })

    zone_data.sort(key=lambda z: z["median_price"], reverse=True)
    result["zones"] = zone_data[:top_n]

    return result


def get_sales_speed_by_zone(zone_type: str = "district") -> Dict:
    """
    Median days on market broken down by district or barrio.

    Args:
        zone_type: 'district' or 'barrio'

    Returns:
        Dict with 'zones' list sorted by fastest (fewest days first).
    """
    col = "district" if zone_type == "district" else "barrio"

    result = {
        "name": f"Velocidad Venta por {'Distrito' if zone_type == 'district' else 'Barrio'}",
        "zone_type": zone_type,
        "zones": []
    }

    with get_connection() as conn:
        cursor = conn.cursor()

        cursor.execute("PRAGMA table_info(listings)")
        cols = {row[1] for row in cursor.fetchall()}
        if col not in cols:
            result["error"] = f"Column '{col}' not found in listings table"
            return result

        cursor.execute(f"""
            SELECT
                {col},
                julianday(last_seen_date) - julianday(first_seen_date) AS days
            FROM listings
            WHERE status = 'sold_removed'
              AND first_seen_date IS NOT NULL
              AND last_seen_date IS NOT NULL
              AND {col} IS NOT NULL
              AND {col} != ''
              AND julianday(last_seen_date) - julianday(first_seen_date) >= 0
        """)
        rows = cursor.fetchall()

    from collections import defaultdict
    zone_days: dict = defaultdict(list)
    for zone, days in rows:
        if zone and days is not None:
            zone_days[zone].append(days)

    zone_data = []
    for zone, days_list in zone_days.items():
        if len(days_list) < 3:
            continue
        zone_data.append({
            "zone": zone,
            "count": len(days_list),
            "median_days": round(statistics.median(days_list), 1),
            "avg_days": round(statistics.mean(days_list), 1)
        })

    zone_data.sort(key=lambda z: z["median_days"])  # Fastest first
    result["zones"] = zone_data

    return result


# ============================================================================
# Aggregated fetch
# ============================================================================

def get_all_internal_indicators(euribor_rate: float = None) -> Dict[str, Dict]:
    """
    Fetch all internal market indicators at once.

    Args:
        euribor_rate: Current Euríbor 12m rate (%) for affordability calculation.
                      If None, a conservative 3.5 % fallback is used.
    """
    indicators = {
        "price_trend":      get_weekly_price_evolution(),
        "sales_speed":      get_weekly_sales_speed(),
        "supply_demand":    get_supply_demand_ratio(),
        "inventory":        get_inventory_evolution(),
        "rotation":         get_rotation_rate(),
        "dispersion":       get_price_dispersion(),
        "affordability":    get_affordability_index(euribor_rate=euribor_rate),
        "price_drop_ratio": get_price_drop_ratio(),
    }
    return indicators


# ============================================================================
# Test
# ============================================================================

if __name__ == "__main__":
    print("📊 Testing internal market indicators...\n")
    
    indicators = get_all_internal_indicators()
    
    for key, data in indicators.items():
        current = data.get("current", "N/A")
        unit = data.get("unit", "")
        trend_emoji = {"up": "📈", "down": "📉", "stable": "➡️"}.get(data.get("trend", ""), "❓")
        change = data.get("change_pct") or data.get("change", "")
        change_str = f" ({change:+})" if isinstance(change, (int, float)) else ""
        
        print(f"  {trend_emoji} {data['name']:25} | {current} {unit}{change_str}")
    
    print("\n📊 Testing Market Score...")
    from macro_data import get_euribor_data, get_paro_data
    
    score = calculate_market_score(
        price_trend=indicators["price_trend"],
        sales_speed=indicators["sales_speed"],
        supply_demand=indicators["supply_demand"],
        inventory=indicators["inventory"],
        euribor=get_euribor_data(),
        paro=get_paro_data()
    )
    
    print(f"\n  {score['emoji']} Score: {score['score']}/100 — {score['label']}")
    print(f"  {score['description']}")
    
    print("\n📝 Diagnosis:")
    from macro_data import get_all_macro_data
    diagnosis = generate_diagnosis(
        **indicators,
        macro=get_all_macro_data()
    )
    print(diagnosis)
