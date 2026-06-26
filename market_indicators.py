"""
Internal market indicators module for Market Surveillance.
Calculates market health metrics from scraped property data.
"""

import sqlite3
import statistics
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional, Tuple
from database import get_connection
# Canonical, backend-agnostic date coercion (handles SQLite str vs Postgres
# date/datetime). Aliased to keep the existing _as_datetime call sites.
from db.dialect import as_datetime as _as_datetime
from logging_config import get_logger

logger = get_logger(__name__)


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
# Helpers
# ============================================================================

def _remove_outliers(values: list, iqr_factor: float = 1.5) -> list:
    """
    Remove outliers from a numeric list using the IQR fence method.
    Values outside [Q1 - factor*IQR, Q3 + factor*IQR] are dropped.
    Returns the original list unchanged if fewer than 20 values (too small to trim).
    """
    if len(values) < 20:
        return values
    q1, q3 = statistics.quantiles(values, n=4)[0], statistics.quantiles(values, n=4)[2]
    iqr = q3 - q1
    lo, hi = q1 - iqr_factor * iqr, q3 + iqr_factor * iqr
    filtered = [v for v in values if lo <= v <= hi]
    # Sanity check: don't discard more than 20% of values
    return filtered if len(filtered) >= len(values) * 0.8 else values


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
    
    from db.dialect import iso_week

    with get_connection() as conn:
        cursor = conn.cursor()

        cursor.execute(f"""
            SELECT DISTINCT {iso_week('first_seen_date')} as week_num,
                   MIN(first_seen_date) as week_start
            FROM listings
            WHERE first_seen_date IS NOT NULL
            GROUP BY {iso_week('first_seen_date')}
            ORDER BY week_start DESC
            LIMIT ?
        """, (weeks,))

        week_info = cursor.fetchall()
        week_info.reverse()  # Chronological order

        for week_num, week_start in week_info:
            if not week_num:
                continue

            cursor.execute(f"""
                SELECT price FROM listings
                WHERE price > 0
                AND {iso_week('first_seen_date')} = ?
            """, (week_num,))

            prices = [row[0] for row in cursor.fetchall()]

            if len(prices) >= 10:
                # Remove outliers using IQR fence before computing median
                prices = _remove_outliers(prices)
                median_price = statistics.median(prices)

                # Also get €/m²
                cursor.execute(f"""
                    SELECT price / size_sqm FROM listings
                    WHERE price > 0 AND size_sqm > 0
                    AND {iso_week('first_seen_date')} = ?
                """, (week_num,))
                prices_sqm = [row[0] for row in cursor.fetchall()]
                prices_sqm = _remove_outliers(prices_sqm) if prices_sqm else prices_sqm
                median_sqm = statistics.median(prices_sqm) if prices_sqm else 0
                
                result["series"].append({
                    "week": week_num,
                    "week_start": week_start,
                    "median_price": round(median_price),
                    "median_price_sqm": round(median_sqm),
                    "count": len(prices)
                })

        # ── Guard: drop bulk-import weeks (>3× the median count) ────────
        # A bulk import has radically different composition and poisons
        # week-over-week comparisons.
        if len(result["series"]) >= 3:
            counts = sorted(pt["count"] for pt in result["series"])
            median_count = counts[len(counts) // 2]
            result["series"] = [
                pt for pt in result["series"]
                if pt["count"] <= median_count * 3
            ]

        # ── Guard: keep only the trailing run of CONTIGUOUS weeks ───────
        # The Feb→May scraping outage (Postgres migration) leaves a hole in
        # the series. Averaging a pre-gap week into the baseline produced a
        # bogus trend (current Jun week vs a baseline that mixed a Feb week
        # with May weeks → fake +11.7%). Restrict trend math (and the chart
        # line, which would otherwise jump across the gap) to consecutive
        # weeks — each within ~10 days of the next — ending at the latest.
        if len(result["series"]) >= 2:
            contiguous = [result["series"][-1]]
            for pt in reversed(result["series"][:-1]):
                kept_start = _as_datetime(contiguous[-1]["week_start"])
                this_start = _as_datetime(pt["week_start"])
                if kept_start and this_start and (kept_start - this_start).days <= 10:
                    contiguous.append(pt)
                else:
                    break  # gap detected — stop; drop everything before it
            contiguous.reverse()
            result["series"] = contiguous

        if len(result["series"]) >= 2:
            # Guard: if the most recent week has < 40% of the average count
            # of the previous weeks, treat it as an incomplete scrape and
            # exclude it from the change calculation (but keep in series for
            # the chart so the user can see the drop-off).
            avg_prev_count = statistics.mean(
                pt["count"] for pt in result["series"][:-1]
            ) if len(result["series"]) > 1 else 0

            latest = result["series"][-1]

            # Guard 1: count-based — fewer than 60% of average means the scrape
            # is clearly partial (raised from 40% to catch near-complete but biased weeks)
            count_incomplete = (
                avg_prev_count > 0
                and latest["count"] < avg_prev_count * 0.60
            )

            # Guard 2: time-based — if the most recent week started less than 7
            # days ago it is still open (scraping may still add listings), so
            # treat it as incomplete regardless of the count.
            latest_start = _as_datetime(latest["week_start"])
            open_week = bool(latest_start) and (datetime.utcnow() - latest_start).days < 7

            incomplete_week = count_incomplete or open_week

            if incomplete_week and len(result["series"]) >= 3:
                # Use second-to-last as current; rolling avg of older weeks as baseline
                current    = result["series"][-2]
                prior_weeks = result["series"][:-2]
            else:
                current    = result["series"][-1]
                prior_weeks = result["series"][:-1]

            # Use rolling average of up to 3 prior complete weeks as baseline.
            # This smooths out week-to-week composition noise (different mix of
            # property types scrapped) while still catching genuine trends.
            baseline_weeks = prior_weeks[-3:] if len(prior_weeks) >= 3 else prior_weeks
            baseline_price = round(
                statistics.mean(w["median_price"] for w in baseline_weeks)
            ) if baseline_weeks else None

            result["current"]     = current["median_price"]
            result["current_sqm"] = current["median_price_sqm"]
            result["previous"]    = baseline_price
            result["change"]      = (result["current"] - baseline_price) if baseline_price else None
            raw_pct = round(
                (result["change"] / baseline_price * 100) if baseline_price else 0, 2
            )
            # Clamp: any weekly change beyond ±15% is almost certainly a data
            # artefact (composition shift), not a real market movement.
            result["change_pct"]  = max(-15.0, min(15.0, raw_pct))
            result["incomplete_latest_week"] = incomplete_week

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
    
    from db.dialect import iso_week, julianday_diff

    with get_connection() as conn:
        cursor = conn.cursor()

        cursor.execute(f"""
            SELECT DISTINCT {iso_week('last_seen_date')} as week_num,
                   MIN(last_seen_date) as week_start
            FROM listings
            WHERE status = 'sold_removed'
            AND last_seen_date IS NOT NULL
            GROUP BY {iso_week('last_seen_date')}
            ORDER BY week_start DESC
            LIMIT ?
        """, (weeks,))

        week_info = cursor.fetchall()
        week_info.reverse()

        for week_num, week_start in week_info:
            if not week_num:
                continue

            days_expr = julianday_diff('last_seen_date', 'first_seen_date')
            cursor.execute(f"""
                SELECT {days_expr} as days
                FROM listings
                WHERE status = 'sold_removed'
                AND first_seen_date IS NOT NULL
                AND last_seen_date IS NOT NULL
                AND {iso_week('last_seen_date')} = ?
                AND {days_expr} >= 1
            """, (week_num,))

            # Note: 0-day observations (first_seen = last_seen) are excluded above —
            # they are scraper artifacts (listing appeared and vanished in same scrape).
            # The minimum of 1 day ensures we only count listings observed on
            # at least two different scraping days.
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

        # Drop bulk-import weeks (>3× median sold count)
        if len(result["series"]) >= 3:
            sold_counts = sorted(pt["sold_count"] for pt in result["series"])
            med_sold = sold_counts[len(sold_counts) // 2]
            result["series"] = [
                pt for pt in result["series"]
                if pt["sold_count"] <= med_sold * 3
            ]

        # Keep only the trailing run of CONTIGUOUS weeks so the Feb→May
        # scraping gap doesn't poison the current-vs-previous comparison.
        if len(result["series"]) >= 2:
            contiguous = [result["series"][-1]]
            for pt in reversed(result["series"][:-1]):
                kept_start = _as_datetime(contiguous[-1]["week_start"])
                this_start = _as_datetime(pt["week_start"])
                if kept_start and this_start and (kept_start - this_start).days <= 10:
                    contiguous.append(pt)
                else:
                    break
            contiguous.reverse()
            result["series"] = contiguous

        if len(result["series"]) >= 2:
            avg_prev_count = statistics.mean(
                pt["sold_count"] for pt in result["series"][:-1]
            ) if len(result["series"]) > 1 else 0

            incomplete_week = (
                avg_prev_count > 0
                and result["series"][-1]["sold_count"] < avg_prev_count * 0.40
            )

            if incomplete_week and len(result["series"]) >= 3:
                current_weeks = result["series"][-4:-1]  # up to 3 complete weeks
            else:
                current_weeks = result["series"][-3:]    # last 3 weeks

            # Use rolling median of last N weeks for stability
            result["current"] = round(
                statistics.median(w["median_days"] for w in current_weeks), 1
            )
            result["previous"] = current_weeks[0]["median_days"] if current_weeks else None
            result["change"] = round(
                result["current"] - result["previous"], 1
            ) if result["previous"] is not None else None
            result["incomplete_latest_week"] = incomplete_week

            # Faster sales = positive market signal
            if result["change"] is not None and result["change"] < -1:
                result["trend"] = "up"
            elif result["change"] is not None and result["change"] > 1:
                result["trend"] = "down"
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
    
    from db.dialect import iso_week, date_plus_days

    with get_connection() as conn:
        cursor = conn.cursor()

        # Get all weeks with data
        cursor.execute(f"""
            SELECT DISTINCT {iso_week('first_seen_date')} as week_num,
                   MIN(first_seen_date) as week_start
            FROM listings
            WHERE first_seen_date IS NOT NULL
            GROUP BY {iso_week('first_seen_date')}
            ORDER BY week_start DESC
            LIMIT ?
        """, (weeks,))

        week_info = cursor.fetchall()
        week_info.reverse()

        # Skip first week (baseline)
        for week_num, week_start in week_info[1:]:
            if not week_num:
                continue

            # New listings this week
            cursor.execute(f"""
                SELECT COUNT(*) FROM listings
                WHERE {iso_week('first_seen_date')} = ?
            """, (week_num,))
            new_count = cursor.fetchone()[0]

            # Sold/removed this week.
            # mark_stale_as_sold() uses a 14-day threshold before marking a
            # property as sold_removed, and does NOT update last_seen_date —
            # it stays as the last day the scraper found the listing active.
            # So a property that disappeared in week W has last_seen_date ≈ W-2.
            # We compensate by shifting the detection window +14 days:
            # "absorbed in week W" ≡ last_seen_date falls in week W-2.
            shifted_last_seen = date_plus_days('last_seen_date', "'+14'")
            cursor.execute(f"""
                SELECT COUNT(*) FROM listings
                WHERE status = 'sold_removed'
                AND {iso_week(shifted_last_seen)} = ?
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
# Absorption Rate (sold last 30d / active inventory)
# ============================================================================

# mark_stale_as_sold() lag: a property's last_seen_date stays as the day
# the scraper last saw it active.  We only learn it has gone ~14 days later.
# Any window over sold_removed listings must therefore be shifted back 14
# days to reflect "what was sold during this calendar window".
_STALE_LAG_DAYS = 14


def _weekly_anchors(cursor, weeks: int) -> list:
    """Return up to *weeks* Monday dates (chronological) that we have data
    for, plus the most recent observation if it isn't already a Monday.
    Reused by absorption rate, months of supply, etc."""
    cursor.execute("""
        SELECT DISTINCT last_seen_date FROM listings
        WHERE last_seen_date IS NOT NULL
        ORDER BY last_seen_date
    """)
    all_dates = [r[0] for r in cursor.fetchall()]
    sampled: list = []
    for d in all_dates:
        dt = _as_datetime(d)
        if dt is not None and dt.weekday() == 0:  # Monday
            sampled.append(d)
    if all_dates and (not sampled or sampled[-1] != all_dates[-1]):
        sampled.append(all_dates[-1])
    return sampled[-weeks:]


def get_absorption_rate(window_days: int = 30, weeks: int = 8) -> Dict:
    """
    Absorption Rate = sold in last N days ÷ active inventory × 100

    Standard real-estate market-tightness indicator:
        > 20 %   seller's market   (hot)
        15-20 %  balanced
        < 15 %   buyer's market    (cold)

    Per-week the rate is computed at week-end W as:
        active_at_W   = listings whose first_seen_date <= W and which were
                        either still active or last seen on/after W
        sold_at_W     = sold_removed whose last_seen_date falls inside
                        [W - LAG - window_days, W - LAG]   (LAG = 14 days)
        absorption    = sold_at_W / active_at_W × 100

    Args:
        window_days: size of the absorption window (default 30 — monthly).
        weeks: number of weekly snapshots to return in `series`.
    """
    result = {
        "name": "Tasa de Absorción",
        "unit": "%",
        "series": [],
        "current": None,
        "previous": None,
        "change": None,
        "trend": "stable",
        "window_days": window_days,
        "active": None,
        "sold_window": None,
    }

    with get_connection() as conn:
        cursor = conn.cursor()
        for week_end in _weekly_anchors(cursor, weeks):
            # Active at week_end
            cursor.execute("""
                SELECT COUNT(*) FROM listings
                WHERE first_seen_date <= ?
                  AND (last_seen_date >= ? OR status = 'active')
            """, (week_end, week_end))
            active = cursor.fetchone()[0]

            # Sold during window [week_end - LAG - N, week_end - LAG]
            from db.dialect import date_plus_days
            cursor.execute(f"""
                SELECT COUNT(*) FROM listings
                WHERE status = 'sold_removed'
                  AND last_seen_date >= {date_plus_days('?', '?')}
                  AND last_seen_date <  {date_plus_days('?', '?')}
            """, (
                week_end, f"-{_STALE_LAG_DAYS + window_days}",
                week_end, f"-{_STALE_LAG_DAYS}",
            ))
            sold = cursor.fetchone()[0]

            if active > 0:
                result["series"].append({
                    "week_end": week_end,
                    "active": active,
                    "sold_window": sold,
                    "absorption_pct": round(sold / active * 100, 2),
                })

        if len(result["series"]) >= 2:
            current = result["series"][-1]
            previous = result["series"][-2]
            result["current"] = current["absorption_pct"]
            result["previous"] = previous["absorption_pct"]
            result["change"] = round(result["current"] - result["previous"], 2)
            result["active"] = current["active"]
            result["sold_window"] = current["sold_window"]

            # Higher absorption = hotter market = "up" (positive trend)
            if result["change"] > 1:
                result["trend"] = "up"
            elif result["change"] < -1:
                result["trend"] = "down"
            else:
                result["trend"] = "stable"

    return result


# ============================================================================
# Months of Supply (inventory ÷ monthly sales rate)
# ============================================================================

def get_months_of_supply(lookback_months: int = 3, weeks: int = 8) -> Dict:
    """
    Months of Supply = active inventory ÷ avg monthly sales (last K months)

    The international benchmark for real-estate market temperature:
        < 4 months   very hot
        4-6 months   balanced
        > 6 months   cold market

    Per-week, computed at week-end W as:
        active_at_W   = listings active at W (same definition as absorption)
        sold_window   = sold_removed with last_seen_date in
                        [W - LAG - 30·K, W - LAG]
        monthly_rate  = sold_window / lookback_months
        months_supply = active_at_W / monthly_rate     (∞ if no sales)

    Args:
        lookback_months: window for the sales-rate denominator.
        weeks: number of weekly snapshots in `series`.
    """
    MAX_MONTHS = 36.0  # cap to avoid ∞ in charts

    result = {
        "name": "Meses de Stock",
        "unit": "meses",
        "series": [],
        "current": None,
        "previous": None,
        "change": None,
        "trend": "stable",
        "lookback_months": lookback_months,
        "active": None,
        "sold_window": None,
    }

    window_days = 30 * lookback_months

    with get_connection() as conn:
        cursor = conn.cursor()
        for week_end in _weekly_anchors(cursor, weeks):
            cursor.execute("""
                SELECT COUNT(*) FROM listings
                WHERE first_seen_date <= ?
                  AND (last_seen_date >= ? OR status = 'active')
            """, (week_end, week_end))
            active = cursor.fetchone()[0]

            from db.dialect import date_plus_days
            cursor.execute(f"""
                SELECT COUNT(*) FROM listings
                WHERE status = 'sold_removed'
                  AND last_seen_date >= {date_plus_days('?', '?')}
                  AND last_seen_date <  {date_plus_days('?', '?')}
            """, (
                week_end, f"-{_STALE_LAG_DAYS + window_days}",
                week_end, f"-{_STALE_LAG_DAYS}",
            ))
            sold = cursor.fetchone()[0]

            if active > 0:
                if sold > 0:
                    monthly_rate = sold / lookback_months
                    months = round(min(active / monthly_rate, MAX_MONTHS), 1)
                else:
                    months = MAX_MONTHS
                result["series"].append({
                    "week_end": week_end,
                    "active": active,
                    "sold_window": sold,
                    "months_of_supply": months,
                    "capped": sold == 0,
                })

        if len(result["series"]) >= 2:
            current = result["series"][-1]
            previous = result["series"][-2]
            result["current"] = current["months_of_supply"]
            result["previous"] = previous["months_of_supply"]
            result["change"] = round(result["current"] - result["previous"], 2)
            result["active"] = current["active"]
            result["sold_window"] = current["sold_window"]

            # More months of supply = colder market = "down" (negative trend)
            if result["change"] > 0.5:
                result["trend"] = "down"
            elif result["change"] < -0.5:
                result["trend"] = "up"
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
            dt = _as_datetime(d)
            if dt is not None and dt.weekday() == 0:  # Monday
                sampled.append(d)
        
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

        anchor = _as_datetime(row[0])
        if anchor is None:
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
        "current": None,                   # monthly payment (€/month) — kept as the primary KPI value
        "monthly_payment": None,           # explicit alias of `current`
        "annual_cost": None,
        "median_price": None,
        "loan_amount": None,
        "rate_used": None,
        "reference_income_annual": REFERENCE_INCOME,
        "reference_income_monthly": round(REFERENCE_INCOME / 12),
        "price_to_income": None,           # YEARS of gross income to buy (price ÷ annual income)
        "payment_to_income_pct": None,     # % of monthly gross income consumed by the mortgage
        "affordable": None,                # True if monthly < 33 % gross monthly income
        "trend": "stable"
    }

    with get_connection() as conn:
        cursor = conn.cursor()

        # Use listings from the last 4 complete scrape weeks instead of ALL
        # active listings.  The full active set includes the bulk-import week
        # which has a very different composition and inflates the median.
        from db.dialect import iso_week
        cursor.execute(f"""
            SELECT {iso_week('first_seen_date')} AS wk, COUNT(*) AS cnt
            FROM listings WHERE first_seen_date IS NOT NULL AND price > 0
            GROUP BY wk ORDER BY wk DESC LIMIT 8
        """)
        week_rows = cursor.fetchall()
        # Detect normal weeks (skip bulk imports >3× median count)
        if len(week_rows) >= 3:
            counts = sorted(r[1] for r in week_rows)
            med_cnt = counts[len(counts) // 2]
            normal_weeks = [r[0] for r in week_rows if r[1] <= med_cnt * 3]
        else:
            normal_weeks = [r[0] for r in week_rows]
        # Take last 4 normal weeks
        recent = normal_weeks[:4]

        if recent:
            placeholders = ",".join("?" for _ in recent)
            cursor.execute(f"""
                SELECT price FROM listings
                WHERE price > 0
                AND {iso_week('first_seen_date')} IN ({placeholders})
            """, recent)
        else:
            cursor.execute("""
                SELECT price FROM listings
                WHERE price > 0 AND status = 'active'
            """)
        prices = [row[0] for row in cursor.fetchall()]

    if not prices:
        return result

    prices = _remove_outliers(prices)
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
    result["monthly_payment"] = round(monthly)
    result["annual_cost"] = round(monthly * 12)

    # Price-to-income (years of gross income needed to buy the median property)
    result["price_to_income"] = round(median_price / REFERENCE_INCOME, 1)

    # Affordability: % of gross monthly income consumed by the mortgage
    gross_monthly = REFERENCE_INCOME / 12
    result["payment_to_income_pct"] = round(monthly / gross_monthly * 100, 1)
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
# Rent Burden  (Tasa de esfuerzo de alquiler)
# ============================================================================

def get_rent_burden() -> Dict:
    """
    Rent burden = median monthly rent / household net monthly income × 100.

    Uses the latest snapshot from `rental_prices` (scraped from Idealista)
    and a reference net annual household income for Madrid (INE Encuesta de
    Condiciones de Vida — last available: ~33 000 €/year net for the
    Comunidad de Madrid).

    Returns:
        Dict with overall burden %, per-district breakdown, severity label,
        and alert thresholds.

    Severity thresholds (EU / Eurostat standard):
        ≤ 30 %   → affordable
        30–40 %  → strained
        40–50 %  → overburdened
        > 50 %   → severely overburdened
    """
    # Reference: INE "Renta media por hogar" Comunidad de Madrid ≈ 42 000 € bruto.
    # After IRPF + SS ≈ 33 000 € net.  Monthly net ≈ 2 750 €.
    REFERENCE_NET_ANNUAL = 33_000          # configurable
    monthly_income = REFERENCE_NET_ANNUAL / 12

    result: Dict = {
        "name": "Tasa de Esfuerzo de Alquiler",
        "unit": "%",
        "current": None,
        "monthly_income_ref": round(monthly_income),
        "median_rent": None,
        "severity": None,
        "trend": "stable",
        "by_district": [],
    }

    from database import get_connection

    with get_connection() as conn:
        cur = conn.cursor()

        # Latest date in rental_prices
        cur.execute("SELECT MAX(date_recorded) FROM rental_prices")
        latest_date = cur.fetchone()[0]
        if not latest_date:
            return result

        # Overall Madrid median rent (median of barrio medians)
        cur.execute("""
            SELECT median_rent FROM rental_prices
            WHERE date_recorded = ?
              AND median_rent > 0
            ORDER BY median_rent
        """, (latest_date,))
        rents = [r[0] for r in cur.fetchall()]

    if not rents:
        return result

    import statistics
    madrid_median = statistics.median(rents)
    burden_pct = round(madrid_median / monthly_income * 100, 1)

    result["current"] = burden_pct
    result["median_rent"] = round(madrid_median)

    # Severity label
    if burden_pct <= 30:
        result["severity"] = "affordable"
        result["trend"] = "up"
    elif burden_pct <= 40:
        result["severity"] = "strained"
        result["trend"] = "stable"
    elif burden_pct <= 50:
        result["severity"] = "overburdened"
        result["trend"] = "down"
    else:
        result["severity"] = "severely_overburdened"
        result["trend"] = "down"

    # Per-district breakdown
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT distrito,
                   ROUND(CAST(AVG(median_rent) AS NUMERIC), 0) AS district_rent,
                   COUNT(*) AS barrios
            FROM rental_prices
            WHERE date_recorded = ?
              AND median_rent > 0
            GROUP BY distrito
            ORDER BY district_rent DESC
        """, (latest_date,))

        by_district = []
        for row in cur.fetchall():
            d_rent = row[1]
            d_burden = round(d_rent / monthly_income * 100, 1)
            if d_burden <= 30:
                sev = "affordable"
            elif d_burden <= 40:
                sev = "strained"
            elif d_burden <= 50:
                sev = "overburdened"
            else:
                sev = "severely_overburdened"
            by_district.append({
                "distrito": row[0],
                "median_rent": int(d_rent),
                "burden_pct": d_burden,
                "severity": sev,
                "barrios": row[2],
            })
        result["by_district"] = by_district

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


def get_price_gini() -> Dict:
    """
    Gini coefficient of active-listing prices: a single 0–1 measure of how
    unequally asking prices are spread. More robust than the mean/median gap
    in ``get_price_dispersion`` because it uses the whole distribution rather
    than just two central moments — a handful of luxury outliers move the
    mean/median dispersion sharply but barely nudge Gini.

        0   → perfect equality (every flat priced the same)
        ~0.3-0.5 → typical for a metropolitan housing market
        1   → maximal inequality (all value in one listing)

    A *rising* Gini over time is a gentrification / polarisation signal: the
    high end pulls away from the rest of the stock.

    Uses the same source as ``get_price_dispersion`` (active listings with a
    positive price) so the two indicators are directly comparable.

    Returns:
        {"name", "unit": "índice", "current": float|None, "n": int,
         "mean_price", "median_price", "trend", "error"}
    """
    result: Dict = {
        "name": "Coeficiente de Gini",
        "unit": "índice",
        "current": None,
        "n": 0,
        "mean_price": None,
        "median_price": None,
        "trend": "stable",
        "error": None,
    }

    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            # Sorted ascending so the rank weights line up with the prices.
            cursor.execute("""
                SELECT price FROM listings
                WHERE price > 0 AND status = 'active'
                ORDER BY price ASC
            """)
            prices = [row[0] for row in cursor.fetchall()]

        n = len(prices)
        result["n"] = n
        if n < 2:
            result["error"] = "Datos insuficientes para Gini (≥2 activos)"
            return result

        total = sum(prices)
        if total <= 0:
            result["error"] = "Suma de precios no positiva"
            return result

        # G = Σ (2·i − n − 1)·x_i / (n · Σ x_i), with i = 1..n over sorted x.
        weighted = sum((2 * i - n - 1) * x for i, x in enumerate(prices, start=1))
        gini = round(weighted / (n * total), 3)

        result["current"] = gini
        result["mean_price"] = round(total / n)
        result["median_price"] = round(statistics.median(prices))

        # Level bands (no historical Gini stored yet, so trend is by absolute
        # polarisation rather than period-over-period change).
        if gini > 0.45:
            result["trend"] = "up"     # highly polarised
        elif gini < 0.30:
            result["trend"] = "down"   # homogeneous
        else:
            result["trend"] = "stable"

    except Exception as exc:
        logger.exception("Error computing price Gini coefficient")
        result["error"] = str(exc)

    return result


def get_price_volatility() -> Dict:
    """
    Rolling volatility of the city-wide median €/m², in 7-day and 30-day
    windows. Volatility = coefficient of variation (std ÷ mean × 100) of the
    daily ``median_price_sqm`` snapshots, so it is comparable regardless of
    the absolute price level.

    A *short-term* volatility (7d) rising clearly above the *baseline* (30d)
    is a leading signal: the market is becoming turbulent, which historically
    precedes a change of trend. A 7d below the 30d baseline means it is
    settling.

    Source is ``market_snapshots`` (city scope, ``median_price_sqm``), the
    same daily series the dashboard charts, so no extra heavy query. Windows
    are filtered by *date* (not by count) to stay robust to the odd missing
    snapshot day.

    Returns:
        {"name", "unit": "%", "current" (=7d CV), "vol_7d", "vol_30d",
         "std_7d_sqm", "std_30d_sqm", "n_7d", "n_30d", "change" (7d−30d),
         "trend", "error"}
    """
    result: Dict = {
        "name": "Volatilidad de Precios",
        "unit": "%",
        "current": None,
        "vol_7d": None,
        "vol_30d": None,
        "std_7d_sqm": None,
        "std_30d_sqm": None,
        "n_7d": 0,
        "n_30d": 0,
        "change": None,
        "trend": "stable",
        "error": None,
    }

    def _cv(values: List[float]) -> Tuple[Optional[float], Optional[float]]:
        """(coefficient of variation %, absolute std). Needs ≥2 points and a
        positive mean; otherwise (None, None)."""
        if len(values) < 2:
            return None, None
        mean = statistics.mean(values)
        if mean <= 0:
            return None, None
        std = statistics.stdev(values)
        return round(std / mean * 100, 2), round(std, 1)

    try:
        from database import get_snapshot_series

        series = get_snapshot_series("city", None, "median_price_sqm", days=30)
        # Keep only usable points, ordered by date (the query already orders).
        points = [
            (row["date_computed"], row["metric_value"])
            for row in series
            if row.get("metric_value") and row["metric_value"] > 0
        ]
        if len(points) < 2:
            result["error"] = "Serie de €/m² insuficiente (≥2 snapshots diarios)"
            return result

        cutoff_7d = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        vals_30d = [v for _, v in points]
        vals_7d = [v for d, v in points if d >= cutoff_7d]

        result["n_30d"] = len(vals_30d)
        result["n_7d"] = len(vals_7d)

        result["vol_30d"], result["std_30d_sqm"] = _cv(vals_30d)
        result["vol_7d"], result["std_7d_sqm"] = _cv(vals_7d)
        result["current"] = result["vol_7d"]

        v7, v30 = result["vol_7d"], result["vol_30d"]
        if v7 is not None and v30 is not None:
            result["change"] = round(v7 - v30, 2)
            # Ratio-based trend so it scales with the baseline. A near-flat
            # baseline (v30≈0) would make any blip look explosive, so require
            # a meaningful absolute 7d volatility before flagging turbulence.
            if v30 > 0 and v7 > v30 * 1.2 and v7 > 1.0:
                result["trend"] = "up"     # turbulence building → watch for a turn
            elif v30 > 0 and v7 < v30 * 0.8:
                result["trend"] = "down"   # settling
            else:
                result["trend"] = "stable"
        elif v7 is None:
            # Not enough points in the trailing week yet — fall back to the
            # 30d figure as the headline so the KPI still shows something.
            result["current"] = v30

    except Exception as exc:
        logger.exception("Error computing price volatility")
        result["error"] = str(exc)

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
    macro: Dict = None,
    notarial_gap: Dict = None,
    rent_burden: Dict = None,
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

    def add(level, emoji, title, detail, metric, code=None, params=None):
        alerts.append({
            "level": level,
            "emoji": emoji,
            "title": title,
            "detail": detail,
            "metric": metric,
            "code": code or metric,
            "params": params or {},
        })

    # ── Price alerts ──────────────────────────────────────────────────────────
    if price_trend:
        chg = price_trend.get("change_pct") or 0
        if chg >= 10:
            add("critical", "🚨", "Subida brusca de precios",
                f"Los precios subieron un {chg:+.1f}% respecto a la media de las semanas anteriores.",
                "price_trend", code="price_spike", params={"pct": round(chg, 1)})
        elif chg <= -10:
            add("critical", "🚨", "Caída brusca de precios",
                f"Los precios bajaron un {chg:+.1f}% respecto a la media de las semanas anteriores.",
                "price_trend", code="price_crash", params={"pct": round(abs(chg), 1)})
        elif chg >= 2:
            add("warning", "⚠️", "Precios al alza",
                f"Subida del {chg:+.1f}% respecto a la semana anterior.",
                "price_trend", code="price_up", params={"pct": round(chg, 1)})
        elif chg <= -2:
            add("warning", "⚠️", "Precios a la baja",
                f"Bajada del {chg:+.1f}% respecto a la semana anterior.",
                "price_trend", code="price_down", params={"pct": round(abs(chg), 1)})

        # Trend breakpoint
        bp = price_trend.get("breakpoint", {})
        if bp and bp.get("breakpoint"):
            before = bp.get("direction_before", "")
            after = bp.get("direction_after", "")
            week = bp.get("breakpoint_week", "")
            add("warning", "🔄", "Cambio de tendencia de precios",
                f"La tendencia pasó de '{before}' a '{after}' alrededor de {week}.",
                "price_trend", code="trend_change", params={"before": before, "after": after, "week": week})

    # ── Sales speed alerts ────────────────────────────────────────────────────
    if sales_speed:
        speed = sales_speed.get("current")
        chg = sales_speed.get("change") or 0
        if speed is not None:
            if speed <= 5:
                add("info", "⚡", "Mercado muy activo",
                    f"Mediana de venta: {speed:.0f} días. Alta presión de demanda.",
                    "sales_speed", code="market_hot", params={"days": round(speed)})
            if chg >= 7:
                add("warning", "🐌", "Ventas ralentizándose",
                    f"El tiempo en mercado aumentó {chg:+.0f} días esta semana.",
                    "sales_speed", code="sales_slow", params={"days": round(chg)})
            elif chg <= -7:
                add("warning", "🏃", "Aceleración de ventas",
                    f"El tiempo en mercado bajó {chg:+.0f} días esta semana.",
                    "sales_speed", code="sales_fast", params={"days": round(abs(chg))})

    # ── Supply / demand alerts ────────────────────────────────────────────────
    if supply_demand:
        ratio = supply_demand.get("current")
        if ratio is not None:
            if ratio >= 5:
                add("critical", "🚨", "Exceso severo de oferta",
                    f"Ratio O/D = {ratio:.1f}×. Entran {ratio:.0f}× más propiedades de las que salen.",
                    "supply_demand", code="supply_excess_severe", params={"ratio": round(ratio, 1)})
            elif ratio >= 3:
                add("warning", "⚠️", "Exceso de oferta",
                    f"Ratio O/D = {ratio:.1f}×. El inventario está creciendo rápido.",
                    "supply_demand", code="supply_excess", params={"ratio": round(ratio, 1)})
            elif ratio <= 0.5:
                add("warning", "🔥", "Demanda muy superior a oferta",
                    f"Ratio O/D = {ratio:.1f}×. El stock se absorbe rápidamente.",
                    "supply_demand", code="demand_high", params={"ratio": round(ratio, 1)})

    # ── Inventory alerts ──────────────────────────────────────────────────────
    if inventory:
        chg_pct = inventory.get("change_pct") or 0
        current = inventory.get("current")
        if chg_pct >= 10:
            add("warning", "📦", "Inventario creciendo rápido",
                f"El stock activo creció un {chg_pct:+.1f}% — ahora {current:,} propiedades.",
                "inventory", code="inventory_up", params={"pct": round(chg_pct, 1), "count": current})
        elif chg_pct <= -10:
            add("warning", "📉", "Inventario cayendo rápido",
                f"El stock activo cayó un {chg_pct:+.1f}% — ahora {current:,} propiedades.",
                "inventory", code="inventory_down", params={"pct": round(abs(chg_pct), 1), "count": current})

    # ── Rotation rate alerts ──────────────────────────────────────────────────
    if rotation:
        rate = rotation.get("current")
        chg = rotation.get("change") or 0
        if rate is not None:
            if rate >= 20:
                add("info", "🔥", "Rotación muy alta",
                    f"El {rate:.1f}% del inventario se vendió en las últimas {rotation.get('window_weeks', 4)} semanas.",
                    "rotation", code="rotation_high", params={"rate": round(rate, 1), "weeks": rotation.get("window_weeks", 4)})
            elif rate < 2:
                add("warning", "⚠️", "Rotación muy baja",
                    f"Solo el {rate:.1f}% del inventario se vendió en las últimas {rotation.get('window_weeks', 4)} semanas.",
                    "rotation", code="rotation_low", params={"rate": round(rate, 1), "weeks": rotation.get("window_weeks", 4)})
            if chg <= -5:
                add("warning", "📉", "Tasa de rotación bajando",
                    f"La rotación bajó {chg:+.1f}pp respecto al periodo anterior.",
                    "rotation", code="rotation_down", params={"change": round(abs(chg), 1)})

    # ── Affordability alerts ──────────────────────────────────────────────────
    if affordability:
        pti = affordability.get("price_to_income")
        if pti and pti >= 10:
            add("critical", "🚨", "Ratio precio/ingreso extremo",
                f"El precio mediano equivale a {pti:.1f} años de ingreso bruto de referencia.",
                "affordability", code="pti_extreme", params={"pti": round(pti, 1)})

    # ── Macro alerts ──────────────────────────────────────────────────────────
    if macro:
        euribor = macro.get("euribor", {})
        euribor_val = euribor.get("current")
        euribor_trend = euribor.get("trend")
        euribor_series = euribor.get("series", [])

        if euribor_val and euribor_val >= 4.0:
            add("critical", "📈", "Euríbor muy elevado",
                f"Euríbor 12M en {euribor_val:.2f}% — encarece significativamente las hipotecas.",
                "euribor", code="euribor_high", params={"rate": round(euribor_val, 2)})
        elif euribor_trend == "down" and euribor_val:
            add("info", "📉", "Euríbor bajando",
                f"Euríbor 12M en {euribor_val:.2f}% con tendencia bajista. Buena señal para hipotecas.",
                "euribor", code="euribor_down", params={"rate": round(euribor_val, 2)})

        # ── Euríbor: impacto real en cuota vs hace 2 años ─────────────────
        # Calcula la diferencia de cuota mensual para una hipoteca de referencia
        # comparando el Euríbor actual con el de hace ~24 meses en el histórico.
        if euribor_val is not None and len(euribor_series) >= 3:
            try:
                def _monthly_payment(rate_pct: float,
                                     principal: float = 200_000,
                                     years: int = 25,
                                     spread: float = 1.0) -> float:
                    """Standard annuity formula. rate_pct = annual %, spread in pp."""
                    annual = (rate_pct + spread) / 100
                    monthly_r = annual / 12
                    n = years * 12
                    if monthly_r <= 0:
                        return principal / n
                    return principal * monthly_r * (1 + monthly_r) ** n / ((1 + monthly_r) ** n - 1)

                # Rate from 2 years ago: go back ~24 points in the monthly series
                lookback = min(24, len(euribor_series) - 1)
                past_rate = euribor_series[-lookback - 1]["value"] if lookback > 0 else euribor_series[0]["value"]
                past_date_str = euribor_series[-lookback - 1].get("date_str", "hace 2 años") if lookback > 0 else ""

                cuota_now  = _monthly_payment(euribor_val)
                cuota_past = _monthly_payment(past_rate)
                diff = round(cuota_now - cuota_past)

                if abs(diff) >= 50:   # only surface if meaningful
                    if diff > 0:
                        add("warning", "💸", "Hipotecas más caras que hace 2 años",
                            f"Con Euríbor al {euribor_val:.2f}%, una hipoteca de 200.000\u00a0€ a 25 años "
                            f"cuesta {diff:+,}\u00a0€/mes más que cuando el Euríbor era {past_rate:.2f}% "
                            f"({past_date_str}). El coste adicional acumulado en 25 años supera "
                            f"los {abs(diff) * 12 * 25 / 1000:.0f}.000\u00a0€.",
                            "euribor_impact", code="mortgage_expensive",
                            params={"rate": round(euribor_val, 2), "diff": diff,
                                    "past_rate": round(past_rate, 2), "past_date": past_date_str})
                    else:
                        add("info", "💚", "Hipotecas más baratas que hace 2 años",
                            f"Con Euríbor al {euribor_val:.2f}%, una hipoteca de 200.000\u00a0€ a 25 años "
                            f"cuesta {diff:,}\u00a0€/mes menos que cuando el Euríbor era {past_rate:.2f}% "
                            f"({past_date_str}).",
                            "euribor_impact", code="mortgage_cheap",
                            params={"rate": round(euribor_val, 2), "diff": abs(diff),
                                    "past_rate": round(past_rate, 2), "past_date": past_date_str})
            except Exception:
                logger.debug("Euríbor impact alert enrichment failed", exc_info=True)

    # ── Notarial gap alerts ───────────────────────────────────────────────────
    if notarial_gap and notarial_gap.get("current") is not None:
        gap = notarial_gap["current"]
        yr  = notarial_gap.get("notarial_year", "")
        max_d = notarial_gap.get("max_distrito", "")
        max_g = notarial_gap.get("max_gap", 0)
        if gap >= 40:
            add("critical", "🏛️", "Sobreprecio extremo vs precio real",
                f"La oferta en Idealista supera en {gap:.1f}% el precio escriturado notarial {yr}. "
                f"Mayor tensión en {max_d} (+{max_g:.0f}%). Margen de negociación elevado.",
                "notarial_gap", code="notarial_gap_extreme",
                params={"gap": round(gap, 1), "yr": yr, "max_d": max_d, "max_g": round(max_g)})
        elif gap >= 25:
            add("warning", "🏛️", "Oferta por encima del precio real",
                f"Gap medio Idealista vs notarial {yr}: +{gap:.1f}%. "
                f"Los vendedores piden más de lo que se escritura en la mayoría de distritos.",
                "notarial_gap", code="notarial_gap_high",
                params={"gap": round(gap, 1), "yr": yr})
        elif gap < 5:
            add("info", "🏛️", "Precios de oferta alineados con el notarial",
                f"Gap medio Madrid vs notarial {yr}: {gap:+.1f}%. "
                "Las diferencias entre oferta y precio real son mínimas.",
                "notarial_gap", code="notarial_gap_aligned",
                params={"gap": round(gap, 1), "yr": yr})

    # ── Rent burden alerts ────────────────────────────────────────────────────
    if rent_burden and rent_burden.get("current"):
        rb = rent_burden["current"]
        med = rent_burden.get("median_rent", 0)
        if rb >= 50:
            add("critical", "🏠", "Esfuerzo de alquiler extremo",
                f"La renta mediana ({med} €/mes) supone el {rb}% del ingreso neto de referencia.",
                "rent_burden", code="rent_burden_extreme",
                params={"pct": rb, "rent": med})
        elif rb >= 40:
            add("warning", "🏠", "Esfuerzo de alquiler muy alto",
                f"La renta mediana ({med} €/mes) supone el {rb}% del ingreso neto de referencia.",
                "rent_burden", code="rent_burden_high",
                params={"pct": rb, "rent": med})
        elif rb >= 30:
            add("info", "🏠", "Esfuerzo de alquiler moderado",
                f"La renta mediana ({med} €/mes) supone el {rb}% del ingreso neto de referencia.",
                "rent_burden", code="rent_burden_moderate",
                params={"pct": rb, "rent": med})

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
        from db.dialect import date_offset_days

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
                    AVG(ph.change_percent)                                 AS avg_drop_pct
                FROM price_history ph
                INNER JOIN listings l ON l.listing_id = ph.listing_id
                WHERE l.status = 'active'
                  AND ph.change_amount < 0
                  AND ph.date_recorded >= """ + date_offset_days('?') + """
                """,
                (f"-{window_days}",),
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
                  AND ph.change_amount < 0
                  AND ph.date_recorded >= """ + date_offset_days('?') + """
                  AND ph.date_recorded <  """ + date_offset_days('?') + """
                """,
                (f"-{window_days * 2}", f"-{window_days}"),
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
        logger.exception("Error computing price-drop ratio")
        result["error"] = str(exc)

    return result


def get_price_pressure_index(window_days: int = 30, absorption: Dict = None) -> Dict:
    """
    Net repricing pressure, amplified by market velocity. Leading indicator:
    it tends to move *before* the median price does.

        pct_up   = % of active listings with ≥1 price increase in the window
        pct_down = % of active listings with ≥1 price drop in the window
        net      = pct_up − pct_down                       (pp)
        velocity = clamp(absorption_pct / 15, 0.5, 2.0)    (15% ≈ balanced)
        PPI      = net × velocity

    Positive PPI → upward pressure (more hikes than cuts in a moving market).
    Negative PPI → downward pressure (widespread cuts). The velocity factor
    means the same repricing balance counts for more in a fast market and
    less in a stagnant one.

    Args:
        window_days: size of the repricing window (default 30 — monthly).
        absorption:  optional pre-computed get_absorption_rate() result, to
                     avoid re-querying when called from get_all_internal_indicators.

    Returns:
        {
            "name": str, "current": float, "unit": "índice",
            "trend": "up"|"down"|"stable", "change": float,
            "pct_up": float, "pct_down": float, "net_repricing": float,
            "velocity_factor": float, "total_active": int,
            "window_days": int, "error": None,
        }
    """
    result: Dict = {
        "name": "Presión de Precios",
        "unit": "índice",
        "current": None,
        "trend": "stable",
        "change": 0.0,
        "pct_up": None,
        "pct_down": None,
        "net_repricing": None,
        "velocity_factor": None,
        "total_active": 0,
        "window_days": window_days,
        "error": None,
    }

    try:
        from db.dialect import date_offset_days

        # Velocity factor from absorption (sold/active). 15% ≈ balanced → 1.0.
        if absorption is None:
            absorption = get_absorption_rate()
        absorption_pct = absorption.get("current") if absorption else None
        if absorption_pct is None:
            velocity_factor = 1.0  # neutral when absorption can't be computed
        else:
            velocity_factor = max(0.5, min(2.0, absorption_pct / 15.0))
        result["velocity_factor"] = round(velocity_factor, 2)

        with get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("SELECT COUNT(*) FROM listings WHERE status = 'active'")
            total_active = cursor.fetchone()[0]
            result["total_active"] = total_active

            if total_active == 0:
                result["error"] = "No hay propiedades activas"
                return result

            def _reprice_counts(lo_days: int, hi_days: int = None):
                """DISTINCT active listings with an increase / a drop in
                [lo_days ago, hi_days ago).  hi_days=None means 'until now'."""
                where_hi = (
                    " AND ph.date_recorded < " + date_offset_days('?')
                    if hi_days is not None else ""
                )
                params = [f"-{lo_days}"] + ([f"-{hi_days}"] if hi_days is not None else [])
                cursor.execute(
                    """
                    SELECT
                        COUNT(DISTINCT CASE WHEN ph.change_amount > 0 THEN ph.listing_id END),
                        COUNT(DISTINCT CASE WHEN ph.change_amount < 0 THEN ph.listing_id END)
                    FROM price_history ph
                    INNER JOIN listings l ON l.listing_id = ph.listing_id
                    WHERE l.status = 'active'
                      AND ph.date_recorded >= """ + date_offset_days('?') + where_hi,
                    tuple(params),
                )
                row = cursor.fetchone()
                return (row[0] or 0), (row[1] or 0)

            up, down = _reprice_counts(window_days)
            pct_up = round(up / total_active * 100, 1)
            pct_down = round(down / total_active * 100, 1)
            net = round(pct_up - pct_down, 1)
            result["pct_up"] = pct_up
            result["pct_down"] = pct_down
            result["net_repricing"] = net
            result["current"] = round(net * velocity_factor, 1)

            # Previous window (same length, shifted back) for trend. Uses the
            # current velocity factor — we care about the repricing swing.
            prev_up, prev_down = _reprice_counts(window_days * 2, window_days)
            prev_net = round(prev_up / total_active * 100 - prev_down / total_active * 100, 1)
            prev_ppi = round(prev_net * velocity_factor, 1)
            change = round(result["current"] - prev_ppi, 1)
            result["change"] = change

            if change > 0.5:
                result["trend"] = "up"    # pressure building upward
            elif change < -0.5:
                result["trend"] = "down"  # pressure building downward
            else:
                result["trend"] = "stable"

    except Exception as exc:
        logger.exception("Error computing price-pressure index")
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
    afiliados_ss: Dict = None,
    affordability: Dict = None,
    price_drop_ratio: Dict = None,
    notarial_gap: Dict = None,
) -> Dict:
    """
    Calculate composite market health score (0-100).

    Score interpretation:
    - 75-100: 🟢 Bullish  (mercado alcista — demanda sólida, vendedores con poder)
    - 40-74:  🟡 Neutral  (mercado en transición — señales mixtas)
    - 0-39:   🔴 Bearish  (mercado bajista — demanda débil, vendedores bajo presión)

    Weights:
    - Price trend       : 20 %
    - Sales speed       : 20 %
    - Supply/demand     : 15 %
    - Affordability     : 15 %
    - Euríbor + trend   : 10 %
    - Price drop ratio  : 10 %
    - Notarial gap      :  5 %   ← NUEVO (tensión precio oferta vs precio real)
    - Employment        :  5 %
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
        if current_speed <= 5:
            scores["speed"] = 90
        elif current_speed <= 10:
            scores["speed"] = 75
        elif current_speed <= 20:
            scores["speed"] = 55
        elif current_speed <= 40:
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
    # 7. Notarial gap (5 %) — gap between asking prices and real prices
    #    A large gap signals an overheated market (bearish for buyers).
    #    A small or negative gap signals a realistic / deflating market.
    # ------------------------------------------------------------------
    if notarial_gap and notarial_gap.get("current") is not None:
        gap = notarial_gap["current"]
        if gap < 5:
            scores["notarial_gap"] = 85   # prices almost aligned with reality
        elif gap < 15:
            scores["notarial_gap"] = 70
        elif gap < 25:
            scores["notarial_gap"] = 55
        elif gap < 40:
            scores["notarial_gap"] = 35
        else:
            scores["notarial_gap"] = 15   # extreme overvaluation vs real txns
    else:
        scores["notarial_gap"] = 50

    # ------------------------------------------------------------------
    # 8. Employment (5 %) — lower unemployment → bullish
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
    # 9. Social Security Affiliates (3 %) — growth → bullish
    # ------------------------------------------------------------------
    if afiliados_ss and afiliados_ss.get("change_pct") is not None:
        aff_change = afiliados_ss["change_pct"]
        if aff_change > 2:
            scores["social_security"] = 85   # strong job creation
        elif aff_change > 0.5:
            scores["social_security"] = 70   # moderate growth
        elif aff_change > -0.5:
            scores["social_security"] = 50   # flat
        elif aff_change > -2:
            scores["social_security"] = 30   # moderate decline
        else:
            scores["social_security"] = 15   # sharp decline
    else:
        scores["social_security"] = 50

    # ------------------------------------------------------------------
    # Weighted total
    # ------------------------------------------------------------------
    weights = {
        "prices":          0.20,
        "speed":           0.20,
        "supply_demand":   0.15,
        "affordability":   0.15,
        "euribor":         0.10,
        "price_drops":     0.10,
        "notarial_gap":    0.04,
        "employment":      0.03,
        "social_security": 0.03,
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
    # Maps to the actual (Spanish) column names in the listings table.
    col = "distrito" if zone_type == "district" else "barrio"

    result = {
        "name": f"Precio por {'Distrito' if zone_type == 'district' else 'Barrio'}",
        "zone_type": zone_type,
        "zones": [],
        "total_zones": 0
    }

    with get_connection() as conn:
        cursor = conn.cursor()

        # Pull all active listings with zone + price; median is computed in
        # Python below (avoids relying on a SQL MEDIAN that SQLite lacks).
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

    from db.dialect import is_postgres, julianday_diff

    with get_connection() as conn:
        cursor = conn.cursor()

        # Cheap column-existence check (PRAGMA in SQLite, information_schema in Postgres)
        if is_postgres():
            cursor.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema='public' AND table_name='listings'"
            )
            cols = {row[0] for row in cursor.fetchall()}
        else:
            cursor.execute("PRAGMA table_info(listings)")
            cols = {row[1] for row in cursor.fetchall()}
        if col not in cols:
            result["error"] = f"Column '{col}' not found in listings table"
            return result

        days_expr = julianday_diff('last_seen_date', 'first_seen_date')
        cursor.execute(f"""
            SELECT
                {col},
                {days_expr} AS days
            FROM listings
            WHERE status = 'sold_removed'
              AND first_seen_date IS NOT NULL
              AND last_seen_date IS NOT NULL
              AND {col} IS NOT NULL
              AND {col} != ''
              AND {days_expr} >= 0
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

def get_rental_yield(min_listings: int = 3) -> Dict:
    """
    Compute average gross rental yield across all barrios with enough data.

    Yield formula per barrio:
        yield% = (median_monthly_rent × 12) / median_sale_price × 100

    Returns a standard indicator dict compatible with the dashboard:
        name, current (%), unit, trend, change, description,
        top_barrios (list of top-5 by yield), all_yields (full list)
    """
    from database import get_rental_yields

    all_yields = get_rental_yields(min_listings=min_listings)

    if not all_yields:
        return {
            "name":        "Rentabilidad Bruta Alquiler",
            "current":     None,
            "unit":        "%",
            "trend":       "stable",
            "change":      0,
            "description": "Sin datos de alquiler aún. Ejecuta el scraper para obtener datos.",
            "top_barrios": [],
            "all_yields":  [],
        }

    yields = [r["yield_pct"] for r in all_yields]
    avg_yield = round(sum(yields) / len(yields), 2)

    # Classify trend by average yield vs typical Madrid benchmarks
    # Madrid gross yields typically range 3–5 %; below 3 % is low, above 5 % is high
    if avg_yield >= 5.0:
        trend       = "up"
        description = (
            f"Rentabilidad media del {avg_yield:.1f}% — mercado favorable para inversión."
        )
    elif avg_yield >= 3.5:
        trend       = "stable"
        description = (
            f"Rentabilidad media del {avg_yield:.1f}% — en línea con el mercado madrileño."
        )
    else:
        trend       = "down"
        description = (
            f"Rentabilidad media del {avg_yield:.1f}% — precios de venta elevados respecto al alquiler."
        )

    top_barrios = all_yields[:5]  # already sorted desc by yield_pct

    return {
        "name":        "Rentabilidad Bruta Alquiler",
        "current":     avg_yield,
        "unit":        "%",
        "trend":       trend,
        "change":      0,       # would need historical comparison
        "description": description,
        "top_barrios": top_barrios,
        "all_yields":  all_yields,
        "barrio_count": len(all_yields),
    }


def get_notarial_gap_indicator() -> Dict:
    """
    Build a standard indicator dict for the avg Idealista vs notarial gap.
    Gap > 0 means asking prices exceed real transaction prices (bearish for buyers).
    """
    try:
        from database import get_notarial_gap_by_district
        gap_data = get_notarial_gap_by_district()
        if not gap_data:
            return {"name": "Sobreprecio vs Notarial", "current": None, "unit": "%"}

        import statistics
        gaps = [r["gap_pct"] for r in gap_data]
        avg_gap   = round(statistics.mean(gaps), 1)
        max_row   = max(gap_data, key=lambda r: r["gap_pct"])
        min_row   = min(gap_data, key=lambda r: r["gap_pct"])
        year      = max(r["notarial_year"] for r in gap_data)

        return {
            "name":            "Sobreprecio vs Notarial",
            "current":         avg_gap,
            "unit":            "%",
            "notarial_year":   year,
            "max_distrito":    max_row["distrito"],
            "max_gap":         max_row["gap_pct"],
            "min_distrito":    min_row["distrito"],
            "min_gap":         min_row["gap_pct"],
            "all_gaps":        gap_data,
        }
    except Exception as e:
        logger.exception("Error computing notarial gap indicator")
        return {"name": "Sobreprecio vs Notarial", "current": None, "unit": "%", "error": str(e)}


def get_lanzamientos_indicator() -> Dict:
    """
    Quarterly evictions (lanzamientos practicados) for Madrid from CGPJ data.

    Returns the latest quarter value, YoY change, and a historical series
    suitable for charting (last 12 quarters = 3 years).

    Breakdown by type: alquiler (LAU), hipoteca, otros.
    """
    result: Dict = {
        "name":        "Lanzamientos CGPJ",
        "unit":        "lanzamientos/trimestre",
        "current":     None,
        "quarter_label": None,
        "alquiler":    None,
        "hipoteca":    None,
        "otros":       None,
        "alquiler_pct": None,
        "yoy_change":  None,
        "yoy_change_pct": None,
        "trend":       "stable",
        "series":      [],
    }

    try:
        with get_connection() as conn:
            cur = conn.cursor()

            # Latest available quarter
            cur.execute("""
                SELECT year, quarter, total, alquiler, hipoteca, otros, alquiler_pct
                FROM cgpj_lanzamientos
                WHERE tsj = 'Madrid'
                ORDER BY year DESC, quarter DESC
                LIMIT 1
            """)
            latest = cur.fetchone()
            if not latest:
                return result

            yr, qt, total, alq, hip, otros, alq_pct = latest
            result["current"]      = total
            result["quarter_label"] = f"{yr} T{qt}"
            result["alquiler"]     = alq
            result["hipoteca"]     = hip
            result["otros"]        = otros
            result["alquiler_pct"] = alq_pct

            # YoY: same quarter, previous year
            cur.execute("""
                SELECT total FROM cgpj_lanzamientos
                WHERE tsj = 'Madrid' AND year = ? AND quarter = ?
            """, (yr - 1, qt))
            row_prev = cur.fetchone()
            if row_prev and row_prev[0] and total:
                yoy = total - row_prev[0]
                yoy_pct = round(yoy / row_prev[0] * 100, 1)
                result["yoy_change"]     = yoy
                result["yoy_change_pct"] = yoy_pct
                if yoy_pct > 5:
                    result["trend"] = "up"
                elif yoy_pct < -5:
                    result["trend"] = "down"

            # Historical series: last 12 quarters
            cur.execute("""
                SELECT year, quarter, total, alquiler, hipoteca, otros, alquiler_pct
                FROM cgpj_lanzamientos
                WHERE tsj = 'Madrid' AND total IS NOT NULL
                ORDER BY year DESC, quarter DESC
                LIMIT 12
            """)
            rows = cur.fetchall()
            series = []
            for r in reversed(rows):
                y, q, tot, a, h, o, ap = r
                series.append({
                    "label":       f"{y} T{q}",
                    "year":        y,
                    "quarter":     q,
                    "total":       tot,
                    "alquiler":    a,
                    "hipoteca":    h,
                    "otros":       o,
                    "alquiler_pct": ap,
                })
            result["series"] = series

    except Exception as e:
        logger.exception("Error computing lanzamientos indicator")
        result["error"] = str(e)

    return result


def get_morosidad_indicator() -> Dict:
    """
    Rental delinquency (morosidad de alquiler) indicator for Madrid.

    SOURCE: Observatorio del Alquiler — annual report published each February/March.
    https://observatoriodelalquiler.org/estudios/

    This indicator is STATIC — it must be updated manually each year when
    the Observatorio publishes its new report. Fields to update:
        - series: append new year entry
        - current / yoy_change_pct / data_year

    Data points (Comunidad de Madrid):
        2024: 8,831 € avg debt  (derived: 10,420 / 1.18)
        2025: 10,420 €          +18.0 % YoY   (published Mar 2026)

    National reference:
        2024: 7,958 €   +4.2 % YoY
        2025: 8,490 €   +16.5 % YoY
    """
    series = [
        {"year": 2024, "madrid": 8_831, "national": 7_958, "yoy_pct": 4.2},
        {"year": 2025, "madrid": 10_420, "national": 8_490, "yoy_pct": 18.0},
    ]
    latest = series[-1]
    prev   = series[-2]

    yoy_pct = round((latest["madrid"] - prev["madrid"]) / prev["madrid"] * 100, 1)

    return {
        "name":              "Morosidad Alquiler",
        "unit":              "€",
        "current":           latest["madrid"],
        "previous":          prev["madrid"],
        "yoy_change_pct":    yoy_pct,
        "national_avg":      latest["national"],
        "data_year":         latest["year"],
        "source":            "Observatorio del Alquiler",
        "source_url":        "https://observatoriodelalquiler.org/estudios/",
        "trend":             "up",   # higher debt = worse
        "series":            series,
    }


def get_all_internal_indicators(euribor_rate: float = None) -> Dict[str, Dict]:
    """
    Fetch all internal market indicators at once.

    Args:
        euribor_rate: Current Euríbor 12m rate (%) for affordability calculation.
                      If None, a conservative 3.5 % fallback is used.
    """
    # Computed once and reused so the price-pressure velocity factor doesn't
    # re-query absorption.
    absorption = get_absorption_rate()
    indicators = {
        "price_trend":       get_weekly_price_evolution(),
        "sales_speed":       get_weekly_sales_speed(),
        "supply_demand":     get_supply_demand_ratio(),
        "inventory":         get_inventory_evolution(),
        "rotation":          get_rotation_rate(),
        "absorption_rate":   absorption,
        "months_of_supply":  get_months_of_supply(),
        "dispersion":        get_price_dispersion(),
        "gini":              get_price_gini(),
        "volatility":        get_price_volatility(),
        "affordability":     get_affordability_index(euribor_rate=euribor_rate),
        "price_drop_ratio":  get_price_drop_ratio(),
        "price_pressure":    get_price_pressure_index(absorption=absorption),
        "rental_yield":      get_rental_yield(),
        "notarial_gap":      get_notarial_gap_indicator(),
        "rent_burden":       get_rent_burden(),
        "lanzamientos":      get_lanzamientos_indicator(),
        "morosidad":         get_morosidad_indicator(),
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
