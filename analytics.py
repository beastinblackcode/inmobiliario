"""
Advanced analytics module for Madrid Real Estate Tracker.
Provides temporal trends, quality scoring, and price analysis.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional


def calculate_days_on_market(row) -> int:
    """Calculate days a property has been on market."""
    try:
        first_seen = pd.to_datetime(row['first_seen_date'])
        last_seen = pd.to_datetime(row['last_seen_date'])
        return (last_seen - first_seen).days
    except:
        return 0


def get_price_trends(df: pd.DataFrame, period: str = 'W') -> pd.DataFrame:
    """
    Calculate price trends over time.
    
    Args:
        df: DataFrame with listings
        period: Pandas period ('D'=daily, 'W'=weekly, 'M'=monthly)
        
    Returns:
        DataFrame with price trends
    """
    if df.empty or 'first_seen_date' not in df.columns:
        return pd.DataFrame()
    
    # Convert to datetime
    df_copy = df.copy()
    df_copy['date'] = pd.to_datetime(df_copy['first_seen_date'])
    
    # Filter valid prices
    df_copy = df_copy[df_copy['price'] > 0]
    
    if df_copy.empty:
        return pd.DataFrame()
    
    # Group by period
    trends = df_copy.groupby(pd.Grouper(key='date', freq=period)).agg({
        'price': ['mean', 'median', 'count'],
        'listing_id': 'count'
    }).reset_index()
    
    trends.columns = ['date', 'avg_price', 'median_price', 'price_count', 'total_count']
    
    return trends


def get_price_per_sqm_evolution(df: pd.DataFrame, period: str = 'D', distrito: Optional[str] = None) -> pd.DataFrame:
    """
    Calculate price per m² evolution over time.
    
    Args:
        df: DataFrame with listings
        period: Pandas period ('D'=daily, 'W'=weekly, 'M'=monthly)
        distrito: Optional filter for specific distrito
        
    Returns:
        DataFrame with date, avg_price_sqm, median_price_sqm, count
    """
    if df.empty or 'first_seen_date' not in df.columns:
        return pd.DataFrame()
    
    # Filter by distrito if specified
    df_copy = df.copy()
    if distrito and distrito != 'Todos':
        df_copy = df_copy[df_copy['distrito'] == distrito]
    
    # Convert to datetime
    df_copy['date'] = pd.to_datetime(df_copy['first_seen_date'])
    
    # Filter out unrealistic property sizes (likely data errors)
    # Properties smaller than 10 m² are likely errors
    df_copy = df_copy[df_copy['size_sqm'] >= 10]
    
    # Calculate price_per_sqm if not present
    if 'price_per_sqm' not in df_copy.columns:
        df_copy['price_per_sqm'] = df_copy.apply(
            lambda row: row['price'] / row['size_sqm'] 
            if pd.notna(row.get('size_sqm')) and row.get('size_sqm') > 0 
            else None,
            axis=1
        )
    
    # Filter valid data and remove extreme outliers
    # Realistic Madrid prices: 2,000 - 50,000 €/m²
    df_copy = df_copy[
        df_copy['price_per_sqm'].notna() & 
        (df_copy['price_per_sqm'] > 0) &
        (df_copy['price_per_sqm'] >= 2000) &  # Minimum realistic price
        (df_copy['price_per_sqm'] <= 50000)   # Maximum realistic price (even luxury)
    ]
    
    if df_copy.empty:
        return pd.DataFrame()
    
    # Group by period
    evolution = df_copy.groupby(pd.Grouper(key='date', freq=period)).agg({
        'price_per_sqm': ['mean', 'median', 'count']
    }).reset_index()
    
    evolution.columns = ['date', 'avg_price_sqm', 'median_price_sqm', 'count']
    
    # Remove rows with no data
    evolution = evolution[evolution['count'] > 0]
    
    return evolution



def get_velocity_metrics(df: pd.DataFrame) -> Dict:
    """
    Calculate market velocity metrics.
    
    Returns:
        Dictionary with velocity metrics
    """
    if df.empty:
        return {
            'avg_days_on_market': 0,
            'median_days_on_market': 0,
            'total_active': 0,
            'total_sold': 0,
            'new_last_7_days': 0,
            'sold_last_7_days': 0
        }
    
    # Calculate days on market
    df_copy = df.copy()
    df_copy['days_on_market'] = df_copy.apply(calculate_days_on_market, axis=1)
    
    # Date calculations
    today = datetime.now()
    week_ago = today - timedelta(days=7)
    
    # Convert dates
    df_copy['first_seen_dt'] = pd.to_datetime(df_copy['first_seen_date'])
    df_copy['last_seen_dt'] = pd.to_datetime(df_copy['last_seen_date'])
    
    # Calculate metrics
    active = df_copy[df_copy['status'] == 'active']
    # Filter out initial historical data - only count real sales during tracking period
    sold = df_copy[(df_copy['status'] == 'sold_removed') & (df_copy['first_seen_date'] > '2026-01-14')]
    
    new_last_week = len(df_copy[df_copy['first_seen_dt'] >= week_ago])
    sold_last_week = len(sold[sold['last_seen_dt'] >= week_ago])
    
    return {
        'avg_days_on_market': df_copy['days_on_market'].mean(),
        'median_days_on_market': df_copy['days_on_market'].median(),
        'total_active': len(active),
        'total_sold': len(sold),
        'new_last_7_days': new_last_week,
        'sold_last_7_days': sold_last_week
    }


def calculate_barrio_stats(df: pd.DataFrame) -> Dict:
    """Calculate avg price/m² by barrio (min 5 listings for reliability)."""
    if df.empty:
        return {}
    stats = {}
    for barrio in df['barrio'].unique():
        if pd.isna(barrio):
            continue
        barrio_df = df[(df['barrio'] == barrio) & df['price_per_sqm'].notna()]
        if len(barrio_df) >= 5:
            stats[barrio] = {'avg_price_sqm': barrio_df['price_per_sqm'].mean()}
    return stats


def calculate_distrito_stats(df: pd.DataFrame) -> Dict:
    """
    Calculate statistics by distrito.
    
    Returns:
        Dictionary mapping distrito to stats
    """
    if df.empty:
        return {}
    
    stats = {}
    
    for distrito in df['distrito'].unique():
        if pd.isna(distrito):
            continue
            
        distrito_df = df[df['distrito'] == distrito]
        
        # Filter valid data
        valid_prices = distrito_df[distrito_df['price'] > 0]
        valid_price_sqm = distrito_df[distrito_df['price_per_sqm'].notna()]
        
        stats[distrito] = {
            'avg_price': valid_prices['price'].mean() if len(valid_prices) > 0 else 0,
            'avg_price_sqm': valid_price_sqm['price_per_sqm'].mean() if len(valid_price_sqm) > 0 else 0,
            'median_price': valid_prices['price'].median() if len(valid_prices) > 0 else 0,
            'count': len(distrito_df)
        }
    
    return stats


def calculate_quality_score(row, distrito_stats: Dict, barrio_stats: Dict) -> float:
    """
    Calculate opportunity score (0-100). Higher = better deal.

    Weights:
    - €/m² vs barrio average   35 pts  (falls back to distrito if barrio unavailable)
    - €/m² vs distrito average 15 pts
    - Price drop history        25 pts  (num_drops + total magnitude)
    - Days on market            15 pts  (negotiation pressure)
    - Seller type (particular)  10 pts
    """
    score = 0.0

    sqm = row.get('price_per_sqm')

    # ── 1. €/m² vs BARRIO (35 pts) ───────────────────────────────────────────
    barrio = row.get('barrio')
    if pd.notna(sqm) and barrio in barrio_stats:
        avg = barrio_stats[barrio]['avg_price_sqm']
        if avg > 0:
            ratio = sqm / avg
            if ratio < 0.70:
                score += 35
            elif ratio < 0.80:
                score += 28
            elif ratio < 0.90:
                score += 18
            elif ratio < 1.00:
                score += 8
            elif ratio > 1.30:
                score -= 20
            elif ratio > 1.20:
                score -= 12
            elif ratio > 1.10:
                score -= 5
    elif pd.notna(sqm) and row.get('distrito') in distrito_stats:
        # Fallback: use distrito stats scaled to barrio weight
        avg = distrito_stats[row['distrito']]['avg_price_sqm']
        if avg > 0:
            ratio = sqm / avg
            if ratio < 0.70:
                score += 35
            elif ratio < 0.80:
                score += 28
            elif ratio < 0.90:
                score += 18
            elif ratio < 1.00:
                score += 8
            elif ratio > 1.30:
                score -= 20
            elif ratio > 1.20:
                score -= 12
            elif ratio > 1.10:
                score -= 5

    # ── 2. €/m² vs DISTRITO (15 pts) ─────────────────────────────────────────
    if pd.notna(sqm) and row.get('distrito') in distrito_stats:
        avg = distrito_stats[row['distrito']]['avg_price_sqm']
        if avg > 0:
            ratio = sqm / avg
            if ratio < 0.70:
                score += 15
            elif ratio < 0.80:
                score += 12
            elif ratio < 0.90:
                score += 8
            elif ratio < 1.00:
                score += 3
            elif ratio > 1.30:
                score -= 10
            elif ratio > 1.20:
                score -= 6
            elif ratio > 1.10:
                score -= 3

    # ── 3. Historial de bajadas (25 pts) ──────────────────────────────────────
    num_drops = row.get('num_drops', 0) or 0
    total_drop_pct = abs(row.get('total_drop_pct', 0) or 0)

    if num_drops >= 3:
        score += 20
    elif num_drops == 2:
        score += 13
    elif num_drops == 1:
        score += 6

    # Bonus por magnitud acumulada
    if total_drop_pct >= 15:
        score += 5
    elif total_drop_pct >= 8:
        score += 3
    elif total_drop_pct >= 4:
        score += 1

    # ── 4. Días en mercado (15 pts) ───────────────────────────────────────────
    dom = row.get('days_on_market', 0) or 0
    if dom > 120:
        score += 15
    elif dom > 90:
        score += 12
    elif dom > 60:
        score += 8
    elif dom > 30:
        score += 4

    # ── 5. Tipo de vendedor (10 pts) ──────────────────────────────────────────
    if row.get('seller_type') == 'Particular':
        score += 10

    return min(100.0, max(0.0, score))


def rank_opportunities(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rank properties by opportunity score (0-100).

    Score weights:
    - €/m² vs barrio avg   35%
    - €/m² vs distrito avg 15%
    - Price drop history   25%
    - Days on market       15%
    - Seller type          10%
    """
    if df.empty:
        return df

    df_copy = df.copy()

    # Filter out unrealistic sizes
    df_copy = df_copy[df_copy['size_sqm'] >= 10]

    df_copy['days_on_market'] = df_copy.apply(calculate_days_on_market, axis=1)

    if 'price_per_sqm' not in df_copy.columns:
        df_copy['price_per_sqm'] = df_copy.apply(
            lambda row: row['price'] / row['size_sqm']
            if pd.notna(row.get('size_sqm')) and row.get('size_sqm') > 0 else None,
            axis=1,
        )

    df_copy = df_copy[
        df_copy['price_per_sqm'].notna() &
        (df_copy['price_per_sqm'] >= 2000) &
        (df_copy['price_per_sqm'] <= 50000)
    ]

    if df_copy.empty:
        return df_copy

    # ── Enrich with price-drop history ───────────────────────────────────────
    try:
        from database import get_connection
        with get_connection() as conn:
            drop_df = pd.read_sql_query(
                """
                SELECT
                    listing_id,
                    COUNT(*) AS num_drops,
                    SUM(ABS(change_percent)) AS total_drop_pct
                FROM price_history
                WHERE change_amount < 0
                GROUP BY listing_id
                """,
                conn,
            )
        df_copy = df_copy.merge(drop_df, on='listing_id', how='left')
        df_copy['num_drops'] = df_copy['num_drops'].fillna(0).astype(int)
        df_copy['total_drop_pct'] = df_copy['total_drop_pct'].fillna(0.0)
    except Exception:
        df_copy['num_drops'] = 0
        df_copy['total_drop_pct'] = 0.0

    # ── Reference stats ───────────────────────────────────────────────────────
    distrito_stats = calculate_distrito_stats(df_copy)
    barrio_stats   = calculate_barrio_stats(df_copy)

    # ── Score ─────────────────────────────────────────────────────────────────
    df_copy['quality_score'] = df_copy.apply(
        lambda row: calculate_quality_score(row, distrito_stats, barrio_stats),
        axis=1,
    )

    # ── vs distrito % (for display) ───────────────────────────────────────────
    df_copy['vs_distrito_avg'] = df_copy.apply(
        lambda row: ((row['price_per_sqm'] / distrito_stats[row['distrito']]['avg_price_sqm'] - 1) * 100)
        if row.get('distrito') in distrito_stats
        and pd.notna(row.get('price_per_sqm'))
        and distrito_stats[row['distrito']]['avg_price_sqm'] > 0
        else 0,
        axis=1,
    )

    return df_copy.sort_values('quality_score', ascending=False)


def identify_bargains(df: pd.DataFrame, threshold: float = -15.0) -> pd.DataFrame:
    """
    Identify properties priced below distrito average.
    
    Args:
        df: DataFrame with listings
        threshold: Percentage below average (negative number)
        
    Returns:
        DataFrame with bargains
    """
    df_ranked = rank_opportunities(df)
    
    # Filter bargains
    bargains = df_ranked[df_ranked['vs_distrito_avg'] < threshold]
    
    return bargains.sort_values('vs_distrito_avg')


def get_new_vs_sold_trends(df: pd.DataFrame, days: int = 30) -> Dict:
    """
    Get new vs sold trends for last N days.
    
    Returns:
        Dictionary with daily counts
    """
    if df.empty:
        return {'dates': [], 'new': [], 'sold': []}
    
    df_copy = df.copy()
    df_copy['first_seen_dt'] = pd.to_datetime(df_copy['first_seen_date'])
    df_copy['last_seen_dt'] = pd.to_datetime(df_copy['last_seen_date'])
    
    # Last N days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    # Count new and sold by day
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    new_counts = []
    sold_counts = []
    
    for date in date_range:
        new_count = len(df_copy[df_copy['first_seen_dt'].dt.date == date.date()])
        # Filter out initial historical data - only count real sales during tracking period
        sold_count = len(df_copy[
            (df_copy['status'] == 'sold_removed') & 
            (df_copy['first_seen_date'] > '2026-01-14') &
            (df_copy['last_seen_dt'].dt.date == date.date())
        ])
        
        new_counts.append(new_count)
        sold_counts.append(sold_count)
    
    return {
        'dates': [d.strftime('%Y-%m-%d') for d in date_range],
        'new': new_counts,
        'sold': sold_counts
    }


# ============================================================================
# PRICE HISTORY ANALYTICS
# ============================================================================

def get_price_drops_dataframe(days: int = 7, min_drop_percent: float = 5.0) -> pd.DataFrame:
    """
    Get recent price drops as a DataFrame for dashboard display.
    
    Args:
        days: Number of days to look back
        min_drop_percent: Minimum drop percentage to include
        
    Returns:
        DataFrame with price drop information
    """
    from database import get_recent_price_drops
    
    drops = get_recent_price_drops(days=days, min_drop_percent=min_drop_percent)
    
    if not drops:
        return pd.DataFrame()
    
    df = pd.DataFrame(drops)
    
    # Calculate price per sqm for old and new prices
    df['old_price_sqm'] = df.apply(
        lambda row: row['old_price'] / row['size_sqm'] if row['size_sqm'] and row['size_sqm'] > 0 else None,
        axis=1
    )
    df['new_price_sqm'] = df.apply(
        lambda row: row['new_price'] / row['size_sqm'] if row['size_sqm'] and row['size_sqm'] > 0 else None,
        axis=1
    )
    
    # Sort by drop percentage (biggest drops first)
    df = df.sort_values('change_percent', ascending=True)
    
    return df


def get_property_evolution_dataframe(listing_id: str) -> pd.DataFrame:
    """
    Get complete price evolution for a property as DataFrame.
    
    Args:
        listing_id: The listing ID
        
    Returns:
        DataFrame with price history
    """
    from database import get_price_history
    
    history = get_price_history(listing_id)
    
    if not history:
        return pd.DataFrame()
    
    df = pd.DataFrame(history)
    df['date_recorded'] = pd.to_datetime(df['date_recorded'])
    
    return df


def get_desperate_sellers_dataframe(min_drops: int = 2, min_total_drop_pct: float = 10.0) -> pd.DataFrame:
    """
    Get properties with multiple price drops as DataFrame.
    
    Args:
        min_drops: Minimum number of price drops
        min_total_drop_pct: Minimum total drop percentage
        
    Returns:
        DataFrame with desperate sellers
    """
    from database import get_properties_with_multiple_drops
    
    sellers = get_properties_with_multiple_drops(
        min_drops=min_drops,
        min_total_drop_pct=min_total_drop_pct
    )
    
    if not sellers:
        return pd.DataFrame()
    
    df = pd.DataFrame(sellers)
    
    # Calculate price per sqm
    df['current_price_sqm'] = df.apply(
        lambda row: row['current_price'] / row['size_sqm'] if row['size_sqm'] and row['size_sqm'] > 0 else None,
        axis=1
    )
    df['initial_price_sqm'] = df.apply(
        lambda row: row['initial_price'] / row['size_sqm'] if row['size_sqm'] and row['size_sqm'] > 0 else None,
        axis=1
    )
    
    # Sort by urgency score
    df = df.sort_values('urgency_score', ascending=False)
    
    return df


def get_price_history_summary() -> Dict:
    """
    Get summary statistics about price history.
    
    Returns:
        Dictionary with summary stats
    """
    from database import get_connection
    
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # Total records
        cursor.execute("SELECT COUNT(*) FROM price_history")
        total_records = cursor.fetchone()[0]
        
        # Properties with price changes
        cursor.execute("""
            SELECT COUNT(DISTINCT listing_id) 
            FROM price_history 
            WHERE change_amount IS NOT NULL
        """)
        properties_with_changes = cursor.fetchone()[0]
        
        # Total price changes
        cursor.execute("""
            SELECT COUNT(*) 
            FROM price_history 
            WHERE change_amount IS NOT NULL
        """)
        total_changes = cursor.fetchone()[0]
        
        # Price drops vs increases
        cursor.execute("""
            SELECT 
                SUM(CASE WHEN change_amount < 0 THEN 1 ELSE 0 END) as drops,
                SUM(CASE WHEN change_amount > 0 THEN 1 ELSE 0 END) as increases
            FROM price_history 
            WHERE change_amount IS NOT NULL
        """)
        result = cursor.fetchone()
        drops = result[0] or 0
        increases = result[1] or 0
        
        # Average drop/increase
        cursor.execute("""
            SELECT 
                AVG(CASE WHEN change_amount < 0 THEN change_percent ELSE NULL END) as avg_drop,
                AVG(CASE WHEN change_amount > 0 THEN change_percent ELSE NULL END) as avg_increase
            FROM price_history 
            WHERE change_amount IS NOT NULL
        """)
        result = cursor.fetchone()
        avg_drop = result[0] or 0
        avg_increase = result[1] or 0
        
        return {
            'total_records': total_records,
            'properties_with_changes': properties_with_changes,
            'total_changes': total_changes,
            'price_drops': drops,
            'price_increases': increases,
            'avg_drop_percent': round(avg_drop, 2),
            'avg_increase_percent': round(avg_increase, 2)
        }


def explain_score(row: dict, distrito_stats: Dict, barrio_stats: Dict) -> List[Dict]:
    """
    Return a breakdown of score factors for a single property row.
    Each factor: {label, points, max_points, description}
    """
    breakdown = []
    sqm = row.get('price_per_sqm')

    # ── 1. €/m² vs barrio ────────────────────────────────────────────────────
    barrio = row.get('barrio')
    pts = 0
    ref = "sin datos de barrio"
    if pd.notna(sqm) and barrio in barrio_stats:
        avg = barrio_stats[barrio]['avg_price_sqm']
        ratio = sqm / avg if avg > 0 else 1
        ref = f"media barrio: €{avg:,.0f}/m²"
        if ratio < 0.70:   pts = 35
        elif ratio < 0.80: pts = 28
        elif ratio < 0.90: pts = 18
        elif ratio < 1.00: pts = 8
        elif ratio > 1.30: pts = -20
        elif ratio > 1.20: pts = -12
        elif ratio > 1.10: pts = -5
        desc = f"{(ratio-1)*100:+.1f}% vs {ref}"
    elif pd.notna(sqm) and row.get('distrito') in distrito_stats:
        avg = distrito_stats[row['distrito']]['avg_price_sqm']
        ratio = sqm / avg if avg > 0 else 1
        ref = f"media distrito (fallback): €{avg:,.0f}/m²"
        if ratio < 0.70:   pts = 35
        elif ratio < 0.80: pts = 28
        elif ratio < 0.90: pts = 18
        elif ratio < 1.00: pts = 8
        elif ratio > 1.30: pts = -20
        elif ratio > 1.20: pts = -12
        elif ratio > 1.10: pts = -5
        desc = f"{(ratio-1)*100:+.1f}% vs {ref}"
    else:
        desc = "sin referencia de barrio/distrito"
    breakdown.append({"label": "€/m² vs barrio", "points": pts, "max_points": 35, "description": desc})

    # ── 2. €/m² vs distrito ──────────────────────────────────────────────────
    pts = 0
    if pd.notna(sqm) and row.get('distrito') in distrito_stats:
        avg = distrito_stats[row['distrito']]['avg_price_sqm']
        ratio = sqm / avg if avg > 0 else 1
        if ratio < 0.70:   pts = 15
        elif ratio < 0.80: pts = 12
        elif ratio < 0.90: pts = 8
        elif ratio < 1.00: pts = 3
        elif ratio > 1.30: pts = -10
        elif ratio > 1.20: pts = -6
        elif ratio > 1.10: pts = -3
        desc = f"{(ratio-1)*100:+.1f}% vs media distrito €{avg:,.0f}/m²"
    else:
        desc = "sin datos de distrito"
    breakdown.append({"label": "€/m² vs distrito", "points": pts, "max_points": 15, "description": desc})

    # ── 3. Historial de bajadas ───────────────────────────────────────────────
    num_drops = int(row.get('num_drops', 0) or 0)
    total_drop_pct = abs(float(row.get('total_drop_pct', 0) or 0))
    pts_drops = 20 if num_drops >= 3 else (13 if num_drops == 2 else (6 if num_drops == 1 else 0))
    pts_mag   = 5 if total_drop_pct >= 15 else (3 if total_drop_pct >= 8 else (1 if total_drop_pct >= 4 else 0))
    pts = pts_drops + pts_mag
    desc = f"{num_drops} bajada{'s' if num_drops != 1 else ''}, {total_drop_pct:.1f}% acumulado"
    breakdown.append({"label": "Historial bajadas", "points": pts, "max_points": 25, "description": desc})

    # ── 4. Días en mercado ────────────────────────────────────────────────────
    dom = int(row.get('days_on_market', 0) or 0)
    if dom > 120:   pts = 15
    elif dom > 90:  pts = 12
    elif dom > 60:  pts = 8
    elif dom > 30:  pts = 4
    else:           pts = 0
    breakdown.append({"label": "Días en mercado", "points": pts, "max_points": 15,
                      "description": f"{dom} días publicado"})

    # ── 5. Tipo de vendedor ───────────────────────────────────────────────────
    seller = row.get('seller_type', '')
    pts = 10 if seller == 'Particular' else 0
    breakdown.append({"label": "Vendedor", "points": pts, "max_points": 10,
                      "description": seller or "desconocido"})

    return breakdown
