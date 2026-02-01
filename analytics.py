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


def calculate_quality_score(row, distrito_stats: Dict) -> float:
    """
    Calculate quality-price score (0-100).
    
    Factors:
    - Price/m² vs distrito average (40%)
    - Size (20%)
    - Seller type (10%)
    - Days on market (15%)
    - Orientation (15%)
    
    Returns:
        Score from 0-100 (higher is better value)
    """
    score = 50.0  # Base score
    
    # Price/m² comparison (lower is better - 40 points max)
    if pd.notna(row.get('price_per_sqm')) and row.get('distrito') in distrito_stats:
        avg_price_sqm = distrito_stats[row['distrito']]['avg_price_sqm']
        
        if avg_price_sqm > 0:
            price_ratio = row['price_per_sqm'] / avg_price_sqm
            
            if price_ratio < 0.7:  # 30% below average - excellent
                score += 20
            elif price_ratio < 0.8:  # 20% below average
                score += 15
            elif price_ratio < 0.9:  # 10% below average
                score += 10
            elif price_ratio < 1.0:  # Below average
                score += 5
            elif price_ratio > 1.3:  # 30% above average
                score -= 20
            elif price_ratio > 1.2:  # 20% above average
                score -= 15
            elif price_ratio > 1.1:  # 10% above average
                score -= 10
    
    # Size bonus (larger is better - 20 points max)
    if pd.notna(row.get('size_sqm')):
        if row['size_sqm'] > 120:
            score += 10
        elif row['size_sqm'] > 100:
            score += 8
        elif row['size_sqm'] > 80:
            score += 5
        elif row['size_sqm'] < 40:
            score -= 5
    
    # Seller type (Particular often better deals - 10 points max)
    if row.get('seller_type') == 'Particular':
        score += 10
    
    # Days on market (longer = more negotiable - 15 points max)
    if pd.notna(row.get('days_on_market')):
        if row['days_on_market'] > 90:
            score += 15
        elif row['days_on_market'] > 60:
            score += 10
        elif row['days_on_market'] > 30:
            score += 5
    
    # Orientation (Exterior is better - 15 points max)
    if row.get('orientation') == 'Exterior':
        score += 15
    elif row.get('orientation') == 'Interior':
        score += 5
    
    # Rooms (more rooms = more value - 5 points max)
    if pd.notna(row.get('rooms')):
        if row['rooms'] >= 3:
            score += 5
        elif row['rooms'] >= 2:
            score += 3
    
    return min(100.0, max(0.0, score))


def rank_opportunities(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rank properties by quality-price ratio.
    
    Returns:
        DataFrame sorted by quality score (descending)
    """
    if df.empty:
        return df
    
    df_copy = df.copy()
    
    # Filter out unrealistic property sizes (likely data errors)
    df_copy = df_copy[df_copy['size_sqm'] >= 10]
    
    # Calculate derived metrics
    df_copy['days_on_market'] = df_copy.apply(calculate_days_on_market, axis=1)
    
    if 'price_per_sqm' not in df_copy.columns:
        df_copy['price_per_sqm'] = df_copy.apply(
            lambda row: row['price'] / row['size_sqm'] if pd.notna(row.get('size_sqm')) and row.get('size_sqm') > 0 else None,
            axis=1
        )
    
    # Filter out extreme price outliers (2,000 - 50,000 €/m²)
    df_copy = df_copy[
        df_copy['price_per_sqm'].notna() &
        (df_copy['price_per_sqm'] >= 2000) &
        (df_copy['price_per_sqm'] <= 50000)
    ]
    
    if df_copy.empty:
        return df_copy
    
    
    # Calculate distrito stats
    distrito_stats = calculate_distrito_stats(df_copy)
    
    # Calculate quality score
    df_copy['quality_score'] = df_copy.apply(
        lambda row: calculate_quality_score(row, distrito_stats),
        axis=1
    )
    
    # Calculate vs distrito average
    df_copy['vs_distrito_avg'] = df_copy.apply(
        lambda row: ((row['price_per_sqm'] / distrito_stats[row['distrito']]['avg_price_sqm'] - 1) * 100)
        if row['distrito'] in distrito_stats and pd.notna(row.get('price_per_sqm')) and distrito_stats[row['distrito']]['avg_price_sqm'] > 0
        else 0,
        axis=1
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
