"""
Database module for Madrid Real Estate Tracker.
Manages SQLite database operations for property listings.
"""

import sqlite3
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set
from contextlib import contextmanager
from pathlib import Path


DATABASE_PATH = "real_estate.db"


def is_streamlit_cloud():
    """Detect if running on Streamlit Cloud."""
    try:
        import streamlit as st
        # If secrets exist and contain database config, we're on cloud
        return "database" in st.secrets
    except:
        return False


def download_database_from_cloud():
    """
    Download database from Google Drive if running on Streamlit Cloud.
    Returns True if download was successful or not needed.
    """
    # Only download if on Streamlit Cloud and DB doesn't exist
    if not is_streamlit_cloud():
        # Running locally - check if DB exists
        if not Path(DATABASE_PATH).exists():
            import streamlit as st
            st.error(f"❌ Database file not found: {DATABASE_PATH}")
            st.info("💡 Run the scraper first: `python scraper.py`")
            return False
        return True
    
    # On Streamlit Cloud
    if Path(DATABASE_PATH).exists():
        import streamlit as st
        st.info("✅ Database already exists, using cached version")
        return True
    
    try:
        import streamlit as st
        import gdown
        
        # Get Google Drive file ID from secrets
        if "database" not in st.secrets:
            st.error("❌ Database configuration missing in secrets")
            st.info("💡 Add Google Drive file ID in Streamlit Cloud Settings → Secrets")
            st.code("""[database]
google_drive_file_id = "YOUR_FILE_ID_HERE" """, language="toml")
            return False
        
        file_id = st.secrets["database"]["google_drive_file_id"]
        
        if not file_id or file_id == "YOUR_GOOGLE_DRIVE_FILE_ID_HERE":
            st.error("❌ Please configure Google Drive file ID in Streamlit secrets")
            st.info("💡 Get file ID from Google Drive share link")
            return False
        
        # Download from Google Drive
        st.info(f"📥 Downloading database from Google Drive...")
        st.caption(f"File ID: {file_id[:10]}...")
        url = f"https://drive.google.com/uc?id={file_id}"
        
        try:
            output = gdown.download(url, DATABASE_PATH, quiet=False)
            
            if output and Path(DATABASE_PATH).exists():
                file_size = Path(DATABASE_PATH).stat().st_size / (1024 * 1024)  # MB
                st.success(f"✅ Database downloaded successfully ({file_size:.1f} MB)")
                return True
            else:
                st.error("❌ Failed to download database")
                st.info("💡 Check that the Google Drive file is shared publicly")
                st.info(f"💡 Try this URL in your browser: {url}")
                return False
        except Exception as download_error:
            st.error(f"❌ Download error: {str(download_error)}")
            st.info("💡 Possible issues:")
            st.info("  - File ID is incorrect")
            st.info("  - File is not shared publicly")
            st.info("  - File is too large")
            return False
            
    except Exception as e:
        import streamlit as st
        st.error(f"❌ Error downloading database: {e}")
        import traceback
        st.code(traceback.format_exc())
        return False


@contextmanager
def get_connection():
    """
    Context manager for database connections.
    Enables WAL mode for better concurrent access.
    """
    conn = sqlite3.connect(
        DATABASE_PATH,
        timeout=30.0,  # Increased timeout to 30 seconds
        check_same_thread=False
    )
    conn.row_factory = sqlite3.Row  # Enable column access by name
    
    # Enable WAL mode for better concurrent access
    # This allows multiple readers and one writer simultaneously
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")  # 30 seconds in milliseconds
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


def init_database():
    """Initialize database schema."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS listings (
                listing_id TEXT PRIMARY KEY,
                title TEXT,
                url TEXT,
                price INTEGER,
                distrito TEXT,
                barrio TEXT,
                rooms INTEGER,
                size_sqm REAL,
                floor TEXT,
                orientation TEXT,
                seller_type TEXT,
                is_new_development BOOLEAN,
                description TEXT,
                first_seen_date TEXT,
                last_seen_date TEXT,
                status TEXT DEFAULT 'active'
            )
        """)
        
        # Create indexes for common queries
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_status 
            ON listings(status)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_distrito 
            ON listings(distrito)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_last_seen 
            ON listings(last_seen_date)
        """)
        
        print("✓ Database initialized successfully")


def migrate_add_description_column():
    """
    Migration: Add description column to existing databases.
    Safe to run multiple times - will only add column if it doesn't exist.
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # Check if description column exists
        cursor.execute("PRAGMA table_info(listings)")
        columns = [col[1] for col in cursor.fetchall()]
        
        if 'description' not in columns:
            print("📝 Adding description column to database...")
            cursor.execute("ALTER TABLE listings ADD COLUMN description TEXT")
            print("✓ Description column added successfully")
        else:
            print("✓ Description column already exists")


def get_active_listing_ids() -> Set[str]:
    """
    Retrieve all active listing IDs from database.
    Used for ETL comparison to detect removed properties.
    
    Returns:
        Set of listing IDs with status='active'
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT listing_id 
            FROM listings 
            WHERE status = 'active'
        """)
        return {row[0] for row in cursor.fetchall()}


def insert_listing(data: Dict) -> bool:
    """
    Insert a new listing into the database.
    Also creates initial price history record.
    
    Args:
        data: Dictionary with listing fields
        
    Returns:
        True if successful, False otherwise
    """
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            today = datetime.now().strftime("%Y-%m-%d")
            
            cursor.execute("""
                INSERT INTO listings (
                    listing_id, title, url, price, distrito, barrio,
                    rooms, size_sqm, floor, orientation, seller_type,
                    is_new_development, description, first_seen_date, last_seen_date, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                data.get('listing_id'),
                data.get('title'),
                data.get('url'),
                data.get('price'),
                data.get('distrito'),
                data.get('barrio'),
                data.get('rooms'),
                data.get('size_sqm'),
                data.get('floor'),
                data.get('orientation'),
                data.get('seller_type'),
                data.get('is_new_development', False),
                data.get('description'),
                today,
                today,
                'active'
            ))
            
            # Create initial price history record
            if data.get('price'):
                _insert_price_change_internal(
                    cursor,
                    listing_id=data.get('listing_id'),
                    new_price=data.get('price'),
                    date=today
                )
            
            return True
    except sqlite3.IntegrityError:
        # Listing already exists
        return False
    except Exception as e:
        print(f"Error inserting listing {data.get('listing_id')}: {e}")
        return False


def update_listing(listing_id: str, data: Dict) -> bool:
    """
    Update an existing listing's last_seen_date and price.
    Detects price changes and records them in price_history.
    
    Args:
        listing_id: Unique listing identifier
        data: Dictionary with updated fields
        
    Returns:
        True if successful, False otherwise
    """
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            today = datetime.now().strftime("%Y-%m-%d")
            
            # Get current price before updating
            current_price = get_current_price(listing_id)
            new_price = data.get('price')
            
            # Update listing
            cursor.execute("""
                UPDATE listings 
                SET last_seen_date = ?,
                    price = ?,
                    title = ?,
                    rooms = ?,
                    size_sqm = ?,
                    floor = ?,
                    orientation = ?,
                    seller_type = ?,
                    description = ?
                WHERE listing_id = ?
            """, (
                today,
                new_price,
                data.get('title'),
                data.get('rooms'),
                data.get('size_sqm'),
                data.get('floor'),
                data.get('orientation'),
                data.get('seller_type'),
                data.get('description'),
                listing_id
            ))
            
            # Check if price changed
            if current_price and new_price and current_price != new_price:
                # Record price change
                _insert_price_change_internal(
                    cursor,
                    listing_id=listing_id,
                    new_price=new_price,
                    date=today
                )
                
                # Log the price change
                change_pct = ((new_price - current_price) / current_price) * 100
                change_symbol = "📉" if new_price < current_price else "📈"
                print(f"  {change_symbol} Price change: {current_price:,}€ → {new_price:,}€ ({change_pct:+.1f}%)")
            
            return True
    except Exception as e:
        print(f"Error updating listing {listing_id}: {e}")
        return False


def mark_as_sold(listing_ids: Set[str]) -> int:
    """
    Mark listings as sold/removed (batch operation).
    
    Args:
        listing_ids: Set of listing IDs to mark as sold
        
    Returns:
        Number of listings updated
    """
    if not listing_ids:
        return 0
    
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            placeholders = ','.join('?' * len(listing_ids))
            cursor.execute(f"""
                UPDATE listings 
                SET status = 'sold_removed'
                WHERE listing_id IN ({placeholders})
                AND status = 'active'
            """, tuple(listing_ids))
            return cursor.rowcount
    except Exception as e:
        print(f"Error marking listings as sold: {e}")
        return 0


def get_listings(
    status: Optional[str] = None,
    distrito: Optional[List[str]] = None,
    min_price: Optional[int] = None,
    max_price: Optional[int] = None,
    seller_type: Optional[str] = None
) -> List[Dict]:
    """
    Query listings with optional filters.
    
    Args:
        status: Filter by status ('active' or 'sold_removed')
        distrito: List of districts to filter by
        min_price: Minimum price filter
        max_price: Maximum price filter
        seller_type: Filter by seller type
        
    Returns:
        List of listing dictionaries
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        
        query = "SELECT * FROM listings WHERE 1=1"
        params = []
        
        if status:
            query += " AND status = ?"
            params.append(status)
        
        if distrito:
            placeholders = ','.join('?' * len(distrito))
            query += f" AND distrito IN ({placeholders})"
            params.extend(distrito)
        
        if min_price is not None:
            query += " AND price >= ?"
            params.append(min_price)
        
        if max_price is not None:
            query += " AND price <= ?"
            params.append(max_price)
        
        if seller_type and seller_type != 'All':
            query += " AND seller_type = ?"
            params.append(seller_type)
        
        cursor.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]


def get_sold_last_n_days(days: int = 30) -> int:
    """
    Count properties marked as sold in the last N days.
    
    Args:
        days: Number of days to look back
        
    Returns:
        Count of sold properties
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        cutoff_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        
        cursor.execute("""
            SELECT COUNT(*) 
            FROM listings 
            WHERE status = 'sold_removed'
            AND last_seen_date >= ?
        """, (cutoff_date,))
        
        return cursor.fetchone()[0]


def get_database_stats() -> Dict:
    """
    Get general database statistics.
    
    Returns:
        Dictionary with stats
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM listings WHERE status = 'active'")
        active_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM listings WHERE status = 'sold_removed'")
        sold_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT AVG(price) FROM listings WHERE status = 'active' AND price > 0")
        avg_price = cursor.fetchone()[0] or 0
        
        cursor.execute("""
            SELECT AVG(price / size_sqm) 
            FROM listings 
            WHERE status = 'active' 
            AND price > 0 
            AND size_sqm > 0
        """)
        avg_price_per_sqm = cursor.fetchone()[0] or 0
        
        return {
            'active_count': active_count,
            'sold_count': sold_count,
            'avg_price': round(avg_price, 2),
            'avg_price_per_sqm': round(avg_price_per_sqm, 2)
        }


def get_price_trends_by_zone(zone_type: str = 'distrito', min_properties: int = 10) -> List[Dict]:
    """
    Calculate price trends by zone (distrito or barrio).
    
    Compares average prices on earliest date vs latest date to detect trends.
    
    Args:
        zone_type: 'distrito' or 'barrio'
        min_properties: Minimum number of properties required per zone
        
    Returns:
        List of dictionaries with trend data per zone
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # Get earliest and latest dates
        cursor.execute("SELECT MIN(first_seen_date), MAX(last_seen_date) FROM listings")
        dates = cursor.fetchone()
        earliest_date, latest_date = dates[0], dates[1]
        
        if not earliest_date or not latest_date or earliest_date == latest_date:
            return []
        
        # Calculate average price per zone on earliest date
        query_first = f"""
            SELECT 
                {zone_type},
                AVG(price) as avg_price,
                COUNT(*) as count
            FROM listings
            WHERE first_seen_date = ?
            AND price > 0
            AND {zone_type} IS NOT NULL
            GROUP BY {zone_type}
            HAVING COUNT(*) >= ?
        """
        
        cursor.execute(query_first, (earliest_date, min_properties))
        first_prices = {row[0]: {'price': row[1], 'count': row[2]} for row in cursor.fetchall()}
        
        # Calculate average price per zone on latest date
        query_last = f"""
            SELECT 
                {zone_type},
                AVG(price) as avg_price,
                COUNT(*) as count
            FROM listings
            WHERE last_seen_date = ?
            AND status = 'active'
            AND price > 0
            AND {zone_type} IS NOT NULL
            GROUP BY {zone_type}
            HAVING COUNT(*) >= ?
        """
        
        cursor.execute(query_last, (latest_date, min_properties))
        last_prices = {row[0]: {'price': row[1], 'count': row[2]} for row in cursor.fetchall()}
        
        # Calculate trends for zones present in both dates
        results = []
        for zone in first_prices.keys():
            if zone in last_prices:
                first_price = first_prices[zone]['price']
                last_price = last_prices[zone]['price']
                price_change = last_price - first_price
                price_change_pct = (price_change / first_price * 100) if first_price > 0 else 0
                
                # Only include if there's a meaningful change (>0.1%)
                if abs(price_change_pct) > 0.1:
                    results.append({
                        'zone': zone,
                        'earliest_date': earliest_date,
                        'latest_date': latest_date,
                        'first_avg_price': round(first_price, 2),
                        'last_avg_price': round(last_price, 2),
                        'property_count': last_prices[zone]['count'],
                        'price_change': round(price_change, 2),
                        'price_change_pct': round(price_change_pct, 2)
                    })
        
        # Sort by price change percentage (descending = biggest drops first)
        results.sort(key=lambda x: x['price_change_pct'])
        
        return results


# ============================================================================
# PRICE HISTORY FUNCTIONS
# ============================================================================

def get_current_price(listing_id: str) -> Optional[int]:
    """
    Get the current price of a listing from the listings table.
    
    Args:
        listing_id: The listing ID
        
    Returns:
        Current price in euros, or None if not found
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT price FROM listings WHERE listing_id = ?",
            (listing_id,)
        )
        result = cursor.fetchone()
        return result[0] if result else None


def get_listing_by_url(url_or_id: str) -> Optional[Dict]:
    """
    Find a listing by its Idealista URL or listing ID.
    
    Args:
        url_or_id: Either a full Idealista URL or just the listing ID
                   Examples:
                   - "https://www.idealista.com/inmueble/110506346/"
                   - "110506346"
    
    Returns:
        Dictionary with listing data or None if not found
    """
    import re
    
    # Extract listing ID from URL if it's a URL
    if 'idealista.com' in url_or_id or '/inmueble/' in url_or_id:
        # Pattern: /inmueble/XXXXXXXX/
        pattern = r'/inmueble/(\d+)'
        match = re.search(pattern, url_or_id)
        if match:
            listing_id = match.group(1)
        else:
            return None
    else:
        # Assume it's already a listing ID
        listing_id = url_or_id.strip()
    
    # Query database
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                listing_id, title, url, price, distrito, barrio,
                rooms, size_sqm, floor, orientation, seller_type,
                is_new_development, description, 
                first_seen_date, last_seen_date, status
            FROM listings
            WHERE listing_id = ?
        """, (listing_id,))
        
        result = cursor.fetchone()
        
        if result:
            return {
                'listing_id': result[0],
                'title': result[1],
                'url': result[2],
                'price': result[3],
                'distrito': result[4],
                'barrio': result[5],
                'rooms': result[6],
                'size_sqm': result[7],
                'floor': result[8],
                'orientation': result[9],
                'seller_type': result[10],
                'is_new_development': result[11],
                'description': result[12],
                'first_seen_date': result[13],
                'last_seen_date': result[14],
                'status': result[15]
            }
        return None


def _insert_price_change_internal(cursor, listing_id: str, new_price: int, date: str) -> None:
    """
    Internal function to insert price change using existing cursor.
    Used by insert_listing and update_listing to avoid nested transactions.
    
    Args:
        cursor: Database cursor from existing connection
        listing_id: The listing ID
        new_price: The new price in euros
        date: Date of the price change (YYYY-MM-DD format)
    """
    # Get the most recent price from history
    cursor.execute("""
        SELECT price 
        FROM price_history 
        WHERE listing_id = ?
        ORDER BY date_recorded DESC
        LIMIT 1
    """, (listing_id,))
    
    result = cursor.fetchone()
    
    if result:
        # Calculate change from previous price
        old_price = result[0]
        change_amount = new_price - old_price
        change_percent = ((new_price - old_price) / old_price) * 100 if old_price > 0 else 0
    else:
        # First price record for this listing
        change_amount = None
        change_percent = None
    
    # Insert new price record
    cursor.execute("""
        INSERT INTO price_history (listing_id, price, date_recorded, change_amount, change_percent)
        VALUES (?, ?, ?, ?, ?)
    """, (listing_id, new_price, date, change_amount, change_percent))


def insert_price_change(listing_id: str, new_price: int, date: str) -> bool:
    """
    Insert a new price record into price_history.
    Automatically calculates change_amount and change_percent from previous price.
    
    Args:
        listing_id: The listing ID
        new_price: The new price in euros
        date: Date of the price change (YYYY-MM-DD format)
        
    Returns:
        True if successful, False otherwise
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        _insert_price_change_internal(cursor, listing_id, new_price, date)
        return True


def get_price_history(listing_id: str) -> List[Dict]:
    """
    Get complete price history for a listing.
    
    Args:
        listing_id: The listing ID
        
    Returns:
        List of price records ordered by date (oldest first)
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                id,
                listing_id,
                price,
                date_recorded,
                change_amount,
                change_percent
            FROM price_history
            WHERE listing_id = ?
            ORDER BY date_recorded ASC
        """, (listing_id,))
        
        columns = ['id', 'listing_id', 'price', 'date_recorded', 'change_amount', 'change_percent']
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_recent_price_drops(days: int = 7, min_drop_percent: float = 5.0) -> List[Dict]:
    """
    Find properties with price drops in the last N days.
    
    Args:
        days: Number of days to look back
        min_drop_percent: Minimum drop percentage to include (positive number)
        
    Returns:
        List of properties with recent price drops
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # Calculate cutoff date
        from datetime import datetime, timedelta
        cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        cursor.execute("""
            SELECT 
                ph.listing_id,
                l.title,
                l.distrito,
                l.barrio,
                l.url,
                ph.price as new_price,
                ph.change_amount,
                ph.change_percent,
                ph.date_recorded,
                l.size_sqm,
                l.rooms
            FROM price_history ph
            JOIN listings l ON ph.listing_id = l.listing_id
            WHERE ph.date_recorded >= ?
            AND ph.change_amount IS NOT NULL
            AND ph.change_percent <= ?
            AND l.status = 'active'
            ORDER BY ph.change_percent ASC
        """, (cutoff_date, -min_drop_percent))
        
        columns = [
            'listing_id', 'title', 'distrito', 'barrio', 'url',
            'new_price', 'change_amount', 'change_percent', 'date_recorded',
            'size_sqm', 'rooms'
        ]
        
        results = []
        for row in cursor.fetchall():
            record = dict(zip(columns, row))
            # Calculate old price
            record['old_price'] = record['new_price'] - record['change_amount']
            # Calculate days since change
            change_date = datetime.strptime(record['date_recorded'], '%Y-%m-%d')
            record['days_since_change'] = (datetime.now() - change_date).days
            results.append(record)
        
        return results


def get_property_price_stats(listing_id: str) -> Optional[Dict]:
    """
    Get comprehensive price statistics for a property.
    
    Args:
        listing_id: The listing ID
        
    Returns:
        Dictionary with price statistics, or None if not found
    """
    history = get_price_history(listing_id)
    
    if not history:
        return None
    
    # Calculate statistics
    initial_price = history[0]['price']
    current_price = history[-1]['price']
    total_change = current_price - initial_price
    total_change_pct = ((current_price - initial_price) / initial_price) * 100 if initial_price > 0 else 0
    
    # Count price changes (excluding initial record)
    price_changes = [h for h in history if h['change_amount'] is not None]
    num_changes = len(price_changes)
    
    # Calculate average days between changes
    if num_changes > 0:
        first_date = datetime.strptime(history[0]['date_recorded'], '%Y-%m-%d')
        last_date = datetime.strptime(history[-1]['date_recorded'], '%Y-%m-%d')
        total_days = (last_date - first_date).days
        avg_days_between = total_days / num_changes if num_changes > 0 else 0
    else:
        avg_days_between = 0
    
    # Count drops vs increases
    drops = sum(1 for h in price_changes if h['change_amount'] < 0)
    increases = sum(1 for h in price_changes if h['change_amount'] > 0)
    
    return {
        'listing_id': listing_id,
        'initial_price': initial_price,
        'current_price': current_price,
        'total_change': total_change,
        'total_change_pct': total_change_pct,
        'num_changes': num_changes,
        'num_drops': drops,
        'num_increases': increases,
        'avg_days_between_changes': round(avg_days_between, 1),
        'first_seen': history[0]['date_recorded'],
        'last_updated': history[-1]['date_recorded']
    }


def get_properties_with_multiple_drops(min_drops: int = 2, min_total_drop_pct: float = 10.0) -> List[Dict]:
    """
    Find properties with multiple price drops (desperate sellers).
    
    Args:
        min_drops: Minimum number of price drops
        min_total_drop_pct: Minimum total drop percentage
        
    Returns:
        List of properties with multiple drops
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # Get all active listings
        cursor.execute("""
            SELECT listing_id, title, distrito, barrio, price, url, size_sqm, rooms
            FROM listings
            WHERE status = 'active'
        """)
        
        results = []
        for row in cursor.fetchall():
            listing_id = row[0]
            stats = get_property_price_stats(listing_id)
            
            if stats and stats['num_drops'] >= min_drops and stats['total_change_pct'] <= -min_total_drop_pct:
                results.append({
                    'listing_id': listing_id,
                    'title': row[1],
                    'distrito': row[2],
                    'barrio': row[3],
                    'current_price': row[4],
                    'url': row[5],
                    'size_sqm': row[6],
                    'rooms': row[7],
                    'initial_price': stats['initial_price'],
                    'total_drop': stats['total_change'],
                    'total_drop_pct': stats['total_change_pct'],
                    'num_drops': stats['num_drops'],
                    'num_changes': stats['num_changes'],
                    'urgency_score': min(100, int(abs(stats['total_change_pct']) * stats['num_drops']))
                })
        
        # Sort by urgency score (highest first)
        results.sort(key=lambda x: x['urgency_score'], reverse=True)
        
        return results



if __name__ == "__main__":
    # Initialize database when run directly
    init_database()
    stats = get_database_stats()
    print(f"\nDatabase Statistics:")
    print(f"  Active listings: {stats['active_count']}")
    print(f"  Sold/Removed: {stats['sold_count']}")
    print(f"  Average price: €{stats['avg_price']:,.2f}")
    print(f"  Average price/m²: €{stats['avg_price_per_sqm']:,.2f}")
