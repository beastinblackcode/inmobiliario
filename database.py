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

from db.connection import get_db, set_database_path, close_db


DATABASE_PATH = "real_estate.db"


def is_streamlit_cloud() -> bool:
    """
    Detect if running on Streamlit Community Cloud.
    Uses the STREAMLIT_SHARING_MODE env var (set automatically by Streamlit Cloud)
    and falls back to checking for the [database] secret.
    """
    import os
    # Streamlit Cloud sets this env var automatically
    if os.environ.get("STREAMLIT_SHARING_MODE"):
        return True
    # Fallback: check for the database secret block
    try:
        import streamlit as st
        return "database" in st.secrets
    except Exception:
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

    Uses a thread-local singleton (see db/connection.py) so that PRAGMAs
    are executed only once per thread and the connection is reused across
    all calls within the same Streamlit session or scraper run.

    The connection is **not** closed on exit — it stays alive for the
    thread's lifetime and is reused on the next call.
    """
    conn = get_db()
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    # NOTE: no conn.close() — the singleton keeps the connection alive


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
        
        # ── Simple indexes (legacy, kept for backward compatibility) ────
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

        # ── Composite indexes for real query patterns ─────────────────
        # Sidebar filter: status + distrito + price
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_active_distrito_price
            ON listings(status, distrito, price)
        """)
        # Barrio lookup with price (barrio detail pages, valuation)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_active_barrio_price
            ON listings(status, barrio, price)
        """)
        # Price history: fast lookup by listing + date
        # (table created in migration_add_price_history.py — skip if missing)
        try:
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_price_history_listing_date
                ON price_history(listing_id, date_recorded)
            """)
        except sqlite3.OperationalError:
            pass  # table doesn't exist yet
        # Sold/removed recent (price-drop analysis, trends)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_status_last_seen
            ON listings(status, last_seen_date DESC)
        """)

        # ── Rental prices snapshot table ────────────────────────────────────
        # One row per (barrio, date_recorded): lightweight daily snapshot of
        # median rental asking price.  Used to compute gross rental yield.
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rental_prices (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                distrito        TEXT    NOT NULL,
                barrio          TEXT    NOT NULL,
                date_recorded   TEXT    NOT NULL,
                median_rent     REAL    NOT NULL,
                listing_count   INTEGER NOT NULL DEFAULT 0,
                UNIQUE(barrio, date_recorded)
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_rental_barrio_date
            ON rental_prices(barrio, date_recorded)
        """)

        # ── Watchlist table ─────────────────────────────────────────────────
        # User-saved properties for price tracking and alerts.
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS watchlist (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                listing_id      TEXT    NOT NULL UNIQUE,
                added_date      TEXT    NOT NULL DEFAULT (date('now')),
                note            TEXT,
                price_at_add    INTEGER,
                alert_on_drop   BOOLEAN NOT NULL DEFAULT 1
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_watchlist_listing
            ON watchlist(listing_id)
        """)

        # ── Notarial prices table ────────────────────────────────────────────
        # Real transaction prices (escrituradas) from the Notarial portal.
        # precio_m2 stored in actual €/m² (CSV values × 1000).
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS notarial_prices (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                distrito    TEXT    NOT NULL,
                periodo     INTEGER NOT NULL,
                precio_m2   REAL    NOT NULL,
                UNIQUE(distrito, periodo)
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_notarial_distrito
            ON notarial_prices(distrito)
        """)

        # ── Market snapshots table ─────────────────────────────────────────
        # Pre-computed daily metrics (filled by compute_snapshots.py after
        # each scraper run).  Dashboard/Tendencias read from here instead
        # of running heavy aggregation queries on every render.
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS market_snapshots (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                date_computed   TEXT NOT NULL,
                scope_type      TEXT NOT NULL,
                scope_value     TEXT,
                metric_name     TEXT NOT NULL,
                metric_value    REAL,
                UNIQUE(date_computed, scope_type, scope_value, metric_name)
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_snapshots_lookup
            ON market_snapshots(scope_type, scope_value, metric_name, date_computed)
        """)

        print("✓ Database initialized successfully")

    # Import notarial CSV if table is empty
    _auto_import_notarial()



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


def migrate_create_scraping_log_table():
    """
    Migration: Create scraping_log table for tracking execution stats.
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS scraping_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                start_time TEXT,
                end_time TEXT,
                duration_minutes REAL,
                properties_processed INTEGER,
                new_listings INTEGER,
                updated_listings INTEGER,
                total_requests INTEGER,
                cost_estimate_usd REAL,
                status TEXT
            )
        """)
        print("✓ Scraping log table initialized")


def log_scraping_execution(
    start_time: datetime,
    end_time: datetime,
    properties_processed: int,
    new_listings: int,
    updated_listings: int,
    total_requests: int,
    cost_estimate_usd: float,
    status: str = 'success'
):
    """
    Log a scraping execution to the database.
    """
    try:
        duration_minutes = (end_time - start_time).total_seconds() / 60.0
        
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO scraping_log (
                    start_time, end_time, duration_minutes,
                    properties_processed, new_listings, updated_listings,
                    total_requests, cost_estimate_usd, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                start_time.strftime("%Y-%m-%d %H:%M:%S"),
                end_time.strftime("%Y-%m-%d %H:%M:%S"),
                round(duration_minutes, 2),
                properties_processed,
                new_listings,
                updated_listings,
                total_requests,
                round(cost_estimate_usd, 4),
                status
            ))
            print(f"  📝 Logged execution: {duration_minutes:.1f} min, ${cost_estimate_usd:.4f}")
    except Exception as e:
        print(f"Error logging execution: {e}")


def get_scraping_log(limit: int = 50) -> List[Dict]:
    """
    Retrieve scraping execution history.
    """
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            # Check if table exists first (migration might not have run yet if scraper hasn't run)
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='scraping_log'")
            if not cursor.fetchone():
                return []
                
            cursor.execute("""
                SELECT 
                    id, start_time, end_time, duration_minutes,
                    properties_processed, new_listings, updated_listings,
                    total_requests, cost_estimate_usd, status
                FROM scraping_log
                ORDER BY start_time DESC
                LIMIT ?
            """, (limit,))
            
            columns = [
                'id', 'start_time', 'end_time', 'duration_minutes',
                'properties_processed', 'new_listings', 'updated_listings',
                'total_requests', 'cost_estimate_usd', 'status'
            ]
            
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
    except Exception as e:
        print(f"Error retrieving scraping log: {e}")
        return []



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
        # Listing already exists (likely was 'sold_removed' and now reappeared)
        # Fallback to update which handles status='active' reactivation
        # print(f"  ♻️ Listing {data.get('listing_id')} exists. Reactivating...")
        return update_listing(data.get('listing_id'), data)
    except Exception as e:
        print(f"Error inserting listing {data.get('listing_id')}: {e}")
        return False


def update_listing(listing_id: str, data: Dict) -> bool:
    """
    Update an existing listing's last_seen_date and price.
    Detects price changes and records them in price_history.
    IMPORTANT: Reactivates properties that were marked as sold_removed.
    
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
            
            # Get current price and status before updating
            cursor.execute("SELECT price, status FROM listings WHERE listing_id = ?", (listing_id,))
            result = cursor.fetchone()
            current_price = result[0] if result else None
            current_status = result[1] if result else None
            new_price = data.get('price')
            
            # Update listing - IMPORTANT: Set status to 'active' to reactivate sold properties
            cursor.execute("""
                UPDATE listings 
                SET last_seen_date = ?,
                    status = 'active',
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
            
            # Log reactivation if property was previously sold
            if current_status == 'sold_removed':
                print(f"  ♻️ Reactivated property (was marked as sold)")
            
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


def mark_stale_as_sold(days_threshold: int = 7) -> int:
    """
    Mark listings as sold/removed using a two-tier approach:

    Tier 1 (days_threshold, default 7 days):
        Mark as sold if not seen in N days AND their barrio has been
        successfully scraped during that window (proves the scraper
        covered that zone and the listing is genuinely gone).

    Tier 2 (hard_cutoff = 21 days):
        Mark as sold if not seen in 21+ days regardless of barrio
        coverage. This prevents "ghost listings" from accumulating
        when a barrio has persistent scraping gaps.

    Circuit breaker: max 1000 marks per batch to prevent runaway
    false positives from a single bad run.

    Args:
        days_threshold: Days without updates before marking as sold
                        when barrio coverage is confirmed (default: 7)

    Returns:
        Number of listings marked as sold
    """
    MAX_BATCH_SIZE = 1000
    HARD_CUTOFF_DAYS = 21  # Absolute max regardless of barrio coverage

    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cutoff_date = (datetime.now() - timedelta(days=days_threshold)).strftime("%Y-%m-%d")
            hard_cutoff_date = (datetime.now() - timedelta(days=HARD_CUTOFF_DAYS)).strftime("%Y-%m-%d")

            # Tier 1: Not seen in N days + barrio was scraped recently
            cursor.execute("""
                SELECT listing_id FROM listings
                WHERE status = 'active'
                AND last_seen_date < ?
                AND barrio IN (
                    SELECT DISTINCT barrio FROM listings
                    WHERE last_seen_date >= ?
                    AND barrio IS NOT NULL
                )
                LIMIT ?
            """, (cutoff_date, cutoff_date, MAX_BATCH_SIZE))
            tier1_ids = [row[0] for row in cursor.fetchall()]

            # Tier 2: Not seen in 21+ days (regardless of barrio coverage)
            remaining = MAX_BATCH_SIZE - len(tier1_ids)
            tier2_ids = []
            if remaining > 0:
                cursor.execute("""
                    SELECT listing_id FROM listings
                    WHERE status = 'active'
                    AND last_seen_date < ?
                    AND listing_id NOT IN (
                        SELECT listing_id FROM listings
                        WHERE status = 'active' AND last_seen_date < ?
                        AND barrio IN (
                            SELECT DISTINCT barrio FROM listings
                            WHERE last_seen_date >= ? AND barrio IS NOT NULL
                        )
                    )
                    LIMIT ?
                """, (hard_cutoff_date, cutoff_date, cutoff_date, remaining))
                tier2_ids = [row[0] for row in cursor.fetchall()]

            ids_to_mark = tier1_ids + tier2_ids

            if not ids_to_mark:
                return 0

            if len(ids_to_mark) >= MAX_BATCH_SIZE:
                print(f"⚠️ Circuit breaker: limiting to {MAX_BATCH_SIZE} marks. "
                      f"There may be more stale listings.")

            placeholders = ','.join('?' * len(ids_to_mark))
            cursor.execute(f"""
                UPDATE listings
                SET status = 'sold_removed'
                WHERE listing_id IN ({placeholders})
            """, tuple(ids_to_mark))

            marked = cursor.rowcount
            print(f"📊 Marked {marked} listings as sold_removed "
                  f"(tier1={len(tier1_ids)} @{days_threshold}d, "
                  f"tier2={len(tier2_ids)} @{HARD_CUTOFF_DAYS}d hard cutoff)")

            return marked
    except Exception as e:
        print(f"Error marking stale listings as sold: {e}")
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


def get_listings_page(
    status: Optional[str] = None,
    distrito: Optional[List[str]] = None,
    min_price: Optional[int] = None,
    max_price: Optional[int] = None,
    seller_type: Optional[str] = None,
    page: int = 1,
    page_size: int = 0,
    order_by: str = "last_seen_date DESC",
) -> tuple:
    """
    Query listings with optional filters, SQL-computed derived columns,
    and optional pagination.

    Compared to ``get_listings()``, this version:
    - computes ``price_per_sqm`` and ``days_on_market`` inside SQLite
      (eliminates expensive ``df.apply()`` in Python)
    - supports LIMIT / OFFSET pagination (set *page_size=0* to disable)

    Returns:
        (rows: List[Dict], total_count: int)
    """
    with get_connection() as conn:
        cursor = conn.cursor()

        conditions: List[str] = []
        params: list = []

        if status:
            conditions.append("status = ?")
            params.append(status)
        if distrito:
            placeholders = ",".join("?" * len(distrito))
            conditions.append(f"distrito IN ({placeholders})")
            params.extend(distrito)
        if min_price is not None:
            conditions.append("price >= ?")
            params.append(min_price)
        if max_price is not None:
            conditions.append("price <= ?")
            params.append(max_price)
        if seller_type and seller_type != "All":
            conditions.append("seller_type = ?")
            params.append(seller_type)

        where = " AND ".join(conditions) if conditions else "1=1"

        # Total count (for UI pager)
        total = cursor.execute(
            f"SELECT COUNT(*) FROM listings WHERE {where}", params
        ).fetchone()[0]

        # Derived columns computed in SQL — no more df.apply()
        derived = """,
            CASE WHEN size_sqm > 0
                 THEN ROUND(price * 1.0 / size_sqm, 2)
                 ELSE NULL
            END AS price_per_sqm,
            CAST(
                julianday(COALESCE(last_seen_date, date('now')))
                - julianday(COALESCE(first_seen_date, last_seen_date, date('now')))
            AS INTEGER) AS days_on_market"""

        sql = f"SELECT *{derived} FROM listings WHERE {where} ORDER BY {order_by}"

        if page_size > 0:
            sql += " LIMIT ? OFFSET ?"
            params.extend([page_size, (page - 1) * page_size])

        cursor.execute(sql, params)
        rows = [dict(row) for row in cursor.fetchall()]

        return rows, total


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


def get_seller_stats() -> Dict:
    """
    Get seller type distribution for active listings.

    Returns:
        Dictionary with total, particular, professional, other counts + pcts,
        plus by_district breakdown.
    """
    with get_connection() as conn:
        cursor = conn.cursor()

        # Global counts
        cursor.execute("""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN seller_type = 'Particular' THEN 1 ELSE 0 END) AS particular,
                SUM(CASE WHEN seller_type = 'Agencia' THEN 1 ELSE 0 END) AS professional,
                SUM(CASE WHEN seller_type IS NULL
                         OR seller_type NOT IN ('Particular', 'Agencia') THEN 1 ELSE 0 END) AS other
            FROM listings
            WHERE status = 'active'
        """)
        row = cursor.fetchone()
        total = row[0] or 1
        particular = row[1] or 0
        professional = row[2] or 0
        other = row[3] or 0

        # By district
        cursor.execute("""
            SELECT
                distrito,
                COUNT(*) AS total,
                SUM(CASE WHEN seller_type = 'Particular' THEN 1 ELSE 0 END) AS particular,
                SUM(CASE WHEN seller_type = 'Agencia' THEN 1 ELSE 0 END) AS professional
            FROM listings
            WHERE status = 'active' AND distrito IS NOT NULL
            GROUP BY distrito
            ORDER BY distrito
        """)
        by_district = []
        for dr in cursor.fetchall():
            dt = dr[1] or 1
            by_district.append({
                "distrito": dr[0],
                "total": dr[1],
                "particular_pct": round((dr[2] or 0) / dt * 100, 1),
                "professional_pct": round((dr[3] or 0) / dt * 100, 1),
            })

        return {
            "total": total,
            "particular": particular,
            "professional": professional,
            "other": other,
            "particular_pct": round(particular / total * 100, 1),
            "professional_pct": round(professional / total * 100, 1),
            "other_pct": round(other / total * 100, 1),
            "by_district": by_district,
        }


def get_scraping_activity(days: int = 30) -> List[Dict]:
    """
    Get daily scraping activity statistics.
    Shows how many new properties were discovered each day.
    
    Args:
        days: Number of days to look back
        
    Returns:
        List of dictionaries with date and count of new properties
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        cutoff_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        
        cursor.execute("""
            SELECT 
                DATE(first_seen_date) as scrape_date,
                COUNT(*) as new_properties
            FROM listings
            WHERE first_seen_date >= ?
            GROUP BY DATE(first_seen_date)
            ORDER BY scrape_date DESC
        """, (cutoff_date,))
        
        return [
            {
                'date': row[0],
                'count': row[1]
            }
            for row in cursor.fetchall()
        ]


def get_price_trends_by_zone(zone_type: str = 'distrito', min_properties: int = 10) -> List[Dict]:
    """
    Calculate price trends by zone (distrito or barrio).
    
    Compares average prices per m² on earliest date vs latest date to detect trends.
    
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
        
        # Calculate average price per m² per zone on earliest date
        query_first = f"""
            SELECT 
                {zone_type},
                AVG(CAST(price AS FLOAT) / NULLIF(size_sqm, 0)) as avg_price_sqm,
                COUNT(*) as count
            FROM listings
            WHERE first_seen_date = ?
            AND price > 0
            AND size_sqm > 0
            AND {zone_type} IS NOT NULL
            GROUP BY {zone_type}
            HAVING COUNT(*) >= ?
        """
        
        cursor.execute(query_first, (earliest_date, min_properties))
        first_prices = {row[0]: {'price': row[1], 'count': row[2]} for row in cursor.fetchall()}
        
        # Calculate average price per m² per zone on latest date
        query_last = f"""
            SELECT 
                {zone_type},
                AVG(CAST(price AS FLOAT) / NULLIF(size_sqm, 0)) as avg_price_sqm,
                COUNT(*) as count
            FROM listings
            WHERE last_seen_date = ?
            AND status = 'active'
            AND price > 0
            AND size_sqm > 0
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


def get_price_history_for_listings(listing_ids: List[str]) -> List[Dict]:
    """
    Get price history for multiple listings at once.
    
    Args:
        listing_ids: List of listing IDs
        
    Returns:
        List of price records with listing_id, date, new_price, price_change
    """
    if not listing_ids:
        return []
    
    with get_connection() as conn:
        cursor = conn.cursor()
        placeholders = ','.join('?' * len(listing_ids))
        cursor.execute(f"""
            SELECT 
                listing_id,
                date_recorded as date,
                price as new_price,
                change_amount as price_change
            FROM price_history
            WHERE listing_id IN ({placeholders})
            ORDER BY date_recorded ASC
        """, tuple(listing_ids))
        
        columns = ['listing_id', 'date', 'new_price', 'price_change']
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
                l.rooms,
                l.floor
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
            'size_sqm', 'rooms', 'floor'
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



def get_daily_price_drops(days: int = 30) -> List[Dict]:
    """
    Get daily statistics for price drops.
    Returns a list of dicts with date, drop_count, active_count, and drop_pct.
    """
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            
            # Generate date range
            dates = []
            for i in range(days):
                date = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
                dates.append(date)
            
            stats = []
            
            for date in dates:
                # 1. Count price drops on this date (distinct listings)
                # CHANGE: Only count distinct listings that dropped price on this date
                cursor.execute("""
                    SELECT COUNT(DISTINCT listing_id) 
                    FROM price_history 
                    WHERE date_recorded = ? 
                    AND change_amount < 0
                """, (date,))
                drop_count = cursor.fetchone()[0]
                
                # 2. Count active listings on this date (approximate)
                # A listing was active if first_seen <= date AND (status='active' OR last_seen >= date)
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM listings 
                    WHERE first_seen_date <= ? 
                    AND (status = 'active' OR last_seen_date >= ?)
                """, (date, date))
                active_count = cursor.fetchone()[0]
                
                drop_pct = (drop_count / active_count * 100) if active_count > 0 else 0
                
                stats.append({
                    "date": date,
                    "drop_count": drop_count,
                    "active_count": active_count,
                    "drop_pct": round(drop_pct, 2)
                })
            
            # Sort by date ascending for charts
            stats.sort(key=lambda x: x['date'])
            
            return stats
            
    except Exception as e:
        print(f"Error getting daily price drops: {e}")
        return []

# ============================================================================
# OPPORTUNITY SCORE HELPERS
# ============================================================================

def get_barrio_price_stats(min_listings: int = 5) -> Dict[str, Dict]:
    """
    Return median price/m² and listing count per barrio for active listings.

    Args:
        min_listings: Minimum active listings required to include a barrio.

    Returns:
        Dict keyed by barrio name:
          { barrio: { median_price_sqm, avg_price_sqm, listing_count } }
    """
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT
                    barrio,
                    COUNT(*)                                         AS listing_count,
                    AVG(CAST(price AS FLOAT) / NULLIF(size_sqm, 0)) AS avg_price_sqm
                FROM listings
                WHERE status  = 'active'
                  AND price   > 0
                  AND size_sqm > 10
                  AND barrio IS NOT NULL
                GROUP BY barrio
                HAVING COUNT(*) >= ?
            """, (min_listings,))

            result = {}
            for row in cursor.fetchall():
                barrio, count, avg_sqm = row
                result[barrio] = {
                    "median_price_sqm": round(avg_sqm, 2) if avg_sqm else None,
                    "listing_count":    count,
                }
            return result
    except Exception as exc:
        print(f"Error getting barrio price stats: {exc}")
        return {}


def get_drop_counts_for_listings(listing_ids: List[str]) -> Dict[str, int]:
    """
    Return the number of price drops per listing_id.

    Args:
        listing_ids: List of listing IDs to query.

    Returns:
        Dict mapping listing_id → number of price drops (0 if none).
    """
    if not listing_ids:
        return {}
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            placeholders = ",".join("?" * len(listing_ids))
            cursor.execute(f"""
                SELECT listing_id, COUNT(*) AS drop_count
                FROM price_history
                WHERE listing_id IN ({placeholders})
                  AND change_amount < 0
                GROUP BY listing_id
            """, tuple(listing_ids))
            counts = {row[0]: row[1] for row in cursor.fetchall()}
            # Ensure every requested listing_id has an entry (default 0)
            return {lid: counts.get(lid, 0) for lid in listing_ids}
    except Exception as exc:
        print(f"Error getting drop counts: {exc}")
        return {lid: 0 for lid in listing_ids}


def get_price_evolution_by_barrio(barrios: List[str], weeks: int = 16) -> List[Dict]:
    """
    Return weekly median price/m² evolution for a list of barrios.

    Args:
        barrios: List of barrio names to include.
        weeks:   Number of weeks to look back.

    Returns:
        List of dicts with: barrio, week_start, median_price_sqm, listing_count.
        Sorted by barrio then week_start ascending.
    """
    if not barrios:
        return []
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cutoff = (datetime.now() - timedelta(weeks=weeks)).strftime("%Y-%m-%d")
            placeholders = ",".join("?" * len(barrios))
            cursor.execute(f"""
                SELECT
                    barrio,
                    strftime('%Y-%W', last_seen_date)               AS week,
                    date(last_seen_date, 'weekday 1', '-7 days')    AS week_start,
                    AVG(CAST(price AS FLOAT) / NULLIF(size_sqm,0))  AS median_price_sqm,
                    COUNT(*)                                         AS listing_count
                FROM listings
                WHERE barrio IN ({placeholders})
                  AND last_seen_date >= ?
                  AND last_seen_date IS NOT NULL
                  AND price   > 0
                  AND size_sqm > 10
                GROUP BY barrio, week
                ORDER BY barrio, week_start
            """, (*barrios, cutoff))
            cols = ["barrio", "week", "week_start", "median_price_sqm", "listing_count"]
            return [dict(zip(cols, row)) for row in cursor.fetchall()]
    except Exception as exc:
        print(f"Error getting barrio price evolution: {exc}")
        return []


def get_barrio_summary(barrios: List[str]) -> List[Dict]:
    """
    Return a rich summary row per barrio for the comparator:
      barrio, distrito, active_count, median_price, median_price_sqm,
      avg_size_sqm, avg_rooms, avg_days_market.
    """
    if not barrios:
        return []
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            placeholders = ",".join("?" * len(barrios))
            cursor.execute(f"""
                SELECT
                    barrio,
                    MAX(distrito)                                    AS distrito,
                    COUNT(*)                                         AS active_count,
                    AVG(price)                                       AS median_price,
                    AVG(CAST(price AS FLOAT) / NULLIF(size_sqm, 0)) AS median_price_sqm,
                    AVG(size_sqm)                                    AS avg_size_sqm,
                    AVG(rooms)                                       AS avg_rooms,
                    AVG(
                        julianday(last_seen_date) - julianday(first_seen_date)
                    )                                                AS avg_days_market
                FROM listings
                WHERE barrio IN ({placeholders})
                  AND status = 'active'
                  AND price   > 0
                GROUP BY barrio
            """, tuple(barrios))
            cols = [
                "barrio", "distrito", "active_count", "median_price",
                "median_price_sqm", "avg_size_sqm", "avg_rooms", "avg_days_market",
            ]
            return [dict(zip(cols, row)) for row in cursor.fetchall()]
    except Exception as exc:
        print(f"Error getting barrio summary: {exc}")
        return []


# ============================================================================
# RENTAL PRICE FUNCTIONS
# ============================================================================

def migrate_create_rental_prices_table():
    """
    Migration: Create rental_prices table on existing databases.
    Safe to run multiple times (CREATE TABLE IF NOT EXISTS).
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rental_prices (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                distrito        TEXT    NOT NULL,
                barrio          TEXT    NOT NULL,
                date_recorded   TEXT    NOT NULL,
                median_rent     REAL    NOT NULL,
                listing_count   INTEGER NOT NULL DEFAULT 0,
                UNIQUE(barrio, date_recorded)
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_rental_barrio_date
            ON rental_prices(barrio, date_recorded)
        """)
        print("✓ Rental prices table initialized")


def upsert_rental_snapshot(
    distrito: str,
    barrio: str,
    median_rent: float,
    listing_count: int,
    date: Optional[str] = None,
) -> bool:
    """
    Insert or replace today's rental snapshot for a barrio.

    Args:
        distrito:       District name (e.g. "Centro")
        barrio:         Neighbourhood name (e.g. "Sol")
        median_rent:    Median monthly asking rent in euros
        listing_count:  Number of rental listings used to compute the median
        date:           Date string YYYY-MM-DD (defaults to today)

    Returns:
        True if successful, False otherwise
    """
    if date is None:
        date = datetime.now().strftime("%Y-%m-%d")
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO rental_prices
                    (distrito, barrio, date_recorded, median_rent, listing_count)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(barrio, date_recorded)
                DO UPDATE SET
                    median_rent   = excluded.median_rent,
                    listing_count = excluded.listing_count,
                    distrito      = excluded.distrito
            """, (distrito, barrio, date, round(median_rent, 2), listing_count))
        return True
    except Exception as exc:
        print(f"Error upserting rental snapshot for {barrio}: {exc}")
        return False


def get_rental_yields(min_listings: int = 3) -> List[Dict]:
    """
    Compute gross rental yield per barrio/distrito.

    Yield = (median_rent_monthly × 12) / median_sale_price × 100

    Uses the most recent rental snapshot and active sale listing medians.
    Only includes barrios with at least `min_listings` rental listings.

    Returns:
        List of dicts sorted by yield desc, each with:
          distrito, barrio, median_rent, median_sale_price,
          yield_pct, rental_listing_count, sale_listing_count,
          date_recorded
    """
    try:
        with get_connection() as conn:
            cursor = conn.cursor()

            # Check table exists (migration may not have run yet)
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='rental_prices'"
            )
            if not cursor.fetchone():
                return []

            # Latest rental snapshot per barrio
            cursor.execute("""
                SELECT
                    r.distrito,
                    r.barrio,
                    r.median_rent,
                    r.listing_count  AS rental_count,
                    r.date_recorded
                FROM rental_prices r
                INNER JOIN (
                    SELECT barrio, MAX(date_recorded) AS latest
                    FROM rental_prices
                    GROUP BY barrio
                ) latest_r ON r.barrio = latest_r.barrio
                          AND r.date_recorded = latest_r.latest
                WHERE r.listing_count >= ?
            """, (min_listings,))

            rental_rows = {
                row[1]: {           # keyed by barrio
                    "distrito":      row[0],
                    "barrio":        row[1],
                    "median_rent":   row[2],
                    "rental_count":  row[3],
                    "date_recorded": row[4],
                }
                for row in cursor.fetchall()
            }

            if not rental_rows:
                return []

            # Median sale price per barrio from active listings
            cursor.execute("""
                SELECT
                    barrio,
                    AVG(price)  AS median_sale,   -- approx median via AVG (fast)
                    COUNT(*)    AS sale_count
                FROM listings
                WHERE status = 'active'
                  AND price > 0
                  AND barrio IS NOT NULL
                GROUP BY barrio
                HAVING COUNT(*) >= 5
            """)

            sale_rows = {
                row[0]: {"median_sale": row[1], "sale_count": row[2]}
                for row in cursor.fetchall()
            }

            results = []
            for barrio, rental in rental_rows.items():
                if barrio not in sale_rows:
                    continue
                sale = sale_rows[barrio]
                median_sale = sale["median_sale"]
                if not median_sale or median_sale <= 0:
                    continue

                annual_rent = rental["median_rent"] * 12
                yield_pct   = round(annual_rent / median_sale * 100, 2)

                results.append({
                    "distrito":            rental["distrito"],
                    "barrio":              barrio,
                    "median_rent":         round(rental["median_rent"], 0),
                    "median_sale_price":   round(median_sale, 0),
                    "yield_pct":           yield_pct,
                    "rental_listing_count": rental["rental_count"],
                    "sale_listing_count":  sale["sale_count"],
                    "date_recorded":       rental["date_recorded"],
                })

            results.sort(key=lambda x: x["yield_pct"], reverse=True)
            return results

    except Exception as exc:
        print(f"Error computing rental yields: {exc}")
        return []


# =============================================================================
# BARRIO RANKING
# =============================================================================

def get_barrio_ranking(min_listings: int = 5) -> List[Dict]:
    """
    Return a composite ranking (0-100) for every barrio with enough data.

    Score components (buyer perspective):
        25 % — Precio bajo      : €/m² vs Madrid median (lower = better)
        20 % — Tendencia        : weekly price change   (falling = better)
        20 % — Tiempo mercado   : avg days on market    (longer = more room to negotiate)
        20 % — Rentabilidad     : gross rental yield    (higher = better)
        15 % — Oferta           : active listings count (more = better choice)

    Each component is normalised 0-100 across all barrios before weighting.

    Returns list of dicts sorted by ranking_score descending:
        barrio, distrito, ranking_score, active_count,
        avg_price_sqm, days_on_market, yield_pct,
        price_trend_pct, score_precio, score_tendencia,
        score_tiempo, score_rentabilidad, score_oferta
    """
    try:
        with get_connection() as conn:
            cursor = conn.cursor()

            # ── Base stats per barrio (active listings) ──────────────────────
            cursor.execute("""
                SELECT
                    barrio,
                    MAX(distrito)                                       AS distrito,
                    COUNT(*)                                            AS active_count,
                    AVG(CAST(price AS FLOAT) / NULLIF(size_sqm, 0))    AS avg_price_sqm,
                    AVG(julianday('now') - julianday(first_seen_date))  AS avg_days_market
                FROM listings
                WHERE status  = 'active'
                  AND price   > 0
                  AND size_sqm > 10
                  AND barrio IS NOT NULL
                GROUP BY barrio
                HAVING COUNT(*) >= ?
            """, (min_listings,))
            base_rows = {row[0]: {
                "barrio":          row[0],
                "distrito":        row[1],
                "active_count":    row[2],
                "avg_price_sqm":   round(row[3], 0) if row[3] else None,
                "days_on_market":  round(row[4], 0) if row[4] else None,
            } for row in cursor.fetchall()}

            if not base_rows:
                return []

            # ── Weekly price trend per barrio ─────────────────────────────────
            cursor.execute("""
                SELECT
                    l.barrio,
                    AVG(CASE WHEN ph.date_recorded >= date('now', '-7 days')
                             THEN ph.price END) AS price_last_week,
                    AVG(CASE WHEN ph.date_recorded >= date('now', '-14 days')
                              AND ph.date_recorded <  date('now', '-7 days')
                             THEN ph.price END) AS price_prev_week
                FROM price_history ph
                JOIN listings l ON l.listing_id = ph.listing_id
                WHERE l.barrio IS NOT NULL
                GROUP BY l.barrio
            """)
            trend_by_barrio = {}
            for row in cursor.fetchall():
                barrio, last, prev = row
                if last and prev and prev > 0:
                    trend_by_barrio[barrio] = (last - prev) / prev * 100

            # ── Rental yield per barrio (latest snapshot) ─────────────────────
            yield_by_barrio = {}
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='rental_prices'"
            )
            if cursor.fetchone():
                cursor.execute("""
                    SELECT r.barrio, r.median_rent,
                           AVG(CAST(l.price AS FLOAT) / NULLIF(l.size_sqm, 0)) AS avg_sqm
                    FROM rental_prices r
                    JOIN listings l ON l.barrio = r.barrio AND l.status = 'active'
                    WHERE r.date_recorded = (
                        SELECT MAX(date_recorded) FROM rental_prices rr
                        WHERE rr.barrio = r.barrio
                    )
                      AND r.listing_count >= 3
                    GROUP BY r.barrio
                    HAVING COUNT(l.listing_id) >= 3
                """)
                for row in cursor.fetchall():
                    barrio, rent, sqm = row
                    if rent and sqm and sqm > 0:
                        typical_sqm = 80
                        sale_price = sqm * typical_sqm
                        y = (rent * 12) / sale_price * 100
                        if 0 < y < 20:
                            yield_by_barrio[barrio] = round(y, 2)

        # ── Merge all signals ─────────────────────────────────────────────────
        rows = []
        for barrio, base in base_rows.items():
            rows.append({
                **base,
                "price_trend_pct": trend_by_barrio.get(barrio),
                "yield_pct":       yield_by_barrio.get(barrio),
            })

        if not rows:
            return []

        # ── Normalise each component 0-100 ────────────────────────────────────
        def _norm(values: list, invert: bool = False) -> list:
            """Min-max normalise; invert=True means lower raw → higher score."""
            valid = [v for v in values if v is not None]
            if not valid or max(valid) == min(valid):
                return [50 if v is not None else None for v in values]
            lo, hi = min(valid), max(valid)
            normed = []
            for v in values:
                if v is None:
                    normed.append(None)
                else:
                    n = (v - lo) / (hi - lo) * 100
                    normed.append(round(100 - n if invert else n, 1))
            return normed

        prices     = [r["avg_price_sqm"]   for r in rows]
        days       = [r["days_on_market"]   for r in rows]
        trends     = [r["price_trend_pct"]  for r in rows]
        yields     = [r["yield_pct"]        for r in rows]
        counts     = [r["active_count"]     for r in rows]

        score_precio      = _norm(prices, invert=True)   # low price → high score
        score_tendencia   = _norm(trends, invert=True)   # falling trend → high score
        score_tiempo      = _norm(days,   invert=False)  # long days → high score
        score_rentabilidad = _norm(yields, invert=False) # high yield → high score
        score_oferta      = _norm(counts, invert=False)  # more listings → high score

        WEIGHTS = dict(precio=0.25, tendencia=0.20, tiempo=0.20,
                       rentabilidad=0.20, oferta=0.15)

        results = []
        for i, row in enumerate(rows):
            sp  = score_precio[i]      or 50
            st  = score_tendencia[i]   or 50
            sti = score_tiempo[i]      or 50
            sr  = score_rentabilidad[i] or 50
            so  = score_oferta[i]      or 50

            composite = round(
                WEIGHTS["precio"]       * sp  +
                WEIGHTS["tendencia"]    * st  +
                WEIGHTS["tiempo"]       * sti +
                WEIGHTS["rentabilidad"] * sr  +
                WEIGHTS["oferta"]       * so
            )
            results.append({
                **row,
                "ranking_score":     composite,
                "score_precio":      round(sp),
                "score_tendencia":   round(st),
                "score_tiempo":      round(sti),
                "score_rentabilidad": round(sr),
                "score_oferta":      round(so),
            })

        results.sort(key=lambda x: x["ranking_score"], reverse=True)

        # ── NLP urgency rate per barrio ───────────────────────────────────────
        try:
            with get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT l.barrio,
                           COUNT(*)                                    AS total,
                           SUM(CASE WHEN s.urgency = 1 THEN 1 ELSE 0 END) AS urgent
                    FROM listings l
                    LEFT JOIN listing_signals s ON s.listing_id = l.listing_id
                    WHERE l.status = 'active' AND l.barrio IS NOT NULL
                    GROUP BY l.barrio
                """)
                urgency_by_barrio = {
                    row[0]: round(row[2] / row[1] * 100, 1) if row[1] else 0
                    for row in cursor.fetchall()
                }
            for r in results:
                r["urgency_pct"] = urgency_by_barrio.get(r["barrio"], 0)
        except Exception:
            for r in results:
                r["urgency_pct"] = None

        # Add rank position
        for idx, r in enumerate(results, start=1):
            r["rank"] = idx

        return results

    except Exception as exc:
        print(f"Error computing barrio ranking: {exc}")
        return []


# =============================================================================
# WATCHLIST — user-saved properties for price tracking
# =============================================================================

def migrate_create_watchlist_table():
    """Migration: create watchlist table on existing databases. Safe to run multiple times."""
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS watchlist (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    listing_id      TEXT    NOT NULL UNIQUE,
                    added_date      TEXT    NOT NULL DEFAULT (date('now')),
                    note            TEXT,
                    price_at_add    INTEGER,
                    alert_on_drop   BOOLEAN NOT NULL DEFAULT 1
                )
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_watchlist_listing
                ON watchlist(listing_id)
            """)
            conn.commit()
            print("✓ Watchlist table ready")
    except Exception as exc:
        print(f"Migration error (watchlist): {exc}")


def add_to_watchlist(listing_id: str, note: str = "", alert_on_drop: bool = True) -> bool:
    """
    Add a listing to the watchlist. Records the current price automatically.
    Returns True if added, False if already present.
    """
    try:
        current_price = get_current_price(listing_id)
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR IGNORE INTO watchlist (listing_id, added_date, note, price_at_add, alert_on_drop)
                VALUES (?, date('now'), ?, ?, ?)
            """, (listing_id, note or "", current_price, 1 if alert_on_drop else 0))
            conn.commit()
            return cursor.rowcount > 0
    except Exception as exc:
        print(f"Error adding to watchlist: {exc}")
        return False


def remove_from_watchlist(listing_id: str) -> bool:
    """Remove a listing from the watchlist. Returns True if removed."""
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM watchlist WHERE listing_id = ?", (listing_id,))
            conn.commit()
            return cursor.rowcount > 0
    except Exception as exc:
        print(f"Error removing from watchlist: {exc}")
        return False


def is_in_watchlist(listing_id: str) -> bool:
    """Return True if the listing is currently in the watchlist."""
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM watchlist WHERE listing_id = ?", (listing_id,))
            return cursor.fetchone() is not None
    except Exception:
        return False


def get_watchlist_ids() -> set:
    """Return the set of all listing_ids currently in the watchlist."""
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT listing_id FROM watchlist")
            return {row[0] for row in cursor.fetchall()}
    except Exception:
        return set()


def get_watchlist(include_sold: bool = True) -> List[Dict]:
    """
    Return all watchlist entries joined with current listing data.

    Each entry has:
        listing_id, url, barrio, distrito, price, size_sqm, rooms,
        price_per_sqm, status, price_at_add, price_change, price_change_pct,
        added_date, note, alert_on_drop, days_watched, num_drops
    """
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            query = """
                SELECT
                    w.listing_id,
                    w.added_date,
                    w.note,
                    w.price_at_add,
                    w.alert_on_drop,
                    l.url,
                    l.barrio,
                    l.distrito,
                    l.price          AS current_price,
                    l.size_sqm,
                    l.rooms,
                    CAST(l.price AS FLOAT) / NULLIF(l.size_sqm, 0) AS price_per_sqm,
                    l.status,
                    l.first_seen_date,
                    julianday('now') - julianday(w.added_date) AS days_watched
                FROM watchlist w
                LEFT JOIN listings l ON l.listing_id = w.listing_id
                ORDER BY w.added_date DESC
            """
            cursor.execute(query)
            rows = cursor.fetchall()

        # Get drop counts for watchlist listings
        ids = [r[0] for r in rows]
        drop_counts = get_drop_counts_for_listings(ids) if ids else {}

        results = []
        for row in rows:
            (lid, added_date, note, price_at_add, alert_on_drop,
             url, barrio, distrito, current_price, size_sqm, rooms,
             price_per_sqm, status, first_seen, days_watched) = row

            if not include_sold and status != "active":
                continue

            price_change     = None
            price_change_pct = None
            if price_at_add and current_price:
                price_change     = current_price - price_at_add
                price_change_pct = price_change / price_at_add * 100

            results.append({
                "listing_id":       lid,
                "url":              url or "#",
                "barrio":           barrio or "—",
                "distrito":         distrito or "—",
                "current_price":    current_price,
                "size_sqm":         size_sqm,
                "rooms":            rooms,
                "price_per_sqm":    round(price_per_sqm, 0) if price_per_sqm else None,
                "status":           status or "unknown",
                "price_at_add":     price_at_add,
                "price_change":     price_change,
                "price_change_pct": round(price_change_pct, 1) if price_change_pct is not None else None,
                "added_date":       added_date,
                "note":             note or "",
                "alert_on_drop":    bool(alert_on_drop),
                "days_watched":     int(days_watched) if days_watched else 0,
                "num_drops":        drop_counts.get(lid, 0),
            })
        return results
    except Exception as exc:
        print(f"Error getting watchlist: {exc}")
        return []


def get_watchlist_price_drops(since_days: int = 1) -> List[Dict]:
    """
    Return watchlist entries where the price dropped in the last `since_days` days.
    Used to build the email alert section.
    """
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT
                    w.listing_id,
                    l.url,
                    l.barrio,
                    l.distrito,
                    l.price             AS current_price,
                    l.size_sqm,
                    l.rooms,
                    l.status,
                    ph.price - ph.change_amount AS old_price,
                    ph.price                    AS new_price,
                    ph.change_amount,
                    ph.change_percent,
                    ph.date_recorded            AS drop_date
                FROM watchlist w
                JOIN listings l       ON l.listing_id = w.listing_id
                JOIN price_history ph ON ph.listing_id = w.listing_id
                WHERE ph.change_amount < 0
                  AND ph.date_recorded >= date('now', ? || ' days')
                  AND l.status = 'active'
                ORDER BY ph.change_percent ASC
            """, (f"-{since_days}",))
            rows = cursor.fetchall()

        results = []
        for row in rows:
            (lid, url, barrio, distrito, current_price, size_sqm, rooms, status,
             old_price, new_price, change_amount, change_percent, drop_date) = row
            results.append({
                "listing_id":     lid,
                "url":            url or "#",
                "barrio":         barrio or "—",
                "distrito":       distrito or "—",
                "current_price":  current_price,
                "size_sqm":       size_sqm,
                "rooms":          rooms,
                "status":         status,
                "old_price":      old_price,
                "new_price":      new_price,
                "change_amount":  change_amount,
                "change_percent": round(change_percent, 1) if change_percent else None,
                "drop_date":      drop_date,
            })
        return results
    except Exception as exc:
        print(f"Error getting watchlist price drops: {exc}")
        return []


def get_new_opportunity_listings(hours: int = 24, min_score: int = 70) -> List[Dict]:
    """
    Return new listings (first seen within `hours` hours) with a high opportunity score.

    Opportunity score (0-100):
        40% → price vs barrio median €/m²  (needs barrio_price_stats)
        30% → days on market               (0 for brand-new listings → neutral 50)
        30% → price drops                  (0 for brand-new → neutral 50)

    Args:
        hours:     Look-back window in hours (default 24 → "today's new listings").
        min_score: Minimum opportunity score to include in results.

    Returns:
        List of dicts sorted by score descending, each with keys:
        listing_id, url, barrio, distrito, price, size_sqm, rooms, price_per_sqm,
        vs_barrio_pct, barrio_median_sqm, score_oportunidad, first_seen_date.
    """
    try:
        with get_connection() as conn:
            cursor = conn.cursor()

            # 1. Fetch listings first seen in the last `hours` hours
            cursor.execute("""
                SELECT
                    listing_id, url, barrio, distrito,
                    price, size_sqm, rooms,
                    CAST(price AS FLOAT) / NULLIF(size_sqm, 0) AS price_per_sqm,
                    first_seen_date, floor
                FROM listings
                WHERE status = 'active'
                  AND price  > 0
                  AND size_sqm > 10
                  AND first_seen_date >= datetime('now', ? || ' hours')
                ORDER BY first_seen_date DESC
            """, (f"-{hours}",))

            rows = cursor.fetchall()
            if not rows:
                return []

        # 2. Get barrio price stats to compute vs-barrio %
        barrio_stats = get_barrio_price_stats(min_listings=3)

        results = []
        for row in rows:
            (lid, url, barrio, distrito,
             price, size_sqm, rooms, price_per_sqm,
             first_seen, floor) = row

            # vs barrio %  (positive = more expensive, negative = cheaper = good)
            barrio_median = None
            vs_barrio_pct = None
            if barrio and barrio in barrio_stats:
                barrio_median = barrio_stats[barrio].get("median_price_sqm")
                if barrio_median and price_per_sqm:
                    vs_barrio_pct = (price_per_sqm - barrio_median) / barrio_median * 100

            # Price component  (40 %)
            if vs_barrio_pct is not None:
                pct_clamped = max(-20.0, min(20.0, vs_barrio_pct))
                price_score = 100 * (1 - (pct_clamped + 20) / 40)   # 0 pct → 50, -20 → 100, +20 → 0
            else:
                price_score = 50  # neutral when no barrio data

            # For brand-new listings: days=0 → speed score neutral (50)
            # and drops=0 → drop score neutral (50)
            speed_score = 50
            drop_score  = 50

            score = round(0.40 * price_score + 0.30 * speed_score + 0.30 * drop_score)

            if score < min_score:
                continue

            results.append({
                "listing_id":       lid,
                "url":              url,
                "barrio":           barrio or "—",
                "distrito":         distrito or "—",
                "price":            price,
                "size_sqm":         size_sqm,
                "rooms":            rooms,
                "price_per_sqm":    round(price_per_sqm, 0) if price_per_sqm else None,
                "barrio_median_sqm": round(barrio_median, 0) if barrio_median else None,
                "vs_barrio_pct":    round(vs_barrio_pct, 1) if vs_barrio_pct is not None else None,
                "score_oportunidad": score,
                "first_seen_date":  first_seen,
                "floor":            floor,
            })

        results.sort(key=lambda x: x["score_oportunidad"], reverse=True)

        # Enrich with NLP signals
        try:
            from nlp_analyzer import get_signals_for_listings
            ids = [r["listing_id"] for r in results]
            signals_map = get_signals_for_listings(ids)
            for r in results:
                sig = signals_map.get(r["listing_id"], {})
                r["urgency"]    = sig.get("urgency", False)
                r["direct"]     = sig.get("direct", False)
                r["negotiable"] = sig.get("negotiable", False)
                r["renovated"]  = sig.get("renovated", False)
                r["needs_work"] = sig.get("needs_work", False)
                r["nlp_bonus"]  = sig.get("nlp_bonus", 0)
        except Exception:
            pass

        return results

    except Exception as exc:
        print(f"Error getting new opportunity listings: {exc}")
        return []


def get_rental_yield_history(weeks: int = 12) -> List[Dict]:
    """
    Return weekly average gross rental yield (all barrios combined) for the last `weeks` weeks.

    Uses `rental_prices` snapshots + the corresponding active sale prices.

    Returns:
        List of dicts sorted by week ascending:
        [ { week: "YYYY-WW", week_start: "YYYY-MM-DD", avg_yield_pct: float, barrio_count: int }, … ]
    """
    try:
        with get_connection() as conn:
            cursor = conn.cursor()

            # Check table exists
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='rental_prices'"
            )
            if not cursor.fetchone():
                return []

            # Get weekly snapshots from rental_prices
            cursor.execute("""
                SELECT
                    strftime('%Y-%W', date_recorded)   AS week,
                    MIN(date_recorded)                 AS week_start,
                    barrio,
                    AVG(median_rent)                   AS avg_rent,
                    SUM(listing_count)                 AS cnt
                FROM rental_prices
                WHERE date_recorded >= date('now', ? || ' days')
                  AND median_rent > 0
                  AND listing_count >= 3
                GROUP BY strftime('%Y-%W', date_recorded), barrio
                HAVING SUM(listing_count) >= 3
            """, (f"-{weeks * 7}",))

            rental_rows = cursor.fetchall()
            if not rental_rows:
                return []

            # Build weekly median sale price per barrio from sale listing snapshots
            # We approximate using the listings table (current state) crossed with rental week data
            # For each barrio, get the median sale price from the closest active snapshot
            cursor.execute("""
                SELECT
                    barrio,
                    AVG(CAST(price AS FLOAT) / NULLIF(size_sqm, 0)) AS avg_sqm,
                    COUNT(*) AS cnt
                FROM listings
                WHERE status = 'active'
                  AND price > 0
                  AND size_sqm > 10
                  AND barrio IS NOT NULL
                GROUP BY barrio
                HAVING COUNT(*) >= 3
            """)
            sale_by_barrio = {row[0]: row[1] for row in cursor.fetchall() if row[1]}

        if not sale_by_barrio:
            return []

        # Aggregate: for each (week, barrio) compute yield, then average across barrios per week
        from collections import defaultdict
        weekly_yields: dict = defaultdict(list)
        weekly_starts: dict = {}

        for week, week_start, barrio, avg_rent, cnt in rental_rows:
            sale_sqm = sale_by_barrio.get(barrio)
            if not sale_sqm or sale_sqm <= 0:
                continue
            # Estimate median sale price using avg size=80m² as proxy (or just use per-m² × typical sqm)
            # Better: use avg_sqm × typical_size to get median price, but we don't have size here.
            # Use the rental/sqm approach: yield = (rent*12) / (avg_sqm * typical_sqm)
            # Simpler: yield = (rent * 12) / (sale_sqm * 80) -- approximate
            typical_sqm = 80  # Madrid typical apartment size
            approx_sale_price = sale_sqm * typical_sqm
            if approx_sale_price <= 0:
                continue
            y = (avg_rent * 12) / approx_sale_price * 100
            if 0 < y < 20:  # sanity filter
                weekly_yields[week].append(y)
                weekly_starts[week] = week_start

        if not weekly_yields:
            return []

        results = []
        for week in sorted(weekly_yields.keys()):
            yields_list = weekly_yields[week]
            results.append({
                "week":          week,
                "week_start":    weekly_starts[week],
                "avg_yield_pct": round(sum(yields_list) / len(yields_list), 2),
                "barrio_count":  len(yields_list),
            })

        return results

    except Exception as exc:
        print(f"Error getting rental yield history: {exc}")
        return []


def get_price_drop_stats() -> Dict:
    """
    Returns comprehensive price-drop statistics for the dashboard.

    Sections:
      - overview: headline KPIs
      - by_barrio: per-neighbourhood breakdown (min 5 active listings)
      - recent_drops: listings whose price dropped in the last 7 days
      - drop_magnitude_buckets: histogram of drop sizes
    """
    try:
        with get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()

            # ── Overview KPIs ────────────────────────────────────────────────
            cur.execute("""
                SELECT COUNT(DISTINCT listing_id) AS total_active
                FROM listings
                WHERE status = 'active'
            """)
            row0 = cur.fetchone()
            total_active = row0["total_active"] if row0 else 0

            cur.execute("""
                SELECT COUNT(DISTINCT ph.listing_id) AS with_drops
                FROM price_history ph
                JOIN listings l ON l.listing_id = ph.listing_id
                WHERE ph.change_amount < 0 AND l.status = 'active'
            """)
            row0 = cur.fetchone()
            with_drops = row0["with_drops"] if row0 else 0

            cur.execute("""
                SELECT AVG(ph.change_percent)  AS avg_pct,
                       MIN(ph.change_percent)  AS min_pct,
                       COUNT(*)                AS n_events
                FROM price_history ph
                JOIN listings l ON l.listing_id = ph.listing_id
                WHERE ph.change_amount < 0 AND l.status = 'active'
            """)
            row = cur.fetchone() or {}
            avg_drop_pct = round(row["avg_pct"] or 0, 2)
            max_drop_pct = round(row["min_pct"] or 0, 2)   # most negative = largest drop
            n_drop_events = row["n_events"] or 0

            # Average days from first_seen_date to first price drop
            cur.execute("""
                SELECT AVG(julianday(ph.date_recorded) - julianday(l.first_seen_date))
                FROM price_history ph
                JOIN listings l ON l.listing_id = ph.listing_id
                WHERE ph.change_amount < 0
                  AND ph.date_recorded = (
                      SELECT MIN(p2.date_recorded)
                      FROM price_history p2
                      WHERE p2.listing_id = ph.listing_id AND p2.change_amount < 0
                  )
            """)
            avg_days_to_drop = round((cur.fetchone()[0] or 0), 1)

            # Listings that dropped in the last 7 days
            cur.execute("""
                SELECT COUNT(DISTINCT listing_id)
                FROM price_history
                WHERE change_amount < 0
                  AND date_recorded >= date('now', '-7 days')
            """)
            recent_7d = (cur.fetchone() or [0])[0] or 0

            # Listings that dropped in the last 30 days
            cur.execute("""
                SELECT COUNT(DISTINCT listing_id)
                FROM price_history
                WHERE change_amount < 0
                  AND date_recorded >= date('now', '-30 days')
            """)
            recent_30d = (cur.fetchone() or [0])[0] or 0

            overview = {
                "total_active":      total_active,
                "with_drops":        with_drops,
                "drop_pct_of_total": round(100 * with_drops / total_active, 1) if total_active else 0,
                "avg_drop_pct":      avg_drop_pct,
                "max_drop_pct":      max_drop_pct,
                "n_drop_events":     n_drop_events,
                "avg_days_to_drop":  avg_days_to_drop,
                "recent_7d":         recent_7d,
                "recent_30d":        recent_30d,
            }

            # ── Per-barrio breakdown ─────────────────────────────────────────
            cur.execute("""
                SELECT
                    l.barrio,
                    l.distrito,
                    COUNT(DISTINCT l.listing_id)              AS total,
                    COUNT(DISTINCT ph.listing_id)             AS with_drops,
                    ROUND(AVG(ph.change_percent), 2)          AS avg_drop_pct,
                    ROUND(MIN(ph.change_percent), 2)          AS max_drop_pct,
                    ROUND(
                        100.0 * COUNT(DISTINCT ph.listing_id)
                        / NULLIF(COUNT(DISTINCT l.listing_id), 0)
                    , 1)                                      AS drop_rate_pct
                FROM listings l
                LEFT JOIN price_history ph
                    ON ph.listing_id = l.listing_id AND ph.change_amount < 0
                WHERE l.status = 'active'
                GROUP BY l.barrio
                HAVING total >= 5
                ORDER BY drop_rate_pct DESC
            """)
            by_barrio = [dict(r) for r in cur.fetchall()]

            # ── Recent drops (last 7 days) with listing detail ───────────────
            cur.execute("""
                SELECT
                    ph.listing_id,
                    l.title,
                    l.barrio,
                    l.distrito,
                    l.price             AS current_price,
                    ph.change_amount,
                    ph.change_percent,
                    ph.date_recorded,
                    l.url,
                    l.size_sqm,
                    l.rooms
                FROM price_history ph
                JOIN listings l ON l.listing_id = ph.listing_id
                WHERE ph.change_amount < 0
                  AND ph.date_recorded >= date('now', '-7 days')
                  AND l.status = 'active'
                ORDER BY ph.change_percent ASC
                LIMIT 50
            """)
            recent_drops = [dict(r) for r in cur.fetchall()]

            # ── Drop magnitude histogram ─────────────────────────────────────
            # Buckets: 0-2%, 2-5%, 5-10%, 10-20%, >20%
            buckets = {"0-2%": 0, "2-5%": 0, "5-10%": 0, "10-20%": 0, ">20%": 0}
            cur.execute("""
                SELECT change_percent FROM price_history
                WHERE change_amount < 0
            """)
            for (pct,) in cur.fetchall():
                if pct is None:
                    continue
                p = abs(pct)
                if p < 2:
                    buckets["0-2%"] += 1
                elif p < 5:
                    buckets["2-5%"] += 1
                elif p < 10:
                    buckets["5-10%"] += 1
                elif p < 20:
                    buckets["10-20%"] += 1
                else:
                    buckets[">20%"] += 1

            return {
                "overview":               overview,
                "by_barrio":              by_barrio,
                "recent_drops":           recent_drops,
                "drop_magnitude_buckets": buckets,
            }

    except Exception as exc:
        print(f"Error getting price drop stats: {exc}")
        return {
            "overview":               {},
            "by_barrio":              [],
            "recent_drops":           [],
            "drop_magnitude_buckets": {},
        }


def get_price_trend_by_district(weeks: int = 12) -> List[Dict]:
    """
    Weekly average €/m² per district, using first_seen_date as the time axis.
    Returns a flat list of {week, week_label, distrito, avg_sqm, n_listings}.
    """
    try:
        with get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("""
                SELECT
                    strftime('%Y-%W', first_seen_date)            AS week,
                    date(first_seen_date, 'weekday 1', '-7 days') AS week_start,
                    distrito,
                    ROUND(AVG(CAST(price AS FLOAT) / NULLIF(size_sqm, 0)), 0) AS avg_sqm,
                    COUNT(*) AS n_listings
                FROM listings
                WHERE size_sqm > 20
                  AND price > 50000
                  AND first_seen_date IS NOT NULL
                  AND first_seen_date >= date('now', ? || ' days')
                GROUP BY week, distrito
                HAVING n_listings >= 3
                ORDER BY week, distrito
            """, (f"-{weeks * 7}",))
            return [dict(r) for r in cur.fetchall()]
    except Exception as exc:
        print(f"Error getting price trend: {exc}")
        return []


def get_market_summary_trend(weeks: int = 12) -> List[Dict]:
    """
    Weekly market price trend (median, IQR-filtered) using first_seen_date.

    Groups listings by when they first appeared on Idealista (entry price).
    Each week's central tendency is computed in Python using statistics.median()
    after removing outliers with the same IQR fence method already used in
    market_indicators.get_weekly_price_evolution().  This replaces the old
    SQL AVG() approach which was sensitive to expensive one-off listings
    (e.g. a €5M penthouse distorting an entire week's average).

    A bulk-import guard drops any week whose listing count exceeds 3× the
    median weekly count — these weeks represent data loads, not real market
    activity, and would otherwise skew week-over-week comparisons.

    Output keys (avg_sqm, avg_price, n_listings) are kept unchanged so that
    the PriceTrendChart frontend component requires no modification.
    The values are medians, not means — the field names are intentionally
    left as-is to minimise blast radius.
    """
    import statistics as _stats
    from collections import defaultdict

    def _iqr_filter(values: list, factor: float = 1.5) -> list:
        """Drop values outside [Q1 - factor*IQR, Q3 + factor*IQR].
        Returns the original list unchanged if fewer than 20 values."""
        if len(values) < 20:
            return values
        q1 = _stats.quantiles(values, n=4)[0]
        q3 = _stats.quantiles(values, n=4)[2]
        iqr = q3 - q1
        lo, hi = q1 - factor * iqr, q3 + factor * iqr
        filtered = [v for v in values if lo <= v <= hi]
        # Safety: never discard more than 20 % of the sample
        return filtered if len(filtered) >= len(values) * 0.8 else values

    try:
        with get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            # Fetch one row per listing; grouping + median happen in Python
            cur.execute("""
                SELECT
                    strftime('%Y-%W', first_seen_date)            AS week,
                    date(first_seen_date, 'weekday 1', '-7 days') AS week_start,
                    price,
                    CAST(price AS FLOAT) / NULLIF(size_sqm, 0)    AS price_sqm
                FROM listings
                WHERE size_sqm > 20
                  AND price > 50000
                  AND CAST(price AS FLOAT) / NULLIF(size_sqm, 0) < 25000
                  AND first_seen_date IS NOT NULL
                  AND first_seen_date >= date('now', ? || ' days')
                ORDER BY week
            """, (f"-{weeks * 7}",))
            rows = cur.fetchall()
    except Exception as exc:
        print(f"Error getting market summary trend: {exc}")
        return []

    # --- Group by week ---------------------------------------------------
    week_prices: dict  = defaultdict(list)
    week_sqm: dict     = defaultdict(list)
    week_starts: dict  = {}

    for row in rows:
        w = row["week"]
        if not w:
            continue
        week_starts[w] = row["week_start"]
        if row["price"]:
            week_prices[w].append(float(row["price"]))
        if row["price_sqm"]:
            week_sqm[w].append(float(row["price_sqm"]))

    # --- Median + IQR per week ------------------------------------------
    result = []
    for week in sorted(week_prices.keys()):
        prices  = week_prices[week]
        sqm_vals = week_sqm[week]

        if len(prices) < 5:
            continue

        prices_clean  = _iqr_filter(prices)
        sqm_clean     = _iqr_filter(sqm_vals) if sqm_vals else sqm_vals

        result.append({
            "week":       week,
            "week_start": week_starts.get(week),
            "avg_sqm":    round(_stats.median(sqm_clean))   if sqm_clean    else None,
            "avg_price":  round(_stats.median(prices_clean)),
            "n_listings": len(prices_clean),
        })

    # --- Bulk-import guard: drop weeks >3× median count -----------------
    if len(result) >= 3:
        counts       = sorted(pt["n_listings"] for pt in result)
        median_count = counts[len(counts) // 2]
        result       = [pt for pt in result if pt["n_listings"] <= median_count * 3]

    return result


# ---------------------------------------------------------------------------
# Custom Alerts
# ---------------------------------------------------------------------------

def init_alerts_table() -> None:
    """Create custom_alerts table and run additive migrations if needed."""
    with get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS custom_alerts (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                name          TEXT NOT NULL,
                distritos     TEXT,          -- JSON list, empty = all
                barrios       TEXT,          -- JSON list, empty = all
                max_price     INTEGER,
                min_size      INTEGER,
                max_sqm_price INTEGER,
                min_rooms     INTEGER,
                seller_type   TEXT,          -- 'Particular', 'Agencia', or NULL = all
                min_score     INTEGER,       -- minimum opportunity score (0-100), NULL = no filter
                last_checked  TEXT,          -- ISO datetime of last time user viewed matches
                active        INTEGER DEFAULT 1,
                created_at    TEXT DEFAULT (datetime('now'))
            )
        """)
        # Additive migrations for existing tables (safe to run repeatedly)
        for col, definition in [
            ("min_score",    "INTEGER"),
            ("last_checked", "TEXT"),
        ]:
            try:
                conn.execute(f"ALTER TABLE custom_alerts ADD COLUMN {col} {definition}")
            except Exception:
                pass  # Column already exists
        conn.commit()


def touch_alert_checked(alert_id: int) -> None:
    """Update last_checked timestamp for an alert (called when user views matches)."""
    with get_connection() as conn:
        conn.execute(
            "UPDATE custom_alerts SET last_checked = datetime('now') WHERE id = ?",
            (alert_id,),
        )
        conn.commit()


def get_alerts() -> List[Dict]:
    """Return all active custom alerts."""
    try:
        with get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("SELECT * FROM custom_alerts WHERE active = 1 ORDER BY created_at DESC")
            return [dict(r) for r in cur.fetchall()]
    except Exception:
        return []


def add_alert(name: str, distritos: list = None, barrios: list = None,
              max_price: int = None, min_size: int = None,
              max_sqm_price: int = None, min_rooms: int = None,
              seller_type: str = None, min_score: int = None) -> int:
    """Insert a new alert. Returns the new alert ID."""
    import json
    with get_connection() as conn:
        cur = conn.execute("""
            INSERT INTO custom_alerts
              (name, distritos, barrios, max_price, min_size, max_sqm_price,
               min_rooms, seller_type, min_score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            name,
            json.dumps(distritos or []),
            json.dumps(barrios or []),
            max_price,
            min_size,
            max_sqm_price,
            min_rooms,
            seller_type,
            min_score,
        ))
        conn.commit()
        return cur.lastrowid


def delete_alert(alert_id: int) -> None:
    """Soft-delete an alert."""
    with get_connection() as conn:
        conn.execute("UPDATE custom_alerts SET active = 0 WHERE id = ?", (alert_id,))
        conn.commit()


def get_alert_matches(
    alert: Dict,
    hours: int = 24,
    since_datetime: str = None,
) -> List[Dict]:
    """
    Return listings that match the given alert criteria.

    Time window (pick one):
    - since_datetime: ISO datetime string — match listings first seen AFTER this timestamp.
      Used for "new since last check" mode.
    - hours: fallback rolling window (last N hours). Used when last_checked is None.

    Also applies min_score filter if set on the alert, joining with nlp_signals for
    NLP badge columns (urgency, direct, negotiable, renovated, needs_work).
    """
    import json
    try:
        distritos = json.loads(alert.get("distritos") or "[]")
        barrios   = json.loads(alert.get("barrios")   or "[]")

        # Time filter
        if since_datetime:
            conditions = ["l.first_seen_date > ?", "l.status = 'active'"]
            params: List = [since_datetime]
        else:
            conditions = ["l.first_seen_date >= datetime('now', ? || ' hours')", "l.status = 'active'"]
            params = [f"-{hours}"]

        if distritos:
            placeholders = ",".join("?" * len(distritos))
            conditions.append(f"l.distrito IN ({placeholders})")
            params.extend(distritos)
        if barrios:
            placeholders = ",".join("?" * len(barrios))
            conditions.append(f"l.barrio IN ({placeholders})")
            params.extend(barrios)
        if alert.get("max_price"):
            conditions.append("l.price <= ?")
            params.append(alert["max_price"])
        if alert.get("min_size"):
            conditions.append("l.size_sqm >= ?")
            params.append(alert["min_size"])
        if alert.get("max_sqm_price"):
            conditions.append("CAST(l.price AS FLOAT) / NULLIF(l.size_sqm, 0) <= ?")
            params.append(alert["max_sqm_price"])
        if alert.get("min_rooms"):
            conditions.append("l.rooms >= ?")
            params.append(alert["min_rooms"])
        if alert.get("seller_type"):
            conditions.append("l.seller_type = ?")
            params.append(alert["seller_type"])

        where = " AND ".join(conditions)

        # min_score filter applied in Python (score is computed by analytics, not stored)
        # We pull nlp_signals columns for display, and filter by score in Python below.
        with get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute(f"""
                SELECT l.listing_id, l.title, l.price, l.size_sqm, l.rooms,
                       l.barrio, l.distrito, l.seller_type, l.url,
                       l.first_seen_date,
                       ROUND(CAST(l.price AS FLOAT) / NULLIF(l.size_sqm, 0), 0) AS price_sqm,
                       n.urgency, n.direct_sale AS direct,
                       n.negotiable, n.renovated, n.needs_work,
                       n.score_oportunidad
                FROM listings l
                LEFT JOIN nlp_signals n ON n.listing_id = l.listing_id
                WHERE {where}
                ORDER BY l.first_seen_date DESC, l.price ASC
                LIMIT 50
            """, params)
            rows = [dict(r) for r in cur.fetchall()]

        # Apply min_score filter (score may be NULL for listings without NLP)
        min_score = alert.get("min_score")
        if min_score:
            rows = [r for r in rows if (r.get("score_oportunidad") or 0) >= min_score]

        return rows[:20]

    except Exception as exc:
        print(f"Error checking alert matches: {exc}")
        return []


def count_alert_new_matches(alert: Dict) -> int:
    """
    Count new listings since the alert's last_checked timestamp (or last 24h if never checked).
    Used to show unread badges in the UI without running the full match query.
    """
    since = alert.get("last_checked")
    matches = get_alert_matches(alert, hours=24, since_datetime=since)
    return len(matches)


def _auto_import_notarial() -> None:
    """Import notarial CSV on first run if the table is empty."""
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM notarial_prices")
            if cursor.fetchone()[0] > 0:
                return  # Already imported
        # Find the CSV (same directory as this file)
        csv_path = Path(__file__).parent / "notarial_madrid_historico.csv"
        if csv_path.exists():
            import_notarial_csv(str(csv_path))
    except Exception as e:
        print(f"Warning: could not auto-import notarial CSV: {e}")


def import_notarial_csv(csv_path: str) -> int:
    """
    Import notarial transaction prices from CSV into the database.
    CSV values are in k€/m² → stored as actual €/m² (× 1000).
    Returns number of rows inserted/updated.
    """
    import csv
    rows_upserted = 0
    with get_connection() as conn:
        cursor = conn.cursor()
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    distrito  = row["distrito"].strip()
                    periodo   = int(row["periodo"])
                    precio_m2 = float(row["precio_m2"]) * 1000  # k€/m² → €/m²
                    cursor.execute("""
                        INSERT INTO notarial_prices (distrito, periodo, precio_m2)
                        VALUES (?, ?, ?)
                        ON CONFLICT(distrito, periodo) DO UPDATE
                        SET precio_m2 = excluded.precio_m2
                    """, (distrito, periodo, round(precio_m2, 1)))
                    rows_upserted += 1
                except (ValueError, KeyError):
                    continue
    print(f"✓ Notarial CSV imported: {rows_upserted} rows")
    return rows_upserted


def get_notarial_prices(distrito: Optional[str] = None) -> List[Dict]:
    """
    Return notarial real transaction prices.
    If distrito is given, returns only that district's yearly series.
    Otherwise returns all rows ordered by distrito, periodo.
    """
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            if distrito:
                cursor.execute("""
                    SELECT distrito, periodo, precio_m2
                    FROM notarial_prices
                    WHERE distrito = ?
                    ORDER BY periodo
                """, (distrito,))
            else:
                cursor.execute("""
                    SELECT distrito, periodo, precio_m2
                    FROM notarial_prices
                    ORDER BY distrito, periodo
                """)
            return [dict(r) for r in cursor.fetchall()]
    except Exception as e:
        print(f"Error in get_notarial_prices: {e}")
        return []


def get_notarial_gap_by_district() -> List[Dict]:
    """
    Compare latest notarial price (most recent year) vs current Idealista
    asking €/m² per district.

    Returns list of dicts with:
        distrito, notarial_year, notarial_price, idealista_price, gap_pct
    gap_pct > 0 means Idealista asks MORE than notarial (overvalued vs reality).
    """
    try:
        with get_connection() as conn:
            cursor = conn.cursor()

            # Latest notarial price per district
            cursor.execute("""
                SELECT n.distrito, n.periodo AS notarial_year, n.precio_m2 AS notarial_price
                FROM notarial_prices n
                INNER JOIN (
                    SELECT distrito, MAX(periodo) AS max_yr
                    FROM notarial_prices
                    GROUP BY distrito
                ) latest ON n.distrito = latest.distrito AND n.periodo = latest.max_yr
            """)
            notarial_rows = {r["distrito"]: dict(r) for r in cursor.fetchall()}

            # Current Idealista median €/m² per district (active listings)
            # Exclude obvious data-quality outliers (size < 20 m² or price/m² > €25k)
            cursor.execute("""
                SELECT distrito,
                       AVG(CAST(price AS FLOAT) / size_sqm) AS idealista_price
                FROM listings
                WHERE status = 'active'
                  AND price > 0
                  AND size_sqm >= 20
                  AND (CAST(price AS FLOAT) / size_sqm) BETWEEN 500 AND 25000
                GROUP BY distrito
                HAVING COUNT(*) >= 5
            """)
            idealista_rows = {r["distrito"]: r["idealista_price"] for r in cursor.fetchall()}

        results = []
        for dist, nd in notarial_rows.items():
            id_price = idealista_rows.get(dist)
            if id_price and nd["notarial_price"] > 0:
                gap_pct = 100 * (id_price - nd["notarial_price"]) / nd["notarial_price"]
                results.append({
                    "distrito":        dist,
                    "notarial_year":   nd["notarial_year"],
                    "notarial_price":  round(nd["notarial_price"]),
                    "idealista_price": round(id_price),
                    "gap_pct":         round(gap_pct, 1),
                })
        return sorted(results, key=lambda x: x["gap_pct"], reverse=True)
    except Exception as e:
        print(f"Error in get_notarial_gap_by_district: {e}")
        return []


# ---------------------------------------------------------------------------
# Market Snapshots — read helpers
# ---------------------------------------------------------------------------

def get_snapshot(
    scope_type: str,
    scope_value: Optional[str],
    metric_name: str,
    date_str: Optional[str] = None,
) -> Optional[float]:
    """
    Return the most recent value of a single metric.

    If *date_str* is given, return exactly that day's value.
    Otherwise return the latest available value.
    """
    try:
        with get_connection() as conn:
            if date_str:
                row = conn.execute(
                    """SELECT metric_value FROM market_snapshots
                       WHERE scope_type = ? AND scope_value IS ?
                         AND metric_name = ? AND date_computed = ?""",
                    (scope_type, scope_value, metric_name, date_str),
                ).fetchone()
            else:
                row = conn.execute(
                    """SELECT metric_value FROM market_snapshots
                       WHERE scope_type = ? AND scope_value IS ?
                         AND metric_name = ?
                       ORDER BY date_computed DESC LIMIT 1""",
                    (scope_type, scope_value, metric_name),
                ).fetchone()
            return row[0] if row else None
    except Exception:
        return None


def get_snapshot_series(
    scope_type: str,
    scope_value: Optional[str],
    metric_name: str,
    days: int = 90,
) -> List[Dict]:
    """
    Return a time-series of a single metric (last *days* days).

    Returns list of {date_computed, metric_value} dicts ordered by date.
    """
    try:
        with get_connection() as conn:
            cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            rows = conn.execute(
                """SELECT date_computed, metric_value
                   FROM market_snapshots
                   WHERE scope_type = ? AND scope_value IS ?
                     AND metric_name = ? AND date_computed >= ?
                   ORDER BY date_computed""",
                (scope_type, scope_value, metric_name, cutoff),
            ).fetchall()
            return [dict(r) for r in rows]
    except Exception:
        return []


def get_latest_snapshots(
    scope_type: str,
    metric_name: str,
) -> List[Dict]:
    """
    Return the latest value of *metric_name* for every scope_value
    of the given *scope_type*.

    Useful for "compare all distritos" views.
    Returns list of {scope_value, metric_value, date_computed} dicts.
    """
    try:
        with get_connection() as conn:
            rows = conn.execute(
                """
                SELECT scope_value, metric_value, date_computed
                FROM market_snapshots s1
                WHERE scope_type = ?
                  AND metric_name = ?
                  AND date_computed = (
                      SELECT MAX(date_computed)
                      FROM market_snapshots s2
                      WHERE s2.scope_type = s1.scope_type
                        AND s2.scope_value IS s1.scope_value
                        AND s2.metric_name = s1.metric_name
                  )
                ORDER BY metric_value DESC
                """,
                (scope_type, metric_name),
            ).fetchall()
            return [dict(r) for r in rows]
    except Exception:
        return []


if __name__ == "__main__":
    # Initialize database when run directly
    init_database()
    stats = get_database_stats()
    print(f"\nDatabase Statistics:")
    print(f"  Active listings: {stats['active_count']}")
    print(f"  Sold/Removed: {stats['sold_count']}")
    print(f"  Average price: €{stats['avg_price']:,.2f}")
    print(f"  Average price/m²: €{stats['avg_price_per_sqm']:,.2f}")
