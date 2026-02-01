"""
Database migration script to add price history tracking.

This script:
1. Creates a backup of the current database
2. Creates the price_history table
3. Populates initial price records for all active listings
4. Verifies the migration was successful

Run this script BEFORE updating the scraper or dashboard code.
"""

import sqlite3
import shutil
from datetime import datetime
from pathlib import Path


DATABASE_PATH = "real_estate.db"
BACKUP_SUFFIX = datetime.now().strftime("%Y%m%d_%H%M%S")


def backup_database():
    """Create a backup of the current database."""
    backup_path = f"real_estate_backup_{BACKUP_SUFFIX}.db"
    
    print("=" * 60)
    print("📦 Creating database backup...")
    print("=" * 60)
    
    try:
        shutil.copy2(DATABASE_PATH, backup_path)
        backup_size = Path(backup_path).stat().st_size / (1024 * 1024)  # MB
        print(f"✅ Backup created: {backup_path} ({backup_size:.1f} MB)")
        return backup_path
    except Exception as e:
        print(f"❌ Error creating backup: {e}")
        raise


def create_price_history_table(conn):
    """Create the price_history table with indexes."""
    print("\n" + "=" * 60)
    print("🏗️  Creating price_history table...")
    print("=" * 60)
    
    cursor = conn.cursor()
    
    # Create table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS price_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_id TEXT NOT NULL,
            price INTEGER NOT NULL,
            date_recorded TEXT NOT NULL,
            change_amount INTEGER,
            change_percent REAL,
            FOREIGN KEY (listing_id) REFERENCES listings(listing_id)
        )
    """)
    
    # Create indexes
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_listing_price 
        ON price_history(listing_id, date_recorded)
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_date_recorded 
        ON price_history(date_recorded)
    """)
    
    conn.commit()
    print("✅ Table and indexes created successfully")


def populate_initial_records(conn):
    """Populate initial price records for all active listings."""
    print("\n" + "=" * 60)
    print("📝 Populating initial price records...")
    print("=" * 60)
    
    cursor = conn.cursor()
    
    # Get count of active listings
    cursor.execute("SELECT COUNT(*) FROM listings WHERE status = 'active'")
    total_active = cursor.fetchone()[0]
    print(f"Found {total_active:,} active listings")
    
    # Insert initial price records
    # Use first_seen_date as the initial record date
    cursor.execute("""
        INSERT INTO price_history (listing_id, price, date_recorded, change_amount, change_percent)
        SELECT 
            listing_id,
            price,
            first_seen_date,
            NULL,
            NULL
        FROM listings
        WHERE status = 'active'
        AND price IS NOT NULL
        AND price > 0
    """)
    
    inserted_count = cursor.rowcount
    conn.commit()
    
    print(f"✅ Inserted {inserted_count:,} initial price records")
    
    if inserted_count != total_active:
        skipped = total_active - inserted_count
        print(f"⚠️  Skipped {skipped} listings (NULL or invalid price)")


def verify_migration(conn):
    """Verify the migration was successful."""
    print("\n" + "=" * 60)
    print("🔍 Verifying migration...")
    print("=" * 60)
    
    cursor = conn.cursor()
    
    # Check table exists
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='price_history'
    """)
    if not cursor.fetchone():
        print("❌ ERROR: price_history table not found!")
        return False
    print("✅ Table exists")
    
    # Check indexes exist
    cursor.execute("""
        SELECT COUNT(*) FROM sqlite_master 
        WHERE type='index' AND tbl_name='price_history'
    """)
    index_count = cursor.fetchone()[0]
    print(f"✅ {index_count} indexes created")
    
    # Check record count
    cursor.execute("SELECT COUNT(*) FROM price_history")
    total_records = cursor.fetchone()[0]
    print(f"✅ {total_records:,} price records in history")
    
    # Check for orphaned records (should be 0)
    cursor.execute("""
        SELECT COUNT(*) FROM listings l
        WHERE l.status = 'active'
        AND NOT EXISTS (
            SELECT 1 FROM price_history ph 
            WHERE ph.listing_id = l.listing_id
        )
        AND l.price IS NOT NULL
        AND l.price > 0
    """)
    orphaned = cursor.fetchone()[0]
    
    if orphaned > 0:
        print(f"⚠️  WARNING: {orphaned} active listings without price history")
    else:
        print("✅ All active listings have price history")
    
    # Show sample data
    print("\n📊 Sample price history records:")
    cursor.execute("""
        SELECT 
            ph.listing_id,
            l.title,
            ph.price,
            ph.date_recorded
        FROM price_history ph
        JOIN listings l ON ph.listing_id = l.listing_id
        ORDER BY ph.date_recorded DESC
        LIMIT 5
    """)
    
    print("\n{:<15} {:<50} {:>12} {:>12}".format(
        "Listing ID", "Title", "Price (€)", "Date"
    ))
    print("-" * 95)
    
    for row in cursor.fetchall():
        listing_id, title, price, date = row
        title_short = title[:47] + "..." if len(title) > 50 else title
        print("{:<15} {:<50} {:>12,} {:>12}".format(
            listing_id, title_short, price, date
        ))
    
    return True


def main():
    """Run the migration."""
    print("\n" + "=" * 60)
    print("🚀 PRICE HISTORY MIGRATION")
    print("=" * 60)
    print(f"Database: {DATABASE_PATH}")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # Check database exists
    if not Path(DATABASE_PATH).exists():
        print(f"\n❌ ERROR: Database not found: {DATABASE_PATH}")
        print("Please run the scraper first to create the database.")
        return False
    
    try:
        # Step 1: Backup
        backup_path = backup_database()
        
        # Step 2: Connect to database
        conn = sqlite3.connect(DATABASE_PATH)
        
        # Step 3: Create table
        create_price_history_table(conn)
        
        # Step 4: Populate initial records
        populate_initial_records(conn)
        
        # Step 5: Verify
        success = verify_migration(conn)
        
        # Close connection
        conn.close()
        
        # Final summary
        print("\n" + "=" * 60)
        if success:
            print("✅ MIGRATION COMPLETED SUCCESSFULLY")
            print("=" * 60)
            print(f"Backup saved: {backup_path}")
            print("\nNext steps:")
            print("1. Update scraper.py to detect price changes")
            print("2. Update database.py with new functions")
            print("3. Test with: python scraper.py --test-mode")
        else:
            print("⚠️  MIGRATION COMPLETED WITH WARNINGS")
            print("=" * 60)
            print("Please review the warnings above.")
            print(f"Backup available: {backup_path}")
        
        print("\nTo rollback:")
        print(f"  cp {backup_path} {DATABASE_PATH}")
        print("=" * 60)
        
        return success
        
    except Exception as e:
        print(f"\n❌ MIGRATION FAILED: {e}")
        print("\nTo rollback:")
        print(f"  cp {backup_path} {DATABASE_PATH}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
