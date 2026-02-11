"""
Test script for the new 7-day sold detection logic.
Tests that mark_stale_as_sold() only marks properties as sold after 7 days.
"""

import sqlite3
from datetime import datetime, timedelta
import sys
import os

# Add parent directory to path to import database module
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import mark_stale_as_sold, init_database, get_connection

def cleanup_test_data():
    """Remove test data from database."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM listings WHERE listing_id LIKE 'TEST%'")
        cursor.execute("DELETE FROM price_history WHERE listing_id LIKE 'TEST%'")
        print("✓ Cleaned up test data")

def insert_test_data():
    """Insert test properties with different last_seen_date values."""
    with get_connection() as conn:
        cursor = conn.cursor()
        today = datetime.now().strftime('%Y-%m-%d')
        
        # Test property 1: Seen today (should NOT be marked as sold)
        cursor.execute("""
            INSERT INTO listings 
            (listing_id, title, url, price, distrito, barrio, first_seen_date, last_seen_date, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, ('TEST001', 'Test Property 1 - Recent', 'http://test.com/1', 300000, 
              'Centro', 'Sol', today, today, 'active'))
        
        # Test property 2: Seen 5 days ago (should NOT be marked as sold - within 7 days)
        date_5_days_ago = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
        cursor.execute("""
            INSERT INTO listings 
            (listing_id, title, url, price, distrito, barrio, first_seen_date, last_seen_date, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, ('TEST002', 'Test Property 2 - 5 days old', 'http://test.com/2', 400000, 
              'Salamanca', 'Goya', date_5_days_ago, date_5_days_ago, 'active'))
        
        # Test property 3: Seen exactly 7 days ago (should NOT be marked - boundary case)
        date_7_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        cursor.execute("""
            INSERT INTO listings 
            (listing_id, title, url, price, distrito, barrio, first_seen_date, last_seen_date, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, ('TEST003', 'Test Property 3 - Exactly 7 days', 'http://test.com/3', 500000, 
              'Chamberí', 'Almagro', date_7_days_ago, date_7_days_ago, 'active'))
        
        # Test property 4: Seen 8 days ago (SHOULD be marked as sold)
        date_8_days_ago = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')
        cursor.execute("""
            INSERT INTO listings 
            (listing_id, title, url, price, distrito, barrio, first_seen_date, last_seen_date, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, ('TEST004', 'Test Property 4 - 8 days old', 'http://test.com/4', 600000, 
              'Retiro', 'Ibiza', date_8_days_ago, date_8_days_ago, 'active'))
        
        # Test property 5: Seen 30 days ago (SHOULD be marked as sold)
        date_30_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        cursor.execute("""
            INSERT INTO listings 
            (listing_id, title, url, price, distrito, barrio, first_seen_date, last_seen_date, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, ('TEST005', 'Test Property 5 - 30 days old', 'http://test.com/5', 700000, 
              'Chamartín', 'El Viso', date_30_days_ago, date_30_days_ago, 'active'))
        
        # Test property 6: Already marked as sold (should remain sold)
        cursor.execute("""
            INSERT INTO listings 
            (listing_id, title, url, price, distrito, barrio, first_seen_date, last_seen_date, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, ('TEST006', 'Test Property 6 - Already sold', 'http://test.com/6', 800000, 
              'Moncloa-Aravaca', 'Aravaca', date_30_days_ago, date_30_days_ago, 'sold_removed'))
        
        print("✓ Inserted 6 test properties")

def verify_results():
    """Verify that the correct properties were marked as sold."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT listing_id, title, last_seen_date, status 
            FROM listings 
            WHERE listing_id LIKE 'TEST%'
            ORDER BY listing_id
        """)
        
        results = cursor.fetchall()
        
        print("\n" + "=" * 80)
        print("TEST RESULTS")
        print("=" * 80)
        
        all_passed = True
        
        for listing_id, title, last_seen, status in results:
            # Calculate days since last seen
            last_seen_date = datetime.strptime(last_seen, '%Y-%m-%d')
            days_ago = (datetime.now() - last_seen_date).days
            
            # Determine expected status
            if listing_id == 'TEST006':
                expected = 'sold_removed'  # Already sold
            elif days_ago > 7:
                expected = 'sold_removed'  # Should be marked as sold
            else:
                expected = 'active'  # Should still be active
            
            # Check if actual matches expected
            passed = (status == expected)
            all_passed = all_passed and passed
            
            status_icon = "✅" if passed else "❌"
            print(f"{status_icon} {listing_id}: {title[:30]:30} | "
                  f"Last seen: {days_ago:2} days ago | "
                  f"Status: {status:13} | Expected: {expected}")
        
        print("=" * 80)
        
        return all_passed

def run_tests():
    """Run the complete test suite."""
    print("=" * 80)
    print("TESTING: 7-Day Sold Detection Logic")
    print("=" * 80)
    print()
    
    # Initialize database
    print("1. Initializing database...")
    init_database()
    print("✓ Database initialized")
    print()
    
    # Cleanup any existing test data
    print("2. Cleaning up any existing test data...")
    cleanup_test_data()
    print()
    
    # Insert test data
    print("3. Inserting test data...")
    insert_test_data()
    print()
    
    # Run the function
    print("4. Running mark_stale_as_sold(days_threshold=7)...")
    marked_count = mark_stale_as_sold(days_threshold=7)
    print(f"✓ Marked {marked_count} properties as sold")
    print()
    
    # Verify results
    print("5. Verifying results...")
    all_passed = verify_results()
    print()
    
    # Cleanup
    print("6. Cleaning up test data...")
    cleanup_test_data()
    print()
    
    # Final result
    print("=" * 80)
    if all_passed:
        print("✅ ALL TESTS PASSED!")
        print("The 7-day sold detection logic is working correctly.")
    else:
        print("❌ SOME TESTS FAILED!")
        print("Please review the results above.")
    print("=" * 80)
    
    return all_passed

if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
