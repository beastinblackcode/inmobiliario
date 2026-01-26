#!/usr/bin/env python3
"""
Test script to verify description extraction from Idealista listings.
This script scrapes a single barrio to test the description extraction functionality.
"""

import os
import sys
from dotenv import load_dotenv

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scraper import scrape_barrio, get_proxy_config, BARRIO_URLS
from database import init_database, get_connection

def test_description_extraction():
    """Test description extraction on a single barrio."""
    
    # Load environment variables
    load_dotenv()
    
    # Initialize database
    print("📦 Initializing database...")
    init_database()
    
    # Get proxy config
    proxies = get_proxy_config()
    if not proxies:
        print("⚠️  No Bright Data credentials found. Running without proxy.")
    
    # Test on a single barrio (Salamanca - Goya)
    test_barrio = ("Salamanca", "Goya", "/venta-viviendas/madrid/barrio-de-salamanca/goya/")
    distrito, barrio, url_path = test_barrio
    
    print(f"\n🔍 Testing description extraction on: {distrito} - {barrio}")
    print(f"   URL: {url_path}")
    print("-" * 60)
    
    # Track seen IDs
    seen_ids = set()
    
    # Scrape the barrio
    count = scrape_barrio(distrito, barrio, url_path, proxies, seen_ids)
    
    print(f"\n✅ Scraped {count} listings")
    
    # Query database to check descriptions
    print("\n📊 Checking descriptions in database...")
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # Get total listings
        cursor.execute("SELECT COUNT(*) FROM listings WHERE barrio = ?", (barrio,))
        total = cursor.fetchone()[0]
        
        # Get listings with descriptions
        cursor.execute("""
            SELECT COUNT(*) FROM listings 
            WHERE barrio = ? AND description IS NOT NULL AND description != ''
        """, (barrio,))
        with_desc = cursor.fetchone()[0]
        
        # Get sample descriptions
        cursor.execute("""
            SELECT listing_id, title, description 
            FROM listings 
            WHERE barrio = ? AND description IS NOT NULL AND description != ''
            LIMIT 5
        """, (barrio,))
        samples = cursor.fetchall()
        
        print(f"\n📈 Results:")
        print(f"   Total listings: {total}")
        print(f"   With descriptions: {with_desc} ({with_desc/total*100:.1f}%)")
        print(f"   Without descriptions: {total - with_desc} ({(total-with_desc)/total*100:.1f}%)")
        
        if samples:
            print(f"\n📝 Sample descriptions:")
            for i, row in enumerate(samples, 1):
                listing_id, title, desc = row
                print(f"\n   {i}. {title[:50]}...")
                print(f"      ID: {listing_id}")
                print(f"      Description: {desc[:150]}{'...' if len(desc) > 150 else ''}")
        else:
            print("\n⚠️  No descriptions found! Check CSS selectors.")

if __name__ == "__main__":
    test_description_extraction()
