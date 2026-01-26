#!/usr/bin/env python3
"""
Script to inspect Idealista HTML and find the correct CSS selector for descriptions.
"""

import os
import sys
from dotenv import load_dotenv
from bs4 import BeautifulSoup

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scraper import fetch_page, get_proxy_config, BASE_URL

def inspect_html():
    """Fetch and inspect Idealista HTML structure."""
    
    # Load environment variables
    load_dotenv()
    
    # Get proxy config
    proxies = get_proxy_config()
    
    # Test URL
    url = BASE_URL + "/venta-viviendas/madrid/barrio-de-salamanca/goya/"
    
    print(f"🔍 Fetching HTML from: {url}")
    print("-" * 60)
    
    # Fetch page
    html = fetch_page(url, proxies)
    
    if not html:
        print("❌ Failed to fetch page")
        return
    
    print(f"✅ Fetched {len(html)} bytes of HTML\n")
    
    # Parse with BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')
    
    # Find all listing articles
    articles = soup.find_all('article', class_='item')
    
    print(f"📦 Found {len(articles)} listing articles\n")
    
    if not articles:
        print("⚠️  No articles found. Trying alternative selectors...")
        articles = soup.find_all('article')
        print(f"   Found {len(articles)} articles without class filter\n")
    
    # Inspect first few articles
    for i, article in enumerate(articles[:3], 1):
        print(f"\n{'='*60}")
        print(f"ARTICLE {i}")
        print(f"{'='*60}")
        
        # Get listing ID
        listing_id = article.get('data-element-id', 'N/A')
        print(f"Listing ID: {listing_id}")
        
        # Find all divs and their classes
        print(f"\n📋 All elements with text (potential description containers):")
        
        # Check all common description containers
        potential_desc_elements = []
        
        # Try various selectors
        selectors_to_try = [
            ('div', 'item-description'),
            ('p', 'item-description'),
            ('div', 'description'),
            ('span', 'item-description'),
            ('div', 'item-detail-char'),
            ('div', 'comment'),
            ('p', 'comment'),
            ('div', 'item-multimedia-container'),
        ]
        
        for tag, class_name in selectors_to_try:
            elem = article.find(tag, class_=class_name)
            if elem:
                text = elem.get_text(strip=True)
                if text and len(text) > 20:
                    potential_desc_elements.append((f"{tag}.{class_name}", text[:100]))
        
        # Also check all divs/p/spans with substantial text
        for tag in ['div', 'p', 'span']:
            for elem in article.find_all(tag):
                classes = elem.get('class', [])
                text = elem.get_text(strip=True)
                if text and len(text) > 50 and len(text) < 500:
                    # Skip if already found
                    class_str = '.'.join(classes) if classes else 'no-class'
                    selector = f"{tag}.{class_str}"
                    if not any(selector in existing[0] for existing in potential_desc_elements):
                        potential_desc_elements.append((selector, text[:100]))
        
        if potential_desc_elements:
            for selector, text in potential_desc_elements:
                print(f"\n   ✓ {selector}")
                print(f"     Text: {text}...")
        else:
            print("   ⚠️  No description-like elements found")
            
            # Print article structure for debugging
            print(f"\n   📝 Article structure:")
            for child in article.children:
                if hasattr(child, 'name') and child.name:
                    classes = child.get('class', [])
                    class_str = '.'.join(classes) if classes else ''
                    print(f"      - {child.name}{('.' + class_str) if class_str else ''}")

if __name__ == "__main__":
    inspect_html()
