"""
Geocoding utility to get coordinates for Madrid barrios.
Uses Nominatim API (OpenStreetMap) to geocode barrio names.
"""

import time
import requests
from typing import Optional, Tuple
import json

def geocode_location(query: str) -> Optional[Tuple[float, float]]:
    """
    Geocode a location using Nominatim API.
    
    Args:
        query: Location query string
        
    Returns:
        Tuple of (latitude, longitude) or None if not found
    """
    url = "https://nominatim.openstreetmap.org/search"
    params = {
        'q': query,
        'format': 'json',
        'limit': 1
    }
    headers = {
        'User-Agent': 'MadridRealEstateTracker/1.0'
    }
    
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        if data:
            lat = float(data[0]['lat'])
            lon = float(data[0]['lon'])
            return (lat, lon)
        return None
    except Exception as e:
        print(f"Error geocoding '{query}': {e}")
        return None


def geocode_barrio(distrito: str, barrio: str) -> Optional[Tuple[float, float]]:
    """
    Geocode a Madrid barrio.
    
    Args:
        distrito: District name
        barrio: Barrio (neighborhood) name
        
    Returns:
        Tuple of (latitude, longitude) or None if not found
    """
    # Try different query formats
    queries = [
        f"{barrio}, {distrito}, Madrid, Spain",
        f"{barrio}, Madrid, Spain",
        f"Barrio de {barrio}, Madrid, Spain"
    ]
    
    for query in queries:
        coords = geocode_location(query)
        if coords:
            return coords
        time.sleep(1)  # Rate limiting
    
    return None


def generate_coordinates_file(output_file: str = "barrio_coordinates.json"):
    """
    Generate coordinates file for all barrios from scraper.py.
    """
    # Import barrio list from scraper
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from scraper import BARRIO_URLS
    
    coordinates = {}
    total = len(BARRIO_URLS)
    
    print(f"Geocoding {total} barrios...")
    print("This will take ~3-5 minutes (rate limiting)...")
    print()
    
    for i, (distrito, barrio, _) in enumerate(BARRIO_URLS, 1):
        print(f"[{i}/{total}] {distrito} - {barrio}...", end=" ")
        
        coords = geocode_barrio(distrito, barrio)
        
        if coords:
            coordinates[f"{distrito}|{barrio}"] = {
                "lat": coords[0],
                "lon": coords[1]
            }
            print(f"✓ ({coords[0]:.4f}, {coords[1]:.4f})")
        else:
            # Use Madrid center as fallback
            coordinates[f"{distrito}|{barrio}"] = {
                "lat": 40.4168,
                "lon": -3.7038
            }
            print("✗ (using Madrid center)")
        
        # Rate limiting
        if i < total:
            time.sleep(1.5)
    
    # Save to JSON
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(coordinates, f, indent=2, ensure_ascii=False)
    
    print()
    print(f"✅ Saved {len(coordinates)} coordinates to {output_file}")
    print(f"   Success rate: {sum(1 for v in coordinates.values() if v['lat'] != 40.4168)}/{total}")


if __name__ == "__main__":
    generate_coordinates_file()
