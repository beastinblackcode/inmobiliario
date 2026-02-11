"""
Script to validate all barrio URLs in the scraper.
Checks which URLs return 404 errors and should be removed.
"""

import requests
from scraper import BARRIO_URLS, BASE_URL, get_proxy_config
import time

def validate_url(url, proxies=None):
    """Check if a URL is valid (returns 200)."""
    try:
        response = requests.get(
            url,
            proxies=proxies,
            headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
            },
            timeout=30,
            verify=False
        )
        return response.status_code
    except Exception as e:
        return f"Error: {e}"

def main():
    print("=" * 80)
    print("VALIDATING BARRIO URLs")
    print("=" * 80)
    print(f"Total barrios to check: {len(BARRIO_URLS)}\n")
    
    # Get proxy config
    proxies = get_proxy_config()
    if not proxies:
        print("⚠️  No proxy configured. Some requests may fail.")
        response = input("Continue anyway? (y/n): ")
        if response.lower() != 'y':
            return
    
    invalid_urls = []
    valid_count = 0
    error_count = 0
    
    for i, (distrito, barrio, url_path) in enumerate(BARRIO_URLS, 1):
        full_url = BASE_URL + url_path
        print(f"[{i}/{len(BARRIO_URLS)}] Checking {distrito} - {barrio}...", end=' ')
        
        status = validate_url(full_url, proxies)
        
        if status == 200:
            print(f"✅ OK")
            valid_count += 1
        elif status == 404:
            print(f"❌ 404 NOT FOUND")
            invalid_urls.append((distrito, barrio, url_path, status))
            error_count += 1
        else:
            print(f"⚠️  Status: {status}")
            invalid_urls.append((distrito, barrio, url_path, status))
            error_count += 1
        
        # Rate limiting
        time.sleep(0.5)
    
    # Summary
    print("\n" + "=" * 80)
    print("VALIDATION SUMMARY")
    print("=" * 80)
    print(f"Total checked: {len(BARRIO_URLS)}")
    print(f"✅ Valid: {valid_count}")
    print(f"❌ Invalid: {error_count}")
    print("=" * 80)
    
    if invalid_urls:
        print("\n❌ INVALID URLs (should be removed):")
        print("=" * 80)
        for distrito, barrio, url_path, status in invalid_urls:
            print(f"Status {status}: {distrito} - {barrio}")
            print(f"  URL: {BASE_URL}{url_path}")
            print(f"  Entry: (\"{distrito}\", \"{barrio}\", \"{url_path}\"),")
            print()
        
        # Generate code to remove
        print("\n" + "=" * 80)
        print("BARRIOS TO REMOVE FROM scraper.py:")
        print("=" * 80)
        for distrito, barrio, url_path, status in invalid_urls:
            print(f'    (\"{distrito}\", \"{barrio}\", \"{url_path}\"),  # ❌ {status}')
    else:
        print("\n✅ All URLs are valid!")

if __name__ == "__main__":
    main()
