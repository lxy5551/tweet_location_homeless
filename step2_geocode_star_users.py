"""
Step 2.5: Geocode star users' raw_location
- Read star-users.json
- Geocode raw_location using Google Maps API
- Output: {city}_star-users-geocoded.json
"""

import json
import os
import sys
from geocoder import init_geocoder, geocode_location, get_cache, set_cache

# ============== Config ==============
OUTPUT_DIR = "User_Location_Analysis"

# ============== Utils ==============

def safe_print(text):
    """Safe print for Windows console"""
    try:
        print(text)
    except UnicodeEncodeError:
        print(text.encode('ascii', 'replace').decode('ascii'))
    sys.stdout.flush()

def load_geocode_cache():
    """Load existing geocode cache"""
    import glob
    cache = {}

    cache_files = glob.glob("geocode_cache_*.json")
    if os.path.exists("geocode_cache.json"):
        cache_files.append("geocode_cache.json")

    for cache_file in cache_files:
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                cache.update(data)
        except Exception:
            pass

    return cache

# ============== Main ==============

def geocode_star_users(city_name):
    """Geocode star users for a city"""
    safe_print(f"\n{'='*60}")
    safe_print(f"GEOCODE STAR USERS - {city_name.upper()}")
    safe_print(f"{'='*60}")

    # Load star users
    star_file = os.path.join(OUTPUT_DIR, f"{city_name}_star-users.json")
    if not os.path.exists(star_file):
        safe_print(f"  Star users file not found: {star_file}")
        return

    with open(star_file, 'r', encoding='utf-8') as f:
        star_users = json.load(f)

    safe_print(f"  Found {len(star_users)} star users")

    # Load cache and init geocoder
    cache = load_geocode_cache()
    set_cache(cache)
    init_geocoder()

    # Geocode each user's raw_location
    results = {}
    geocoded_count = 0
    cached_count = 0
    null_count = 0

    for user_id, info in star_users.items():
        raw_location = info.get('raw_location')
        username = info.get('username')

        if not raw_location or not raw_location.strip():
            # No location
            results[user_id] = {
                **info,
                'geocoded_location': None,
                'status': 'no_location'
            }
            null_count += 1
            safe_print(f"  @{username}: (no location)")
            continue

        raw_location = raw_location.strip()

        # Check cache first
        current_cache = get_cache()
        if raw_location in current_cache:
            geocoded = current_cache[raw_location]
            cached_count += 1
        else:
            geocoded = geocode_location(raw_location)
            geocoded_count += 1

        results[user_id] = {
            **info,
            'geocoded_location': geocoded,
            'status': 'geocoded'
        }

        safe_print(f"  @{username}: '{raw_location}' -> {geocoded}")

    # Save results
    output_file = os.path.join(OUTPUT_DIR, f"{city_name}_star-users-geocoded.json")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    # Summary
    safe_print(f"\n[Summary]")
    safe_print(f"  Total star users: {len(star_users)}")
    safe_print(f"  No location: {null_count}")
    safe_print(f"  From cache: {cached_count}")
    safe_print(f"  New geocoded: {geocoded_count}")
    safe_print(f"  Output: {output_file}")

    # Show location distribution
    from collections import Counter
    locations = [r['geocoded_location'] for r in results.values() if r['geocoded_location'] and r['geocoded_location'] != 'non-location']
    if locations:
        safe_print(f"\n[Location Distribution]")
        for loc, count in Counter(locations).most_common(10):
            safe_print(f"  {loc}: {count}")

def main():
    import argparse

    all_cities = [
        'baltimore', 'buffalo', 'el paso', 'fayetteville', 'portland',
        'rockford', 'san_francisco', 'scranton', 'southbend'
    ]

    parser = argparse.ArgumentParser(description='Step 2.5: Geocode star users')
    parser.add_argument('--cities', type=str, help='Comma-separated city names')
    parser.add_argument('--range', type=str, help='City range, e.g., "1-5"')
    args = parser.parse_args()

    if args.cities:
        cities = [c.strip() for c in args.cities.split(',')]
    elif args.range:
        start, end = map(int, args.range.split('-'))
        cities = all_cities[start-1:end]
    else:
        cities = all_cities

    safe_print("="*60)
    safe_print("STEP 2.5: GEOCODE STAR USERS")
    safe_print("="*60)
    safe_print(f"Cities: {cities}")

    for city in cities:
        try:
            geocode_star_users(city)
        except Exception as e:
            safe_print(f"\nError processing {city}: {e}")
            import traceback
            traceback.print_exc()

    safe_print("\n" + "="*60)
    safe_print("STEP 2.5 DONE!")
    safe_print("="*60)

if __name__ == "__main__":
    main()
