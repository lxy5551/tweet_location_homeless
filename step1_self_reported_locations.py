"""
Step 1: Process self-reported locations
- Extract userID, username, self-reported location from posts
- Geocode locations
- If location is specific (city-level) -> save to self-reported-location.json
- Output: {city}_self-reported-location.json with userID as key
"""

import json
import os
import sys
from geocoder import init_geocoder, geocode_location as _geocode_location, get_cache

# ============== Config ==============
RAW_DATA_DIR = "raw_x_data"
OUTPUT_DIR = "User_Location_Analysis"

# State abbreviation mapping
STATE_ABBREV_MAP = {
    'california': 'CA', 'texas': 'TX', 'new york': 'NY', 'florida': 'FL', 'illinois': 'IL',
    'pennsylvania': 'PA', 'ohio': 'OH', 'michigan': 'MI', 'georgia': 'GA', 'north carolina': 'NC',
    'new jersey': 'NJ', 'virginia': 'VA', 'washington': 'WA', 'arizona': 'AZ', 'massachusetts': 'MA',
    'tennessee': 'TN', 'indiana': 'IN', 'missouri': 'MO', 'maryland': 'MD', 'wisconsin': 'WI',
    'colorado': 'CO', 'minnesota': 'MN', 'south carolina': 'SC', 'alabama': 'AL', 'louisiana': 'LA',
    'kentucky': 'KY', 'oregon': 'OR', 'oklahoma': 'OK', 'connecticut': 'CT', 'utah': 'UT',
    'nevada': 'NV', 'arkansas': 'AR', 'mississippi': 'MS', 'kansas': 'KS', 'new mexico': 'NM',
    'nebraska': 'NE', 'west virginia': 'WV', 'idaho': 'ID', 'hawaii': 'HI', 'new hampshire': 'NH',
    'maine': 'ME', 'montana': 'MT', 'rhode island': 'RI', 'delaware': 'DE', 'south dakota': 'SD',
    'north dakota': 'ND', 'alaska': 'AK', 'vermont': 'VT', 'wyoming': 'WY', 'district of columbia': 'DC'
}

US_STATES = set(STATE_ABBREV_MAP.values())

# Country-level locations (vague)
COUNTRY_LEVEL = {'United States', 'United Kingdom', 'Canada', 'Australia', 'India',
                 'Deutschland', 'Danmark', 'France', 'Germany', 'Spain', 'Italy',
                 'Mexico', 'Brazil', 'Japan', 'China', 'Korea', 'USA', 'US', 'U.S.A.'}

# ============== Utils ==============

def safe_print(text):
    """Safe print for Windows console"""
    try:
        print(text)
    except UnicodeEncodeError:
        print(text.encode('ascii', 'replace').decode('ascii'))
    sys.stdout.flush()

# Geocoder (使用 Google Maps API)
def init_geolocator(city_name):
    init_geocoder()

def geocode_location(location_str, max_retries=2):
    """Geocode a location string (使用 Google Maps API)"""
    return _geocode_location(location_str)

def is_vague_location(parsed_location):
    """Check if location is vague (country-level or invalid)"""
    if not parsed_location or parsed_location == "non-location":
        return True
    if parsed_location in COUNTRY_LEVEL:
        return True
    # State-only is vague
    if parsed_location in US_STATES:
        return True
    for state_name in STATE_ABBREV_MAP.keys():
        if parsed_location.lower() == state_name:
            return True
    return False

# ============== Main ==============

def extract_users_from_posts(city_name):
    """Extract unique users with their info from posts"""
    posts_file = os.path.join(RAW_DATA_DIR, city_name, "posts_english_2015-2025_all_info.json")

    if not os.path.exists(posts_file):
        safe_print(f"  Posts file not found: {posts_file}")
        return {}

    with open(posts_file, 'r', encoding='utf-8') as f:
        posts = json.load(f)

    users = {}  # userID -> {username, location, followers_count, following_count}
    for post in posts:
        author = post.get('author', {})
        user_id = author.get('id')
        if user_id and user_id not in users:
            public_metrics = author.get('public_metrics', {})
            users[user_id] = {
                'username': author.get('username'),
                'location': author.get('location'),
                'followers_count': public_metrics.get('followers_count', 0),
                'following_count': public_metrics.get('following_count', 0)
            }

    return users

def process_self_reported_locations(city_name):
    """Process self-reported locations for a city"""
    safe_print(f"\n{'='*60}")
    safe_print(f"STEP 1: Self-Reported Locations - {city_name.upper()}")
    safe_print(f"{'='*60}")

    # Extract users
    safe_print(f"\n[1] Extracting users from posts...")
    users = extract_users_from_posts(city_name)
    safe_print(f"  Found {len(users)} unique users")

    if not users:
        return

    # Output file path
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_file = os.path.join(OUTPUT_DIR, f"{city_name}_self-reported-location.json")

    # Load existing results if any (for resume)
    results = {}
    if os.path.exists(output_file):
        with open(output_file, 'r', encoding='utf-8') as f:
            results = json.load(f)
        safe_print(f"  Loaded {len(results)} existing results")

    # Initialize geocoder
    init_geolocator(city_name)

    # Process each user
    safe_print(f"\n[2] Geocoding self-reported locations...")
    vague_count = 0
    new_count = 0

    for i, (user_id, info) in enumerate(users.items(), 1):
        # Skip already processed
        if user_id in results:
            continue

        raw_location = info['location']

        # No location -> vague
        if not raw_location or not raw_location.strip():
            vague_count += 1
            continue

        # Geocode
        parsed = geocode_location(raw_location)

        if is_vague_location(parsed):
            vague_count += 1
            continue

        # Specific location found
        results[user_id] = {
            'username': info['username'],
            'parsed_location': parsed,
            'raw_location': raw_location
        }
        new_count += 1

        # Save every 10 new results
        if new_count % 10 == 0:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            safe_print(f"  [{i}/{len(users)}] Saved {len(results)} results...")

    # Final save
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    safe_print(f"\n[Summary]")
    safe_print(f"  Total users: {len(users)}")
    safe_print(f"  Specific location (saved): {len(results)}")
    safe_print(f"  Vague/no location: {vague_count}")
    safe_print(f"  New this run: {new_count}")
    safe_print(f"  Output: {output_file}")


def main():
    import argparse

    all_cities = [
        'baltimore', 'buffalo', 'el paso', 'fayetteville', 'portland',
        'rockford', 'san_francisco', 'scranton', 'southbend'
    ]

    parser = argparse.ArgumentParser(description='Step 1: Process self-reported locations')
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
    safe_print("STEP 1: SELF-REPORTED LOCATIONS")
    safe_print("="*60)
    safe_print(f"Cities: {cities}")

    for city in cities:
        try:
            process_self_reported_locations(city)
        except Exception as e:
            safe_print(f"\nError processing {city}: {e}")
            import traceback
            traceback.print_exc()

    safe_print("\n" + "="*60)
    safe_print("STEP 1 DONE!")
    safe_print("="*60)

if __name__ == "__main__":
    main()
