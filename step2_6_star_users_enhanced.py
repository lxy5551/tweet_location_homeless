"""
Step 2.6: Enhanced location detection for star users
Uses additional signals from posts:
1. AI-detected places from tweet text (text_annotated_places)
2. Network locations (mentioned_users_locations, replied_to_user_location)
3. Geo-tagged places (rare but accurate)

Input: star-users-geocoded.json (users without location)
Output: star-users-enhanced.json
"""

import json
import os
import sys
from collections import Counter, defaultdict

# ============== Config ==============
RAW_DATA_DIR = "raw_x_data"
INPUT_DIR = "User_Location_Analysis"
OUTPUT_DIR = "User_Location_Analysis"

# ============== Utils ==============

def safe_print(text):
    try:
        print(text)
    except UnicodeEncodeError:
        print(text.encode('ascii', 'replace').decode('ascii'))
    sys.stdout.flush()

def load_json(filepath):
    if os.path.exists(filepath):
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

# ============== Main ==============

def enhance_star_users(city_name):
    safe_print(f"\n{'='*60}")
    safe_print(f"ENHANCE STAR USERS - {city_name.upper()}")
    safe_print(f"{'='*60}")

    # Load star users
    star_file = os.path.join(INPUT_DIR, f"{city_name}_star-users-geocoded.json")
    star_users = load_json(star_file)
    if not star_users:
        safe_print(f"  No star users file found: {star_file}")
        return

    # Find users without location
    no_loc_users = {}
    has_loc_users = {}
    for uid, info in star_users.items():
        geo = info.get('geocoded_location')
        if not geo or geo == 'non-location':
            no_loc_users[info['username']] = uid
        else:
            has_loc_users[uid] = info

    safe_print(f"  Total star users: {len(star_users)}")
    safe_print(f"  Already have location: {len(has_loc_users)}")
    safe_print(f"  Need enhancement: {len(no_loc_users)}")

    if not no_loc_users:
        safe_print("  All star users already have locations!")
        return

    # Load posts
    posts_file = os.path.join(RAW_DATA_DIR, city_name, "posts_english_2015-2025_all_info.json")
    if not os.path.exists(posts_file):
        safe_print(f"  Posts file not found: {posts_file}")
        return

    with open(posts_file, 'r', encoding='utf-8') as f:
        posts = json.load(f)

    safe_print(f"  Loaded {len(posts)} posts")

    # Collect location signals for users without location
    user_geo_places = defaultdict(list)      # geo-tagged (most reliable)
    user_text_places = defaultdict(list)     # AI-detected from text
    user_network_locs = defaultdict(list)    # from interactions

    for post in posts:
        author = post.get('author', {}).get('username')
        if author not in no_loc_users:
            continue

        # 1. Geo-tagged place (most reliable)
        place = post.get('place')
        if place and place.get('full_name'):
            user_geo_places[author].append(place['full_name'])

        # 2. AI-detected places from text
        loc_signals = post.get('location_signals', {})
        for p in loc_signals.get('text_annotated_places', []):
            if p.get('probability', 0) > 0.9:
                user_text_places[author].append(p['text'])

        # 3. Network locations
        network = post.get('network_locations', {})
        for m in network.get('mentioned_users_locations', []):
            if m.get('location'):
                user_network_locs[author].append(m['location'])
        if network.get('replied_to_user_location'):
            user_network_locs[author].append(network['replied_to_user_location'])

    safe_print(f"\n[Signal Coverage]")
    safe_print(f"  Users with geo-tagged places: {len(user_geo_places)}")
    safe_print(f"  Users with AI-detected places: {len(user_text_places)}")
    safe_print(f"  Users with network locations: {len(user_network_locs)}")

    # Determine location for each user
    enhanced_results = dict(has_loc_users)  # Start with users who have location
    stats = {'geo': 0, 'text': 0, 'network': 0, 'none': 0}

    for username, uid in no_loc_users.items():
        original_info = star_users[uid]
        location = None
        source = None
        confidence = None

        # Priority 1: Geo-tagged (most reliable)
        if username in user_geo_places:
            places = Counter(user_geo_places[username])
            top_place, count = places.most_common(1)[0]
            location = top_place
            source = 'geo-tagged'
            confidence = 'high'
            stats['geo'] += 1

        # Priority 2: Network locations (interactions with Portland locals)
        elif username in user_network_locs:
            locs = Counter(user_network_locs[username])
            top_loc, count = locs.most_common(1)[0]
            # Only use if appears multiple times
            if count >= 2:
                location = top_loc
                source = 'network'
                confidence = 'medium'
                stats['network'] += 1
            elif username in user_text_places:
                # Fall back to text
                places = Counter(user_text_places[username])
                top_place, count = places.most_common(1)[0]
                if count >= 2:
                    location = top_place
                    source = 'text-mention'
                    confidence = 'low'
                    stats['text'] += 1
                else:
                    stats['none'] += 1
            else:
                stats['none'] += 1

        # Priority 3: AI-detected text mentions
        elif username in user_text_places:
            places = Counter(user_text_places[username])
            top_place, count = places.most_common(1)[0]
            if count >= 2:  # Must mention at least twice
                location = top_place
                source = 'text-mention'
                confidence = 'low'
                stats['text'] += 1
            else:
                stats['none'] += 1
        else:
            stats['none'] += 1

        enhanced_results[uid] = {
            'username': username,
            'raw_location': original_info.get('raw_location'),
            'followers_count': original_info.get('followers_count'),
            'following_count': original_info.get('following_count'),
            'geocoded_location': original_info.get('geocoded_location'),
            'enhanced_location': location,
            'enhanced_source': source,
            'enhanced_confidence': confidence
        }

    # Save results
    output_file = os.path.join(OUTPUT_DIR, f"{city_name}_star-users-enhanced.json")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(enhanced_results, f, ensure_ascii=False, indent=2)

    # Summary
    safe_print(f"\n[Enhancement Results]")
    safe_print(f"  From geo-tagged: {stats['geo']}")
    safe_print(f"  From network: {stats['network']}")
    safe_print(f"  From text mentions: {stats['text']}")
    safe_print(f"  Still no location: {stats['none']}")
    safe_print(f"  Output: {output_file}")

    # Show examples
    safe_print(f"\n[Examples of enhanced locations]")
    count = 0
    for uid, info in enhanced_results.items():
        if info.get('enhanced_location') and count < 10:
            safe_print(f"  @{info['username']}: {info['enhanced_location']} ({info['enhanced_source']})")
            count += 1

def main():
    import argparse

    all_cities = [
        'baltimore', 'buffalo', 'el paso', 'fayetteville', 'portland',
        'rockford', 'san_francisco', 'scranton', 'southbend'
    ]

    parser = argparse.ArgumentParser(description='Step 2.6: Enhance star users location')
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
    safe_print("STEP 2.6: ENHANCE STAR USERS")
    safe_print("="*60)
    safe_print(f"Cities: {cities}")

    for city in cities:
        try:
            enhance_star_users(city)
        except Exception as e:
            safe_print(f"\nError processing {city}: {e}")
            import traceback
            traceback.print_exc()

    safe_print("\n" + "="*60)
    safe_print("STEP 2.6 DONE!")
    safe_print("="*60)

if __name__ == "__main__":
    main()
