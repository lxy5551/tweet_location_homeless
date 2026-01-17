"""
Step 1.5: Extract location from posts data
For users without self-reported location, try to find location from:
1. Geo-tagged places (tweet place field) - most reliable
2. Network locations (who they interact with)
3. AI-detected places from tweet text

Input: posts_english_2015-2025_all_info.json
       {city}_self-reported-location.json (to skip already located users)
Output: {city}_posts-location.json (users with location found from posts)
        {city}_no-location-users.json (users still without location, for step 2)
"""

import json
import os
import sys
from collections import Counter, defaultdict

# ============== Config ==============
RAW_DATA_DIR = "raw_x_data"
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

def extract_posts_location(city_name):
    safe_print(f"\n{'='*60}")
    safe_print(f"STEP 1.5: POSTS LOCATION - {city_name.upper()}")
    safe_print(f"{'='*60}")

    # Load self-reported locations (already processed)
    self_reported = load_json(os.path.join(OUTPUT_DIR, f"{city_name}_self-reported-location.json"))
    self_reported_ids = set(self_reported.keys())
    safe_print(f"  Already have self-reported location: {len(self_reported_ids)} users")

    # Load posts
    posts_file = os.path.join(RAW_DATA_DIR, city_name, "posts_english_2015-2025_all_info.json")
    if not os.path.exists(posts_file):
        safe_print(f"  Posts file not found: {posts_file}")
        return

    with open(posts_file, 'r', encoding='utf-8') as f:
        posts = json.load(f)
    safe_print(f"  Loaded {len(posts)} posts")

    # Extract all users from posts (excluding self-reported)
    all_users = {}  # user_id -> {username, raw_location, followers, following}
    for post in posts:
        author = post.get('author', {})
        user_id = author.get('id')
        if user_id and user_id not in self_reported_ids and user_id not in all_users:
            public_metrics = author.get('public_metrics', {})
            all_users[user_id] = {
                'username': author.get('username'),
                'raw_location': author.get('location'),
                'followers_count': public_metrics.get('followers_count', 0),
                'following_count': public_metrics.get('following_count', 0)
            }

    safe_print(f"  Users without self-reported location: {len(all_users)}")

    # Build username -> user_id mapping
    username_to_id = {info['username']: uid for uid, info in all_users.items()}

    # Collect location signals from posts
    user_geo_places = defaultdict(list)      # geo-tagged (most reliable)
    user_network_locs = defaultdict(list)    # from interactions
    user_text_places = defaultdict(list)     # AI-detected from text

    for post in posts:
        author = post.get('author', {})
        user_id = author.get('id')

        if user_id not in all_users:
            continue

        username = author.get('username')

        # 1. Geo-tagged place (most reliable)
        place = post.get('place')
        if place and place.get('full_name'):
            user_geo_places[username].append(place['full_name'])

        # 2. Network locations
        network = post.get('network_locations', {})
        for m in network.get('mentioned_users_locations', []):
            if m.get('location'):
                user_network_locs[username].append(m['location'])
        if network.get('replied_to_user_location'):
            user_network_locs[username].append(network['replied_to_user_location'])

        # 3. AI-detected places from text
        loc_signals = post.get('location_signals', {})
        for p in loc_signals.get('text_annotated_places', []):
            if p.get('probability', 0) > 0.9:
                user_text_places[username].append(p['text'])

    safe_print(f"\n[Signal Coverage]")
    safe_print(f"  Users with geo-tagged places: {len(user_geo_places)}")
    safe_print(f"  Users with network locations: {len(user_network_locs)}")
    safe_print(f"  Users with AI-detected places: {len(user_text_places)}")

    # Determine location for each user
    posts_location = {}  # Users with location found from posts
    no_location = {}     # Users still without location

    stats = {'geo': 0, 'network': 0, 'text': 0, 'none': 0}

    for user_id, info in all_users.items():
        username = info['username']
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

        # Priority 2: Network locations (need multiple mentions)
        elif username in user_network_locs:
            locs = Counter(user_network_locs[username])
            top_loc, count = locs.most_common(1)[0]
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
            if count >= 2:
                location = top_place
                source = 'text-mention'
                confidence = 'low'
                stats['text'] += 1
            else:
                stats['none'] += 1
        else:
            stats['none'] += 1

        if location:
            posts_location[user_id] = {
                'username': username,
                'location': location,
                'raw_location': info['raw_location'],
                'source': source,
                'confidence': confidence,
                'followers_count': info['followers_count'],
                'following_count': info['following_count']
            }
        else:
            no_location[user_id] = {
                'username': username,
                'raw_location': info['raw_location'],
                'followers_count': info['followers_count'],
                'following_count': info['following_count']
            }

    # Save results
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    posts_loc_file = os.path.join(OUTPUT_DIR, f"{city_name}_posts-location.json")
    with open(posts_loc_file, 'w', encoding='utf-8') as f:
        json.dump(posts_location, f, ensure_ascii=False, indent=2)

    no_loc_file = os.path.join(OUTPUT_DIR, f"{city_name}_no-location-users.json")
    with open(no_loc_file, 'w', encoding='utf-8') as f:
        json.dump(no_location, f, ensure_ascii=False, indent=2)

    # Summary
    safe_print(f"\n[Results]")
    safe_print(f"  From geo-tagged: {stats['geo']}")
    safe_print(f"  From network: {stats['network']}")
    safe_print(f"  From text mentions: {stats['text']}")
    safe_print(f"  Total found: {len(posts_location)}")
    safe_print(f"  Still no location: {len(no_location)}")
    safe_print(f"\n  Output: {posts_loc_file}")
    safe_print(f"  Output: {no_loc_file}")

    # Show examples
    if posts_location:
        safe_print(f"\n[Examples]")
        count = 0
        for uid, info in posts_location.items():
            if count < 10:
                safe_print(f"  @{info['username']}: {info['location']} ({info['source']})")
                count += 1

def main():
    import argparse

    all_cities = [
        'baltimore', 'buffalo', 'el paso', 'fayetteville', 'portland',
        'rockford', 'san_francisco', 'scranton', 'southbend'
    ]

    parser = argparse.ArgumentParser(description='Step 1.5: Extract location from posts')
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
    safe_print("STEP 1.5: POSTS LOCATION")
    safe_print("="*60)
    safe_print(f"Cities: {cities}")

    for city in cities:
        try:
            extract_posts_location(city)
        except Exception as e:
            safe_print(f"\nError processing {city}: {e}")
            import traceback
            traceback.print_exc()

    safe_print("\n" + "="*60)
    safe_print("STEP 1.5 DONE!")
    safe_print("="*60)

if __name__ == "__main__":
    main()
