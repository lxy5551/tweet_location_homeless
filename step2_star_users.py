"""
Step 2: Classify users into star users vs remaining users
- Star users: followers > 1000 AND following > 1000
- Input: {city}_no-location-users.json (from step 1.5)
- Output:
  - {city}_star-users.json (for friend analysis - expensive)
  - {city}_remaining-users.json (for friend analysis)
"""

import json
import os
import sys

# ============== Config ==============
RAW_DATA_DIR = "raw_x_data"
OUTPUT_DIR = "User_Location_Analysis"

STAR_THRESHOLD = 1000  # Both followers and following must exceed this

# ============== Utils ==============

def safe_print(text):
    """Safe print for Windows console"""
    try:
        print(text)
    except UnicodeEncodeError:
        print(text.encode('ascii', 'replace').decode('ascii'))
    sys.stdout.flush()

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

def load_self_reported_locations(city_name):
    """Load already processed self-reported locations"""
    file_path = os.path.join(OUTPUT_DIR, f"{city_name}_self-reported-location.json")
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def identify_star_users(city_name):
    """Identify star users for a city"""
    safe_print(f"\n{'='*60}")
    safe_print(f"STEP 2: Classify Users - {city_name.upper()}")
    safe_print(f"{'='*60}")

    # Try to load no-location-users.json (from step 1.5)
    no_loc_file = os.path.join(OUTPUT_DIR, f"{city_name}_no-location-users.json")
    if os.path.exists(no_loc_file):
        safe_print(f"\n[1] Loading users without location (from step 1.5)...")
        with open(no_loc_file, 'r', encoding='utf-8') as f:
            users = json.load(f)
        safe_print(f"  Users to classify: {len(users)}")
    else:
        # Fallback: extract from posts and exclude self-reported
        safe_print(f"\n[1] No step 1.5 output found, extracting from posts...")
        self_reported = load_self_reported_locations(city_name)
        self_reported_ids = set(self_reported.keys())
        safe_print(f"  Already have location: {len(self_reported_ids)} users")

        all_users = extract_users_from_posts(city_name)
        users = {uid: {'username': info['username'], 'raw_location': info['location'],
                       'followers_count': info['followers_count'], 'following_count': info['following_count']}
                 for uid, info in all_users.items() if uid not in self_reported_ids}
        safe_print(f"  Users to classify: {len(users)}")

    # Classify users
    safe_print(f"\n[2] Classifying users (star threshold: >{STAR_THRESHOLD} followers AND following)...")
    star_users = {}
    remaining_users = {}

    for user_id, info in users.items():
        followers = info.get('followers_count') or 0
        following = info.get('following_count') or 0

        if followers > STAR_THRESHOLD and following > STAR_THRESHOLD:
            # Star user
            star_users[user_id] = {
                'username': info['username'],
                'raw_location': info.get('raw_location'),
                'followers_count': followers,
                'following_count': following
            }
        else:
            # Normal user needing friend analysis
            remaining_users[user_id] = {
                'username': info['username'],
                'raw_location': info.get('raw_location'),
                'followers_count': followers,
                'following_count': following
            }

    # Save star users
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    star_file = os.path.join(OUTPUT_DIR, f"{city_name}_star-users.json")
    with open(star_file, 'w', encoding='utf-8') as f:
        json.dump(star_users, f, ensure_ascii=False, indent=2)

    # Save remaining users (for step 3 friend analysis)
    remaining_file = os.path.join(OUTPUT_DIR, f"{city_name}_remaining-users.json")
    with open(remaining_file, 'w', encoding='utf-8') as f:
        json.dump(remaining_users, f, ensure_ascii=False, indent=2)

    safe_print(f"\n[Summary]")
    safe_print(f"  Star users: {len(star_users)}")
    safe_print(f"  Remaining users: {len(remaining_users)}")

    safe_print(f"\n[Output Files]")
    safe_print(f"  Star users: {star_file}")
    safe_print(f"  Remaining users: {remaining_file}")

def main():
    import argparse

    all_cities = [
        'baltimore', 'buffalo', 'el paso', 'fayetteville', 'portland',
        'rockford', 'san_francisco', 'scranton', 'southbend'
    ]

    parser = argparse.ArgumentParser(description='Step 2: Classify users into star vs remaining')
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
    safe_print("STEP 2: CLASSIFY USERS")
    safe_print("="*60)
    safe_print(f"Cities: {cities}")

    for city in cities:
        try:
            identify_star_users(city)
        except Exception as e:
            safe_print(f"\nError processing {city}: {e}")
            import traceback
            traceback.print_exc()

    safe_print("\n" + "="*60)
    safe_print("STEP 2 DONE!")
    safe_print("="*60)

if __name__ == "__main__":
    main()
