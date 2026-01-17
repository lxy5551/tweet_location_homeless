"""
Step 3: Friend Analysis for remaining users
Sub-steps:
  3.1 - Fetch followers and followings (API calls, slow)
  3.2 - Extract friend profiles from followers/followings (fast, no API)
  3.3 - Geocode friend locations (API calls, slow)
  3.4 - Analyze user locations and generate results (fast, no API)

Chunk support for parallel processing:
  --chunk 1/20  means process chunk 1 out of 20 total chunks

Output: {city}_friend-analysis.json
"""

import json
import os
import sys

# Reuse functions from location_analysis_pipeline
from location_analysis_pipeline import (
    safe_print,
    fetch_all_followers_followings,
    fetch_friend_profiles,
    geocode_friend_locations,
    generate_final_analysis,
    set_max_fetch_count,
    RAW_DATA_DIR,
    OUTPUT_DIR
)

# Star user cost optimization thresholds
STAR_SKIP_THRESHOLD = 5000    # Skip star users with followers > 5k (too expensive)
STAR_MAX_FETCH = 200          # Only fetch first 200 followers/followings for eligible star users

def load_remaining_users(city_name, chunk=None, user_type='remaining'):
    """Load users that need friend analysis

    Args:
        city_name: Name of the city
        chunk: Optional tuple (chunk_num, total_chunks) e.g., (1, 20) for chunk 1 of 20
        user_type: 'remaining' or 'star' - which user file to load

    Returns:
        List of usernames (possibly filtered by chunk)
    """
    if user_type == 'star':
        file_path = os.path.join(OUTPUT_DIR, f"{city_name}_star-users.json")
    else:
        file_path = os.path.join(OUTPUT_DIR, f"{city_name}_remaining-users.json")

    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        # Return list of usernames
        usernames = [info['username'] for info in data.values()]

        # Apply chunk filter if specified
        if chunk:
            chunk_num, total_chunks = chunk
            chunk_size = len(usernames) // total_chunks
            remainder = len(usernames) % total_chunks

            # Calculate start and end indices for this chunk
            start = 0
            for i in range(1, chunk_num):
                start += chunk_size + (1 if i <= remainder else 0)

            end = start + chunk_size + (1 if chunk_num <= remainder else 0)

            usernames = usernames[start:end]

        return usernames
    return []

def load_star_users_with_counts(city_name):
    """Load star users with their follower counts for filtering

    Returns:
        Dict of {username: followers_count}
    """
    file_path = os.path.join(OUTPUT_DIR, f"{city_name}_star-users.json")

    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        # Return dict of username -> followers_count
        return {info['username']: info.get('followers_count', 0) for info in data.values()}
    return {}

def filter_star_users(usernames, city_name, step_name=""):
    """Filter star users to only include those with <= STAR_SKIP_THRESHOLD followers

    Args:
        usernames: List of usernames
        city_name: City name
        step_name: Step name for logging

    Returns:
        Filtered list of usernames
    """
    user_counts = load_star_users_with_counts(city_name)
    original_count = len(usernames)
    filtered = [u for u in usernames if user_counts.get(u, 0) <= STAR_SKIP_THRESHOLD]
    skipped = original_count - len(filtered)

    if skipped > 0:
        safe_print(f"  Skipped {skipped} users with >{STAR_SKIP_THRESHOLD} followers")
        safe_print(f"  Processing {len(filtered)} eligible star users")

    return filtered

def load_followers_followings(city_name):
    """Load already-fetched followers and followings data"""
    city_dir = os.path.join(RAW_DATA_DIR, city_name)
    followers_file = os.path.join(city_dir, f"follower_{city_name}.json")
    followings_file = os.path.join(city_dir, f"following_{city_name}.json")

    all_followers = {}
    all_followings = {}

    if os.path.exists(followers_file):
        with open(followers_file, 'r', encoding='utf-8') as f:
            all_followers = json.load(f)

    if os.path.exists(followings_file):
        with open(followings_file, 'r', encoding='utf-8') as f:
            all_followings = json.load(f)

    return all_followers, all_followings

def step_3_1(city_name, chunk=None, num_threads=1, user_type='remaining'):
    """Step 3.1: Fetch followers and followings"""
    chunk_str = f" (Chunk {chunk[0]}/{chunk[1]})" if chunk else ""
    type_str = " [STAR USERS]" if user_type == 'star' else ""
    safe_print(f"\n{'='*60}")
    safe_print(f"STEP 3.1: Fetch Followers/Followings - {city_name.upper()}{chunk_str}{type_str}")
    safe_print(f"{'='*60}")

    usernames = load_remaining_users(city_name, chunk, user_type)
    safe_print(f"  Found {len(usernames)} users")

    if not usernames:
        safe_print(f"  No users, skipping...")
        return

    # For star users: apply cost optimization
    if user_type == 'star':
        # Filter out users with too many followers (> 10k)
        usernames = filter_star_users(usernames, city_name, "3.1")

        # Set max fetch count for star users
        set_max_fetch_count(STAR_MAX_FETCH)
        safe_print(f"  Max fetch per user: {STAR_MAX_FETCH} followers/followings")
    else:
        # For remaining users, no limit
        set_max_fetch_count(None)

    if not usernames:
        safe_print(f"  No eligible users after filtering, skipping...")
        return

    # Use different file suffix for star users
    file_suffix = f"_star" if user_type == 'star' else ""
    all_followers, all_followings = fetch_all_followers_followings(usernames, city_name + file_suffix, num_threads)
    safe_print(f"  Fetched {len(all_followers)} followers, {len(all_followings)} followings")

    # For star users: validate results and remove suspicious empty ones
    if user_type == 'star':
        user_counts = load_star_users_with_counts(city_name)
        suspicious_count = 0
        users_to_remove = []

        for username in usernames:
            expected = user_counts.get(username, 0)
            actual_followers = len(all_followers.get(username, {}).get('followers', []))
            actual_followings = len(all_followings.get(username, {}).get('followings', []))

            # If expected >1000 but got 0, likely rate limited
            if expected > 1000 and actual_followers == 0 and actual_followings == 0:
                suspicious_count += 1
                users_to_remove.append(username)

        if suspicious_count > 0:
            safe_print(f"  WARNING: {suspicious_count} users with suspicious empty results (rate limiting?)")
            safe_print(f"  Removing these from saved data for retry next run...")

            # Remove from saved data so they'll be retried
            city_dir = os.path.join(RAW_DATA_DIR, city_name + file_suffix)
            followers_file = os.path.join(city_dir, f"follower_{city_name}{file_suffix}.json")
            followings_file = os.path.join(city_dir, f"following_{city_name}{file_suffix}.json")

            for username in users_to_remove:
                all_followers.pop(username, None)
                all_followings.pop(username, None)

            # Re-save without suspicious entries
            with open(followers_file, 'w', encoding='utf-8') as f:
                json.dump(all_followers, f, ensure_ascii=False, indent=2)
            with open(followings_file, 'w', encoding='utf-8') as f:
                json.dump(all_followings, f, ensure_ascii=False, indent=2)

            safe_print(f"  Saved {len(all_followers)} valid results (removed {suspicious_count} suspicious)")

    # Reset max fetch count after done
    set_max_fetch_count(None)

def step_3_2(city_name, chunk=None, user_type='remaining'):
    """Step 3.2: Extract friend profiles from followers/followings"""
    chunk_str = f" (Chunk {chunk[0]}/{chunk[1]})" if chunk else ""
    type_str = " [STAR USERS]" if user_type == 'star' else ""
    safe_print(f"\n{'='*60}")
    safe_print(f"STEP 3.2: Extract Friend Profiles - {city_name.upper()}{chunk_str}{type_str}")
    safe_print(f"{'='*60}")

    usernames = load_remaining_users(city_name, chunk, user_type)
    safe_print(f"  Found {len(usernames)} users")

    if not usernames:
        safe_print(f"  No users, skipping...")
        return

    # For star users: filter out those with too many followers
    if user_type == 'star':
        usernames = filter_star_users(usernames, city_name, "3.2")
        if not usernames:
            safe_print(f"  No eligible users after filtering, skipping...")
            return

    file_suffix = f"_star" if user_type == 'star' else ""
    all_followers, all_followings = load_followers_followings(city_name + file_suffix)
    safe_print(f"  Loaded {len(all_followers)} followers, {len(all_followings)} followings")

    fetch_friend_profiles(usernames, all_followers, all_followings, city_name + file_suffix)

def step_3_3(city_name, chunk=None, user_type='remaining'):
    """Step 3.3: Geocode friend locations"""
    chunk_str = f" (Chunk {chunk[0]}/{chunk[1]})" if chunk else ""
    type_str = " [STAR USERS]" if user_type == 'star' else ""
    safe_print(f"\n{'='*60}")
    safe_print(f"STEP 3.3: Geocode Friend Locations - {city_name.upper()}{chunk_str}{type_str}")
    safe_print(f"{'='*60}")

    usernames = load_remaining_users(city_name, chunk, user_type)
    safe_print(f"  Found {len(usernames)} users")

    if not usernames:
        safe_print(f"  No users, skipping...")
        return

    # For star users: filter out those with too many followers
    if user_type == 'star':
        usernames = filter_star_users(usernames, city_name, "3.3")
        if not usernames:
            safe_print(f"  No eligible users after filtering, skipping...")
            return

    file_suffix = f"_star" if user_type == 'star' else ""
    geocode_friend_locations(usernames, city_name + file_suffix)

def step_3_4(city_name, chunk=None, user_type='remaining'):
    """Step 3.4: Analyze user locations and generate results"""
    chunk_str = f" (Chunk {chunk[0]}/{chunk[1]})" if chunk else ""
    type_str = " [STAR USERS]" if user_type == 'star' else ""
    safe_print(f"\n{'='*60}")
    safe_print(f"STEP 3.4: Analyze User Locations - {city_name.upper()}{chunk_str}{type_str}")
    safe_print(f"{'='*60}")

    usernames = load_remaining_users(city_name, chunk, user_type)
    safe_print(f"  Found {len(usernames)} users")

    if not usernames:
        safe_print(f"  No users, skipping...")
        return

    # For star users: filter out those with too many followers
    if user_type == 'star':
        usernames = filter_star_users(usernames, city_name, "3.4")
        if not usernames:
            safe_print(f"  No eligible users after filtering, skipping...")
            return

    file_suffix = f"_star" if user_type == 'star' else ""
    results = generate_final_analysis(usernames, city_name + file_suffix)

    # Save results (with chunk suffix if chunked, different name for star users)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    if user_type == 'star':
        if chunk:
            output_file = os.path.join(OUTPUT_DIR, f"{city_name}_star-friend-analysis_chunk{chunk[0]}.json")
        else:
            output_file = os.path.join(OUTPUT_DIR, f"{city_name}_star-friend-analysis.json")
    else:
        if chunk:
            output_file = os.path.join(OUTPUT_DIR, f"{city_name}_friend-analysis_chunk{chunk[0]}.json")
        else:
            output_file = os.path.join(OUTPUT_DIR, f"{city_name}_friend-analysis.json")

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    # Summary
    from collections import Counter
    categories = Counter(r['category'] for r in results.values())
    safe_print(f"\n[Summary]")
    safe_print(f"  Total analyzed: {len(results)}")
    for cat, count in categories.most_common():
        safe_print(f"  - {cat}: {count}")
    safe_print(f"  Output: {output_file}")

def process_friend_analysis(city_name, substep=None, chunk=None, num_threads=1, user_type='remaining'):
    """Run friend analysis for users (all sub-steps or specific one)

    Args:
        user_type: 'remaining' or 'star' - which users to process
    """
    if substep == "3.1":
        step_3_1(city_name, chunk, num_threads, user_type)
    elif substep == "3.2":
        step_3_2(city_name, chunk, user_type)
    elif substep == "3.3":
        step_3_3(city_name, chunk, user_type)
    elif substep == "3.4":
        step_3_4(city_name, chunk, user_type)
    else:
        # Run all sub-steps
        step_3_1(city_name, chunk, num_threads, user_type)
        step_3_2(city_name, chunk, user_type)
        step_3_3(city_name, chunk, user_type)
        step_3_4(city_name, chunk, user_type)

def parse_chunk(chunk_str):
    """Parse chunk string like '1/20' into tuple (1, 20)"""
    if not chunk_str:
        return None
    try:
        parts = chunk_str.split('/')
        if len(parts) != 2:
            raise ValueError("Invalid chunk format")
        chunk_num = int(parts[0])
        total_chunks = int(parts[1])
        if chunk_num < 1 or chunk_num > total_chunks:
            raise ValueError(f"Chunk {chunk_num} out of range 1-{total_chunks}")
        return (chunk_num, total_chunks)
    except Exception as e:
        safe_print(f"Error parsing chunk '{chunk_str}': {e}")
        safe_print("Expected format: N/M (e.g., 1/20 for chunk 1 of 20)")
        sys.exit(1)

def show_chunk_distribution(city_name, total_chunks):
    """Show how users would be distributed across chunks"""
    file_path = os.path.join(OUTPUT_DIR, f"{city_name}_remaining-users.json")
    if not os.path.exists(file_path):
        safe_print(f"  File not found: {file_path}")
        return

    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    total_users = len(data)
    chunk_size = total_users // total_chunks
    remainder = total_users % total_chunks

    safe_print(f"\n{city_name.upper()}: {total_users} users into {total_chunks} chunks")
    safe_print("-" * 40)

    start = 0
    for i in range(1, total_chunks + 1):
        size = chunk_size + (1 if i <= remainder else 0)
        end = start + size
        safe_print(f"  Chunk {i:2d}/{total_chunks}: users {start+1:4d}-{end:4d} ({size} users)")
        start = end

def main():
    import argparse

    all_cities = [
        'baltimore', 'buffalo', 'el paso', 'fayetteville', 'portland',
        'rockford', 'san_francisco', 'scranton', 'southbend'
    ]

    parser = argparse.ArgumentParser(description='Step 3: Friend analysis for remaining users')
    parser.add_argument('--cities', type=str, help='Comma-separated city names')
    parser.add_argument('--range', type=str, help='City range, e.g., "1-5"')
    parser.add_argument('--substep', type=str, choices=['3.1', '3.2', '3.3', '3.4'],
                        help='Run specific sub-step: 3.1=followers/followings, 3.2=friend profiles, 3.3=geocoding, 3.4=analysis')
    parser.add_argument('--chunk', type=str,
                        help='Process specific chunk, e.g., "1/20" for chunk 1 of 20')
    parser.add_argument('--threads', type=int, default=1,
                        help='Number of threads for parallel API calls (default: 1, recommended: 5-10)')
    parser.add_argument('--show-chunks', type=int, metavar='N',
                        help='Show how users would be split into N chunks (does not run processing)')
    parser.add_argument('--user-type', type=str, choices=['remaining', 'star'], default='remaining',
                        help='Which users to process: remaining (default) or star')
    args = parser.parse_args()

    if args.cities:
        cities = [c.strip() for c in args.cities.split(',')]
    elif args.range:
        start, end = map(int, args.range.split('-'))
        cities = all_cities[start-1:end]
    else:
        cities = all_cities

    # Show chunk distribution mode
    if args.show_chunks:
        safe_print("="*60)
        safe_print(f"CHUNK DISTRIBUTION PREVIEW ({args.show_chunks} chunks)")
        safe_print("="*60)
        for city in cities:
            show_chunk_distribution(city, args.show_chunks)
        return

    # Parse chunk parameter
    chunk = parse_chunk(args.chunk)

    step_label = f"STEP {args.substep}" if args.substep else "STEP 3 (ALL)"
    chunk_label = f" - Chunk {chunk[0]}/{chunk[1]}" if chunk else ""
    user_type_label = " [STAR USERS]" if args.user_type == 'star' else ""

    safe_print("="*60)
    safe_print(f"FRIEND ANALYSIS - {step_label}{chunk_label}{user_type_label}")
    safe_print("="*60)
    safe_print(f"Cities: {cities}")
    safe_print(f"User type: {args.user_type}")

    if args.substep:
        substep_desc = {
            '3.1': 'Fetch followers/followings (slow, API calls)',
            '3.2': 'Extract friend profiles (fast)',
            '3.3': 'Geocode locations (slow, API calls)',
            '3.4': 'Analyze user locations (fast)'
        }
        safe_print(f"Sub-step: {substep_desc.get(args.substep, args.substep)}")

    if args.threads > 1:
        safe_print(f"Threads: {args.threads} (parallel mode)")

    for city in cities:
        try:
            process_friend_analysis(city, args.substep, chunk, args.threads, args.user_type)
        except Exception as e:
            safe_print(f"\nError processing {city}: {e}")
            import traceback
            traceback.print_exc()

    safe_print("\n" + "="*60)
    safe_print(f"{step_label}{chunk_label} DONE!")
    safe_print("="*60)

if __name__ == "__main__":
    main()
