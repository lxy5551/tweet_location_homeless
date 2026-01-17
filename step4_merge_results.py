"""
Step 4: Merge all location results into final_user_locations
- Combines: self-reported, posts-location, star-friend-analysis, friend-analysis
- Output: final_user_locations/{city}_final.json
- Priority: self-reported > posts-location > star-friend-analysis > friend-analysis
"""

import json
import os
import sys
from collections import Counter

# ============== Config ==============
INPUT_DIR = "User_Location_Analysis"
OUTPUT_DIR = "final_user_locations"

# ============== Utils ==============

def safe_print(text):
    """Safe print for Windows console"""
    try:
        print(text)
    except UnicodeEncodeError:
        print(text.encode('ascii', 'replace').decode('ascii'))
    sys.stdout.flush()

def load_json(filepath):
    """Load JSON file if exists"""
    if os.path.exists(filepath):
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

# ============== Main ==============

def merge_results(city_name):
    """Merge all location results for a city"""
    safe_print(f"\n{'='*60}")
    safe_print(f"MERGE RESULTS - {city_name.upper()}")
    safe_print(f"{'='*60}")

    # Load all data sources (only final results, not intermediate files)
    self_reported = load_json(os.path.join(INPUT_DIR, f"{city_name}_self-reported-location.json"))
    posts_location = load_json(os.path.join(INPUT_DIR, f"{city_name}_posts-location.json"))
    star_friend_analysis = load_json(os.path.join(INPUT_DIR, f"{city_name}_star-friend-analysis.json"))
    friend_analysis = load_json(os.path.join(INPUT_DIR, f"{city_name}_friend-analysis.json"))

    # Load follower counts from multiple sources
    star_users = load_json(os.path.join(INPUT_DIR, f"{city_name}_star-users.json"))
    remaining_users = load_json(os.path.join(INPUT_DIR, f"{city_name}_remaining-users.json"))

    # Build user_id -> follower_count mapping
    all_follower_counts = {}
    for user_id, info in star_users.items():
        all_follower_counts[user_id] = info.get('followers_count', 0)
    for user_id, info in remaining_users.items():
        all_follower_counts[user_id] = info.get('followers_count', 0)

    # Also build username -> follower_count for lookups
    username_follower_counts = {}
    for user_id, info in star_users.items():
        username = info.get('username')
        if username:
            username_follower_counts[username] = info.get('followers_count', 0)
    for user_id, info in remaining_users.items():
        username = info.get('username')
        if username:
            username_follower_counts[username] = info.get('followers_count', 0)

    safe_print(f"  Self-reported: {len(self_reported)} users")
    safe_print(f"  Posts location: {len(posts_location)} users")
    safe_print(f"  Star friend analysis: {len(star_friend_analysis)} users")
    safe_print(f"  Friend analysis: {len(friend_analysis)} users")

    # Build username -> user_id mapping from self_reported
    username_to_id = {}
    for user_id, info in self_reported.items():
        username = info.get('username')
        if username:
            username_to_id[username] = user_id

    # Merge results (priority: self-reported > posts-location > star-friend-analysis > friend-analysis)
    final_results = {}
    stats = {
        'self_reported': 0,
        'posts_location': 0,
        'star_friend_analysis': 0,
        'friend_analysis': 0,
        'no_location': 0
    }

    # 1. Add self-reported locations (highest priority)
    for user_id, info in self_reported.items():
        username = info.get('username')
        followers = all_follower_counts.get(user_id, 0) or username_follower_counts.get(username, 0)
        final_results[user_id] = {
            'user_id': user_id,
            'username': username,
            'location': info.get('parsed_location'),
            'raw_location': info.get('raw_location'),
            'source': 'self-reported',
            'confidence': 'high',
            'followers_count': followers
        }
        stats['self_reported'] += 1

    # 2. Add posts location (geo-tag, network, text mentions)
    for user_id, info in posts_location.items():
        if user_id in final_results:
            continue

        username = info.get('username')
        followers = all_follower_counts.get(user_id, 0) or username_follower_counts.get(username, 0)
        final_results[user_id] = {
            'user_id': user_id,
            'username': username,
            'location': info.get('location'),
            'raw_location': info.get('raw_location'),
            'source': f"posts-{info.get('source', 'unknown')}",
            'confidence': info.get('confidence', 'medium'),
            'followers_count': followers
        }
        stats['posts_location'] += 1

    # 3. Add star friend analysis results (if available)
    for username, info in star_friend_analysis.items():
        user_id = username_to_id.get(username, f"u_{username}")

        if user_id in final_results:
            continue  # Already have higher priority location

        category = info.get('category')
        location = info.get('estimated_location')
        followers = username_follower_counts.get(username, 0)

        if category == 'Bot' or not location:
            final_results[user_id] = {
                'user_id': user_id,
                'username': username,
                'location': None,
                'source': 'star-friend-analysis',
                'category': category,
                'confidence': 'none',
                'followers_count': followers
            }
            stats['no_location'] += 1
        else:
            confidence = 'medium' if category == 'City-level' else 'low'
            final_results[user_id] = {
                'user_id': user_id,
                'username': username,
                'location': location,
                'source': 'star-friend-analysis',
                'category': category,
                'confidence': confidence,
                'followers_count': followers
            }
            stats['star_friend_analysis'] += 1

    # 4. Add friend analysis results (lowest priority)
    # Note: friend_analysis uses username as key, not user_id
    for username, info in friend_analysis.items():
        # Find user_id from username (if available)
        user_id = username_to_id.get(username, f"u_{username}")

        if user_id in final_results:
            continue  # Already have higher priority location

        category = info.get('category')
        location = info.get('estimated_location')
        followers = all_follower_counts.get(user_id, 0) or username_follower_counts.get(username, 0)

        if category == 'Bot' or not location:
            final_results[user_id] = {
                'user_id': user_id,
                'username': username,
                'location': None,
                'source': 'friend-analysis',
                'category': category,
                'confidence': 'none',
                'followers_count': followers
            }
            stats['no_location'] += 1
        else:
            confidence = 'medium' if category == 'City-level' else 'low'
            final_results[user_id] = {
                'user_id': user_id,
                'username': username,
                'location': location,
                'source': 'friend-analysis',
                'category': category,
                'confidence': confidence,
                'followers_count': followers
            }
            stats['friend_analysis'] += 1

    # Save results
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_file = os.path.join(OUTPUT_DIR, f"{city_name}_final.json")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(final_results, f, ensure_ascii=False, indent=2)

    # Summary
    estimated_total = stats['posts_location'] + stats['star_friend_analysis'] + stats['friend_analysis']
    friends_total = stats['star_friend_analysis'] + stats['friend_analysis']
    has_location = stats['self_reported'] + estimated_total

    safe_print(f"\n[Summary]")
    safe_print(f"  Total users: {len(final_results)}")
    safe_print(f"  - Self-reported: {stats['self_reported']}")
    safe_print(f"  - Estimated: {estimated_total}")
    safe_print(f"    * Post location: {stats['posts_location']}")
    safe_print(f"    * Based on friends: {friends_total}")
    safe_print(f"      - Star users: {stats['star_friend_analysis']}")
    safe_print(f"      - Regular users: {stats['friend_analysis']}")
    safe_print(f"  - No location & No friends (likely bot): {stats['no_location']}")
    safe_print(f"  Output: {output_file}")

    # Location distribution - categorize by city, state, country
    # Separate by user type: star vs regular
    star_sources = {'star-friend-analysis'}
    regular_users = [r for r in final_results.values() if r['location'] and r.get('source') not in star_sources]
    star_users_with_loc = [r for r in final_results.values() if r['location'] and r.get('source') in star_sources]
    regular_locations = [r['location'] for r in regular_users]
    star_locations = [r['location'] for r in star_users_with_loc]

    def parse_locations(locations):
        """Parse locations into cities, states, countries"""
        cities = []
        states = []
        countries = []

        # US state abbreviations
        us_state_abbrevs = {'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
                           'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
                           'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
                           'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
                           'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC'}

        # Full state names to abbreviations
        state_name_to_abbrev = {
            'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR',
            'california': 'CA', 'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE',
            'florida': 'FL', 'georgia': 'GA', 'hawaii': 'HI', 'idaho': 'ID',
            'illinois': 'IL', 'indiana': 'IN', 'iowa': 'IA', 'kansas': 'KS',
            'kentucky': 'KY', 'louisiana': 'LA', 'maine': 'ME', 'maryland': 'MD',
            'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN', 'mississippi': 'MS',
            'missouri': 'MO', 'montana': 'MT', 'nebraska': 'NE', 'nevada': 'NV',
            'new hampshire': 'NH', 'new jersey': 'NJ', 'new mexico': 'NM', 'new york': 'NY',
            'north carolina': 'NC', 'north dakota': 'ND', 'ohio': 'OH', 'oklahoma': 'OK',
            'oregon': 'OR', 'pennsylvania': 'PA', 'rhode island': 'RI', 'south carolina': 'SC',
            'south dakota': 'SD', 'tennessee': 'TN', 'texas': 'TX', 'utah': 'UT',
            'vermont': 'VT', 'virginia': 'VA', 'washington': 'WA', 'west virginia': 'WV',
            'wisconsin': 'WI', 'wyoming': 'WY', 'district of columbia': 'DC'
        }

        for loc in locations:
            parts = [p.strip() for p in loc.split(',')]

            # Check if this is a US location
            has_us_state_abbrev = any(p in us_state_abbrevs for p in parts)
            has_us_state_name = any(p.lower() in state_name_to_abbrev for p in parts)
            has_usa = any(p in {'USA', 'United States', 'US'} for p in parts)

            if has_us_state_abbrev or has_us_state_name or has_usa:
                countries.append('USA')
                state_abbrev = None
                state_idx = -1

                for i, p in enumerate(parts):
                    if p in us_state_abbrevs:
                        state_abbrev = p
                        state_idx = i
                        break
                    elif p.lower() in state_name_to_abbrev:
                        state_abbrev = state_name_to_abbrev[p.lower()]
                        state_idx = i
                        break

                if state_abbrev:
                    states.append(state_abbrev)
                    if state_idx > 0:
                        city = parts[0] + ', ' + state_abbrev
                        cities.append(city)
            else:
                if len(parts) >= 2:
                    country = parts[-1]
                    countries.append(country)
                    if len(parts) >= 3:
                        cities.append(parts[0] + ', ' + country)
                        states.append(parts[1])
                    else:
                        cities.append(parts[0] + ', ' + country)
                elif len(parts) == 1:
                    countries.append(parts[0])

        return cities, states, countries

    def parse_location_single(loc):
        """Parse a single location into (city, state, country)"""
        us_state_abbrevs = {'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
                           'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
                           'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
                           'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
                           'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC'}
        state_name_to_abbrev = {
            'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR',
            'california': 'CA', 'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE',
            'florida': 'FL', 'georgia': 'GA', 'hawaii': 'HI', 'idaho': 'ID',
            'illinois': 'IL', 'indiana': 'IN', 'iowa': 'IA', 'kansas': 'KS',
            'kentucky': 'KY', 'louisiana': 'LA', 'maine': 'ME', 'maryland': 'MD',
            'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN', 'mississippi': 'MS',
            'missouri': 'MO', 'montana': 'MT', 'nebraska': 'NE', 'nevada': 'NV',
            'new hampshire': 'NH', 'new jersey': 'NJ', 'new mexico': 'NM', 'new york': 'NY',
            'north carolina': 'NC', 'north dakota': 'ND', 'ohio': 'OH', 'oklahoma': 'OK',
            'oregon': 'OR', 'pennsylvania': 'PA', 'rhode island': 'RI', 'south carolina': 'SC',
            'south dakota': 'SD', 'tennessee': 'TN', 'texas': 'TX', 'utah': 'UT',
            'vermont': 'VT', 'virginia': 'VA', 'washington': 'WA', 'west virginia': 'WV',
            'wisconsin': 'WI', 'wyoming': 'WY', 'district of columbia': 'DC'
        }

        parts = [p.strip() for p in loc.split(',')]
        city, state, country = None, None, None

        has_us_state_abbrev = any(p in us_state_abbrevs for p in parts)
        has_us_state_name = any(p.lower() in state_name_to_abbrev for p in parts)
        has_usa = any(p in {'USA', 'United States', 'US'} for p in parts)

        if has_us_state_abbrev or has_us_state_name or has_usa:
            country = 'USA'
            for i, p in enumerate(parts):
                if p in us_state_abbrevs:
                    state = p
                    if i > 0:
                        city = parts[0] + ', ' + state
                    break
                elif p.lower() in state_name_to_abbrev:
                    state = state_name_to_abbrev[p.lower()]
                    if i > 0:
                        city = parts[0] + ', ' + state
                    break
        else:
            if len(parts) >= 2:
                country = parts[-1]
                city = parts[0] + ', ' + country

        return city, state, country

    def print_location_stats(locations, label):
        """Print location statistics for a group"""
        if not locations:
            return

        cities, states, countries = parse_locations(locations)

        safe_print(f"\n[{label}] Top 5 Cities")
        invalid_words = {'earth', 'world', 'global', 'planet', 'everywhere', 'nowhere', 'internet', 'usa', 'america', 'refugee', 'heaven', 'hell', 'home'}
        valid_cities = [c for c in cities if not any(w in c.lower() for w in invalid_words)]
        for loc, count in Counter(valid_cities).most_common(5):
            safe_print(f"  {loc}: {count}")

        safe_print(f"\n[{label}] Top 5 States (US)")
        for loc, count in Counter(states).most_common(5):
            safe_print(f"  {loc}: {count}")

        safe_print(f"\n[{label}] Top 3 Countries")
        valid_countries = [c for c in countries if len(c.split()) <= 3 and not any(x in c.lower() for x in ['refugee', 'earth', 'world', 'global', 'planet'])]
        for loc, count in Counter(valid_countries).most_common(3):
            safe_print(f"  {loc}: {count}")

    def print_star_location_stats(users, label):
        """Print location statistics for star users with follower counts"""
        if not users:
            return

        invalid_words = {'earth', 'world', 'global', 'planet', 'everywhere', 'nowhere', 'internet', 'usa', 'america', 'refugee', 'heaven', 'hell', 'home'}

        # Aggregate by city and state with follower counts
        city_counts = Counter()
        city_followers = Counter()
        state_counts = Counter()
        state_followers = Counter()

        for user in users:
            loc = user.get('location')
            followers = user.get('followers_count', 0)
            if not loc:
                continue

            city, state, country = parse_location_single(loc)

            if city and not any(w in city.lower() for w in invalid_words):
                city_counts[city] += 1
                city_followers[city] += followers

            if state:
                state_counts[state] += 1
                state_followers[state] += followers

        safe_print(f"\n[{label}] Top 5 Cities (users / total followers)")
        top_cities = city_counts.most_common(5)
        if top_cities:
            max_followers_city = max(top_cities, key=lambda x: city_followers[x[0]])
            for city, count in top_cities:
                total = city_followers[city]
                note = " <- most influential" if city == max_followers_city[0] else ""
                safe_print(f"  {city}: {count} users / {total:,} followers{note}")

        safe_print(f"\n[{label}] Top 5 States (users / total followers)")
        top_states = state_counts.most_common(5)
        if top_states:
            max_followers_state = max(top_states, key=lambda x: state_followers[x[0]])
            for state, count in top_states:
                total = state_followers[state]
                note = " <- most influential" if state == max_followers_state[0] else ""
                safe_print(f"  {state}: {count} users / {total:,} followers{note}")

    # Print stats for regular users (with follower counts)
    safe_print(f"\n{'='*40}")
    safe_print(f"REGULAR USERS ({len(regular_locations)} with location)")
    safe_print(f"{'='*40}")
    print_star_location_stats(regular_users, "Regular")

    # Print stats for star users (with follower counts)
    safe_print(f"\n{'='*40}")
    safe_print(f"STAR USERS ({len(star_locations)} with location)")
    safe_print(f"{'='*40}")
    print_star_location_stats(star_users_with_loc, "Star")

    return final_results

def print_stats_only(city_name):
    """Print statistics from existing final.json without re-merging"""
    safe_print(f"\n{'='*60}")
    safe_print(f"STATISTICS - {city_name.upper()}")
    safe_print(f"{'='*60}")

    # Load existing final.json
    final_file = os.path.join(OUTPUT_DIR, f"{city_name}_final.json")
    if not os.path.exists(final_file):
        safe_print(f"  Error: {final_file} not found. Run merge first.")
        return

    with open(final_file, 'r', encoding='utf-8') as f:
        final_results = json.load(f)

    # Calculate stats
    stats = {'self_reported': 0, 'posts_location': 0, 'star_friend_analysis': 0, 'friend_analysis': 0, 'no_location': 0}
    for r in final_results.values():
        source = r.get('source', '')
        if source == 'self-reported':
            stats['self_reported'] += 1
        elif source.startswith('posts-'):
            stats['posts_location'] += 1
        elif source == 'star-friend-analysis':
            if r.get('location'):
                stats['star_friend_analysis'] += 1
            else:
                stats['no_location'] += 1
        elif source == 'friend-analysis':
            if r.get('location'):
                stats['friend_analysis'] += 1
            else:
                stats['no_location'] += 1

    # Summary
    estimated_total = stats['posts_location'] + stats['star_friend_analysis'] + stats['friend_analysis']
    friends_total = stats['star_friend_analysis'] + stats['friend_analysis']

    safe_print(f"\n[Summary]")
    safe_print(f"  Total users: {len(final_results)}")
    safe_print(f"  - Self-reported: {stats['self_reported']}")
    safe_print(f"  - Estimated: {estimated_total}")
    safe_print(f"    * Post location: {stats['posts_location']}")
    safe_print(f"    * Based on friends: {friends_total}")
    safe_print(f"      - Star users: {stats['star_friend_analysis']}")
    safe_print(f"      - Regular users: {stats['friend_analysis']}")
    safe_print(f"  - No location & No friends (likely bot): {stats['no_location']}")

    # Location distribution
    star_sources = {'star-friend-analysis'}
    regular_users = [r for r in final_results.values() if r.get('location') and r.get('source') not in star_sources]
    star_users_with_loc = [r for r in final_results.values() if r.get('location') and r.get('source') in star_sources]

    # US state abbreviations
    us_state_abbrevs = {'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
                       'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
                       'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
                       'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
                       'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC'}
    state_name_to_abbrev = {
        'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR',
        'california': 'CA', 'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE',
        'florida': 'FL', 'georgia': 'GA', 'hawaii': 'HI', 'idaho': 'ID',
        'illinois': 'IL', 'indiana': 'IN', 'iowa': 'IA', 'kansas': 'KS',
        'kentucky': 'KY', 'louisiana': 'LA', 'maine': 'ME', 'maryland': 'MD',
        'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN', 'mississippi': 'MS',
        'missouri': 'MO', 'montana': 'MT', 'nebraska': 'NE', 'nevada': 'NV',
        'new hampshire': 'NH', 'new jersey': 'NJ', 'new mexico': 'NM', 'new york': 'NY',
        'north carolina': 'NC', 'north dakota': 'ND', 'ohio': 'OH', 'oklahoma': 'OK',
        'oregon': 'OR', 'pennsylvania': 'PA', 'rhode island': 'RI', 'south carolina': 'SC',
        'south dakota': 'SD', 'tennessee': 'TN', 'texas': 'TX', 'utah': 'UT',
        'vermont': 'VT', 'virginia': 'VA', 'washington': 'WA', 'west virginia': 'WV',
        'wisconsin': 'WI', 'wyoming': 'WY', 'district of columbia': 'DC'
    }

    def parse_loc(loc):
        parts = [p.strip() for p in loc.split(',')]
        city, state = None, None
        has_us_state_abbrev = any(p in us_state_abbrevs for p in parts)
        has_us_state_name = any(p.lower() in state_name_to_abbrev for p in parts)
        has_usa = any(p in {'USA', 'United States', 'US'} for p in parts)
        if has_us_state_abbrev or has_us_state_name or has_usa:
            for i, p in enumerate(parts):
                if p in us_state_abbrevs:
                    state = p
                    if i > 0: city = parts[0] + ', ' + state
                    break
                elif p.lower() in state_name_to_abbrev:
                    state = state_name_to_abbrev[p.lower()]
                    if i > 0: city = parts[0] + ', ' + state
                    break
        else:
            if len(parts) >= 2:
                city = parts[0] + ', ' + parts[-1]
        return city, state

    def print_user_stats(users, label):
        if not users:
            return
        invalid_words = {'earth', 'world', 'global', 'planet', 'everywhere', 'nowhere', 'internet', 'usa', 'america', 'refugee', 'heaven', 'hell', 'home'}
        city_counts, city_followers = Counter(), Counter()
        state_counts, state_followers = Counter(), Counter()
        for user in users:
            loc = user.get('location')
            followers = user.get('followers_count', 0)
            if not loc: continue
            city, state = parse_loc(loc)
            if city and not any(w in city.lower() for w in invalid_words):
                city_counts[city] += 1
                city_followers[city] += followers
            if state:
                state_counts[state] += 1
                state_followers[state] += followers

        safe_print(f"\n[{label}] Top 5 Cities (users / total followers)")
        top_cities = city_counts.most_common(5)
        if top_cities:
            max_followers_city = max(top_cities, key=lambda x: city_followers[x[0]])
            for city, count in top_cities:
                total = city_followers[city]
                note = " <- most influential" if city == max_followers_city[0] else ""
                safe_print(f"  {city}: {count} users / {total:,} followers{note}")
        safe_print(f"\n[{label}] Top 5 States (users / total followers)")
        top_states = state_counts.most_common(5)
        if top_states:
            max_followers_state = max(top_states, key=lambda x: state_followers[x[0]])
            for state, count in top_states:
                total = state_followers[state]
                note = " <- most influential" if state == max_followers_state[0] else ""
                safe_print(f"  {state}: {count} users / {total:,} followers{note}")

    safe_print(f"\n{'='*40}")
    safe_print(f"REGULAR USERS ({len(regular_users)} with location)")
    safe_print(f"{'='*40}")
    print_user_stats(regular_users, "Regular")

    safe_print(f"\n{'='*40}")
    safe_print(f"STAR USERS ({len(star_users_with_loc)} with location)")
    safe_print(f"{'='*40}")
    print_user_stats(star_users_with_loc, "Star")

def main():
    import argparse

    all_cities = [
        'baltimore', 'buffalo', 'el paso', 'fayetteville', 'portland',
        'rockford', 'san_francisco', 'scranton', 'southbend'
    ]

    parser = argparse.ArgumentParser(description='Step 4: Merge all location results')
    parser.add_argument('--cities', type=str, help='Comma-separated city names')
    parser.add_argument('--range', type=str, help='City range, e.g., "1-5"')
    parser.add_argument('--stats-only', action='store_true', help='Only print statistics from existing final.json (no merge)')
    args = parser.parse_args()

    if args.cities:
        cities = [c.strip() for c in args.cities.split(',')]
    elif args.range:
        start, end = map(int, args.range.split('-'))
        cities = all_cities[start-1:end]
    else:
        cities = all_cities

    if args.stats_only:
        safe_print("="*60)
        safe_print("STATISTICS ONLY (no merge)")
        safe_print("="*60)
        for city in cities:
            try:
                print_stats_only(city)
            except Exception as e:
                safe_print(f"\nError processing {city}: {e}")
    else:
        safe_print("="*60)
        safe_print("STEP 4: MERGE RESULTS")
        safe_print("="*60)
        safe_print(f"Cities: {cities}")
        safe_print(f"Output: {OUTPUT_DIR}/")

        for city in cities:
            try:
                merge_results(city)
            except Exception as e:
                safe_print(f"\nError processing {city}: {e}")
                import traceback
                traceback.print_exc()

    safe_print("\n" + "="*60)
    safe_print("DONE!")
    safe_print("="*60)

if __name__ == "__main__":
    main()
