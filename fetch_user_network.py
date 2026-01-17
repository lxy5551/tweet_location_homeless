import json
import requests
import time
from datetime import datetime

# Twitter API configuration
BEARER_TOKEN = 'AAAAAAAAAAAAAAAAAAAAAM083AEAAAAACh%2F9DvM0i0041jGh8yMpAs8%2Fgfk%3DHGhaKviWDaHDz3yz1YF8K9c7RsmIFkeOjXBKB0WpEXwJxgrSCo'

# API endpoints
FOLLOWERS_URL = 'https://api.twitter.com/2/users/{}/followers'
FOLLOWING_URL = 'https://api.twitter.com/2/users/{}/following'

def get_bearer_headers():
    """Create authorization headers"""
    return {
        'Authorization': f'Bearer {BEARER_TOKEN}',
        'User-Agent': 'v2UserLookupPython'
    }

def get_followers(user_id, max_results=1000):
    """Get followers for a user"""
    url = FOLLOWERS_URL.format(user_id)
    headers = get_bearer_headers()
    params = {
        'max_results': min(max_results, 1000),  # API max is 1000 per request
    }

    followers = []

    try:
        while True:
            response = requests.get(url, headers=headers, params=params)

            if response.status_code == 429:
                # Rate limit exceeded
                reset_time = int(response.headers.get('x-rate-limit-reset', 0))
                wait_time = max(reset_time - int(time.time()), 0) + 5
                print(f'  Rate limit hit. Waiting {wait_time} seconds...')
                time.sleep(wait_time)
                continue

            if response.status_code != 200:
                print(f'  Error fetching followers: {response.status_code} - {response.text}')
                return None

            data = response.json()

            if 'data' in data:
                followers.extend([user['id'] for user in data['data']])

            # Check for pagination
            if 'meta' in data and 'next_token' in data['meta']:
                params['pagination_token'] = data['meta']['next_token']
                time.sleep(1)  # Rate limiting pause
            else:
                break

            # Stop if we've reached max_results
            if len(followers) >= max_results:
                followers = followers[:max_results]
                break

        return followers

    except Exception as e:
        print(f'  Exception getting followers: {str(e)}')
        return None

def get_following(user_id, max_results=1000):
    """Get following (followees) for a user"""
    url = FOLLOWING_URL.format(user_id)
    headers = get_bearer_headers()
    params = {
        'max_results': min(max_results, 1000),  # API max is 1000 per request
    }

    following = []

    try:
        while True:
            response = requests.get(url, headers=headers, params=params)

            if response.status_code == 429:
                # Rate limit exceeded
                reset_time = int(response.headers.get('x-rate-limit-reset', 0))
                wait_time = max(reset_time - int(time.time()), 0) + 5
                print(f'  Rate limit hit. Waiting {wait_time} seconds...')
                time.sleep(wait_time)
                continue

            if response.status_code != 200:
                print(f'  Error fetching following: {response.status_code} - {response.text}')
                return None

            data = response.json()

            if 'data' in data:
                following.extend([user['id'] for user in data['data']])

            # Check for pagination
            if 'meta' in data and 'next_token' in data['meta']:
                params['pagination_token'] = data['meta']['next_token']
                time.sleep(1)  # Rate limiting pause
            else:
                break

            # Stop if we've reached max_results
            if len(following) >= max_results:
                following = following[:max_results]
                break

        return following

    except Exception as e:
        print(f'  Exception getting following: {str(e)}')
        return None

def build_user_network(user_ids_file, output_file, max_followers=1000, max_following=1000):
    """Build user network from list of user IDs"""

    # Load user IDs
    with open(user_ids_file, 'r', encoding='utf-8') as f:
        user_ids = json.load(f)

    print(f'Building network for {len(user_ids)} users...')
    print(f'Max followers per user: {max_followers}')
    print(f'Max following per user: {max_following}')
    print(f'Started at: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print('='*80)

    user_network = {}

    for i, user_id in enumerate(user_ids, 1):
        print(f'\n[{i}/{len(user_ids)}] Processing user ID: {user_id}')

        # Get followers
        print(f'  Fetching followers...')
        followers = get_followers(user_id, max_results=max_followers)

        if followers is None:
            print(f'  Skipping user {user_id} - failed to fetch followers')
            continue

        print(f'  Found {len(followers)} followers')

        # Get following
        print(f'  Fetching following...')
        following = get_following(user_id, max_results=max_following)

        if following is None:
            print(f'  Skipping user {user_id} - failed to fetch following')
            continue

        print(f'  Found {len(following)} following')

        # Store in network
        user_network[user_id] = {
            'followers': followers,
            'followees': following
        }

        # Save checkpoint every 5 users
        if i % 5 == 0:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(user_network, f, indent=2)
            print(f'  Checkpoint saved ({i}/{len(user_ids)} completed)')

        # Rate limiting pause between users
        if i < len(user_ids):
            time.sleep(2)

    # Final save
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(user_network, f, indent=2)

    print('\n' + '='*80)
    print(f'Network building complete!')
    print(f'Total users processed: {len(user_network)}')
    print(f'Output saved to: {output_file}')
    print(f'Finished at: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')

if __name__ == '__main__':
    input_file = 'userID_kalamazoo.json'
    output_file = 'user_network_kalamazoo.json'

    build_user_network(
        user_ids_file=input_file,
        output_file=output_file,
        max_followers=1000,  # Limit followers to avoid excessive API calls
        max_following=1000   # Limit following to avoid excessive API calls
    )
