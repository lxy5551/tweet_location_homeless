import json
import requests
import time
from datetime import datetime

# API configuration
API_URL = "https://api.twitterapi.io/twitter/user/followers"
API_KEY = "new1_3ad6c9d6f39f4538ad669faed9fd3bbe"

def get_followers(username, page_size=200):
    """Get followers for a username using third-party API"""
    url = f"{API_URL}?pageSize={page_size}&userName={username}"
    headers = {"X-API-Key": API_KEY}

    try:
        response = requests.get(url, headers=headers, timeout=30)

        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print(f'  Error: {response.status_code} - {response.text[:200]}')
            return None
    except Exception as e:
        print(f'  Exception: {str(e)}')
        return None

def main():
    # Load usernames
    input_file = 'username_kalamazoo.json'
    output_file = 'follower_kalamazoo.json'

    with open(input_file, 'r', encoding='utf-8') as f:
        usernames = json.load(f)

    print(f'Fetching followers for {len(usernames)} users...')
    print(f'Started at: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print('='*80)

    followers_data = {}

    for i, username in enumerate(usernames, 1):
        print(f'\n[{i}/{len(usernames)}] Fetching followers for: {username}')

        result = get_followers(username)

        if result is not None:
            followers_data[username] = result

            # Try to extract follower count if available
            if isinstance(result, dict):
                if 'data' in result:
                    follower_count = len(result.get('data', []))
                    print(f'  Found {follower_count} followers')
                else:
                    print(f'  Response received (check structure)')
            else:
                print(f'  Response received')
        else:
            print(f'  Failed to fetch followers')
            followers_data[username] = None

        # Save checkpoint every 5 users
        if i % 5 == 0:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(followers_data, f, indent=2, ensure_ascii=False)
            print(f'  Checkpoint saved ({i}/{len(usernames)} completed)')

        # Rate limiting pause
        if i < len(usernames):
            time.sleep(2)  # 2 seconds between requests

    # Final save
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(followers_data, f, indent=2, ensure_ascii=False)

    print('\n' + '='*80)
    print(f'Followers fetching complete!')
    print(f'Total users processed: {len(followers_data)}')
    print(f'Successful: {sum(1 for v in followers_data.values() if v is not None)}')
    print(f'Failed: {sum(1 for v in followers_data.values() if v is None)}')
    print(f'Output saved to: {output_file}')
    print(f'Finished at: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')

if __name__ == '__main__':
    main()
