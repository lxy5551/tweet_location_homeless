import json
import requests
import time
from datetime import datetime

# API configuration
API_URL = "https://api.twitterapi.io/twitter/user/followings"
API_KEY = "new1_3ad6c9d6f39f4538ad669faed9fd3bbe"

def get_followings(username, page_size=200):
    """Get followings for a username using third-party API"""
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
    output_file = 'following_kalamazoo.json'

    with open(input_file, 'r', encoding='utf-8') as f:
        usernames = json.load(f)

    print(f'Fetching followings for {len(usernames)} users...')
    print(f'Started at: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print('='*80)

    followings_data = {}

    for i, username in enumerate(usernames, 1):
        print(f'\n[{i}/{len(usernames)}] Fetching followings for: {username}')

        result = get_followings(username)

        if result is not None:
            followings_data[username] = result

            # Try to extract following count if available
            if isinstance(result, dict):
                if 'followings' in result:
                    following_count = len(result.get('followings', []))
                    print(f'  Found {following_count} followings')
                else:
                    print(f'  Response received (check structure)')
            else:
                print(f'  Response received')
        else:
            print(f'  Failed to fetch followings')
            followings_data[username] = None

        # Save checkpoint every 5 users
        if i % 5 == 0:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(followings_data, f, indent=2, ensure_ascii=False)
            print(f'  Checkpoint saved ({i}/{len(usernames)} completed)')

        # Rate limiting pause
        if i < len(usernames):
            time.sleep(2)  # 2 seconds between requests

    # Final save
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(followings_data, f, indent=2, ensure_ascii=False)

    print('\n' + '='*80)
    print(f'Followings fetching complete!')
    print(f'Total users processed: {len(followings_data)}')
    print(f'Successful: {sum(1 for v in followings_data.values() if v is not None)}')
    print(f'Failed: {sum(1 for v in followings_data.values() if v is None)}')
    print(f'Output saved to: {output_file}')
    print(f'Finished at: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')

if __name__ == '__main__':
    main()
