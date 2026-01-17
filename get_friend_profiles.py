import json
import os
import requests
import time

API_KEY = "new1_3ad6c9d6f39f4538ad669faed9fd3bbe"
BASE_URL = "https://api.twitterapi.io/twitter/user"

def get_user_profile(username):
    """获取用户的 profile 信息"""
    url = f"{BASE_URL}/info?userName={username}"
    headers = {"X-API-Key": API_KEY}

    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"  Error fetching {username}: {response.status_code}")
            return None
    except Exception as e:
        print(f"  Exception fetching {username}: {e}")
        return None

def find_friends(followers_data, followings_data):
    """找出同时是 follower 和 following 的用户（即 friends）"""
    follower_usernames = set()
    for user in followers_data.get("followers", []):
        follower_usernames.add(user.get("userName") or user.get("screen_name"))

    following_usernames = set()
    for user in followings_data.get("followings", []):
        following_usernames.add(user.get("userName") or user.get("screen_name"))

    # 互相关注的用户
    friends = follower_usernames & following_usernames
    return friends

def main():
    # 读取数据文件
    print("Loading data files...")

    with open("username_kalamazoo.json", "r", encoding="utf-8") as f:
        usernames = json.load(f)

    with open("follower_kalamazoo.json", "r", encoding="utf-8") as f:
        all_followers = json.load(f)

    with open("following_kalamazoo.json", "r", encoding="utf-8") as f:
        all_followings = json.load(f)

    # 创建输出目录
    output_dir = "kalamazoo_friend-info"
    os.makedirs(output_dir, exist_ok=True)

    print(f"Processing {len(usernames)} original users...\n")

    for username in usernames:
        print(f"Processing: {username}")

        # 获取该用户的 followers 和 followings 数据
        followers_data = all_followers.get(username, {})
        followings_data = all_followings.get(username, {})

        # 找出 friends
        friends = find_friends(followers_data, followings_data)

        if not friends:
            print(f"  No friends found for {username}")
            # 即使没有 friends 也创建空文件
            output_file = os.path.join(output_dir, f"{username}.json")
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump({}, f, ensure_ascii=False, indent=2)
            continue

        print(f"  Found {len(friends)} friends: {friends}")

        # 获取每个 friend 的 user_profile
        friend_profiles = {}
        for friend_username in friends:
            if not friend_username:
                continue
            print(f"  Fetching profile for: {friend_username}")
            profile = get_user_profile(friend_username)
            if profile:
                friend_profiles[friend_username] = profile
            time.sleep(0.5)  # 避免请求过快

        # 保存结果
        output_file = os.path.join(output_dir, f"{username}.json")
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(friend_profiles, f, ensure_ascii=False, indent=2)

        print(f"  Saved to {output_file}\n")

if __name__ == "__main__":
    main()
