"""
Location Analysis Pipeline
从 posts_english_2015-2025_all_info.json 提取用户，分析其位置信息

Pipeline 流程:
1. 提取 unique usernames
2. 获取 followers 和 followings
3. 找出 friends (互相关注)
4. 获取 friends 的 user profile
5. Geocoding friends 的 location
6. 分析并估计原始用户的位置
7. 输出最终结果
"""

import json
import os
import re
import time
import requests
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from geocoder import init_geocoder, geocode_location, geocode_single_location, get_cache, set_cache

# 线程锁，用于安全写入文件
file_lock = threading.Lock()

# Rate limiter for API calls (max requests per second)
_rate_limit_lock = threading.Lock()
_last_request_time = 0
_min_request_interval = 0.3  # Minimum 0.3 seconds between requests (per thread)

def rate_limited_wait():
    """Wait to respect rate limiting"""
    global _last_request_time
    with _rate_limit_lock:
        current_time = time.time()
        elapsed = current_time - _last_request_time
        if elapsed < _min_request_interval:
            time.sleep(_min_request_interval - elapsed)
        _last_request_time = time.time()

# ============== 配置 ==============
API_KEY = "new1_3ad6c9d6f39f4538ad669faed9fd3bbe"
BASE_URL = "https://api.twitterapi.io/twitter"
RAW_DATA_DIR = "raw_x_data"
OUTPUT_DIR = "User_Location_Analysis"

# 州名映射
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

# 美国州缩写集合
US_STATES = set(STATE_ABBREV_MAP.values())

# 需要忽略的国家级位置
COUNTRY_LEVEL = {'United States', 'United Kingdom', 'Canada', 'Australia', 'India',
                 'Deutschland', 'Danmark', 'France', 'Germany', 'Spain', 'Italy',
                 'Mexico', 'Brazil', 'Japan', 'China', 'Korea'}

# ============== 工具函数 ==============

import sys

def safe_print(text):
    """安全打印，处理 Unicode 字符"""
    try:
        print(text)
    except UnicodeEncodeError:
        print(text.encode('ascii', 'replace').decode('ascii'))
    sys.stdout.flush()

def make_api_request(url, max_retries=3):
    """发送 API 请求"""
    headers = {"X-API-Key": API_KEY}
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code == 200:
                return response.json()
            else:
                safe_print(f"  API Error: {response.status_code}")
                time.sleep(1)
        except Exception as e:
            safe_print(f"  Request Error: {e}")
            time.sleep(2)
    return None

# ============== Step 1: 提取 usernames 和 self-reported locations ==============

def extract_usernames_with_locations(city_name):
    """从 posts 文件中提取所有 unique usernames 及其 self-reported locations"""
    posts_file = os.path.join(RAW_DATA_DIR, city_name, "posts_english_2015-2025_all_info.json")

    if not os.path.exists(posts_file):
        safe_print(f"  Posts file not found: {posts_file}")
        return {}, []

    with open(posts_file, 'r', encoding='utf-8') as f:
        posts = json.load(f)

    user_locations = {}  # username -> self-reported location
    for post in posts:
        author = post.get('author', {})
        username = author.get('username')
        location = author.get('location')
        if username and username not in user_locations:
            user_locations[username] = location  # Could be None or string

    usernames = sorted(list(user_locations.keys()))
    return user_locations, usernames

def extract_usernames(city_name):
    """向后兼容：只返回 usernames 列表"""
    _, usernames = extract_usernames_with_locations(city_name)
    return usernames

# ============== Step 1.5: 分析 self-reported locations ==============

def is_vague_location(parsed_location):
    """判断 parsed location 是否模糊（国家级或无效）"""
    if not parsed_location or parsed_location == "non-location":
        return True
    # 国家级位置视为模糊
    if parsed_location in COUNTRY_LEVEL:
        return True
    # 只有州级（如 "Michigan" 或 "MI"）也视为模糊
    if parsed_location in US_STATES:
        return True
    for state_name in STATE_ABBREV_MAP.keys():
        if parsed_location.lower() == state_name:
            return True
    return False

def categorize_users_by_self_location(user_locations, city_name):
    """
    根据 self-reported location 对用户分类
    返回:
      - specific_users: dict {username: {'category': 'City-level (self-reported)', 'estimated_location': xxx}}
      - vague_users: list of usernames (需要 friend analysis)
    """
    init_geolocator(city_name)

    specific_users = {}
    vague_users = []

    total = len(user_locations)
    for i, (username, raw_location) in enumerate(user_locations.items(), 1):
        if i % 50 == 0:
            safe_print(f"  [{i}/{total}] Geocoding self-reported locations...")

        # 无 location 的用户需要 friend analysis
        if not raw_location or not raw_location.strip():
            vague_users.append(username)
            continue

        # Geocode self-reported location
        parsed = geocode_location(raw_location)

        if is_vague_location(parsed):
            vague_users.append(username)
        else:
            # 有明确位置，直接记录
            specific_users[username] = {
                'category': 'City-level (self-reported)',
                'estimated_location': parsed,
                'self_reported_raw': raw_location
            }

    return specific_users, vague_users

# ============== Step 2: 获取 followers 和 followings ==============

def get_followers(username, max_count=None):
    """获取用户的 followers

    Args:
        username: 用户名
        max_count: 最多获取多少个 (None=全部)
    """
    all_followers = []
    next_cursor = None

    while True:
        url = f"{BASE_URL}/user/followers?userName={username}&pageSize=200"
        if next_cursor:
            url += f"&cursor={next_cursor}"

        data = make_api_request(url)
        if not data:
            break

        followers = data.get('followers', [])
        all_followers.extend(followers)

        # Check if we've reached max_count
        if max_count and len(all_followers) >= max_count:
            all_followers = all_followers[:max_count]
            break

        if not data.get('has_next_page', False):
            break
        next_cursor = data.get('next_cursor')
        if not next_cursor or next_cursor == "0":
            break
        time.sleep(0.3)

    return {
        'followers': all_followers,
        'has_next_page': False,
        'status': 'success'
    }

def get_followings(username, max_count=None):
    """获取用户的 followings

    Args:
        username: 用户名
        max_count: 最多获取多少个 (None=全部)
    """
    all_followings = []
    next_cursor = None

    while True:
        url = f"{BASE_URL}/user/followings?userName={username}&pageSize=200"
        if next_cursor:
            url += f"&cursor={next_cursor}"

        data = make_api_request(url)
        if not data:
            break

        followings = data.get('followings', [])
        all_followings.extend(followings)

        # Check if we've reached max_count
        if max_count and len(all_followings) >= max_count:
            all_followings = all_followings[:max_count]
            break

        if not data.get('has_next_page', False):
            break
        next_cursor = data.get('next_cursor')
        if not next_cursor or next_cursor == "0":
            break
        time.sleep(0.3)

    return {
        'followings': all_followings,
        'has_next_page': False,
        'status': 'success'
    }

# Global variable for max fetch count (set by caller)
_max_fetch_count = None

def set_max_fetch_count(count):
    """Set max fetch count for followers/followings"""
    global _max_fetch_count
    _max_fetch_count = count

def fetch_single_user(username):
    """获取单个用户的 followers 和 followings（用于多线程）"""
    # Rate limit to avoid API throttling
    rate_limited_wait()
    followers = get_followers(username, _max_fetch_count)
    rate_limited_wait()
    followings = get_followings(username, _max_fetch_count)
    return username, followers, followings

def fetch_all_followers_followings(usernames, city_name, num_threads=1):
    """获取所有用户的 followers 和 followings

    Args:
        usernames: 用户名列表
        city_name: 城市名
        num_threads: 线程数（默认1=串行，>1=并行）
    """
    city_dir = os.path.join(RAW_DATA_DIR, city_name)
    os.makedirs(city_dir, exist_ok=True)

    followers_file = os.path.join(city_dir, f"follower_{city_name}.json")
    followings_file = os.path.join(city_dir, f"following_{city_name}.json")

    # 加载已有数据（如果存在）
    all_followers = {}
    all_followings = {}

    if os.path.exists(followers_file):
        with open(followers_file, 'r', encoding='utf-8') as f:
            all_followers = json.load(f)
        safe_print(f"  Loaded {len(all_followers)} existing followers records")

    if os.path.exists(followings_file):
        with open(followings_file, 'r', encoding='utf-8') as f:
            all_followings = json.load(f)
        safe_print(f"  Loaded {len(all_followings)} existing followings records")

    # 找出需要获取的用户
    users_to_fetch = [u for u in usernames if u not in all_followers or u not in all_followings]

    if not users_to_fetch:
        safe_print(f"  All data already fetched, skipping...")
        return all_followers, all_followings

    safe_print(f"  Need to fetch: {len(users_to_fetch)} users")
    safe_print(f"  Using {num_threads} thread(s)")

    if num_threads <= 1:
        # 串行处理
        for i, username in enumerate(users_to_fetch, 1):
            safe_print(f"  [{i}/{len(users_to_fetch)}] Fetching {username}...")

            if username not in all_followers:
                all_followers[username] = get_followers(username, _max_fetch_count)
                time.sleep(0.5)

            if username not in all_followings:
                all_followings[username] = get_followings(username, _max_fetch_count)
                time.sleep(0.5)

            # 每10条保存一次
            if i % 10 == 0:
                safe_print(f"  [Checkpoint] Saving progress ({len(all_followers)} records)...")
                with open(followers_file, 'w', encoding='utf-8') as f:
                    json.dump(all_followers, f, ensure_ascii=False, indent=2)
                with open(followings_file, 'w', encoding='utf-8') as f:
                    json.dump(all_followings, f, ensure_ascii=False, indent=2)
    else:
        # 并行处理
        completed = 0
        start_time = time.time()
        last_print_time = start_time

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = {executor.submit(fetch_single_user, u): u for u in users_to_fetch}

            for future in as_completed(futures):
                username = futures[future]
                try:
                    username, followers, followings = future.result()

                    with file_lock:
                        all_followers[username] = followers
                        all_followings[username] = followings
                        completed += 1

                        current_time = time.time()
                        # 每 5 秒或每 10 个打印一次进度
                        if completed % 10 == 0 or completed == len(users_to_fetch) or (current_time - last_print_time) >= 5:
                            elapsed = current_time - start_time
                            speed = completed / elapsed if elapsed > 0 else 0
                            remaining = len(users_to_fetch) - completed
                            eta_seconds = remaining / speed if speed > 0 else 0
                            eta_min = int(eta_seconds // 60)
                            eta_sec = int(eta_seconds % 60)

                            pct = (completed / len(users_to_fetch)) * 100
                            safe_print(f"  Progress: {completed}/{len(users_to_fetch)} ({pct:.1f}%) | Speed: {speed:.2f} users/sec | ETA: {eta_min}m {eta_sec}s")

                            last_print_time = current_time

                        # 每 20 个保存一次
                        if completed % 20 == 0 or completed == len(users_to_fetch):
                            # 先写入临时文件，再重命名（原子操作，更安全）
                            temp_followers = followers_file + '.tmp'
                            temp_followings = followings_file + '.tmp'
                            with open(temp_followers, 'w', encoding='utf-8') as f:
                                json.dump(all_followers, f, ensure_ascii=False, indent=2)
                            with open(temp_followings, 'w', encoding='utf-8') as f:
                                json.dump(all_followings, f, ensure_ascii=False, indent=2)
                            os.replace(temp_followers, followers_file)
                            os.replace(temp_followings, followings_file)

                except Exception as e:
                    safe_print(f"  Error fetching {username}: {e}")

        # 显示完成统计
        total_time = time.time() - start_time
        avg_speed = len(users_to_fetch) / total_time if total_time > 0 else 0
        safe_print(f"  [Done] {len(users_to_fetch)} users in {total_time:.1f}s (avg: {avg_speed:.2f} users/sec)")

    # 最终保存
    safe_print(f"  [Final] Saving all data...")
    with open(followers_file, 'w', encoding='utf-8') as f:
        json.dump(all_followers, f, ensure_ascii=False, indent=2)
    with open(followings_file, 'w', encoding='utf-8') as f:
        json.dump(all_followings, f, ensure_ascii=False, indent=2)

    return all_followers, all_followings

# ============== Step 3: 找出 friends ==============

def find_friends_with_profiles(followers_data, followings_data):
    """找出同时是 follower 和 following 的用户，并返回他们的 profile 数据"""
    # Build lookup dictionaries from followers and followings
    follower_profiles = {}
    for user in followers_data.get("followers", []):
        username = user.get("userName") or user.get("screen_name")
        if username:
            follower_profiles[username] = user

    following_profiles = {}
    for user in followings_data.get("followings", []):
        username = user.get("userName") or user.get("screen_name")
        if username:
            following_profiles[username] = user

    # Find mutual follows (friends)
    friend_usernames = set(follower_profiles.keys()) & set(following_profiles.keys())

    # Return friend profiles (prefer follower data, fallback to following)
    friend_profiles = {}
    for username in friend_usernames:
        friend_profiles[username] = follower_profiles.get(username) or following_profiles.get(username)

    return friend_profiles

def find_friends(followers_data, followings_data):
    """找出同时是 follower 和 following 的用户（仅返回用户名，保持向后兼容）"""
    return set(find_friends_with_profiles(followers_data, followings_data).keys())

# ============== Step 4: 获取 friend profiles ==============

def get_user_profile(username):
    """获取用户的 profile 信息"""
    url = f"{BASE_URL}/user/info?userName={username}"
    return make_api_request(url)

def fetch_friend_profiles(usernames, all_followers, all_followings, city_name):
    """从已获取的 followers/followings 数据中提取 friend profiles（无需额外 API 调用）"""
    output_dir = f"{city_name}_friend-info"
    os.makedirs(output_dir, exist_ok=True)

    for i, username in enumerate(usernames, 1):
        output_file = os.path.join(output_dir, f"{username}.json")

        # 如果已存在，跳过
        if os.path.exists(output_file):
            continue

        safe_print(f"  [{i}/{len(usernames)}] Extracting friend profiles for {username}...")

        followers_data = all_followers.get(username, {})
        followings_data = all_followings.get(username, {})

        # Get friend profiles directly from existing data (no API calls needed)
        friend_profiles = find_friends_with_profiles(followers_data, followings_data)

        safe_print(f"      Found {len(friend_profiles)} friends")

        # 保存该用户的所有 friend profiles
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(friend_profiles, f, ensure_ascii=False, indent=2)

# ============== Step 5: Geocoding ==============

current_city_cache_file = None  # 当前城市的缓存文件

def load_geocode_cache(city_name=None):
    """Load geocode cache from all cache files (merge them)

    缓存文件格式: geocode_cache_{city}.json
    读取时合并所有缓存文件，写入时只写当前城市的缓存
    """
    global current_city_cache_file
    import glob

    location_cache = {}

    # 设置当前城市的缓存文件
    if city_name:
        current_city_cache_file = f"geocode_cache_{city_name}.json"

    # 加载所有 geocode_cache_*.json 文件
    cache_files = glob.glob("geocode_cache_*.json")

    # 也加载旧的 geocode_cache.json（如果存在）
    if os.path.exists("geocode_cache.json"):
        cache_files.append("geocode_cache.json")

    for cache_file in cache_files:
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                location_cache.update(data)
                safe_print(f"  Loaded {len(data)} entries from {cache_file}")
        except Exception as e:
            safe_print(f"  Warning: Could not load {cache_file}: {e}")

    safe_print(f"  Total cached entries: {len(location_cache)} (from {len(cache_files)} files)")

    # 设置 geocoder 模块的缓存
    set_cache(location_cache)

def save_geocode_cache():
    """Save geocode cache to current city's cache file"""
    global current_city_cache_file

    location_cache = get_cache()

    if not current_city_cache_file:
        current_city_cache_file = "geocode_cache.json"

    try:
        with open(current_city_cache_file, 'w', encoding='utf-8') as f:
            json.dump(location_cache, f, ensure_ascii=False, indent=2)
        safe_print(f"  Saved {len(location_cache)} entries to {current_city_cache_file}")
    except Exception as e:
        safe_print(f"  Warning: Could not save cache: {e}")

def init_geolocator(city_name):
    """初始化 geocoder (使用 Google Maps API)"""
    init_geocoder()

def collect_unique_locations(usernames, city_name):
    """Collect all unique location strings from friend profiles"""
    input_dir = f"{city_name}_friend-info"
    unique_locations = set()

    for username in usernames:
        input_file = os.path.join(input_dir, f"{username}.json")
        if not os.path.exists(input_file):
            continue

        with open(input_file, 'r', encoding='utf-8') as f:
            friend_profiles = json.load(f)

        for profile_data in friend_profiles.values():
            if 'data' in profile_data:
                location_str = profile_data['data'].get('location', '')
            else:
                location_str = profile_data.get('location', '')

            if location_str and location_str.strip():
                unique_locations.add(location_str.strip())

    return unique_locations

def batch_geocode_locations(unique_locations, city_name):
    """Geocode all unique locations, using cache when possible"""
    # Load persistent cache (reads all cache files, writes to city-specific file)
    load_geocode_cache(city_name)

    # Get cache from geocoder module
    location_cache = get_cache()

    # Find locations not yet cached
    to_geocode = [loc for loc in unique_locations if loc not in location_cache]

    safe_print(f"  Total unique locations: {len(unique_locations)}")
    safe_print(f"  Already cached: {len(unique_locations) - len(to_geocode)}")
    safe_print(f"  Need to geocode: {len(to_geocode)}")

    if not to_geocode:
        safe_print(f"  All locations already cached!")
        return

    # Initialize geolocator
    init_geolocator(city_name)

    # Geocode new locations with progress
    for i, location_str in enumerate(to_geocode, 1):
        result = geocode_single_location(location_str)
        location_cache[location_str] = result

        # Progress update every 10 or at specific percentages
        if i % 10 == 0 or i == len(to_geocode):
            pct = (i / len(to_geocode)) * 100
            safe_print(f"  Geocoding progress: {i}/{len(to_geocode)} ({pct:.1f}%)")

            # Save cache periodically (every 50)
            if i % 50 == 0:
                save_geocode_cache()

    # Final save
    save_geocode_cache()
    safe_print(f"  Geocoding complete. Cache now has {len(location_cache)} entries.")

def geocode_friend_locations(usernames, city_name):
    """Geocode all friends' locations using batch processing and persistent cache"""
    input_dir = f"{city_name}_friend-info"
    output_dir = f"{city_name}_friend-location"
    os.makedirs(output_dir, exist_ok=True)

    # Step 1: Collect all unique locations
    safe_print(f"  [Phase 1] Collecting unique locations...")
    unique_locations = collect_unique_locations(usernames, city_name)

    # Step 2: Batch geocode (uses persistent cache)
    safe_print(f"  [Phase 2] Batch geocoding unique locations...")
    batch_geocode_locations(unique_locations, city_name)

    # Step 3: Apply geocoded results to all users
    safe_print(f"  [Phase 3] Applying results to users...")
    location_cache = get_cache()  # Get cache from geocoder module

    for i, username in enumerate(usernames, 1):
        input_file = os.path.join(input_dir, f"{username}.json")
        output_file = os.path.join(output_dir, f"{username}.json")

        # Skip if already exists
        if os.path.exists(output_file):
            continue

        if not os.path.exists(input_file):
            continue

        with open(input_file, 'r', encoding='utf-8') as f:
            friend_profiles = json.load(f)

        # Apply cached geocode results (instant, no API calls)
        friend_locations = {}
        for friend_username, profile_data in friend_profiles.items():
            if 'data' in profile_data:
                location_str = profile_data['data'].get('location', '')
            else:
                location_str = profile_data.get('location', '')

            location_str = location_str.strip() if location_str else ''
            if location_str and location_str in location_cache:
                friend_locations[friend_username] = location_cache[location_str]
            else:
                friend_locations[friend_username] = "non-location"

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(friend_locations, f, ensure_ascii=False, indent=2)

    safe_print(f"  Applied results to {len(usernames)} users.")

# ============== Step 6: 分析并估计位置 ==============

def extract_state(location):
    """从位置字符串中提取美国州缩写"""
    match = re.search(r',\s*([A-Z]{2})$', location)
    if match and match.group(1) in US_STATES:
        return match.group(1)
    return None

def infer_location_from_username(username, city_name):
    """从用户名推断位置"""
    username_lower = username.lower()
    city_lower = city_name.lower()

    # 常见的城市名模式
    city_patterns = {
        'kalamazoo': 'Kalamazoo, MI',
        'battlecreek': 'Battle Creek, MI',
        'baltimore': 'Baltimore, MD',
        'buffalo': 'Buffalo, NY',
        'elpaso': 'El Paso, TX',
        'el_paso': 'El Paso, TX',
        'fayetteville': 'Fayetteville, NC',
        'portland': 'Portland, OR',
        'rockford': 'Rockford, IL',
        'sanfrancisco': 'San Francisco, CA',
        'san_francisco': 'San Francisco, CA',
        'scranton': 'Scranton, PA',
        'southbend': 'South Bend, IN',
        'south_bend': 'South Bend, IN',
    }

    for pattern, location in city_patterns.items():
        if pattern in username_lower:
            return location

    return None

def analyze_user_location(username, friend_locations, city_name):
    """分析单个用户的位置"""
    total_friends = len(friend_locations)

    # 没有 friends
    if total_friends == 0:
        # 尝试从用户名推断
        inferred = infer_location_from_username(username, city_name)
        if inferred:
            return {
                'category': 'Inferred from username',
                'estimated_location': inferred
            }
        return {
            'category': 'Bot',
            'estimated_location': None
        }

    # 过滤位置
    all_locations = [loc for loc in friend_locations.values() if loc != 'non-location']
    city_locations = [loc for loc in all_locations if loc not in COUNTRY_LEVEL]

    # 所有 friends 都没有 location
    if len(all_locations) == 0:
        inferred = infer_location_from_username(username, city_name)
        if inferred:
            return {
                'category': 'Inferred from username',
                'estimated_location': inferred
            }
        return {
            'category': 'Bot',
            'estimated_location': None
        }

    # 有 location 但全是国家级
    if len(city_locations) == 0:
        inferred = infer_location_from_username(username, city_name)
        if inferred:
            return {
                'category': 'Inferred from username',
                'estimated_location': inferred
            }
        return {
            'category': 'Bot',
            'estimated_location': None
        }

    # 统计城市级位置
    location_counts = Counter(city_locations)
    top_location, top_count = location_counts.most_common(1)[0]

    # 有明显的主导城市
    if top_count >= 2:
        return {
            'category': 'City-level',
            'estimated_location': top_location
        }

    # 只有 1 次但总数很少
    if top_count == 1 and len(city_locations) <= 3:
        return {
            'category': 'City-level',
            'estimated_location': top_location
        }

    # 尝试州级聚合
    states = []
    for loc in city_locations:
        state = extract_state(loc)
        if state:
            states.append(state)

    if states:
        state_counts = Counter(states)
        top_state, top_state_count = state_counts.most_common(1)[0]

        if top_state_count >= 2:
            # 州名映射
            state_names = {v: k.title() for k, v in STATE_ABBREV_MAP.items()}
            state_name = state_names.get(top_state, top_state)
            return {
                'category': 'State-level',
                'estimated_location': state_name
            }

    # 位置极度分散，尝试从用户名推断
    inferred = infer_location_from_username(username, city_name)
    if inferred:
        return {
            'category': 'Inferred from username',
            'estimated_location': inferred
        }

    # 无法确定
    return {
        'category': 'Bot',
        'estimated_location': None
    }

def generate_final_analysis(usernames, city_name):
    """生成最终分析结果"""
    location_dir = f"{city_name}_friend-location"

    results = {}

    for username in usernames:
        location_file = os.path.join(location_dir, f"{username}.json")

        if os.path.exists(location_file):
            with open(location_file, 'r', encoding='utf-8') as f:
                friend_locations = json.load(f)
        else:
            friend_locations = {}

        analysis = analyze_user_location(username, friend_locations, city_name)
        results[username] = analysis

    return results

# ============== 主流程 ==============

def run_pipeline(city_name):
    """运行完整的分析流程"""
    safe_print(f"\n{'='*60}")
    safe_print(f"Processing: {city_name.upper()}")
    safe_print(f"{'='*60}")

    # Step 1: 提取 usernames 和 self-reported locations
    safe_print(f"\n[Step 1] Extracting usernames and self-reported locations...")
    user_locations, usernames = extract_usernames_with_locations(city_name)
    safe_print(f"  Found {len(usernames)} unique usernames")

    if not usernames:
        safe_print(f"  No usernames found, skipping...")
        return

    # 保存 usernames
    city_dir = os.path.join(RAW_DATA_DIR, city_name)
    username_file = os.path.join(city_dir, f"username_{city_name}.json")
    with open(username_file, 'w', encoding='utf-8') as f:
        json.dump(usernames, f, ensure_ascii=False, indent=2)

    # Step 1.5: 分析 self-reported locations，筛选需要 friend analysis 的用户
    safe_print(f"\n[Step 1.5] Analyzing self-reported locations...")
    specific_users, vague_users = categorize_users_by_self_location(user_locations, city_name)
    safe_print(f"  Users with specific self-reported location: {len(specific_users)}")
    safe_print(f"  Users needing friend analysis: {len(vague_users)}")

    # 如果没有需要 friend analysis 的用户，直接输出结果
    if not vague_users:
        safe_print(f"\n  All users have specific locations, skipping friend analysis...")
        results = specific_users
    else:
        # Step 2: 获取 followers/followings (只对 vague_users)
        safe_print(f"\n[Step 2] Fetching followers and followings for {len(vague_users)} users...")
        all_followers, all_followings = fetch_all_followers_followings(vague_users, city_name)

        # Step 3 & 4: 获取 friend profiles (只对 vague_users)
        safe_print(f"\n[Step 3-4] Fetching friend profiles...")
        fetch_friend_profiles(vague_users, all_followers, all_followings, city_name)

        # Step 5: Geocoding (只对 vague_users)
        safe_print(f"\n[Step 5] Geocoding friend locations...")
        geocode_friend_locations(vague_users, city_name)

        # Step 6: 分析并生成结果 (只对 vague_users)
        safe_print(f"\n[Step 6] Analyzing and generating results...")
        friend_analysis_results = generate_final_analysis(vague_users, city_name)

        # 合并结果: specific_users + friend_analysis_results
        results = {**specific_users, **friend_analysis_results}

    # 保存结果
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_file = os.path.join(OUTPUT_DIR, f"{city_name.title()}_user_location.json")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    # 统计
    categories = Counter(r['category'] for r in results.values())
    safe_print(f"\n[Summary]")
    safe_print(f"  Total users: {len(results)}")
    for cat, count in categories.most_common():
        safe_print(f"  - {cat}: {count}")
    safe_print(f"  Output: {output_file}")

def main():
    """主函数 - 支持命令行参数指定城市"""
    import argparse

    all_cities = [
        'baltimore',
        'buffalo',
        'el paso',
        'fayetteville',
        'portland',
        'rockford',
        'san_francisco',
        'scranton',
        'southbend'
    ]

    parser = argparse.ArgumentParser(description='Location Analysis Pipeline')
    parser.add_argument('--cities', type=str, help='Comma-separated city names, e.g., "baltimore,buffalo"')
    parser.add_argument('--range', type=str, help='City range, e.g., "1-5" for first 5 cities')
    args = parser.parse_args()

    # 确定要处理的城市
    if args.cities:
        cities = [c.strip() for c in args.cities.split(',')]
    elif args.range:
        start, end = map(int, args.range.split('-'))
        cities = all_cities[start-1:end]
    else:
        cities = all_cities

    safe_print("="*60)
    safe_print("LOCATION ANALYSIS PIPELINE")
    safe_print("="*60)
    safe_print(f"Cities to process: {cities}")

    for i, city in enumerate(cities, 1):
        safe_print(f"\n>>> Processing city {i}/{len(cities)}: {city.upper()}")
        try:
            run_pipeline(city)
        except Exception as e:
            safe_print(f"\nError processing {city}: {e}")
            import traceback
            traceback.print_exc()
            continue

    safe_print("\n" + "="*60)
    safe_print("ALL DONE!")
    safe_print("="*60)

if __name__ == "__main__":
    main()
