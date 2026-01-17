import json
import os
import time
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

# 初始化 geocoder
geolocator = Nominatim(user_agent="kalamazoo_friend_location_geocoder_v1")

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

def geocode_location(location, max_retries=2):
    """对 location 进行地理编码"""
    if not location or not location.strip():
        return None

    for attempt in range(max_retries):
        try:
            time.sleep(1.1)  # Respect rate limit
            location_obj = geolocator.geocode(location, timeout=10, addressdetails=True)
            return location_obj
        except (GeocoderTimedOut, GeocoderServiceError) as e:
            if attempt < max_retries - 1:
                time.sleep(2)
                continue
            return None
    return None

def parse_location(location_str):
    """从 location 字符串解析标准化的地理位置"""
    if not location_str or not location_str.strip():
        return "non-location"

    location_str = location_str.strip()

    # 尝试 geocoding
    result = geocode_location(location_str)

    if result:
        address = result.raw.get('address', {})
        city = address.get('city') or address.get('town') or address.get('village') or address.get('county')
        state = address.get('state')
        country = address.get('country')

        if city and country == 'United States':
            state_abbrev = None
            if state:
                state_lower = state.lower()
                for full_name, abbrev in STATE_ABBREV_MAP.items():
                    if full_name in state_lower:
                        state_abbrev = abbrev
                        break
            return f'{city}, {state_abbrev}' if state_abbrev else f'{city}, USA'
        elif city and country:
            return f'{city}, {country}'
        elif country:
            return country
        else:
            return "non-location"
    else:
        return "non-location"

def safe_print(text):
    """安全打印，处理 Unicode 字符"""
    try:
        print(text)
    except UnicodeEncodeError:
        print(text.encode('ascii', 'replace').decode('ascii'))

def main():
    input_dir = "kalamazoo_friend-info"
    output_dir = "kalamazoo_friend-location"

    os.makedirs(output_dir, exist_ok=True)

    # 获取所有输入文件
    input_files = [f for f in os.listdir(input_dir) if f.endswith('.json')]

    safe_print(f"Processing {len(input_files)} files...\n")

    # 用于缓存已解析的 location，避免重复 geocoding
    location_cache = {}

    for file_idx, filename in enumerate(input_files, 1):
        safe_print(f"[{file_idx}/{len(input_files)}] Processing: {filename}")

        input_path = os.path.join(input_dir, filename)
        output_path = os.path.join(output_dir, filename)

        with open(input_path, 'r', encoding='utf-8') as f:
            friend_profiles = json.load(f)

        friend_locations = {}

        for friend_username, profile_data in friend_profiles.items():
            # 从 profile 中提取 location
            # profile 结构可能是 {"status": "success", "data": {...}} 或直接是 profile
            if 'data' in profile_data:
                location_str = profile_data['data'].get('location', '')
            else:
                location_str = profile_data.get('location', '')

            # 检查缓存
            if location_str in location_cache:
                parsed_location = location_cache[location_str]
                safe_print(f"  {friend_username}: '{location_str}' -> {parsed_location} (cached)")
            else:
                # 解析 location
                parsed_location = parse_location(location_str)
                location_cache[location_str] = parsed_location
                safe_print(f"  {friend_username}: '{location_str}' -> {parsed_location}")

            friend_locations[friend_username] = parsed_location

        # 保存结果
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(friend_locations, f, ensure_ascii=False, indent=2)

        safe_print(f"  Saved to {output_path}\n")

    safe_print("=" * 60)
    safe_print("All files processed!")
    safe_print(f"Output directory: {output_dir}")
    safe_print(f"Location cache size: {len(location_cache)} unique locations")

if __name__ == "__main__":
    main()
