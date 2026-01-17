"""
Google Maps Geocoding Module
快速geocoding，替代Nominatim的1秒延迟限制

使用方法:
1. 设置环境变量 GOOGLE_MAPS_API_KEY 或直接修改下面的 API_KEY
2. from geocoder import init_geocoder, geocode_location, geocode_single_location

价格: $5/1000次请求
"""

import os
import time
import googlemaps
from googlemaps.exceptions import ApiError, Timeout, TransportError

# ============== 配置 ==============
# 优先从环境变量读取，否则在这里设置
API_KEY = os.environ.get('GOOGLE_MAPS_API_KEY', 'AIzaSyCknj2L5uttjNk7s2gXItPwvIRavxTEaoU')

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

# 全局变量
gmaps_client = None
location_cache = {}

def init_geocoder(api_key=None):
    """初始化 Google Maps 客户端"""
    global gmaps_client
    key = api_key or API_KEY
    if key == 'YOUR_API_KEY_HERE':
        raise ValueError("请设置 Google Maps API Key! 可以通过环境变量 GOOGLE_MAPS_API_KEY 或直接修改 geocoder.py")
    gmaps_client = googlemaps.Client(key=key)
    print(f"Google Maps Geocoder 已初始化")

def get_cache():
    """获取当前缓存"""
    return location_cache

def set_cache(cache):
    """设置缓存（用于加载持久化缓存）"""
    global location_cache
    location_cache = cache

def _parse_google_result(result):
    """解析 Google Maps geocoding 结果"""
    if not result:
        return "non-location"

    # 提取地址组件
    address_components = result.get('address_components', [])

    city = None
    state = None
    country = None
    country_code = None

    for component in address_components:
        types = component.get('types', [])

        if 'locality' in types:
            city = component.get('long_name')
        elif 'administrative_area_level_2' in types and not city:
            # 县级（作为城市的备选）
            city = component.get('long_name')
        elif 'administrative_area_level_1' in types:
            state = component.get('long_name')
        elif 'country' in types:
            country = component.get('long_name')
            country_code = component.get('short_name')

    # 格式化结果
    if city and country_code == 'US':
        # 美国地址：City, ST
        if state:
            state_lower = state.lower()
            state_abbrev = None
            for full_name, abbrev in STATE_ABBREV_MAP.items():
                if full_name in state_lower:
                    state_abbrev = abbrev
                    break
            if state_abbrev:
                return f'{city}, {state_abbrev}'
        return f'{city}, USA'
    elif city and country:
        return f'{city}, {country}'
    elif country:
        return country
    else:
        return "non-location"

def geocode_single_location(location_str, max_retries=2):
    """Geocode 单个位置字符串（不检查缓存）"""
    global gmaps_client

    if gmaps_client is None:
        init_geocoder()

    for attempt in range(max_retries):
        try:
            # Google Maps API 没有严格的速率限制，但加一点延迟避免burst
            time.sleep(0.05)  # 50ms，比 Nominatim 的 1100ms 快 22 倍

            results = gmaps_client.geocode(location_str)

            if results:
                return _parse_google_result(results[0])
            else:
                return "non-location"

        except (ApiError, Timeout, TransportError) as e:
            if attempt < max_retries - 1:
                time.sleep(0.5)
                continue
            print(f"  Geocoding error for '{location_str}': {e}")
            return "non-location"
        except Exception as e:
            print(f"  Unexpected error for '{location_str}': {e}")
            return "non-location"

    return "non-location"

def geocode_location(location_str):
    """Geocode 位置字符串（带缓存）"""
    global location_cache

    if not location_str or not location_str.strip():
        return "non-location"

    location_str = location_str.strip()

    # 检查缓存
    if location_str in location_cache:
        return location_cache[location_str]

    # Geocode 并缓存
    result = geocode_single_location(location_str)
    location_cache[location_str] = result
    return result


# ============== 兼容性函数（用于替换 Nominatim）==============

def init_geolocator(city_name=None):
    """兼容旧代码的初始化函数"""
    init_geocoder()

# 测试
if __name__ == "__main__":
    # 测试前请设置 API_KEY
    init_geocoder()

    test_locations = [
        "San Francisco, CA",
        "New York City",
        "Portland, Oregon",
        "London, UK",
        "asdfghjkl",  # 无效位置
    ]

    for loc in test_locations:
        result = geocode_location(loc)
        print(f"'{loc}' -> '{result}'")
