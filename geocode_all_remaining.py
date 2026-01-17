import json
import time
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
import sys

print('='*80)
print('GEOCODING ALL REMAINING LOCATIONS FOR SAN FRANCISCO')
print('='*80)

# 加载当前数据
with open('sf_cities_step2.json', 'r', encoding='utf-8') as f:
    cities_data = json.load(f)

# 初始化geocoder
geolocator = Nominatim(user_agent="sf_location_cleaner_comprehensive_v2")

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

# 找到所有需要geocoding的项目（还没有standardized字段的valid locations）
to_geocode = []
for item in cities_data:
    if not item['is_junk'] and 'standardized' not in item:
        to_geocode.append(item)

# 按count排序（先处理高频的）
to_geocode.sort(key=lambda x: x['count'], reverse=True)

total = len(to_geocode)
print(f'\nTotal locations to geocode: {total}')
print(f'Estimated time: {total * 1.2 / 60:.1f} minutes')
print(f'\nStarting at: {time.strftime("%Y-%m-%d %H:%M:%S")}')
print('='*80 + '\n')

# Geocode所有
success_count = 0
fail_count = 0
checkpoint_interval = 100  # 每100个保存一次

for i, item in enumerate(to_geocode, 1):
    location = item['cleaned']
    
    # 显示进度（每10个或最后几个）
    if i % 10 == 0 or i <= 5 or i > total - 5:
        elapsed = (i-1) * 1.2 / 60
        remaining = (total - i) * 1.2 / 60
        print(f'[{i}/{total}] ({i/total*100:.1f}%) | Elapsed: {elapsed:.1f}min | ETA: {remaining:.1f}min')
        print(f'  Geocoding: {location[:60]}')
        sys.stdout.flush()
    
    # Geocode
    result = geocode_location(location)
    
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
            
            item['standardized'] = f'{city}, {state_abbrev}' if state_abbrev else city
            item['geocoded'] = True
            item['type'] = 'geocoded_us'
            success_count += 1
        elif city and country:
            item['standardized'] = f'{city}, {country}'
            item['geocoded'] = True
            item['type'] = 'geocoded_international'
            success_count += 1
        else:
            # Geocoded但没有足够信息
            item['geocoded'] = False
            item['type'] = 'geocoded_insufficient'
            fail_count += 1
    else:
        item['geocoded'] = False
        item['type'] = 'geocoding_failed'
        fail_count += 1
    
    # 定期保存checkpoint
    if i % checkpoint_interval == 0:
        with open('sf_cities_geocoding_progress.json', 'w', encoding='utf-8') as f:
            json.dump(cities_data, f, indent=2, ensure_ascii=False)
        print(f'  >>> Checkpoint saved. Success: {success_count}, Failed: {fail_count}')
        sys.stdout.flush()

# 最终保存
with open('sf_cities_all_geocoded.json', 'w', encoding='utf-8') as f:
    json.dump(cities_data, f, indent=2, ensure_ascii=False)

print('\n' + '='*80)
print('GEOCODING COMPLETE!')
print('='*80)
print(f'Finished at: {time.strftime("%Y-%m-%d %H:%M:%S")}')
print(f'Total processed: {total}')
print(f'Successfully geocoded: {success_count} ({success_count/total*100:.1f}%)')
print(f'Failed: {fail_count} ({fail_count/total*100:.1f}%)')
print(f'\nSaved to: sf_cities_all_geocoded.json')
