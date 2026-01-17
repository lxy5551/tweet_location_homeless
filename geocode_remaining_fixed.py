import json
import time
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
import sys

print('='*80)
print('RESUMING GEOCODING - REMAINING LOCATIONS')
print('='*80)

# 加载progress
try:
    with open('sf_cities_geocoding_progress.json', 'r', encoding='utf-8') as f:
        cities_data = json.load(f)
    print('Loaded progress file successfully')
except:
    with open('sf_cities_all_geocoded.json', 'r', encoding='utf-8') as f:
        cities_data = json.load(f)
    print('Loaded from all_geocoded file')

geolocator = Nominatim(user_agent="sf_location_cleaner_resume_v3")

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
            time.sleep(1.1)
            location_obj = geolocator.geocode(location, timeout=10, addressdetails=True)
            return location_obj
        except (GeocoderTimedOut, GeocoderServiceError):
            if attempt < max_retries - 1:
                time.sleep(2)
                continue
            return None
    return None

# 找到还需要处理的
to_geocode = []
for item in cities_data:
    if not item['is_junk'] and 'standardized' not in item:
        to_geocode.append(item)

to_geocode.sort(key=lambda x: x['count'], reverse=True)

total = len(to_geocode)
print(f'\nRemaining locations to geocode: {total}')
print(f'Estimated time: {total * 1.2 / 60:.1f} minutes\n')

success_count = 0
fail_count = 0
start_num = 1070  # 从断点继续

for i, item in enumerate(to_geocode, start_num):
    location = item['cleaned']
    
    # 安全打印（避免Unicode错误）
    if i % 10 == 0 or i <= start_num + 5 or i > start_num + total - 5:
        try:
            loc_display = location[:60].encode('ascii', 'ignore').decode('ascii')
        except:
            loc_display = '[Unicode location]'
        
        elapsed = (i-start_num) * 1.2 / 60
        remaining = (start_num + total - i) * 1.2 / 60
        print(f'[{i}/{start_num+total-1}] ({(i-start_num)/(total)*100:.1f}%) | Remaining: {remaining:.1f}min')
        print(f'  Geocoding: {loc_display}')
        sys.stdout.flush()
    
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
            item['geocoded'] = False
            item['type'] = 'geocoded_insufficient'
            fail_count += 1
    else:
        item['geocoded'] = False
        item['type'] = 'geocoding_failed'
        fail_count += 1
    
    if (i - start_num) % 100 == 0 or i == start_num + total - 1:
        with open('sf_cities_all_geocoded.json', 'w', encoding='utf-8') as f:
            json.dump(cities_data, f, indent=2, ensure_ascii=False)
        print(f'  >>> Checkpoint saved. Success: {success_count}, Failed: {fail_count}')
        sys.stdout.flush()

print('\n' + '='*80)
print('GEOCODING COMPLETE!')
print('='*80)
print(f'Total processed: {total}')
print(f'Successfully geocoded: {success_count}')
print(f'Failed: {fail_count}')
print(f'\nFinal file saved: sf_cities_all_geocoded.json')
