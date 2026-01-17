import json
import re
import time
from collections import Counter, defaultdict
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
import sys

def remove_emoji(text):
    emoji_pattern = re.compile(
        '['
        '\U0001F600-\U0001F64F'
        '\U0001F300-\U0001F5FF'
        '\U0001F680-\U0001F6FF'
        '\U0001F1E0-\U0001F1FF'
        '\U00002702-\U000027B0'
        '\U000024C2-\U0001F251'
        '\U0001F900-\U0001F9FF'
        '\U0001FA00-\U0001FA6F'
        '\U0001FA70-\U0001FAFF'
        '\U00002600-\U000026FF'
        ']+',
        flags=re.UNICODE
    )
    return emoji_pattern.sub(r'', text).strip()

JUNK_PATTERNS = [
    r'your mom', r'yo mom', r'ur mom', r'your dad', r'my mom', r'my dad',
    r'^hell$', r'^heaven$', r'^mars$', r'^pluto$',
    r'third rock', r'solar system', r'milky way', r'andromeda',
    r'^here$', r'^there$', r'^everywhere', r'^anywhere', r'^nowhere',
    r'^unknown$', r'^classified$', r'^redacted$', r'non.?ya', r'^none',
    r'my house', r'at home', r'my place', r'your place', r'my home',
    r'under.*bed', r'in.*head', r'in.*mind', r'in.*heart', r'in.*dreams',
    r'wakanda', r'gotham', r'middle.?earth', r'wonderland', r'narnia',
    r'flavor.?town', r'toon.?town', r'hamster', r'cartoon',
    r'fuck', r'shit', r'ass(?!am)', r'butt', r'dick',
    r'hyperspace', r'metaverse', r'cyberspace', r'matrix', r'web3',
    r'she/her', r'he/him', r'they/them', r'pronouns',
    r'^\?+$', r'^x$', r'^www$', r'http', r'\.com$', r'\.social$', r'@',
    r'mood:', r'^rt ', r'retweet', r'follow', r'link in bio',
    r'up your', r'deez nuts', r'ligma', r'sugma',
    r'^\d{1,3}\.\d{1,3}\.\d{1,3}', r'ip.*address', r'192\.168',
    r'behind enemy', r'occupied', r'stolen land', r'banana republic',
    r'clown.*world', r'dystopia', r'hell.*earth', r'prison.*planet',
    r'god.*kingdom', r'rapture', r'heaven', r'purgatory',
    r'simulation', r'virtual', r'video.*game', r'minecraft',
    r'varrock', r'runescape', r'pokemon', r'zelda', r'halo',
    r'final fantasy', r'warcraft', r'league', r'fortnite'
]

def is_junk_location(location):
    location_lower = location.lower().strip()
    for pattern in JUNK_PATTERNS:
        if re.search(pattern, location_lower):
            return True
    if len(location_lower) < 2:
        return True
    if not any(c.isalnum() for c in location_lower):
        return True
    if location_lower.isdigit() and len(location_lower) != 5:
        return True
    return False

def extract_zipcode(location):
    match = re.search(r'\b(\d{5})(?:-\d{4})?\b', location)
    if match:
        return match.group(1)
    return None

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

def clean_city_data(city_name, input_file, city_keywords):
    """
    Clean and geocode city data
    city_name: e.g., 'buffalo', 'portland'
    input_file: path to JSON file
    city_keywords: list of keywords to identify the city
    """

    print(f'\n{"="*80}')
    print(f'CLEANING {city_name.upper()} DATA')
    print(f'{"="*80}\n')

    # Load data
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Extract other cities
    other_locations = []
    for record in data:
        author_location = record.get('author', {}).get('location', '')
        if not author_location:
            continue

        author_location_lower = author_location.lower()
        is_target_city = any(kw in author_location_lower for kw in city_keywords)

        if not is_target_city:
            other_locations.append(author_location)

    location_counter = Counter(other_locations)

    print(f'Total records: {len(data)}')
    print(f'Other cities locations: {len(other_locations)}')
    print(f'Unique other cities: {len(location_counter)}')

    # Step 1: Clean
    cities_data = []
    for rank, (location, count) in enumerate(location_counter.most_common(), 1):
        cleaned = remove_emoji(location)
        cities_data.append({
            'rank': rank,
            'original': location,
            'cleaned': cleaned,
            'count': count,
            'is_junk': is_junk_location(cleaned),
            'zipcode': extract_zipcode(cleaned)
        })

    # Step 2: Geocode
    print(f'\nStep 2: Geocoding {len(cities_data)} locations...')
    print(f'Estimated time: {len(cities_data) * 1.2 / 60:.1f} minutes\n')

    geolocator = Nominatim(user_agent=f"{city_name}_location_cleaner_v1")

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

    # Geocode valid locations
    to_geocode = [item for item in cities_data if not item['is_junk']]
    to_geocode.sort(key=lambda x: x['count'], reverse=True)

    success_count = 0
    fail_count = 0

    for i, item in enumerate(to_geocode, 1):
        if i % 50 == 0 or i == 1 or i == len(to_geocode):
            print(f'[{i}/{len(to_geocode)}] ({i/len(to_geocode)*100:.1f}%) Processing...')
            sys.stdout.flush()

        result = geocode_location(item['cleaned'])

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

        # Checkpoint every 100
        if i % 100 == 0:
            output_file = f'{city_name}_cities_all_geocoded.json'
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(cities_data, f, indent=2, ensure_ascii=False)
            print(f'  Checkpoint saved. Success: {success_count}, Failed: {fail_count}')

    # Save final
    output_file = f'{city_name}_cities_all_geocoded.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(cities_data, f, indent=2, ensure_ascii=False)

    print(f'\n{"="*80}')
    print(f'GEOCODING COMPLETE FOR {city_name.upper()}')
    print(f'{"="*80}')
    print(f'Successfully geocoded: {success_count}/{len(to_geocode)} ({success_count/len(to_geocode)*100:.1f}%)')
    print(f'Failed: {fail_count}/{len(to_geocode)} ({fail_count/len(to_geocode)*100:.1f}%)')
    print(f'Saved to: {output_file}\n')

    return cities_data

if __name__ == '__main__':
    # Process Buffalo
    buffalo_data = clean_city_data(
        city_name='buffalo',
        input_file='raw_x_data/buffalo/posts_english_2015-2025_all_info.json',
        city_keywords=['buffalo']
    )

    print('\n' + '='*80)
    print('BUFFALO PROCESSING COMPLETE')
    print('='*80 + '\n')

    # Brief pause before Portland
    time.sleep(5)

    # Process Portland
    portland_data = clean_city_data(
        city_name='portland',
        input_file='raw_x_data/portland/posts_english_2015-2025_all_info.json',
        city_keywords=['portland']
    )

    print('\n' + '='*80)
    print('ALL CITIES PROCESSING COMPLETE')
    print('='*80)
