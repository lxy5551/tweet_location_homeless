import json
import pandas as pd
from collections import Counter
import os

def analyze_city(city_name, city_folder, city_keywords, geocoded_file_name=None):
    """Analyze a single city's data"""

    # Load main data
    json_file = os.path.join(city_folder, 'posts_english_2015-2025_all_info.json')
    if not os.path.exists(json_file):
        return None

    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    total_tweets = len(data)

    # Categorize tweets by user location
    in_city = []
    in_other_cities = []
    no_location = []

    for record in data:
        author_location = record.get('author', {}).get('location', '')

        if not author_location:
            no_location.append(record)
        else:
            author_location_lower = author_location.lower()
            is_target_city = any(kw in author_location_lower for kw in city_keywords)

            if is_target_city:
                in_city.append(record)
            else:
                in_other_cities.append(record)

    # Check if geocoded data exists
    if geocoded_file_name is None:
        geocoded_file_name = f'{city_name}_cities_all_geocoded.json'

    geocoded_success = 0
    geocoded_failed = 0
    junk_filtered = 0
    unclear_location = 0
    top_5_cities = ''

    if os.path.exists(geocoded_file_name):
        with open(geocoded_file_name, 'r', encoding='utf-8') as f:
            geocoded_data = json.load(f)

        # Aggregate standardized cities
        standardized_counter = Counter()

        for item in geocoded_data:
            if item['is_junk']:
                junk_filtered += item['count']
                continue

            if item.get('geocoded') and 'standardized' in item:
                geocoded_success += item['count']
                standardized_counter[item['standardized']] += item['count']
            elif not item.get('geocoded'):
                geocoded_failed += item['count']

        # Get top 5 cities
        top_cities = standardized_counter.most_common(5)
        top_5_cities = ' | '.join([f"{city} ({count})" for city, count in top_cities])

        unclear_location = len(in_other_cities) - geocoded_success - geocoded_failed - junk_filtered
    else:
        # If no geocoded file, all "elsewhere" are unclear
        unclear_location = len(in_other_cities)

    unclear_no_location_total = (unclear_location if unclear_location >= 0 else 0) + len(no_location)
    unclear_no_location_ratio = f'{unclear_no_location_total/total_tweets*100:.1f}%' if total_tweets > 0 else '0%'

    return {
        'City': city_name.replace('_', ' ').title(),
        'Total Tweets': total_tweets,
        'Users in Same City': len(in_city),
        'Users in Same City %': f'{len(in_city)/total_tweets*100:.1f}%' if total_tweets > 0 else '0%',
        'Users Elsewhere Total': len(in_other_cities),
        'Geocoded Successfully': geocoded_success,
        'Top 5 Cities from Geocoded': top_5_cities,
        'Geocoded Failed': geocoded_failed,
        'Unclear Location': unclear_location if unclear_location >= 0 else 0,
        'No Location': len(no_location),
        'No Location %': f'{len(no_location)/total_tweets*100:.1f}%' if total_tweets > 0 else '0%',
        'Unclear + No Location Ratio': unclear_no_location_ratio
    }

# City configurations
cities_config = {
    'san_francisco': {
        'folder': 'raw_x_data/san_francisco',
        'keywords': ['san francisco', 'sf', 'bay area', 'silicon valley'],
        'geocoded_file': 'sf_cities_all_geocoded.json'
    },
    'buffalo': {
        'folder': 'raw_x_data/buffalo',
        'keywords': ['buffalo'],
        'geocoded_file': 'buffalo_cities_all_geocoded.json'
    },
    'portland': {
        'folder': 'raw_x_data/portland',
        'keywords': ['portland'],
        'geocoded_file': 'portland_cities_all_geocoded.json'
    },
    'baltimore': {
        'folder': 'raw_x_data/baltimore',
        'keywords': ['baltimore']
    },
    'el_paso': {
        'folder': 'raw_x_data/el paso',
        'keywords': ['el paso']
    },
    'fayetteville': {
        'folder': 'raw_x_data/fayetteville',
        'keywords': ['fayetteville']
    },
    'kalamazoo': {
        'folder': 'raw_x_data/kalamazoo',
        'keywords': ['kalamazoo']
    },
    'rockford': {
        'folder': 'raw_x_data/rockford',
        'keywords': ['rockford']
    },
    'scranton': {
        'folder': 'raw_x_data/scranton',
        'keywords': ['scranton']
    },
    'southbend': {
        'folder': 'raw_x_data/southbend',
        'keywords': ['south bend', 'southbend']
    }
}

print('='*80)
print('COMPREHENSIVE CITY ANALYSIS')
print('='*80)
print()

results = []

for city_name, config in cities_config.items():
    print(f'Analyzing {city_name.replace("_", " ").title()}...')
    geocoded_file = config.get('geocoded_file', None)
    result = analyze_city(city_name, config['folder'], config['keywords'], geocoded_file)
    if result:
        results.append(result)
        print(f'  Total tweets: {result["Total Tweets"]}')
        print(f'  In city: {result["Users in Same City"]} ({result["Users in Same City %"]})')
        print(f'  Elsewhere: {result["Users Elsewhere Total"]}')
        if result["Geocoded Successfully"] > 0:
            print(f'    - Geocoded successfully: {result["Geocoded Successfully"]}')
            print(f'    - Geocoded failed: {result["Geocoded Failed"]}')
        print()

# Create DataFrame
df = pd.DataFrame(results)

# Save to Excel
output_file = 'all_cities_analysis_final.xlsx'
with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
    df.to_excel(writer, sheet_name='City Summary', index=False)

    # Auto-adjust column widths
    worksheet = writer.sheets['City Summary']
    for idx, col in enumerate(df.columns):
        max_length = max(
            df[col].astype(str).apply(len).max(),
            len(col)
        ) + 2
        worksheet.column_dimensions[chr(65 + idx)].width = max_length

print('='*80)
print(f'Report saved to: {output_file}')
print('='*80)
print()

# Display summary
print('SUMMARY:')
print('-'*80)
total_all_tweets = df['Total Tweets'].sum()
total_in_city = df['Users in Same City'].sum()
total_elsewhere = df['Users Elsewhere Total'].sum()
total_geocoded_success = df['Geocoded Successfully'].sum()
total_geocoded_failed = df['Geocoded Failed'].sum()
total_no_location = df['No Location'].sum()

print(f'Total tweets across all cities: {total_all_tweets:,}')
print(f'Users in same city: {total_in_city:,} ({total_in_city/total_all_tweets*100:.1f}%)')
print(f'Users elsewhere: {total_elsewhere:,} ({total_elsewhere/total_all_tweets*100:.1f}%)')
print(f'  - Geocoded successfully: {total_geocoded_success:,}')
print(f'  - Geocoded failed: {total_geocoded_failed:,}')
print(f'No location: {total_no_location:,} ({total_no_location/total_all_tweets*100:.1f}%)')
print()
