import json
import pandas as pd
from collections import Counter, defaultdict

print('Generating final reports and Excel files...\n')

# 读取完整数据
with open('sf_cities_all_geocoded.json', 'r', encoding='utf-8') as f:
    cities_data = json.load(f)

# 分类数据
cleaned = []
junk = []
failed = []

for item in cities_data:
    if item['is_junk']:
        junk.append(item)
    elif 'standardized' in item and item.get('geocoded'):
        cleaned.append(item)
    else:
        failed.append(item)

# 排序
cleaned.sort(key=lambda x: x['count'], reverse=True)
junk.sort(key=lambda x: x['count'], reverse=True)
failed.sort(key=lambda x: x['count'], reverse=True)

# 聚合standardized locations
standardized_counter = Counter()
standardized_examples = defaultdict(list)

for item in cleaned:
    std = item['standardized']
    standardized_counter[std] += item['count']
    if len(standardized_examples[std]) < 3:
        standardized_examples[std].append(item['original'])

top_cities = standardized_counter.most_common()

# 计算统计
total_locations = len(cities_data)
total_tweets = sum(item['count'] for item in cities_data)
cleaned_tweets = sum(item['count'] for item in cleaned)
junk_tweets = sum(item['count'] for item in junk)
failed_tweets = sum(item['count'] for item in failed)

print(f'Statistics:')
print(f'  Total locations: {total_locations}')
print(f'  Successfully cleaned: {len(cleaned)} ({len(cleaned)/total_locations*100:.1f}%)')
print(f'  Filtered as junk: {len(junk)} ({len(junk)/total_locations*100:.1f}%)')
print(f'  Failed: {len(failed)} ({len(failed)/total_locations*100:.1f}%)')
print(f'  Unique standardized cities: {len(top_cities)}')
print(f'\nCreating Excel file...')

# Sheet 1: Summary
summary_data = {
    'Metric': [
        'Total Unique Locations',
        'Successfully Geocoded',
        'Filtered as Junk',
        'Geocoding Failed',
        '',
        'Total Tweets',
        'Tweets from Cleaned Locations',
        'Tweets from Junk Locations',
        'Tweets from Failed Locations',
        '',
        'Unique Standardized Cities',
        'Geocoding Success Rate',
        'Data Quality Score'
    ],
    'Value': [
        total_locations,
        len(cleaned),
        len(junk),
        len(failed),
        '',
        total_tweets,
        cleaned_tweets,
        junk_tweets,
        failed_tweets,
        '',
        len(top_cities),
        f'{len(cleaned)/total_locations*100:.1f}%',
        f'{cleaned_tweets/total_tweets*100:.1f}%'
    ]
}
df_summary = pd.DataFrame(summary_data)

# Sheet 2: Standardized Cities
cities_list = []
for rank, (city, count) in enumerate(top_cities, 1):
    examples = standardized_examples[city]
    cities_list.append({
        'Rank': rank,
        'Standardized City': city,
        'Tweet Count': count,
        'Percentage': f'{count/cleaned_tweets*100:.2f}%',
        'Original Examples': ' | '.join(examples[:3])
    })
df_cities = pd.DataFrame(cities_list)

# Sheet 3: Cleaned Details
cleaned_records = []
for item in cleaned:
    cleaned_records.append({
        'Original Location': item['original'],
        'Cleaned Location': item['cleaned'],
        'Standardized Location': item['standardized'],
        'Tweet Count': item['count'],
        'Geocoding Type': item.get('type', 'unknown'),
        'Had Zipcode': 'Yes' if item.get('zipcode') else 'No'
    })
df_cleaned = pd.DataFrame(cleaned_records)

# Sheet 4: Filtered Junk
junk_records = []
for item in junk:
    junk_records.append({
        'Original Location': item['original'],
        'Cleaned Location': item['cleaned'],
        'Tweet Count': item['count'],
        'Reason': 'Non-geographic content detected'
    })
df_junk = pd.DataFrame(junk_records)

# Sheet 5: Failed Geocoding
failed_records = []
for item in failed[:500]:
    failed_records.append({
        'Original Location': item['original'],
        'Cleaned Location': item['cleaned'],
        'Tweet Count': item['count'],
        'Status': item.get('type', 'not_geocoded')
    })
df_failed = pd.DataFrame(failed_records)

# 保存Excel
excel_file = 'sf_cities_final_cleaned.xlsx'
with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
    df_summary.to_excel(writer, sheet_name='Summary', index=False)
    df_cities.to_excel(writer, sheet_name='Standardized Cities', index=False)
    df_cleaned.to_excel(writer, sheet_name='Cleaned Details', index=False)
    df_junk.to_excel(writer, sheet_name='Filtered Junk', index=False)
    df_failed.to_excel(writer, sheet_name='Failed Geocoding', index=False)

print(f'Excel created: {excel_file}')

# 生成文本报告
report_file = 'sf_cities_final_report.txt'
with open(report_file, 'w', encoding='utf-8') as f:
    f.write('='*100 + '\n')
    f.write('SAN FRANCISCO OTHER CITIES - FINAL DATA CLEANING REPORT\n')
    f.write('='*100 + '\n\n')

    f.write('EXECUTIVE SUMMARY:\n')
    f.write('-'*100 + '\n')
    f.write(f'  Original dataset: {total_locations} unique locations, {total_tweets} tweets\n')
    f.write(f'  Successfully cleaned: {len(cleaned)} locations ({len(cleaned)/total_locations*100:.1f}%)\n')
    f.write(f'  Standardized to: {len(top_cities)} unique cities\n')
    f.write(f'  Data quality: {cleaned_tweets/total_tweets*100:.1f}% of tweets have valid city-level data\n\n')

    f.write('='*100 + '\n\n')

    f.write('TOP 100 STANDARDIZED CITIES:\n')
    f.write('-'*100 + '\n')

    for rank, (city, count) in enumerate(top_cities[:100], 1):
        examples = ', '.join(standardized_examples[city][:2])
        if len(examples) > 40:
            examples = examples[:37] + '...'
        pct = count/cleaned_tweets*100
        f.write(f'{rank:<4} {city:<40} {count:>5} tweets ({pct:>5.2f}%)  Ex: {examples}\n')

    f.write('\n' + '='*100 + '\n\n')

    f.write('FILTERED LOCATIONS (JUNK): {len(junk)} locations, {junk_tweets} tweets\n\n')
    for i, item in enumerate(junk[:30], 1):
        f.write(f'{i:>3}. {item["original"]:<60} ({item["count"]})\n')

    f.write('\n' + '='*100 + '\n')

print(f'Report created: {report_file}')
print('\nAll files saved to: C:\\Users\\levin\\Downloads\\raw_x_data\\')
