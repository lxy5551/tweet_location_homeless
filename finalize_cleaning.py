import json
import re
from collections import defaultdict, Counter

print('Step 3: Finalizing cleaned data...')

# 加载Step 2结果
with open('sf_cities_step2.json', 'r', encoding='utf-8') as f:
    cities_data = json.load(f)

# 分类数据
cleaned_cities = []
filtered_junk = []
needs_manual_review = []

for item in cities_data:
    # 创建最终条目
    entry = {
        'rank': item['rank'],
        'original_location': item['original'],
        'cleaned_location': item['cleaned'],
        'count': item['count']
    }
    
    if item['is_junk']:
        # 垃圾地点
        entry['status'] = 'filtered_junk'
        entry['reason'] = 'Contains non-geographic content'
        filtered_junk.append(entry)
    elif 'standardized' in item and item.get('geocoded'):
        # 成功geocoded
        entry['standardized_location'] = item['standardized']
        entry['status'] = 'cleaned'
        entry['geocoding_type'] = item.get('type', 'unknown')
        cleaned_cities.append(entry)
    elif item['zipcode']:
        # 有邮编但未映射
        entry['standardized_location'] = f"ZIP {item['zipcode']}"
        entry['status'] = 'cleaned'
        entry['geocoding_type'] = 'zipcode_only'
        cleaned_cities.append(entry)
    else:
        # 需要人工review
        entry['status'] = 'needs_review'
        entry['note'] = 'Not geocoded - may need manual standardization'
        needs_manual_review.append(entry)

# 按count排序
cleaned_cities.sort(key=lambda x: x['count'], reverse=True)
filtered_junk.sort(key=lambda x: x['count'], reverse=True)
needs_manual_review.sort(key=lambda x: x['count'], reverse=True)

# 生成统计
stats = {
    'total_entries': len(cities_data),
    'cleaned': len(cleaned_cities),
    'filtered_junk': len(filtered_junk),
    'needs_review': len(needs_manual_review),
    'total_tweets_cleaned': sum(item['count'] for item in cleaned_cities),
    'total_tweets_filtered': sum(item['count'] for item in filtered_junk),
    'total_tweets_review': sum(item['count'] for item in needs_manual_review)
}

# 保存最终结果
final_output = {
    'statistics': stats,
    'cleaned_locations': cleaned_cities,
    'filtered_as_junk': filtered_junk,
    'needs_manual_review': needs_manual_review
}

with open('sf_cities_cleaned_final.json', 'w', encoding='utf-8') as f:
    json.dump(final_output, f, indent=2, ensure_ascii=False)

# 生成可读报告
with open('cleaning_report.txt', 'w', encoding='utf-8') as f:
    f.write('SAN FRANCISCO OTHER CITIES - DATA CLEANING REPORT\n')
    f.write('=' * 80 + '\n\n')
    
    f.write('STATISTICS:\n')
    f.write(f'  Total unique locations processed: {stats["total_entries"]}\n')
    f.write(f'  Successfully cleaned: {stats["cleaned"]} ({stats["cleaned"]/stats["total_entries"]*100:.1f}%)\n')
    f.write(f'  Filtered as junk: {stats["filtered_junk"]} ({stats["filtered_junk"]/stats["total_entries"]*100:.1f}%)\n')
    f.write(f'  Needs manual review: {stats["needs_review"]} ({stats["needs_review"]/stats["total_entries"]*100:.1f}%)\n\n')
    
    f.write(f'  Tweets from cleaned locations: {stats["total_tweets_cleaned"]}\n')
    f.write(f'  Tweets from filtered locations: {stats["total_tweets_filtered"]}\n')
    f.write(f'  Tweets needing review: {stats["total_tweets_review"]}\n\n')
    
    f.write('=' * 80 + '\n\n')
    
    # Top 30 cleaned cities
    f.write('TOP 30 CLEANED LOCATIONS:\n')
    f.write('-' * 80 + '\n')
    for i, item in enumerate(cleaned_cities[:30], 1):
        f.write(f'{i:>3}. {item["standardized_location"]:<40} ({item["count"]:>3} tweets)\n')
        if item['original_location'] != item['standardized_location']:
            f.write(f'     Original: {item["original_location"]}\n')
    
    f.write('\n' + '=' * 80 + '\n\n')
    
    # Filtered junk examples
    f.write('FILTERED AS JUNK (Top 20 examples):\n')
    f.write('-' * 80 + '\n')
    for i, item in enumerate(filtered_junk[:20], 1):
        f.write(f'{i:>3}. {item["original_location"]:<50} ({item["count"]:>2} tweets)\n')
    
    f.write('\n' + '=' * 80 + '\n\n')
    
    # Needs review examples
    f.write('NEEDS MANUAL REVIEW (Top 20):\n')
    f.write('-' * 80 + '\n')
    for i, item in enumerate(needs_manual_review[:20], 1):
        f.write(f'{i:>3}. {item["cleaned_location"]:<50} ({item["count"]:>2} tweets)\n')

print(f'\nFinal Results:')
print(f'  Cleaned: {stats["cleaned"]} locations')
print(f'  Filtered: {stats["filtered_junk"]} junk locations')
print(f'  Review needed: {stats["needs_review"]} locations')
print(f'\nSaved:')
print(f'  - sf_cities_cleaned_final.json (structured data)')
print(f'  - cleaning_report.txt (human-readable report)')
