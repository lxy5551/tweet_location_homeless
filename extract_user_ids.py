import json

# Load the Kalamazoo data
input_file = 'raw_x_data/kalamazoo/posts_english_2015-2025_all_info.json'
output_file = 'userID_kalamazoo.json'

with open(input_file, 'r', encoding='utf-8') as f:
    data = json.load(f)

# Extract unique users
users_dict = {}

for record in data:
    author = record.get('author', {})
    user_id = author.get('id')

    if user_id and user_id not in users_dict:
        users_dict[user_id] = {
            'user_id': user_id,
            'username': author.get('username'),
            'name': author.get('name'),
            'location': author.get('location'),
            'description': author.get('description'),
            'created_at': author.get('created_at'),
            'verified': author.get('verified'),
            'protected': author.get('protected'),
            'followers_count': author.get('public_metrics', {}).get('followers_count'),
            'following_count': author.get('public_metrics', {}).get('following_count'),
            'tweet_count': author.get('public_metrics', {}).get('tweet_count'),
            'listed_count': author.get('public_metrics', {}).get('listed_count')
        }

# Convert to list
users_list = list(users_dict.values())

# Save to JSON
with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(users_list, f, indent=2, ensure_ascii=False)

print(f'Total tweets: {len(data)}')
print(f'Unique users: {len(users_list)}')
print(f'Output saved to: {output_file}')
print()

# Show sample
print('Sample users:')
for i, user in enumerate(users_list[:5], 1):
    print(f'{i}. {user["username"]} (ID: {user["user_id"]}) - {user["location"]}')
