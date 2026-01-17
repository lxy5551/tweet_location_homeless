import json

# Load the Kalamazoo data
input_file = 'raw_x_data/kalamazoo/posts_english_2015-2025_all_info.json'
output_file = 'userID_kalamazoo.json'

with open(input_file, 'r', encoding='utf-8') as f:
    data = json.load(f)

# Extract unique user IDs
user_ids = set()

for record in data:
    author = record.get('author', {})
    user_id = author.get('id')

    if user_id:
        user_ids.add(user_id)

# Convert to sorted list
user_ids_list = sorted(list(user_ids))

# Save to JSON
with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(user_ids_list, f, indent=2)

print(f'Total tweets: {len(data)}')
print(f'Unique user IDs: {len(user_ids_list)}')
print(f'Output saved to: {output_file}')
print()
print('User IDs:')
for user_id in user_ids_list:
    print(f'  {user_id}')
