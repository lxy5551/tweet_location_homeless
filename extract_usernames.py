import json

# Load the Kalamazoo data
input_file = 'raw_x_data/kalamazoo/posts_english_2015-2025_all_info.json'
output_file = 'username_kalamazoo.json'

with open(input_file, 'r', encoding='utf-8') as f:
    data = json.load(f)

# Extract unique usernames
usernames = set()

for record in data:
    author = record.get('author', {})
    username = author.get('username')

    if username:
        usernames.add(username)

# Convert to sorted list
usernames_list = sorted(list(usernames))

# Save to JSON
with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(usernames_list, f, indent=2)

print(f'Total tweets: {len(data)}')
print(f'Unique usernames: {len(usernames_list)}')
print(f'Output saved to: {output_file}')
print()
print('Usernames:')
for username in usernames_list:
    print(f'  {username}')
