import requests

url = "https://api.twitterapi.io/twitter/user/followings?pageSize=200&userName=BroncosKitFound"

headers = {"X-API-Key": "new1_3ad6c9d6f39f4538ad669faed9fd3bbe"}

response = requests.get(url, headers=headers)

print(response.text)