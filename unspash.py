import os
import requests

URL = "https://api.unsplash.com/"
header = {
    "Accept-Version": "v1"
}
params = {
    'client_id': os.getenv('UNSPLASH_CLIENT_ID'),
    'query': 'mattress',
    "orientation": "squarish",
    'count': 1
}
with requests.get(URL + "/photos/random", headers=header, params=params) as r:
    res = r.json()
res
