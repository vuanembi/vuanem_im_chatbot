import os
import json

import requests

token = os.getenv("TOKEN")
headers = {
    "Charset": "utf-8",
    "Content-type": "application/json",
    "Authorization": f"Bearer {token}",
}
with requests.post(
    "https://slack.com/api/conversations.list", headers=headers
) as r:
    res = r.json()
with open('channels.json', 'w') as f:
    json.dump(res, f)
