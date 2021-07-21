import os
import json

import requests

token = os.getenv("TOKEN")
headers = {
    "Charset": "utf-8",
    "Content-type": "application/json",
    "Authorization": f"Bearer {token}",
}
params = {"types": ",".join(["public_channel", "private_channel"]), "limit": 1000}
channels = []
with requests.Session() as sessions:
    while True:
        with sessions.post(
            "https://slack.com/api/conversations.list", params=params, headers=headers
        ) as r:
            res = r.json()
        channels.extend(res["channels"])
        if res["response_metadata"]["next_cursor"]:
            params["cursor"] = res["response_metadata"]["next_cursor"]
        else:
            break
with open("channels.json", "w") as f:
    json.dump(channels, f)
