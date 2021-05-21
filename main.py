import os
import requests

from models import Report


def push(payload):
    token = os.getenv("TOKEN")
    headers = {
        "charset": "utf-8",
        "Content-type": "application/json",
        "Authorization": f"Bearer {token}",
    }
    with requests.post(
        "https://slack.com/api/chat.postMessage", headers=headers, json=payload
    ) as r:
        res = r.json()
    print(res)

def main():
    report = Report.create("daily")
    report.add_section("Sales", ["SalesOrder", "Transactions"])
    payload = report.build()
    payload
    _ = push(payload)


if __name__ == "__main__":
    main()
