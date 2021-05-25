import os
import requests

from models import Report

sales = (
    "Sales",
    [
        ("SalesOrder", "SUM"),
        ("Transactions", "SUM"),
        ("AOV", "AVG"),
        ("AUSP", "AVG"),
        ("AUSPMattress", "AVG"),
        ("StoreTraffic", "SUM"),
    ],
)

marketing = (
    "Marketing",
    [
        ("FacebookSpend", "SUM"),
        ("GoogleSpend", "SUM"),
        ("TotalLeads", "SUM"),
        ("PhonesCollected", "SUM"),
        ("AcquiredCustomers", "SUM"),
        ("FunnelCR", "AVG"),
        ("FunnelRevenue", "SUM"),
    ],
)


def report_factory(mode):
    report = Report.create(mode)
    if mode == "daily":
        report.add_section(*sales)
        report.add_section(*marketing)
    elif mode == "realtime":
        report.add_section(*sales)
    else:
        raise RuntimeError("Mode not found")
    return report


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
    return res


def main(request):
    request_json = request.get_json()
    if request_json:
        mode = request_json["mode"]
        report = report_factory(mode)
        payload = report.build()
        return push(payload)
    else:
        raise RuntimeError("400 Bad Request")


if __name__ == "__main__":
    main()
