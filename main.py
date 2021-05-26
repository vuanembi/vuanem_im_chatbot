import os
import requests

from models import Report


def create_sales_report(channel_id='C022WMH56KA', mode='daily'):
    sales_order = (
    "SalesOrder",
    [
        ("SalesOrder", "SUM"),
        ("Transactions", "SUM"),
        ("AOVCustomers", "AVG"),
        ("AUSPMattress", "AVG"),
        ("StoreTraffic", "SUM"),
    ],
)
    customers = (
    "Customers",
    [
        ("Customers", "SUM"),
        ("NewCustomers", "SUM"),
    ],
)
    report = Report.create("Sales", mode, channel_id)
    report.add_section(*sales_order)
    report.add_section(*customers)
    return report

def create_profit_report(channel_id='C022WMH56KA', mode='daily'):
    profit = (
        "Profit",
        [
            ("Sales", "SUM"),
            ("COGS", "SUM"),
            ("GrossProfit", "SUM"),
            ("GrossMargin", "AVG"),
        ],
    )
    report = Report.create("Profit", mode, channel_id)
    report.add_section(*profit)
    return report
    
def create_marketing_report(channel_id='C022WMH56KA', mode='daily'):
    spend = (
        "Spend",
        [
            ("FacebookSpend", "SUM"),
            ("GoogleSpend", "SUM"),
        ],
    )
    funnel = (
        "Spend",
        [
            ("TotalLeads", "SUM"),
            ("PhonesCollected", "SUM"),
            ("AcquiredCustomers", "SUM"),
            ("FunnelCR", "AVG"),
            ("FunnelRevenue", "SUM"),
        ],
    )
    report = Report.create("Marketing", mode, channel_id)
    report.add_section(*spend)
    report.add_section(*funnel)
    return report


def report_factory(mode):
    reports = []
    if mode == "daily":
        reports.append(create_sales_report(mode=mode))
        reports.append(create_profit_report(mode=mode))
        reports.append(create_marketing_report(mode=mode))
    elif mode == "realtime":
        reports.append(create_sales_report(mode=mode))
    else:
        raise RuntimeError("Mode not found")
    return reports


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
        reports = report_factory(mode)
        for report in reports:
            report.push()
        return {"reports_pushed": len(reports)}
    else:
        raise RuntimeError("400 Bad Request")
