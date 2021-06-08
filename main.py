import os
import requests

from models import Report
from pexecute.thread import ThreadLoom


def create_sales_report(channel_id="C022WMH56KA", mode="daily"):
    sales_order = (
        "SalesOrder",
        [
            ("SalesOrder", "SUM"),
            ("Transactions", "SUM"),
            ("AUSPMattress", "AVG", "SalesOrderMattress", "QuantityMattress"),
            ("StoreTraffic", "SUM"),
        ],
    )
    customers = (
        "Customers",
        [
            ("Customers", "SUM"),
            ("NewCustomers", "SUM"),
            ("AOVCustomers", "AVG", "SalesOrder", "Customers"),
        ],
    )
    report = Report.create("Sales", mode, channel_id)
    report.add_section(*sales_order)
    report.add_section(*customers)
    return report


def create_profit_report(channel_id="C022WMH56KA", mode="daily"):
    profit = (
        "Profit",
        [
            ("Sales", "SUM"),
            ("COGS", "SUM"),
            ("GrossProfit", "SUM"),
            ("GrossMargin", "AVG", "GrossProfit", "Sales"),
        ],
    )
    report = Report.create("Profit", mode, channel_id)
    report.add_section(*profit)
    return report


def create_marketing_report(channel_id="C022WMH56KA", mode="daily"):
    spend = (
        "Spend",
        [("FacebookSpend", "SUM"), ("GoogleSpend", "SUM"), ("FunnelSpend", "SUM")],
    )
    leads = (
        "Leads",
        [
            ("TotalLeads", "SUM"),
            ("UniqueLeads", "SUM"),
            ("PhonesCollected", "SUM"),
            ("CostPerLead", "AVG", "FunnelSpend", "TotalLeads"),
        ],
    )
    conversions = (
        "Conversions",
        [
            ("AcquiredCustomers", "SUM"),
            ("FunnelCR", "AVG", "AcquiredCustomers", "PhonesCollected"),
            ("FunnelRevenue", "SUM"),
            ("ROAS", "AVG", "FunnelRevenue", "FunnelSpend"),
        ],
    )
    report = Report.create("Marketing", mode, channel_id)
    report.add_section(*spend)
    report.add_section(*leads)
    report.add_section(*conversions)
    return report


def report_factory(mode):
    reports = []
    if mode == "daily":
        # reports.append(create_sales_report(mode=mode))
        # reports.append(create_profit_report(mode=mode))
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
        loom = ThreadLoom(max_runner_cap=10)
        for report in reports:
            report.push()
        #     loom.add_function(report.push)
        # output =  loom.execute()
        # return 
        return {"reports_pushed": len(reports)}
    else:
        raise RuntimeError("400 Bad Request")
