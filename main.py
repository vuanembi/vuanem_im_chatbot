from pexecute.thread import ThreadLoom

from models import Report, Section, Metric

# Metrics

sales_order = Metric("SalesOrder")
transactions = Metric("Transactions")
ausp_mattress = Metric("AUSPMattress", "AVG", "SalesOrderMattress", "QuantityMattress")
store_traffic = Metric("StoreTraffic")

customers = Metric("Customers")
new_customers = Metric("NewCustomers")
aov_customers = Metric("AOVCustomers", "AVG", "SalesOrder", "CustomersB")

sales = Metric("Sales")
cogs = Metric("COGS")
gross_profit = Metric("GrossProfit")
gross_margin = Metric(
    "GrossMargin", "AVG", "GrossProfit", "Sales", _format="percentage"
)

# facebook_spend = Metric("FacebookSpend")
# google_spend = Metric("GoogleSpend")
# funnel_spend = Metric("FunnelSpend")

total_leads = Metric("TotalLeads")
unique_leads = Metric("UniqueLeads")
phones_collected = Metric("PhonesCollected")
# cost_per_lead = Metric("CostPerLead", "AVG", "FunnelSpend", "PhonesCollected")

acquired_customers = Metric("AcquiredCustomers")
funnel_cr = Metric(
    "FunnelCR", "AVG", "AcquiredCustomers", "PhonesCollected", _format="percentage"
)
funnel_revenue = Metric("FunnelRevenue")
# roas = Metric("ROAS", "AVG", "FunnelRevenue", "FunnelSpend")

# Sections

sales_section = Section(
    "Sales", [sales_order, transactions, ausp_mattress, store_traffic]
)
customers_section = Section("Customers", [customers, new_customers, aov_customers])
profit_section = Section("Profit", [sales, cogs, gross_profit, gross_margin])
# spend_section = Section("Spend", [facebook_spend, google_spend, funnel_spend])
leads_section = Section(
    "Funnel",
    [
        total_leads,
        unique_leads,
        phones_collected,
        # cost_per_lead
    ],
)
conversions_section = Section(
    "Conversions",
    [
        acquired_customers,
        funnel_cr,
        funnel_revenue,
        # roas
    ],
)


def report_runs(mode="daily"):
    """Create report runs

    Args:
        mode (str, optional): Mode. Defaults to "daily".

    Returns:
        list: List of reports
    """

    # * Report Channel ID
    sales_report = Report.factory(
        "Sales", [sales_section, customers_section], "C028Q7ZQHD1", mode
    )
    merchandising_report = Report.factory(
        "Merchandising", [profit_section], "C025E8MVDR7", mode
    )
    marketing_report = Report.factory(
        "Marketing",
        [
            # spend_section,
            leads_section,
            conversions_section,
        ],
        "C027V5ABC03",
        mode,
    )

    # * ASM ID & Channel ID
    ASMS = [
        {
            "id": 1572,
            "report_name": "Báo cáo cho ASM Hương",
            "channel_id": "C027V5CP86P",
        },
        {
            "id": 55737,
            "report_name": "Báo cáo cho ASM Tùng",
            "channel_id": "C028Q7K327M",
        },
        {
            "id": 456793,
            "report_name": "Báo cáo cho ASM Thành",
            "channel_id": "C028Q7KE20K",
        },
        {
            "id": 134684,
            "report_name": "Báo cáo cho ASM Đức",
            "channel_id": "C02899T84KZ",
        },
        {
            "id": 465755,
            "report_name": "Báo cáo cho ASM Danh",
            "channel_id": "C028W7NB79A",
        },
        {
            "id": 1575,
            "report_name": "Báo cáo cho ASM Hiền",
            "channel_id": "C0292LYDT4H",
        },
        {
            "id": 238459,
            "report_name": "Báo cáo cho ASM Uyên",
            "channel_id": "C02899VG05V",
        },
        {
            "id": 619317,
            "report_name": "Báo cáo cho ASM Ngân",
            "channel_id": "C029DN1LNLQ",
        },
        {
            "id": 1727,
            "report_name": "Báo cáo cho ASM Thuỳ",
            "channel_id": "C028M144UMT",
        },
        {
            "id": 617334,
            "report_name": "Báo cáo cho ASM Hảo",
            "channel_id": "C028M132H37",
        },
    ]

    asm_reports = [
        Report.factory(
            ASM["report_name"],
            [
                Section(
                    "Sales",
                    [
                        Metric("SalesOrder", _filter=ASM["id"]),
                        Metric("Transactions", _filter=ASM["id"]),
                        Metric(
                            "AUSPMattress",
                            "AVG",
                            "SalesOrderMattress",
                            "QuantityMattress",
                            _filter=ASM["id"],
                        ),
                        Metric("StoreTraffic", _filter=ASM["id"]),
                    ],
                ),
                Section(
                    "Customers",
                    [
                        Metric("Customers", _filter=ASM["id"]),
                        Metric("NewCustomers", _filter=ASM["id"]),
                        Metric(
                            "AOVCustomers",
                            "AVG",
                            "SalesOrder",
                            "CustomersB",
                            _filter=ASM["id"],
                        ),
                    ],
                ),
            ],
            ASM["channel_id"],
            mode,
        )
        for ASM in ASMS
    ]

    if mode == "realtime":
        reports = [sales_report, *asm_reports]
    else:
        reports = [
            sales_report,
            merchandising_report,
            marketing_report,
            *asm_reports
        ]
    return reports


def main(request):
    """API Gateway

    Args:
        request (flask.Request): HTTP request

    Raises:
        RuntimeError: No mode found

    Returns:
        dict: Job responses
    """

    request_json = request.get_json()
    if request_json:
        reports = report_runs(request_json["mode"])
        loom = ThreadLoom(max_runner_cap=10)
        for i in reports:
            loom.add_function(i.run)
        # results = [i.run() for i in reports]
        results = [v["output"] for k, v in loom.execute().items()]
        responses = {"push": "notifications", "results": results}
        return responses
    else:
        raise RuntimeError(request_json)
