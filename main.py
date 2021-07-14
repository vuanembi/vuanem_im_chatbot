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
gross_margin = Metric("GrossMargin", "AVG", "GrossProfit", "Sales")

# facebook_spend = Metric("FacebookSpend")
# google_spend = Metric("GoogleSpend")
# funnel_spend = Metric("FunnelSpend")

total_leads = Metric("TotalLeads")
unique_leads = Metric("UniqueLeads")
phones_collected = Metric("PhonesCollected")
# cost_per_lead = Metric("CostPerLead", "AVG", "FunnelSpend", "PhonesCollected")

acquired_customers = Metric("AcquiredCustomers")
funnel_cr = Metric("FunnelCR", "AVG", "AcquiredCustomers", "PhonesCollected")
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


class ASMReportBase:
    def __init__(self, name):
        self.name = name
        self.metrics = self.get_metrics()
        self.sections = self.get_sections()

    def get_metrics(self):
        return {
            "sales_order": Metric(f"{self.name}SalesOrder"),
            "transactions": Metric(f"{self.name}Transactions"),
            "ausp_mattress": Metric(
                f"{self.name}AUSPMattress",
                "AVG",
                f"{self.name}SalesOrderMattress",
                f"{self.name}Quantity",
            ),
            "store_traffic": Metric(f"{self.name}StoreTraffic"),
            "customers": Metric(f"{self.name}Customers"),
            "new_customers": Metric(f"{self.name}NewCustomers"),
            "aov_customers": Metric(
                f"{self.name}AOVCustomers",
                "AVG",
                f"{self.name}SalesOrder",
                f"{self.name}CustomersB",
            ),
        }

    def get_sections(self):
        return [
            Section(
                "Sales",
                [
                    self.metrics["sales_order"],
                    self.metrics["transactions"],
                    self.metrics["ausp_mattress"],
                    self.metrics["store_traffic"],
                ],
            ),
            Section(
                "KhachHang",
                [
                    self.metrics["customers"],
                    self.metrics["new_customers"],
                    self.metrics["aov_customers"],
                ],
            ),
        ]


def report_runs(mode="daily"):
    sales_report = Report.factory(
        "Sales", [sales_section, customers_section], "C027V5CP86P", mode
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

    ASMS = [
        {
            "asm": "ASMHuong",
            "report_name": "Báo cáo cho ASM Hương",
            "channel_id": "C027V5CP86P",
        },
        # {
        #     "asm": "ASMTung",
        #     "report_name": "Báo cáo cho ASM Tùng",
        #     "channel_id": "C027V5CP86P"
        # },
        # {
        #     "asm": "ASMThanh",
        #     "report_name": "Báo cáo cho ASM Thanh",
        #     "channel_id": "C027V5CP86P"
        # },
        # {
        #     "asm": "ASMDuc",
        #     "report_name": "Báo cáo cho ASM Đức",
        #     "channel_id": "C027V5CP86P"
        # },
        # {
        #     "asm": "ASMDanh",
        #     "report_name": "Báo cáo cho ASM Danh",
        #     "channel_id": "C027V5CP86P"
        # },
        # {
        #     "asm": "ASMHien",
        #     "report_name": "Báo cáo cho ASM Hiền",
        #     "channel_id": "C027V5CP86P"
        # },
        # {
        #     "asm": "ASMHUyen",
        #     "report_name": "Báo cáo cho ASM Uyên",
        #     "channel_id": "C027V5CP86P"
        # },
        # {
        #     "asm": "ASMNgan",
        #     "report_name": "Báo cáo cho ASM Ngân",
        #     "channel_id": "C027V5CP86P"
        # },
        # {
        #     "asm": "ASMThuy",
        #     "report_name": "Báo cáo cho ASM Thuỳ",
        #     "channel_id": "C027V5CP86P"
        # },
    ]

    asm_reports = []
    for ASM in ASMS:
        asm = ASMReportBase(ASM["asm"])
        asm_reports.append(
            Report.factory(ASM["report_name"], asm.sections, ASM["channel_id"], mode)
        )

    if mode == "realtime":
        reports = [sales_report, *asm_reports]
    else:
        reports = [sales_report, merchandising_report, marketing_report]
        reports
    return reports


def main(request):
    request_json = request.get_json()
    if request_json:
        reports = report_runs(request_json["mode"])
        for i in reports:
            i.run()
        responses = {"push": "notifications", "results": [i.run() for i in reports]}
        return responses
    else:
        raise RuntimeError(request_json)
