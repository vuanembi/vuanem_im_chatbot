import os
from datetime import datetime, timedelta, timezone
from abc import abstractmethod, ABCMeta

import jinja2
import requests
from google.cloud import bigquery


class Report(metaclass=ABCMeta):
    def __init__(self, name, channel_id):
        self.report_name = name
        self.channel_id = channel_id
        self.bq_client = bigquery.Client()
        self.sections = []

    @staticmethod
    def create(name, mode, channel_id):
        if mode == "daily":
            return ReportDaily(name, channel_id)
        elif mode == "realtime":
            return ReportRealtime(name, channel_id)

    def add_section(self, name, metrics):
        section = self._add_section(name, metrics, self.bq_client)
        self.sections.append(section)

    @abstractmethod
    def _add_section(self, name, metrics, bq_client):
        raise NotImplementedError

    def fetch_data(self):
        # headers = {"Accept-Version": "v1"}
        # params = {
        #     "client_id": os.getenv("UNSPLASH_CLIENT_ID"),
        #     "query": "burger",
        #     "orientation": "squarish",
        #     "content_filter": "low",
        #     "count": len(self.sections),
        # }
        # with requests.get(
        #     "https://api.unsplash.com/photos/random", headers=headers, params=params
        # ) as r:
        #     res = r.json()
        # urls = [i["urls"]["small"] for i in res]
        rows = [section.build() for section in self.sections]
        return rows

    def build(self):
        rows = self.fetch_data()
        return self.compose(rows)

    def compose(self, rows):
        payload = {}
        payload["channel"] = self.channel_id
        blocks = []
        blocks.extend(self.compose_header())
        for row in rows:
            blocks.append({"type": "divider"})
            blocks.append(row)
        payload["blocks"] = blocks
        payload
        return payload

    def compose_header(self):
        title = self._compose_title()
        prelude = self._compose_prelude()
        return [title, prelude]

    @abstractmethod
    def _compose_title(self):
        raise NotImplementedError

    def _compose_prelude(self):
        query = '''
        SELECT LastUpdated FROM Analytics._agg_Slack_Report
        '''
        rows = self.bq_client.query(query).result()
        row = [row.values() for row in rows][0][0]
        tz = timezone(timedelta(hours=7))
        last_updated = row.astimezone(tz).strftime('%Y-%m-%dT%H:%M:%S%z')
        return {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"Last Updated: {last_updated}",
            },
        }


    def push(self):
        token = os.getenv("TOKEN")
        headers = {
            "charset": "utf-8",
            "Content-type": "application/json",
            "Authorization": f"Bearer {token}",
        }
        payload = self.build()
        payload
        with requests.post(
            "https://slack.com/api/chat.postMessage", headers=headers, json=payload
        ) as r:
            res = r.json()
        return res.get('ok')


class ReportDaily(Report):
    def __init__(self, name, channel_id):
        super().__init__(name, channel_id)

    def _add_section(self, name, metrics, bq_client):
        return Section(name, metrics, "daily", bq_client)

    def _compose_title(self):
        now = datetime.now() - timedelta(days=1)
        return {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{self.report_name} :: Daily :: {now:%Y-%m-%d}",
                "emoji": True,
            },
        }

class ReportRealtime(Report):
    def __init__(self, name, channel_id):
        super().__init__(name, channel_id)

    def _add_section(self, name, metrics, bq_client):
        return Section(name, metrics, "realtime", bq_client)

    def _compose_title(self):
        now = datetime.now()
        return {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{self.report_name} :: Realtime :: {now:%Y-%m-%d}",
                "emoji": True,
            },
        }

class Section:
    def __init__(self, name, metrics, mode, bq_client):
        self.section_name = name
        self.bq_client = bq_client
        self.metrics = [
            Metric.factory(
                metric[0],
                mode,
                metric[1],
                self.safe_index(metric, 2),
                self.safe_index(metric, 3),
            )
            for metric in metrics
        ]

    def safe_index(self, metric, i):
        try:
            return metric[i]
        except IndexError:
            return None

    def fetch_data(self):
        loader = jinja2.FileSystemLoader(searchpath="./queries")
        env = jinja2.Environment(loader=loader)
        template = env.get_template("Section.sql.j2")
        rendered_query = template.render(section=self)
        rows = self.bq_client.query(rendered_query).result()
        row = [row for row in rows][0]
        return row

    def build(self):
        row = self.fetch_data()
        self.hydrate(row)
        return self.compose()

    def hydrate(self, row):
        for k, v in row.items():
            if k != "date":
                for metric in self.metrics:
                    if metric.metric_name == k:
                        metric = metric.update({"metric_name": k, **v})

    def compose(self):
        head = self._compose_head()
        body = self._compose_body()
        return {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": head,
            },
            "fields": body
        }

    def _compose_head(self):
        return f"*{self.section_name}*"

    def _compose_body(self):
        return [metric.build() for metric in self.metrics]


class Metric(metaclass=ABCMeta):
    def __init__(self, name, agg, numerator, denominator):
        self.metric_name = name
        self.agg = agg
        self.title = f"*{self.metric_name}*"
        self.numerator = numerator
        self.denominator = denominator

    @staticmethod
    def factory(name, mode, agg, numerator, denominator):
        if mode == "realtime":
            return MetricRealtime(name, agg, numerator, denominator)
        elif mode == "daily":
            return MetricDaily(name, agg, numerator, denominator)
        else:
            raise NotImplementedError("Metric mode not found")

    def update(self, entries):
        self.__dict__.update(entries)
        return self

    def build(self):
        return self.compose()

    def compose(self):
        text = self._compose_text()
        body = f"{self.title}\n{text}"
        return {"type": "mrkdwn", "text": body}

    @abstractmethod
    def _compose_text(self):
        raise NotImplementedError

    def format_value(self):
        self.d0, self.d1, self.d2, self.mtd = [
            self._format_value(i) for i in [self.d0, self.d1, self.d2, self.mtd]
        ]

    def _format_value(self, value):
        if value is None:
            return f"null"
        elif value >= 1e9:
            return f"{value/1e9:.2f} B"
        elif value >= 1e6:
            return f"{value/1e6:.2f} M"
        elif value >= 1e3:
            return f"{value/1e3:.2f} K"
        elif value <= 1 and value >= -1:
            return f"{value*1e2:.2f} %"
        elif isinstance(value, int):
            return f"{value}"
        else:
            return f"{value:.2f}"


class MetricRealtime(Metric):
    def __init__(self, name, agg, numerator, denominator):
        super().__init__(name, agg, numerator, denominator)

    def _compose_text(self):
        self.format_value()
        return f"> :small_blue_diamond: Today: {self.d0}"


class MetricDaily(Metric):
    def __init__(self, name, agg, numerator, denominator):
        super().__init__(name, agg, numerator, denominator)
        self.numerator = numerator
        self.denominator = denominator

    def _compose_text(self):
        self.compare, self.emoji = self._compare()
        self.format_value()
        dod = f"> - Y-day: {self.d1}"
        compare = f"> - {self.emoji} {self.compare:.2f}%"
        mtd = f"> - MTD : {self.mtd}"
        text = "\n".join([dod, compare, mtd])
        return text

    def _compare(self):
        if self.d2 > 0 and self.d1 > 0:
            compare = ((self.d1 - self.d2) / self.d2) * 100
            if compare > 0:
                emoji = "Tăng :small_red_triangle:"
            else:
                emoji = "Giảm :small_red_triangle_down:"
        else:
            compare = 0
            emoji = "null"
        return compare, emoji
