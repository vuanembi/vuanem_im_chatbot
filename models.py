import os
import re
from datetime import datetime, timedelta, timezone
from abc import abstractmethod, ABC

import jinja2
import requests
from google.cloud import bigquery

BQ_CLIENT = bigquery.Client()

QUERIES_LOADER = jinja2.FileSystemLoader(searchpath="./queries")
QUERIES_ENV = jinja2.Environment(loader=QUERIES_LOADER)


class Report(ABC):
    def __init__(self, name, sections, channel_id):
        """Initialize Report

        Args:
            name (str): Report Name
            channel_id (str): channel_id
        """

        self.name = name
        self.sections = sections
        self.channel_id = channel_id

    @staticmethod
    def factory(name, sections, channel_id, mode):
        if mode == "realtime":
            return ReportRealtime(name, sections, channel_id)
        elif mode == "daily":
            return ReportDaily(name, sections, channel_id)

    def fetch_data(self):
        """Fetch data into components

        Returns:
            list: List of results
        """
        template = QUERIES_ENV.get_template("Report.sql.j2")
        rendered_query = template.render(report=self)
        rows = BQ_CLIENT.query(rendered_query).result()
        row = [dict(row) for row in rows][0]
        return row["LastUpdated"], row["sections"]

    def compose(self, mode):
        """Compose components using ingested data

        Args:
            rows (list): List of results

        Returns:
            dict: Payload
        """

        payload = {}
        payload["channel"] = self.channel_id
        blocks = []
        blocks.extend(self.compose_header())
        for section in self.sections:
            blocks.append({"type": "divider"})
            blocks.append(section.compose(mode))
        payload["blocks"] = blocks
        return payload

    def compose_header(self):
        """Compose Header for report

        Returns:
            tuple: (title, prelude)
        """

        title = self._compose_title()
        prelude = self._compose_prelude(self.last_updated)
        return [title, prelude]

    @abstractmethod
    def _compose_title(self):
        """Compose Title

        Raises:
            NotImplementedError: Abstract Method
        """

        raise NotImplementedError

    def _compose_prelude(self, last_updated):
        tz = timezone(timedelta(hours=7))
        last_updated = last_updated.astimezone(tz).strftime('%Y-%m-%dT%H:%M:%S%z')
        return {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"Last Updated: {last_updated}",
            },
        }

    def push(self, payload):
        """Push payload to Slack API

        Returns:
            bool: OK responses
        """

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
        return res.get("ok")

    def run(self):
        self.last_updated, sections = self.fetch_data()
        self.sections = [Section.from_json(section) for section in sections]
        payload = self.compose(self.mode)
        res = self.push(payload)
        return res


class ReportDaily(Report):
    mode = 'daily'

    def __init__(self, name, sections, channel_id):
        super().__init__(name, sections, channel_id)

    def _compose_title(self):
        now = datetime.now() - timedelta(days=1)
        return {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{self.name} :: Daily :: {now:%Y-%m-%d}",
                "emoji": True,
            },
        }


class ReportRealtime(Report):
    mode = 'realtime'

    def __init__(self, name, sections, channel_id):
        super().__init__(name, sections, channel_id)

    def _compose_title(self):
        now = datetime.now()
        return {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{self.name} :: Realtime :: {now:%Y-%m-%d}",
                "emoji": True,
            },
        }


class Section:
    def __init__(self, name, metrics, from_json=False):
        """Initialize Section

        Args:
            name (str): Section name
            metrics (list): List of metrics tuple
        """

        self.name = name
        self.metrics = metrics
        if from_json:
            self.metrics = [Metric.from_json(metric) for metric in metrics]

    @classmethod
    def from_json(cls, section):
        return cls(section["name"], section["metrics"], True)

    def compose(self, mode):
        """Compose Section components

        Returns:
            dict: Section components
        """

        head = self._compose_head()
        body = self._compose_body(mode)
        return {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": head,
            },
            "fields": body,
        }

    def _compose_head(self):
        return f"*{self.name}*"

    def _compose_body(self, mode):
        return [metric.compose(mode) for metric in self.metrics]


class Metric:
    def __init__(self, name, agg="SUM", numerator=None, denominator=None, values=None):
        """Initialize Metric

        Args:
            name (str): Metric name
            agg (str): Metric aggregation (SQL)
            numerator (str): Numerator
            denominator (str): Denominator
        """

        self.name = name
        self.agg = agg
        self.numerator = numerator
        self.denominator = denominator
        self.values = values

    @classmethod
    def from_json(cls, metric):
        return cls(metric["name"], values=metric["values"])

    def compose(self, mode):
        """Compose Metric components

        Returns:
            dict: Metric components
        """

        text = self._compose_text(mode)
        body = f"{self._compose_title()}\n{text}"
        return {"type": "mrkdwn", "text": body}

    def _compose_title(self):
        return f"*{self.name}*"

    def _compose_text(self, mode):
        self.format_value()
        # self.format_metric_name()
        if mode == "realtime":
            return self._compose_realtime()
        elif mode == "daily":
            return self._compose_daily()

    def _compose_realtime(self):
        return f"> Today: {self.values['d0']}"

    def _compose_daily(self):
        """Componse Metric text body

        Returns:
            str: Text components
        """

        compare, emoji = self._compare()
        dod = f"> Y-day: {self.values['d1']}"
        compare = f"> {emoji} {compare:.2f}%"
        mtd = f"> MTD : {self.values['mtd']}"
        text = "\n".join([dod, compare, mtd])
        return text

    def _compare(self):
        """Compare metric values

        Returns:
            tuple: (compare, emoji)
        """

        if self.values['d2'] > 0 and self.values['d1'] > 0:
            compare = ((self.values['d1'] - self.values['d2']) / self.values['d2']) * 100
            if compare > 0:
                emoji = "Tăng :small_red_triangle:"
            else:
                emoji = "Giảm :small_red_triangle_down:"
        else:
            compare = 0
            emoji = "null"
        return compare, emoji

    def format_value(self):
        """Format metrics value"""

        self.values['d0'], self.values['d1'], self.values['d2'], self.values['mtd'] = [
            self._format_value(i)
            for i in [self.values['d0'], self.values['d1'], self.values['d2'], self.values['mtd']]
        ]

    def _format_value(self, value):
        if value is None:
            return None
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

    def format_metric_name(self):
        pattern = "ASM.+(?=[A-Z])"
        self.name = re.sub(pattern, "", self.name)
