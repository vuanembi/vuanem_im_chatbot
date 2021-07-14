import os
import re
import time
from datetime import datetime, timedelta, timezone
from abc import abstractmethod, ABC

import jinja2
import requests
from google.cloud import bigquery

from utils import format_metric_name, format_value

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

    def get_data(self):
        jobs = [
            metric.get_data() for section in self.sections for metric in section.metrics
        ]
        jobs_done = jobs
        while jobs_done:
            jobs_done = [job for job in jobs if job.state != "DONE"]
            time.sleep(5)

    def compose(self, mode):
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
        prelude = self._compose_prelude()
        return [title, prelude]

    @abstractmethod
    def _compose_title(self):
        """Compose Title

        Raises:
            NotImplementedError: Abstract Method
        """

        raise NotImplementedError

    def _compose_prelude(self):
        template = QUERIES_ENV.get_template("components/LastUpdated.sql.j2")
        rendered_query = template.render()
        rows = BQ_CLIENT.query(rendered_query)
        row = [dict(row) for row in rows][0]
        tz = timezone(timedelta(hours=7))
        last_updated = row["LastUpdated"].astimezone(tz).strftime("%Y-%m-%dT%H:%M:%S%z")
        text = f"{last_updated}{self._compose_easter_egg()}"
        return {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": text,
            },
        }

    def _compose_easter_egg(self):
        today = datetime.now(tz=timezone(timedelta(hours=7)))
        if today.date == 15 and today.month == 5:
            special = "\nBotBụngBự Chúc mừng sinh nhật c Trang :tada:"
        else:
            special = ""
        return special

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
        self.get_data()
        payload = self.compose(self.mode)
        return self.push(payload)


class ReportDaily(Report):
    mode = "daily"

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
    mode = "realtime"

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
    def __init__(self, name, metrics):
        """Initialize Section

        Args:
            name (str): Section name
            metrics (list): List of metrics tuple
        """

        self.name = name
        self.metrics = metrics

    def compose(self, mode):
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
    def __init__(self, name, agg="SUM", numerator=None, denominator=None):
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

    def get_data(self):
        template = QUERIES_ENV.get_template(f"{self.name}.sql.j2")
        rendered_query = template.render(metric=self)
        job = BQ_CLIENT.query(rendered_query)
        job.add_done_callback(self._callback)
        return job

    def _callback(self, job):
        rows = job.result()
        row = [dict(row) for row in rows][0]
        self.values = row["metric"]["values"]
        print(self)

    def compose(self, mode):
        text = self._compose_text(mode)
        body = f"{self._compose_title()}\n{text}"
        return {"type": "mrkdwn", "text": body}

    def _compose_title(self):
        return f"*{self.name}*"

    def _compose_text(self, mode):
        if mode == "realtime":
            return self._compose_realtime()
        elif mode == "daily":
            return self._compose_daily()

    def _compose_realtime(self):
        self.values = {k: format_value(v) for k, v in self.values.items()}
        return f"> Today: {self.values['d0']}"

    def _compose_daily(self):
        compare, emoji = self._compare()
        self.values = {k: format_value(v) for k, v in self.values.items()}
        dod = f"> Y-day: {self.values['d1']}"
        compare = f"> {emoji} {compare:.2f}%"
        mtd = f"> MTD : {self.values['mtd']}"
        text = "\n".join([dod, compare, mtd])
        return text

    def _compare(self):
        if self.values["d2"] > 0 and self.values["d1"] > 0:
            compare = (
                (self.values["d1"] - self.values["d2"]) / self.values["d2"]
            ) * 100
            if compare > 0:
                emoji = "Tăng :small_red_triangle:"
            else:
                emoji = "Giảm :small_red_triangle_down:"
        else:
            compare = 0
            emoji = "null"
        return compare, emoji
