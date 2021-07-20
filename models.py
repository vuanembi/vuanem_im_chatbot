import os
import time
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
            secions (list): List of Sections
            channel_id (str): channel_id
        """

        self.name = name
        self.sections = sections
        self.channel_id = channel_id

    @staticmethod
    def factory(name, sections, channel_id, mode):
        """Factory Method

        Args:
            name (str): Report name
            sections (list): List of Sections
            channel_id (str): Channel ID
            mode (str): Mode

        Returns:
            Report: Report
        """

        if mode == "realtime":
            return ReportRealtime(name, sections, channel_id)
        elif mode == "daily":
            return ReportDaily(name, sections, channel_id)

    def get_data(self):
        """Get Data for all metrics within Report"""

        jobs = [
            metric.get_data() for section in self.sections for metric in section.metrics
        ]
        jobs_done = jobs
        while jobs_done:
            jobs_done = [job for job in jobs if job.state != "DONE"]
            time.sleep(5)

    def compose(self, mode):
        """Compose Payload

        Args:
            mode (str): Mode

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
            list: (title, prelude)
        """

        title = self._compose_title()
        prelude = self._compose_prelude()
        return title, prelude

    @abstractmethod
    def _compose_title(self):
        """Compose Title"""

        pass

    def _compose_prelude(self):
        """Compose Prelude

        Returns:
            dict: Prelude section
        """

        template = QUERIES_ENV.get_template("components/LastUpdated.sql.j2")
        rendered_query = template.render()
        rows = BQ_CLIENT.query(rendered_query)
        row = [dict(row) for row in rows][0]
        tz = timezone(timedelta(hours=7))
        last_updated = row["LastUpdated"].astimezone(tz).strftime("%Y-%m-%dT%H:%M:%S%z")
        text = f"Cập nhật tới @ {last_updated}{self._compose_easter_egg()}"
        return {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": text,
            },
        }

    def _compose_easter_egg(self):
        """Compose Easter Egg

        Returns:
            str: Easter Egg
        """

        today = datetime.now(tz=timezone(timedelta(hours=7)))
        if today.date == 15 and today.month == 5:
            special = "\nBotBụngBự Chúc mừng sinh nhật c Trang :tada:"
        else:
            special = ""
        return special

    def push(self, payload):
        """Push payload to Slack API

        Returns:
            bool: Slack responses
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
                "text": f"{self.name} :: Daily @ {now:%Y-%m-%d}",
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
                "text": f"{self.name} :: Realtime @ {now:%Y-%m-%d}",
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
        """Compose Section

        Args:
            mode (str): Mode

        Returns:
            dict: Section
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
        """Compose Section header

        Returns:
            str: Section header
        """

        return f"*{self.name}*"

    def _compose_body(self, mode):
        """Compose Section body

        Args:
            mode (str): Mode

        Returns:
            list: Section bodies
        """

        return [metric.compose(mode) for metric in self.metrics]


class Metric:
    def __init__(
        self,
        name,
        agg="SUM",
        numerator=None,
        denominator=None,
        _format="numeric",
        _filter=None,
    ):
        """Initialize Metric

        Args:
            name (str): Metric name
            agg (str): Metric aggregation (SQL)
            numerator (str): Numerator
            denominator (str): Denominator
            _format (str): Metric value formatting options
            _filter (any): Filter for SQL
        """

        self.name = name
        self.agg = agg
        self.numerator = numerator
        self.denominator = denominator
        self.format = _format
        self.filter = _filter

    def get_data(self):
        """Get data from BigQuery

        Returns:
            google.cloud.bigquery.QueryJob: Query Job
        """

        template = QUERIES_ENV.get_template(f"{self.name}.sql.j2")
        rendered_query = template.render(metric=self)
        job = BQ_CLIENT.query(rendered_query)
        job.add_done_callback(self._callback)
        return job

    def _callback(self, job):
        """Callback for async BigQuery QueryJob

        Args:
            job (google.cloud.bigquery.QueryJob): Query Job
        """

        rows = job.result()
        row = [dict(row) for row in rows][0]
        self.values = row["metric"]["values"]

    def compose(self, mode):
        """Comopse Metric

        Args:
            mode (str): Mode

        Returns:
            dict: Metric
        """

        text = self._compose_text(mode)
        body = f"{self._compose_title()}\n{text}"
        return {"type": "mrkdwn", "text": body}

    def _compose_title(self):
        """Compose Metric title

        Returns:
            str: Metric title
        """

        return f"*{self.name}*"

    def _compose_text(self, mode):
        """Compose Metric body

        Args:
            mode (str): Mode

        Returns:
            str: Metric body
        """

        if mode == "realtime":
            return self._compose_realtime()
        elif mode == "daily":
            return self._compose_daily()

    def _compose_realtime(self):
        """Compose Realtime Metric values

        Returns:
            str: Realtime values
        """

        self.values = {k: self.format_value(v) for k, v in self.values.items()}
        return f"> Hnay: {self.values['d0']}"

    def _compose_daily(self):
        """Compose Daily Metric values

        Returns:
            str: Daily values
        """

        compare, emoji = self._compare()
        self.values = {k: self.format_value(v) for k, v in self.values.items()}
        dod = f"> Hqua: {self.values['d1']}"
        compare = f"> {emoji} {compare:.2f}%"
        mtd = f"> MTD : {self.values['mtd']}"
        text = "\n".join([dod, compare, mtd])
        return text

    def _compare(self):
        """Compare Metric values

        Returns:
            tuple: (compare, emoji)
        """

        if self.values["d2"] and self.values["d1"]:
            compare = (
                (self.values["d1"] - self.values["d2"]) / self.values["d2"]
            ) * 100
            if compare > 0:
                emoji = "Tăng :small_red_triangle:"
            else:
                emoji = "Giảm :small_red_triangle_down:"
        else:
            compare = 0
            emoji = ":neutral_face:"
        return compare, emoji

    def format_value(self, value):
        if self.format == 'numeric':
            return self.format_numeric(value)
        elif self.format == 'percentage':
            return self.format_percentage(value)

    @staticmethod
    def format_numeric(value):
        if value is None:
            return 0
        elif value >= 1e9:
            return f"{value/1e9:.2f} B"
        elif value >= 1e6:
            return f"{value/1e6:.2f} M"
        elif value >= 1e3:
            return f"{value/1e3:.2f} K"
        elif isinstance(value, int):
            return f"{value}"
        else:
            return f"{value:.2f}"

    @staticmethod
    def format_percentage(value):
        return f"{value*1e2:.2f} %"
