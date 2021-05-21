import os
from datetime import datetime, timedelta
from abc import abstractmethod, ABCMeta

import jinja2
import requests
from google.cloud import bigquery


class Report(metaclass=ABCMeta):
    def __init__(self):
        self.bq_client = bigquery.Client()

    @staticmethod
    def create(mode):
        if mode == "daily":
            return ReportDaily()
        elif mode == "realtime":
            return ReportRealtime()

    def add_section(self, name, metrics):
        self.sections = []
        section = self._add_section(name, metrics, self.bq_client)
        self.sections.append(section)

    @abstractmethod
    def _add_section(self, name, metrics, bq_client):
        raise NotImplementedError

    def fetch_data(self):
        with requests.get(
            "https://shibe.online/api/shibes", params={"count": len(self.sections)}
        ) as r:
            res = r.json()
        rows = [section.build(res[i]) for i, section in enumerate(self.sections)]
        return rows

    def build(self):
        rows = self.fetch_data()
        return self.compose(rows)

    def compose(self, rows):
        payload = {}
        payload["channel"] = os.getenv("CHANNEL_ID")
        blocks = []
        blocks.extend(self.compose_header())
        blocks.extend(rows)
        payload["blocks"] = blocks
        return payload

    def compose_header(self):
        title = self._compose_title()
        prelude = self._compose_prelude()
        divider = {"type": "divider"}
        return [title, prelude, divider]

    @abstractmethod
    def _compose_title(self):
        raise NotImplementedError

    @abstractmethod
    def _compose_prelude(self):
        raise NotImplementedError


class ReportDaily(Report):
    def __init__(self):
        super().__init__()

    def _add_section(self, name, metrics, bq_client):
        return Section(name, metrics, "daily", bq_client)

    def _compose_title(self):
        now = datetime.now() - timedelta(days=1)
        return {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"Daily Report {now:%Y-%m-%d}",
                "emoji": True,
            },
        }

    def _compose_prelude(self):
        return {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"Báo cáo đầu ngày nên BI BOT ko dám ba hoa",
            },
        }


class ReportRealtime(Report):
    def __init__(self):
        super().__init__()

    def _add_section(self, name, metrics, bq_client):
        return Section(name, metrics, "realtime", bq_client)

    def _compose_title(self):
        now = datetime.now()
        return {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"Realtime Report {now:%Y-%m-%d}",
                "emoji": True,
            },
        }

    def _compose_prelude(self):
        weather_mess, temp = self._compose_temp()
        return {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"Nhiệt độ Hà Nội hiện là {temp}°C. {weather_mess}\n*Đưa tay đây nào, nhận báo cáo bạn nhớ :D*",
            },
        }

    def _compose_temp(self):
        with requests.get(
            "https://api.openweathermap.org/data/2.5/weather",
            params={
                "APPID": os.getenv("WEATHER_API_KEY"),
                "id": "1581129",
                "units": "metric",
            },
        ) as r:
            res = r.json()
        weather_cond = res["weather"][0]["id"]
        if weather_cond <= 232:
            weather_mess = "Sắp mưa to bão bùng kinh khủng :zap:. BI BOT đề xuất về sớm để tránh ngập lụt tát nước."
        elif weather_cond >= 300 and weather_cond <= 531:
            weather_mess = "Sắp mưa :umbrella:. BI BOT đề xuất bạn nào ở nhà quần áo đang phơi thì về sớm cất quần áo."
        elif weather_cond == 800:
            weather_mess = "Trời trong xanh :sunny:. BI BOT đề xuất nghỉ sớm để ra ngoài tận hưởng thời tiết."
        elif weather_cond > 800:
            weather_mess = (
                "Trời trong xanh :cloud:. BI BOT đề xuất lên sân thượng ngắm thời tiết."
            )

        temp = res["main"]["temp"]
        return weather_mess, temp


class Section:
    def __init__(self, name, metrics, mode, bq_client):
        self.section_name = name
        self.bq_client = bq_client
        self.metrics = [Metric.factory(metric, mode) for metric in metrics]

    def fetch_data(self):
        loader = jinja2.FileSystemLoader(searchpath="./queries")
        env = jinja2.Environment(loader=loader)
        template = env.get_template("Section.sql.j2")
        rendered_query = template.render(section=self)
        rows = self.bq_client.query(rendered_query).result()
        row = [row for row in rows][0]
        return row

    def build(self, image):
        row = self.fetch_data()
        self.hydrate(row)
        return self.compose(image)

    def hydrate(self, row):
        for k, v in row.items():
            if k != "date":
                for metric in self.metrics:
                    if metric.metric_name == k:
                        metric = metric.update({"metric_name": k, **v})

    def compose(self, image):
        head = self._compose_head()
        body = self._compose_body()
        return {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": head,
            },
            "fields": body,
            "accessory": {
                "type": "image",
                "image_url": image,
                "alt_text": "null",
            },
        }

    def _compose_head(self):
        return f"*{self.section_name}* :moneybag: :star: :moneybag:"

    def _compose_body(self):
        return [metric.build() for metric in self.metrics]


class Metric(metaclass=ABCMeta):
    def __init__(self, name):
        self.metric_name = name
        self.title = f"*{self.metric_name}*"

    @staticmethod
    def factory(name, mode):
        if mode == "realtime":
            return MetricRealtime(name)
        elif mode == "daily":
            return MetricDaily(name)
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
        if value >= 1e9:
            return f"{value/1e9:.2f} B"
        elif value >= 1e6:
            return f"{value/1e6:.2f} M"
        elif value >= 1e3:
            return f"{value/1e3:.2f} K"
        else:
            return value


class MetricRealtime(Metric):
    def __init__(self, name):
        super().__init__(name)

    def _compose_text(self):
        self.format_value()
        return f"> :small_blue_diamond: Today: {self.d0}"


class MetricDaily(Metric):
    def __init__(self, name):
        super().__init__(name)

    def _compose_text(self):
        self.compare, self.emoji = self._compare()
        self.format_value()
        dod = f"> :small_blue_diamond: Y-day: {self.d1}"
        compare = f"> :small_blue_diamond: {self.emoji} {self.compare:.2f}%"
        mtd = f"> :small_blue_diamond: MTD : {self.mtd}"
        text = "\n".join([dod, compare, mtd])
        return text

    def _compare(self):
        if self.d2 > 0:
            compare = ((self.d1 - self.d2) / self.d2) * 100
            if compare > 0:
                emoji = "Tăng :heart:"
            else:
                emoji = "Giảm :broken_heart:"
        return compare, emoji
