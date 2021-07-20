from .utils import process

def test_daily():
    data = {"mode":  "daily"}
    process(data)


def test_realtime():
    data = {"mode": "realtime"}
    process(data)
