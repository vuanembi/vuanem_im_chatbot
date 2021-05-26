from unittest.mock import Mock

from main import main


def test_daily():
    data = {"mode":  "daily"}
    req = Mock(get_json=Mock(return_value=data), args=data)
    res = main(req)
    assert res['reports_pushed'] > 0


def test_realtime():
    data = {"mode": "realtime"}
    req = Mock(get_json=Mock(return_value=data), args=data)
    res = main(req)
    assert res['reports_pushed'] > 0
