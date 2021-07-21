from unittest.mock import Mock

from main import main

def process(data):
    req = Mock(get_json=Mock(return_value=data), args=data)
    res = main(req)
    results = res['results']
    for i in results:
        assert i is True
