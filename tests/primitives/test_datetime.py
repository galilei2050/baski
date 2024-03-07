import pytest
from datetime import datetime
from baski.primitives import json


@pytest.mark.parametrize(
    "date_str",
    [
        "2021-01-01T00:00:00Z",
        "2021-01-01T00:00:00",
        "2021-01-01T00:00:00+00:00",
        "2021-01-01T00:00:00.000Z",
        "2021-01-01T00:00:00.000",
        "2021-01-01T00:00:00.000+00:00",
        "2021-01-01T00:00:00.000000Z",
        "2021-01-01T00:00:00.000000",
        "2021-01-01T00:00:00.000000+00:00",
        "2021-01-01",
        "2021-01-01T00:00:00Z",
        "01/01/2023", '31.01.2023'
    ])
def test_str_to_datetime(date_str):
    assert isinstance(json.datetime_hook({"date": date_str}).get('date'), datetime)
