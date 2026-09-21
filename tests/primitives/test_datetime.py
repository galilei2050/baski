import pytest
from datetime import datetime, timezone
from baski.primitives import json


@pytest.mark.parametrize(
    "date_str",
    [
        "2024-03-10 21:00:48.962971+00:00",
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
    parsed = json.datetime_hook({"date": date_str}).get('date')
    assert isinstance(parsed, datetime)
    assert parsed.tzinfo is not None, "a datetime out of the hook must be comparable with now()"


def test_loads_returns_the_same_instant_dumps_wrote():
    loaded = json.loads('{"z": "2021-01-01T00:00:00Z", "offset": "2021-01-01T00:00:00+00:00"}')
    assert loaded["z"] == loaded["offset"] == datetime(2021, 1, 1, tzinfo=timezone.utc)
