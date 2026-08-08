"""A trace's timestamp must go into Mongo as a date, and into the GCS record as a string.

Stored as a string it carried the writer's UTC offset, so rows from a server (`+00:00`) and rows
from a laptop (`-07:00`) compared against each other by their text. A window query then dropped the
ones whose offset ordered them wrong — silently, since the rows are still there.
"""

import datetime as dt
from typing import cast

from pymongo.asynchronous.database import AsyncDatabase

from baski.agents.trace import TraceCollector, TraceCollectorConfig


def _collector() -> TraceCollector:
    return TraceCollector(
        config=TraceCollectorConfig(
            user_request="anything",
            model="claude-opus-5",
            agent_name="assistant",
            system_prompt="",
            bucket_name="unused",
            database=cast("AsyncDatabase", None),
        )
    )


def test_the_timestamp_is_a_timezone_aware_datetime() -> None:
    created = _collector()._created_at

    assert isinstance(created, dt.datetime)
    assert created.tzinfo is not None, "a naive datetime compares wrong the moment two writers differ"


def test_two_collectors_in_different_zones_still_order_by_the_instant() -> None:
    earlier = _collector()._created_at.astimezone(dt.UTC)
    later = _collector()._created_at.astimezone(dt.timezone(dt.timedelta(hours=-7)))

    assert earlier <= later, "ordering must follow the instant, not the text of the offset"
