"""Timezone-aware datetime helpers used across baski."""

from datetime import UTC, date, datetime, timedelta

import pytz
from dateutil.parser import parse

__all__ = [
    "US_Central",
    "US_Eastern",
    "US_Mountain",
    "US_Pacific",
    "any_to_datetime",
    "as_local",
    "as_utc",
    "convert_values_to_date",
    "date",
    "date_to_datetime",
    "datetime",
    "is_today",
    "midnight",
    "now",
    "timedelta",
    "to_tz",
    "to_utc",
]

US_Eastern = pytz.timezone("US/Eastern")
US_Pacific = pytz.timezone("US/Pacific")
US_Central = pytz.timezone("US/Central")
US_Mountain = pytz.timezone("US/Mountain")


def now() -> datetime:
    """Return the current time in the local timezone."""
    return datetime.now(tz=UTC).astimezone()


def is_today(time_point: datetime) -> bool:
    """Return True when time_point is within 24 hours of now."""
    current = datetime.now(tz=UTC).astimezone()
    return abs(current - time_point) < timedelta(days=1)


def as_local(d: datetime) -> datetime:
    """Convert to the local system timezone."""
    return d.astimezone()


def as_utc(d: datetime) -> datetime:
    """Attach UTC if naive, otherwise convert to UTC."""
    if d.tzinfo is None:
        return pytz.UTC.localize(d)
    return d.astimezone(UTC)


def to_utc(d: datetime) -> datetime:
    """Convert via local time to UTC."""
    return to_tz(d, pytz.UTC)


def to_tz(d: datetime, tzinfo: pytz.BaseTzInfo) -> datetime:
    """Convert via local time to the given timezone."""
    d = as_local(d)
    return d.astimezone(tzinfo)


def midnight(d: datetime) -> datetime:
    """Return d with hour/minute/second/microsecond zeroed."""
    return d.replace(hour=0, minute=0, second=0, microsecond=0)


def any_to_datetime(v: datetime | int | str | None, default: datetime | None = None) -> datetime | None:
    """Coerce datetime/int(epoch)/str to a tz-aware datetime, returning default on failure."""
    if isinstance(v, datetime):
        return as_local(v)
    if isinstance(v, int):
        seconds = v / 1000 if v > 10**10 else float(v)
        return datetime.fromtimestamp(seconds, tz=UTC).astimezone()
    if isinstance(v, str):
        try:
            d = parse(v)
        except ValueError:
            return default
        if d.tzinfo is None:  # it is unclear why in this case we prefer UTC.
            d = pytz.UTC.localize(d)
        return d
    return default


def convert_values_to_date(data: dict) -> dict:  # noqa: ANON002 — polymorphic helper operating on arbitrary nested dicts
    """Recursively replace date-like string values in a dict with datetime objects."""
    return {k: _covert_value(v) for k, v in data.items()}


def _covert_value(v: object) -> object:
    if isinstance(v, dict):
        return convert_values_to_date(v)
    if isinstance(v, str):
        return any_to_datetime(v, v)  # type: ignore[arg-type]
    if isinstance(v, list):
        return [convert_values_to_date(el) for el in v]
    return v


def date_to_datetime(d: date) -> datetime:
    """Promote a date to a local-midnight datetime."""
    return as_local(datetime(d.year, d.month, d.day))  # noqa: DTZ001 — naive intentional; as_local interprets it as local wall-clock midnight
