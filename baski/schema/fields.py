"""Custom marshmallow field types used across baski schemas."""

import datetime as dt
from typing import Any

from marshmallow import fields

from ..primitives.datetime import to_utc


class BigQueryDateTime(fields.DateTime):
    """DateTime that normalizes to UTC and serializes as naive ISO for BigQuery."""

    def _deserialize(self, value: Any, attr: str | None, data: Any, **kwargs: Any) -> dt.datetime:  # noqa: ANN401 — marshmallow override signature
        value = value if isinstance(value, dt.datetime) else super()._deserialize(value, attr, data, **kwargs)
        return to_utc(value)

    def _serialize(self, value: dt.datetime | None, attr: str | None, obj: Any, **kwargs: Any) -> Any:  # noqa: ARG002, ANN401 — marshmallow override signature
        if value is None:
            return None
        return to_utc(value).replace(tzinfo=None).isoformat()


class NotNullFloat(fields.Float):
    """Float field that serializes ``None`` as 0.0."""

    def _serialize(self, value: float | None, attr: str | None, obj: Any, **kwargs: Any) -> Any:  # noqa: ANN401 — marshmallow override signature
        if value is None:
            return 0.0
        return super()._serialize(value, attr, obj, **kwargs)


class NotNullString(fields.String):
    """String field that defaults to empty string and serializes ``None`` as ``""``."""

    def __init__(self, *, dump_default: str = "", load_default: str = "", **kwargs: Any) -> None:  # noqa: ANN401 — marshmallow field kwargs are polymorphic
        """Forward to ``fields.String`` with empty-string defaults."""
        super().__init__(dump_default=dump_default, load_default=load_default, **kwargs)

    def _serialize(self, value: str | None, attr: str | None, obj: Any, **kwargs: Any) -> Any:  # noqa: ANN401 — marshmallow override signature
        if value is None:
            return ""
        return super()._serialize(value, attr, obj, **kwargs)
