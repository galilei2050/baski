import datetime as dt
from typing import Any

from marshmallow import fields

from ..primitives.datetime import to_utc


class BigQueryDateTime(fields.DateTime):
    def _deserialize(self, value: Any, attr: str | None, data: Any, **kwargs: Any) -> dt.datetime:
        value = value if isinstance(value, dt.datetime) else super()._deserialize(value, attr, data, **kwargs)
        return to_utc(value)

    def _serialize(self, value: dt.datetime | None, attr: str | None, obj: Any, **kwargs: Any) -> Any:  # noqa: ARG002 — marshmallow override; we don't need attr/obj/kwargs
        if value is None:
            return None
        return to_utc(value).replace(tzinfo=None).isoformat()


class NotNullFloat(fields.Float):
    def _serialize(self, value: float | None, attr: str | None, obj: Any, **kwargs: Any) -> Any:
        if value is None:
            return 0.0
        return super()._serialize(value, attr, obj, **kwargs)


class NotNullString(fields.String):
    def __init__(self, *, dump_default: str = "", load_default: str = "", **kwargs: Any) -> None:
        super().__init__(dump_default=dump_default, load_default=load_default, **kwargs)

    def _serialize(self, value: str | None, attr: str | None, obj: Any, **kwargs: Any) -> Any:
        if value is None:
            return ""
        return super()._serialize(value, attr, obj, **kwargs)
