from marshmallow import fields

from ..primitives.datetime import to_utc


class BigQueryDateTime(fields.DateTime):
    def _serialize(self, value, attr, obj, **kwargs) -> str | float | None:
        if value is None:
            return None
        return to_utc(value).replace(tzinfo=None).isoformat()


class NotNullFloat(fields.Float):
    def _serialize(self, value, attr, obj, **kwargs) -> str | None:
        if value is None:
            return 0.0
        return super()._serialize(value, attr, obj, **kwargs)


class NotNullString(fields.String):
    def __init__(self, *, dump_default="", load_default="", **kwargs):
        super().__init__(dump_default=dump_default, load_default=load_default, **kwargs)

    def _serialize(self, value, attr, obj, **kwargs) -> str | None:
        if value is None:
            return ""
        return super()._serialize(value, attr, obj, **kwargs)
