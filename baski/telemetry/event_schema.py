from baski.schema import BigQueryDateTime, Schema, String

__all__ = ["EventSchema"]


class EventSchema(Schema):
    user_id = String(required=True)
    event_type = String(required=True)
    timestamp = BigQueryDateTime(required=True, format="iso")
    uuid = String(required=True)

    payload = String(required=True)
