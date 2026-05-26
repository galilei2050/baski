import logging
import uuid
import json
from google.cloud import pubsub
from baski.primitives import datetime
from .event_schema import EventSchema


__all__ = ['Telemetry']


class Telemetry(object):
    _schema = EventSchema()

    def __init__(self, publisher: pubsub.PublisherClient, project_id, topic_name="event", publish=True):
        self.publisher = publisher
        self.topic_path = self.publisher.topic_path(project_id, topic_name)
        self.publish = publish

    def add(self, user_id: str, event_type, payload: dict, timestamp=None):
        try:
            data = {
                "user_id": str(user_id),
                "event_type": event_type,
                "timestamp": datetime.as_local(timestamp) if timestamp else datetime.now(),
                "uuid": str(uuid.uuid4()),
                "payload": json.dumps(_clean_dict(payload)),
            }
            queue_item = self._schema.dumps(data)
            if self.publish:
                self.publisher.publish(self.topic_path, data=queue_item.encode('utf-8'))
        except Exception as e:
            logging.warning(f"Failed to add telemetry event: {e}")


def _clean_dict(data: dict):
    assert isinstance(data, dict), "data must be a dictionary"
    return {k: _clean_value(v) for k, v in data.items() if v is not None}


def _clean_list(data: list):
    assert isinstance(data, (list, set)), "data must be a list or set"
    return [_clean_value(v) for v in data if v is not None]


def _clean_value(value):
    if isinstance(value, dict):
        return _clean_dict(value)
    elif isinstance(value, (list, set)):
        return _clean_list(value)
    elif isinstance(value, str):
        return value.strip()
    elif isinstance(value, (bool, float, int)):
        return value
    elif isinstance(value, datetime.datetime):
        return datetime.to_utc(value).replace(tzinfo=None).isoformat()

    raise ValueError(f"Unsupported type in telemetry data: {type(value)}")
