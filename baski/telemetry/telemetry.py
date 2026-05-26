"""Telemetry client that publishes structured events to a Pub/Sub topic."""

import json
import uuid
from datetime import datetime as _dt
from typing import Any

from google.api_core import exceptions as gapi_exceptions
from google.cloud import pubsub

from ..primitives import datetime
from ..server.logger import LocalLogger, Logger
from .event_schema import EventSchema

__all__ = ["Telemetry"]


class Telemetry:
    """Publishes telemetry events to a Pub/Sub topic, validating them against ``EventSchema``."""

    _schema = EventSchema()

    def __init__(  # noqa: PLR0913 — publisher/project/topic/publish/logger are all independent injection points
        self,
        publisher: pubsub.PublisherClient,
        project_id: str,
        topic_name: str = "event",
        *,
        publish: bool = True,
        logger: Logger | None = None,
    ) -> None:
        """Configure the publisher, topic path, publish toggle, and logger."""
        self.publisher = publisher
        self.topic_path = self.publisher.topic_path(project_id, topic_name)
        self.publish = publish
        self._logger: Logger = logger or LocalLogger()

    def add(
        self,
        user_id: str,
        event_type: str,
        payload: dict,  # noqa: ANON002 — arbitrary user telemetry event payload
        timestamp: _dt | None = None,
    ) -> None:
        """Serialize and (optionally) publish a single telemetry event; logs and swallows failures."""
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
                self.publisher.publish(self.topic_path, data=queue_item.encode("utf-8"))
        except (TypeError, ValueError, gapi_exceptions.GoogleAPICallError) as e:
            self._logger.warning(f"Failed to add telemetry event: {e}")


def _clean_dict(data: dict) -> dict:  # noqa: ANON002 — polymorphic helper for arbitrary nested telemetry payloads
    if not isinstance(data, dict):
        raise TypeError("data must be a dictionary")
    return {k: _clean_value(v) for k, v in data.items() if v is not None}


def _clean_list(data: list | set) -> list:
    if not isinstance(data, list | set):
        raise TypeError("data must be a list or set")
    return [_clean_value(v) for v in data if v is not None]


def _clean_value(value: Any) -> Any:  # noqa: ANN401 — recursive cleaner over arbitrary JSON values
    if isinstance(value, dict):
        return _clean_dict(value)
    if isinstance(value, list | set):
        return _clean_list(value)
    return value
