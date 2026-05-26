"""Filter that records `/start` deeplink attribution to Firestore and PubSub."""

import base64
from typing import Any, ClassVar
from urllib.parse import parse_qsl

from aiogram import types
from aiogram.filters import BaseFilter
from google.cloud import (
    firestore,
    pubsub,
)

from baski.schema import BigQueryDateTime, Integer, NotNullString, Schema, String, ValidationError

__all__ = ["Attribution"]


class Attribution(BaseFilter):
    """Records the /start command payload (base64-encoded UTM string) to Firestore and PubSub.

    Always passes. Set `track=True` only on the entry-point command; configure sinks once via
    `Attribution.setup_firestore(...)` and `Attribution.setup_pubsub(...)`.
    """

    collection: ClassVar[firestore.AsyncCollectionReference | None] = None
    topic: ClassVar[str | None] = None
    publisher: ClassVar[pubsub.PublisherClient | None] = None

    track: bool = False

    @classmethod
    def setup_firestore(cls, collection: firestore.AsyncCollectionReference) -> None:
        """Configure the Firestore collection used by all filter instances."""
        cls.collection = collection

    @classmethod
    def setup_pubsub(cls, topic: str, publisher: pubsub.PublisherClient) -> None:
        """Configure the PubSub topic and publisher used by all filter instances."""
        cls.topic = topic
        cls.publisher = publisher

    async def __call__(
        self,
        event: types.Message,
        **_: Any,  # noqa: ANN401 — aiogram middleware/observer forwarding
    ) -> bool:
        """Record the attribution payload (if any) and always pass the filter."""
        if not self.track or not isinstance(event, types.Message) or not event.text:
            return True
        if not event.text.startswith("/"):
            return True
        attribution_event = _get_attribution_object(event)
        if not attribution_event:
            return True
        await self._sink_to_firestore(attribution_event)
        self._sink_to_pubsub(attribution_event)
        return True

    async def _sink_to_firestore(self, event: dict) -> None:  # noqa: ANON002 — Firestore document payload, dynamic UTM fields
        if self.collection is None:
            return
        await self.collection.add(event)

    def _sink_to_pubsub(self, event: dict) -> None:  # noqa: ANON002 — PubSub event payload, dynamic UTM fields
        if not self.topic or self.publisher is None:
            return
        event_data = attribution_event_schema.dumps(event).encode("utf-8")
        self.publisher.publish(self.topic, event_data)


class AttributionEventSchema(Schema):
    source = NotNullString()
    medium = NotNullString()
    campaign = NotNullString()
    content = NotNullString()
    term = NotNullString()
    timestamp = BigQueryDateTime(required=True)
    user_id = Integer(required=True)


class AttributionDataSchema(Schema):
    source = String(data_key="s")
    medium = String(data_key="m")
    campaign = String(data_key="c")
    content = String(data_key="cnt")
    term = String(data_key="t")


attribution_data_schema = AttributionDataSchema()
attribution_event_schema = AttributionEventSchema()


_DEEPLINK_PARTS = 2  # /start <base64-payload>


def _get_attribution_object(message: types.Message) -> dict | None:  # noqa: ANON002 — marshmallow-loaded payload with dynamic UTM fields
    if not message.text or not message.from_user:
        return None
    parts = message.text.split(" ")
    if len(parts) != _DEEPLINK_PARTS:
        return None
    try:
        cgi_string = base64.standard_b64decode(parts[1]).decode("utf-8")
        data = attribution_data_schema.load(dict(parse_qsl(cgi_string)))
    except (ValidationError, ValueError):
        return None
    data["timestamp"] = message.date
    data["user_id"] = message.from_user.id
    return data
