import typing
import base64
from urllib.parse import parse_qsl
from aiogram import filters, types
from google.cloud import firestore
from google.cloud import pubsub

from baski.schema import NotNullString, BigQueryDateTime, Schema, String, Integer, ValidationError

__all__ = ['Attribution']


class Attribution(filters.Filter):
    collection = None
    topic = None
    publisher = None

    def __init__(self, track: bool = False):
        self.track = track

    async def check(self, *args) -> bool:
        if not isinstance(args, tuple) or not len(args) < 2 or not isinstance(args[0], types.Message):
            return True
        if not self.track:
            return True
        message: types.Message = args[0]
        if not message.is_command():
            return True
        attribution_event = _get_attribution_object(message)
        if not attribution_event:
            return True

        await self.sink_to_firestore(attribution_event)
        self.sink_to_pubsub(attribution_event)

    @classmethod
    def validate(cls, full_config: typing.Dict[str, typing.Any]) -> typing.Optional[typing.Dict[str, typing.Any]]:
        if full_config.pop('track_attribution', False):
            return {"track": True}
        return {}

    @classmethod
    def firestore_sink(cls, collection: firestore.AsyncCollectionReference):
        cls.collection = collection

    @classmethod
    def pubsub_sink(cls, topic: str, publisher: pubsub.PublisherClient):
        cls.topic = topic
        cls.publisher = publisher

    async def sink_to_firestore(self, event: typing.Dict):
        if not self.collection:
            return
        await self.collection.add(event)

    def sink_to_pubsub(self, event: typing.Dict):
        if not self.topic or not self.publisher:
            return
        event_data = attribution_event_schema.dumps(event).encode('utf-8')
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
    source = String(data_key='s')
    medium = String(data_key='m')
    campaign = String(data_key='c')
    content = String(data_key='cnt')
    term = String(data_key='t')


attribution_data_schema = AttributionDataSchema()
attribution_event_schema = AttributionEventSchema()

def _get_attribution_object(message: types.Message):
    parts = message.text.split(' ')
    if not len(parts) == 2:
        return None
    try:
        cgi_string = base64.standard_b64decode(parts[1]).decode('utf-8')
        data = attribution_data_schema.load(dict(parse_qsl(cgi_string)))
        data['timestamp'] = message.date
        data['user_id'] = message.from_user.id
        return data
    except (ValidationError, ValueError):
        return None
