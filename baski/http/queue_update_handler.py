import asyncio
import base64
import logging
import typing

from abc import ABC, abstractmethod
from builtins import AttributeError
from collections import defaultdict
from copy import deepcopy
from datetime import timedelta
from distutils.util import strtobool
from functools import partial, cached_property
from http import HTTPStatus

from google.api_core.exceptions import Aborted, RetryError, DeadlineExceeded
from google.cloud import firestore, pubsub
from google.cloud.exceptions import ServiceUnavailable, GatewayTimeout, InternalServerError
from marshmallow import ValidationError, Schema, fields, EXCLUDE
from tornado.web import HTTPError

from .exceptions import (
    HttpTimeoutError, HttpNotFoundError, HttpUnauthorizedError,
    HttpConnectionError, HttpBadRequestError, HttpServerError
)
from .request_handler import RequestHandler
from ..concurrent import as_async
from ..defs import START_OF_EPOCH
from ..config import AppConfig
from ..primitives import json

__all__ = ['QueueUpdateHandler']


class PubSubMessageSchema(Schema):
    class Meta:
        ordered = True,
        unknown = EXCLUDE

    attributes = fields.Dict()
    data = fields.String(required=True)
    messageId = fields.String(required=True)
    message_id = fields.String(required=True)
    publishTime = fields.DateTime(required=True)
    publish_time = fields.DateTime(required=True)


class PubSubBodySchema(Schema):
    message = fields.Nested(PubSubMessageSchema(), required=True)
    subscription = fields.String(required=True)


class QueueUpdateHandler(RequestHandler, ABC):
    # Required properties
    what = None
    collection_name = None
    topic_id = None
    metric = None

    # Optional properties
    fields = None
    arguments = None
    default_obsolescence_hours = '12'

    body_schema = PubSubBodySchema()

    @abstractmethod
    async def update_one(self, item_id, item: typing.Dict, **kwargs) -> typing.Dict:
        raise NotImplementedError()

    @property
    @abstractmethod
    def publisher(self) -> pubsub.PublisherClient:
        raise NotImplementedError()

    @property
    @abstractmethod
    def db(self) -> firestore.AsyncClient:
        raise NotImplementedError()

    def get_fields(self):
        fields = self.fields if self.fields else []
        fields = set(list(fields) + ['id', 'updated'])
        return list(fields)

    def query(self) -> firestore.AsyncQuery:
        return self.collection.select(self.get_fields())

    async def items(self, limit=None, skip=None) -> typing.Iterable[typing.Tuple[str, dict]]:
        query = self.query()
        if limit:
            query = query.limit(limit)
        if skip:
            query = query.offset(skip)

        result = []
        async for doc in query.stream():
            result.append((doc.id, doc.to_dict()))
        return result

    async def item(self, item_id) -> dict:
        return (await self.collection.document(item_id).get(self.get_fields())).to_dict()

    @cached_property
    def collection(self) -> firestore.AsyncCollectionReference:
        return self.db.collection(self.collection_name)

    def update_from(self, obsolescence):
        return self.now() - timedelta(hours=int(obsolescence))

    def is_actual(self, item: typing.Dict, obsolescence):
        item = item or {}
        updated_dict = item.get('updated') or {}
        return (updated_dict.get(self.topic_id) or START_OF_EPOCH) > self.update_from(obsolescence)

    def prepare(self):
        is_configured = all([self.collection_name, self.topic_id, self.what])
        assert is_configured, "Define cls.topic_id and collection_name"
        super().prepare()

    async def get(self):
        try:
            sync = strtobool(self.get_query_argument('sync', '0'))
            limit = int(self.get_query_argument('limit', '0'))
            skip = int(self.get_query_argument('skip', '0'))
            interval = float(self.get_query_argument('interval', 0.05))
        except ValueError as e:
            raise HTTPError(HTTPStatus.BAD_REQUEST, str(e))

        item_id = self.get_query_argument('id', None)
        args = self._arguments()

        if item_id:
            collected_metrics = defaultdict(int)
            item = await self.item(item_id)
            await self._do_update_one(collected_metrics, item_id, item, **args)
            self.write(collected_metrics if collected_metrics else {'updated': 1})
            return

        try:
            items_to_update = await self.items(limit, skip)
        except ValueError as e:
            raise HTTPError(HTTPStatus.BAD_REQUEST, str(e))

        if sync:
            collected_metrics = await self._do_update_all(items_to_update, **args)
            self.write(collected_metrics)
            return

        await self._do_publish_all(items_to_update, interval=float(interval), **args)
        self.write({"published": len(items_to_update)})

    async def post(self):
        """
        {
            "message": {
                "attributes": {
                    "how":"now",
                    "item_id":"A"
                },
                "data":"ewogICJpbmRleGVzIjogWwogICAgInNucDUwMCIsCiAgICAidGlua29mZiIKICBdCn0=",
                "messageId":"5957931969060311",
                "message_id":"5957931969060311",
                "publishTime":"2022-11-24T09:30:35.953Z",
                "publish_time":"2022-11-24T09:30:35.953Z"
            },
            "subscription":"projects/profitstock/subscriptions/crawer-financials-update"
        }

        """
        message = self.json_body.get('message')
        attributes = (message.get('attributes') or {})
        data = base64.b64decode(message.get('data'))
        logging.info(f'Updating {self.what} attrs={attributes}')
        logging.debug(f'Updating {self.what} attrs={attributes} data={data}')
        item = json.loads(data) if data else None
        collected_metrics = defaultdict(int)
        await self._do_update_one(collected_metrics=collected_metrics, item=item, **attributes)
        self.write(collected_metrics)

    @cached_property
    def project_id(self):
        return AppConfig().project_id

    def _arguments(self):
        args = deepcopy(self.arguments) if isinstance(self.arguments, dict) else {}
        args = args | {'obsolescence': self.default_obsolescence_hours}
        for k, d in args.items():
            args[k] = self.get_query_argument(k, d)
        return args

    async def _do_publish_all(self, items_to_update, interval=0.001, **kwargs):
        topic_path = self.publisher.topic_path(self.project_id, self.topic_id)

        for item_id, item in items_to_update:
            kwargs['item_id'] = item_id
            kwargs = {k: str(v) for k, v in kwargs.items() if v is not None}
            data = json.dumps(item).encode('utf-8')
            f = await as_async(partial(self.publisher.publish, topic_path, data, **kwargs))
            await asyncio.sleep(interval)
            await as_async(f.result)

    async def _do_update_all(self, items_to_update, **kwargs):
        collected_metrics = defaultdict(int)
        for item_id, item in items_to_update:
            await self._do_update_one(collected_metrics, item_id, item, **kwargs)
        return collected_metrics

    async def _do_update_one(self, collected_metrics, item_id, item, **kwargs):
        try:
            if self.is_actual(item, kwargs.get('obsolescence')):
                logging.info('Actual %s %s', self.what, item_id)
                collected_metrics['actual'] += 1
                return
            logging.info('Updating %s %s', self.what, item_id)

            metrics = await self.update_one(item_id, item, **kwargs)
            if isinstance(metrics, dict):
                for k, v in metrics.items():
                    collected_metrics[k] += v
            await self.collection.document(item_id).set({'updated': {self.topic_id: self.now()}}, merge=True)

        # Exceptions that are not actually errors just warnings
        except HttpNotFoundError:
            logging.warning(f"id:\"{item_id}\" {self.what} not found")
            collected_metrics["item_not_found"] += 1
            return

        # Recoverable errors - Just transform to correct http code
        except (HttpTimeoutError, HttpConnectionError) as e:
            logging.info(f"id:\"{item_id}\" {self.what} http timeout: {e}")
            collected_metrics["http_connection_error"] += 1
            raise HTTPError(HTTPStatus.IM_A_TEAPOT, str(e))

        except (HttpUnauthorizedError, HttpServerError, HttpBadRequestError) as e:
            logging.warning(f'id:\"{item_id}\" {self.what} http source: {e}')
            collected_metrics["source_error"] += 1
            raise HTTPError(e.code, str(e))

        except (ServiceUnavailable, DeadlineExceeded, GatewayTimeout, InternalServerError, Aborted, RetryError) as e:
            logging.warning(f'id:\"{item_id}\" {self.what} gcloud error: {e}')
            collected_metrics["grpc_error"] += 1
            raise HTTPError(HTTPStatus.REQUEST_TIMEOUT, str(e))

        # Errors that are not recoverable and human help is required.
        except ValidationError as e:
            logging.critical(f"id:\"{item_id}\" {self.what} validation exception: {e}")
            collected_metrics["invalid_response"] += 1
            return

        except (ValueError, TypeError, KeyError, AssertionError, AttributeError) as e:
            logging.critical(f"id:\"{item_id}\" {self.what} algorithm error: {e}", stack_info=True)
            collected_metrics["internal_exception"] += 1
            return
