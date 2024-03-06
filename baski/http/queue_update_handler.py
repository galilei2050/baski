import asyncio
import base64
import logging
import typing

from abc import ABC, abstractmethod
from builtins import AttributeError
from collections import defaultdict
from copy import deepcopy
from distutils.util import strtobool
from functools import partial, cached_property
from http import HTTPStatus

from dateutil.parser import parse
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
from ..primitives import json, datetime

__all__ = ['QueueUpdateHandler']


class PubSubMessageSchema(Schema):
    class Meta:
        ordered = True,
        unknown = EXCLUDE

    attributes = fields.Dict()
    data = fields.String(allow_none=True, default=None)
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
    arguments = {}

    _default_arguments = {
        'obsolescence': 12,
        'item_id': None,
        'now': datetime.now()
    }

    _arguments_cast_functions = {
        str: str,
        float: float,
        int: int,
        datetime.datetime: parse,
        bool: lambda x: bool(strtobool(x)),
    }

    default_obsolescence_hours = 12

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

    def get_log_msg(self, item_id, message):
        return f'{self.what} [id={item_id}]: {message}'

    def get_fields(self):
        flds = self.fields if self.fields else []
        flds = set(list(flds) + ['id', 'updated'])
        return list(flds)

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
        assert self.collection_name, "Define collection_name in the class to call this method"
        return self.db.collection(self.collection_name)

    def update_from(self, obsolescence):
        return self.now() - datetime.timedelta(hours=int(obsolescence))

    def is_actual(self, item: typing.Dict, obsolescence):
        item = item or {}
        updated_dict = item.get('updated') or {}
        return (updated_dict.get(self.topic_id) or START_OF_EPOCH) > self.update_from(obsolescence)

    def prepare(self):
        is_configured = all([self.topic_id, self.what])
        assert is_configured, "Define cls.topic_id and what"
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
        args = self._cgi_arguments()

        if item_id:
            collected_metrics = defaultdict(int)
            item = await self.item(item_id)
            args['item_id'] = item_id
            await self._do_update_one(collected_metrics, item=item, **args)
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
        try:
            attributes = self._post_arguments(**(message.get('attributes') or {}))
        except ValueError as e:
            logging.error(f"{self.what} attrs={message.get('attributes')} error={e}")
            raise HTTPError(HTTPStatus.BAD_REQUEST, str(e))

        collected_metrics = defaultdict(int)
        data = message.get('data')
        item_id = attributes.get('item_id', None)
        item = None
        if data:
            data = base64.b64decode(message.get('data'))
            logging.debug(f'{self.what} attrs={attributes} data={data}')
            item = json.loads(data) if data else None
        if item_id and item is None:
            item = await self.item(item_id)
        await self._do_update_one(collected_metrics, item=item, **attributes)
        self.write(collected_metrics)

    @cached_property
    def project_id(self):
        return self.db.project

    def _cgi_arguments(self):
        args = self._all_arguments()
        for k, d in args.items():
            args[k] = self._cast_argument_value(k, self.get_query_argument(k, d))
        return args

    def _post_arguments(self, **kwargs):
        args = self._all_arguments()
        for k, d in kwargs.items():
            args[k] = self._cast_argument_value(k, d)
        return args

    def _all_arguments(self):
        return deepcopy((self.arguments or {}) | self._default_arguments)

    def _cast_argument_value(self, key, value):
        args = self._all_arguments()
        if key not in args:
            raise HTTPError(422, f"Unknown argument {key}")

        if args[key] is None and isinstance(value, str):
            return value

        if value is None:
            return None

        expected_type = type(args[key])

        if isinstance(value, expected_type):
            return value

        assert expected_type in self._arguments_cast_functions, f"Unknown type {expected_type}"
        return self._arguments_cast_functions[expected_type](value)

    def _can_cast_argument_value(self, key, value):
        if value is None:
            return True

        assert self._cast_argument_value(key, str(value)) == value, f"Can't cast {value} of {key}"

    async def _do_publish_all(self, items_to_update, interval=0.001, **kwargs):
        def _check_value(key, value):
            self._can_cast_argument_value(key, value)
            return str(value)

        topic_path = self.publisher.topic_path(self.project_id, self.topic_id)
        kwargs = {k: _check_value(k, v) for k, v in kwargs.items() if v is not None}

        for item_id, item in items_to_update:
            kwargs['item_id'] = item_id
            data = json.dumps(item).encode('utf-8')
            f = await as_async(partial(self.publisher.publish, topic_path, data, **kwargs))
            await asyncio.sleep(interval)
            await as_async(f.result)

    async def _do_update_all(self, items_to_update, **kwargs):
        collected_metrics = defaultdict(int)
        for item_id, item in items_to_update:
            kwargs['item_id'] = item_id
            await self._do_update_one(collected_metrics, item=item, **kwargs)
        return collected_metrics

    async def _do_update_one(self, collected_metrics, item_id, item, **kwargs):
        try:
            if self.is_actual(item, kwargs.get('obsolescence', self.default_obsolescence_hours)):
                logging.info(self.get_log_msg(item_id, "is actual"))
                collected_metrics['actual'] += 1
                return
            logging.info(self.get_log_msg(item_id, f' kwargs={kwargs}'))

            metrics = await self.update_one(item_id, item, **kwargs)
            if isinstance(metrics, dict):
                for k, v in metrics.items():
                    collected_metrics[k] += v
            await self.collection.document(item_id).set({'updated': {self.topic_id: self.now()}}, merge=True)

        # Exceptions that are not actually errors just warnings
        except HttpNotFoundError:
            logging.warning(self.get_log_msg(item_id, "not found"))
            collected_metrics["item_not_found"] += 1
            return

        # Recoverable errors - Just transform to correct http code
        except (HttpTimeoutError, HttpConnectionError) as e:
            logging.info(self.get_log_msg(item_id, f"http timeout: {e}"))
            collected_metrics["http_connection_error"] += 1
            raise HTTPError(HTTPStatus.IM_A_TEAPOT, str(e))

        except (HttpUnauthorizedError, HttpServerError, HttpBadRequestError) as e:
            logging.warning(self.get_log_msg(item_id, f"http source: {e}"))
            collected_metrics["source_error"] += 1
            raise HTTPError(e.code, str(e))

        except (ServiceUnavailable, DeadlineExceeded, GatewayTimeout, InternalServerError, Aborted, RetryError) as e:
            logging.warning(self.get_log_msg(item_id, f' gcloud error: {e}'))
            collected_metrics["grpc_error"] += 1
            raise HTTPError(HTTPStatus.REQUEST_TIMEOUT, str(e))

        # Errors that are not recoverable and human help is required.
        except ValidationError as e:
            logging.critical(self.get_log_msg(item_id, f"validation exception: {e}"), exc_info=e)
            collected_metrics["invalid_response"] += 1
            return

        except (ValueError, TypeError, KeyError, AssertionError, AttributeError) as e:
            logging.critical(self.get_log_msg(item_id, f"algorithm error: {e}"), exc_info=e)
            collected_metrics["internal_exception"] += 1
            return
