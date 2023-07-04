import asyncio
import logging
import sys
import traceback
from functools import cached_property
from http import HTTPStatus

from dateutil.parser import parse
from marshmallow import ValidationError
from tornado.web import HTTPError
from tornado.web import RequestHandler as TornadoHandler

from ..env import is_test, is_debug, token
from ..primitives import json, datetime

__ALL__ = ['RequestHandler']


def _prepare_response(payload, *, ok=True):
    d = {
        'ok': bool(ok),
        'result': payload if ok else None,
        'error': payload if not ok else None,
    }
    return json.dumps(d)


class RequestHandler(TornadoHandler):
    _token = token()
    _debug = is_debug()
    _unittest = is_test()

    concurrent_limit = 0  # Concurrent requests for B2 instance, then 429
    body_schema = None

    def prepare(self):
        self._rate_limit()
        self._auth()

    def set_default_headers(self):
        super().set_default_headers()

        # that'll be enough for now, use config otherwise
        self.set_header("Access-Control-Allow-Methods", ','.join(self.SUPPORTED_METHODS))
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Credentials", "true")
        self.set_header("Access-Control-Allow-Headers", "Content-Type")

    def write(self, chunk, cache=None):
        cache = cache or 'no-cache'
        self.set_header("Cache-Control", cache)
        if isinstance(chunk, (dict, list, tuple)):
            chunk = _prepare_response(chunk, ok=self._status_code < 400)
            self.set_header("Content-Type", "application/json; charset=UTF-8")
        super().write(chunk)

    def write_error(self, status_code, message=None, errors=None, **kwargs):
        if status_code == HTTPStatus.FORBIDDEN:
            self.finish(str(status_code))
            return
        if not message:
            exception = kwargs.get('exc_info', (None, None))[1]
            if isinstance(exception, RequestValidationError):
                message = "Validation Error"
                errors = exception.errors
                self.set_status(HTTPStatus.UNPROCESSABLE_ENTITY)
            elif isinstance(exception, HTTPError):
                message = exception.log_message
            else:
                exc_type, exc_value, exc_tb = sys.exc_info()
                message = str(exception)
                formatted_exception = traceback.format_exception(exc_type, exc_value, exc_tb)
                errors = [[li for li in line.split('\n') if li] for line in formatted_exception]

        self.finish({'message': message, 'errors': errors})

    @cached_property
    def json_body(self):
        body = self.request.body
        try:
            if not body:
                return {}
            if self.body_schema:
                return self.body_schema.loads(body)
            return json.loads(body)

        except ValidationError as e:
            logging.warning(f"POST body {body}\nvalidation ERROR: {e.messages}")
            raise RequestValidationError(errors=e.messages, status_code=HTTPStatus.UNPROCESSABLE_ENTITY)
        except (json.JSONDecodeError, ValueError) as e:
            logging.warning(f"POST body {body}\njson decode error: {e}")
            raise HTTPError(HTTPStatus.UNPROCESSABLE_ENTITY, str(e))

    def _rate_limit(self):
        if not self.concurrent_limit:
            return
        web_tasks = [t for t in asyncio.all_tasks() if 'tornado/web.py' in t.get_coro().cr_code.co_filename]
        if len(web_tasks) > self.concurrent_limit:
            raise HTTPError(HTTPStatus.TOO_MANY_REQUESTS,
                            f"Concurrent limit is {self.concurrent_limit} running tasks {len(web_tasks)}")

    def _auth(self):
        if self._unittest or self._debug:
            return

        try:
            actual_token = None

            authorization_header = self.request.headers.get('Authorization', '').split()
            if len(authorization_header) == 2:
                _, actual_token = authorization_header

            cgi_token = self.get_query_argument('token', None)
            if cgi_token and not actual_token:
                actual_token = cgi_token

            if actual_token == self._token:
                return
        except (KeyError, ValueError):
            pass
        raise HTTPError(403)

    def get_date_query_arument(self, f='date'):
        date = self.get_query_argument(f, None)
        date = datetime.as_utc(parse(date)) if date is not None else datetime.as_local(datetime.datetime.now())
        return date

    def now(self):
        return self.get_date_query_arument()


class RequestValidationError(HTTPError):
    def __init__(self, errors, *args, **kwargs):
        super(RequestValidationError, self).__init__(*args, **kwargs)
        self.errors = errors
