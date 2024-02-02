import asyncio
import logging
import pathlib
import ssl
import typing
from datetime import datetime, timedelta
from http import HTTPStatus
from urllib import parse
from urllib.parse import urlparse

import aiohttp
import xmltodict

from .exceptions import *
from ..env import is_debug, is_test
from ..primitives import json

__all__ = [
    'HttpResult', 'HttpClient',
    'CONTENT_TYPE_JSON', 'CONTENT_TYPE_XML', 'CONTENT_TYPE_CSV', 'CONTENT_TYPE_FORM_URLENCODED',
    'CONTENT_TYPE_HTML'
]

_UA = 'Mozilla/5.0 (iPad; CPU OS 16_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Mobile/15E148 Safari/604.1'
_LOCALHOST = ["localhost", "127.0.0.1"]

CONTENT_TYPE_JSON = 'application/json'
CONTENT_TYPE_XML = 'application/xml'
CONTENT_TYPE_FORM_URLENCODED = 'application/x-www-form-urlencoded'
CONTENT_TYPE_CSV = 'text/csv'
CONTENT_TYPE_HTML = 'text/html'
HttpResult = typing.Optional[typing.Union[typing.Dict, typing.AnyStr]]


def get_cipher_list():
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    return [cipher['name'] for cipher in context.get_ciphers()]


class HttpClient(object):
    '''
    Additional functional to the aiohttp.Session
    1. Open/Close session whenever needed
    2. Intervals between requests
    3. Response processing and custom error handle
    4. One time retry for certain statuses
    '''

    _debug = is_debug()
    _unittest = is_test()

    def __init__(
            self,
            base_url,
            req_interval_sec=0.00,
            headers=None,
            proxy=None,
            timeout=aiohttp.ClientTimeout(total=3 * 60)
    ):
        self._session: aiohttp.ClientSession = None
        self._ssl_ctx = ssl.create_default_context()
        self._ssl_ctx.set_ciphers(":".join(get_cipher_list()))
        self._context_cnt = 0
        self._base_url = base_url
        self._proxy = proxy
        self._req_interval = timedelta(seconds=req_interval_sec)
        self._next_req = datetime.now() + self._req_interval
        self._headers = headers or {}
        for h, v in [(aiohttp.hdrs.USER_AGENT, _UA), (aiohttp.hdrs.CONTENT_TYPE, CONTENT_TYPE_JSON)]:
            if h not in self._headers and h.lower() not in self._headers:
                self._headers[h] = v
        self._timeout = timeout

        if self._unittest:
            self._req_interval = timedelta(seconds=0)

    async def __aenter__(self):
        self._context_cnt += 1
        if not self._is_session_open():
            self._session = self._make_session()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._context_cnt -= 1
        if self._context_cnt == 0 and self._is_session_open():
            session_to_dispose = self._session
            self._session = None
            if session_to_dispose.closed is False:
                await session_to_dispose.close()

    def _is_session_open(self):
        return self._session is not None and self._session.closed is False

    async def fetch(
            self,
            url: typing.AnyStr = None,
            method=aiohttp.hdrs.METH_GET,
            data: typing.Any = None,
            max_attempts=2,
            fail_fast=False,
            **cgi
    ) -> HttpResult:
        if self._is_session_open():
            try:
                self._context_cnt += 1
                return await self.request(self._session, url, method, data, max_attempts, fail_fast=fail_fast, **cgi)
            finally:
                self._context_cnt -= 1

        session = self._make_session()
        async with session as session:
            return await self.request(session, url, method, data, max_attempts, fail_fast=fail_fast, **cgi)

    async def request(
            self,
            session: aiohttp.ClientSession,
            url: typing.AnyStr = None,
            method=aiohttp.hdrs.METH_GET,
            data: typing.Any = None,
            max_attempts=2,
            fail_fast=False,
            **cgi):
        if fail_fast and self._next_req > datetime.now():
            raise HttpTimeoutError(code=HTTPStatus.TOO_MANY_REQUESTS, message=f'Rate limit {self._base_url} exceeded')
        await self._wait()
        assert method in aiohttp.hdrs.METH_ALL

        async def retry(err):
            if not max_attempts or max_attempts < 1:
                raise err
            proxy = f'through {self._proxy}' if self._proxy else ''
            logging.warning(f"Another attempt to {self._base_url} due to {err}. {proxy}")
            return await self.request(session, url, method, data, max_attempts - 1, **cgi)

        try:
            logging.debug(f"{method} to {url}")
            async with session.request(
                    method=method,
                    proxy=self._proxy,
                    url=url,
                    data=json.dumps(data) if data else None,
                    params=cgi,
                    ssl_context=self._ssl_ctx
            ) as response:
                return await self._process_response(url=url, response=response, max_attempts=max_attempts-1, **cgi)

        # Specific exceptions
        except aiohttp.ClientResponseError as e:
            self.raise_for_status(e.status, e.message)
            raise e

        except aiohttp.ClientSSLError as e:
            raise HttpConnectionError(code=HTTPStatus.BAD_REQUEST, message=str(e))

        except aiohttp.ServerDisconnectedError as e:
            return await retry(
                HttpConnectionError(code=HTTPStatus.REQUEST_TIMEOUT, message=f"{self._base_url} {e.message}"))

        # More generic exceptions
        except asyncio.TimeoutError:
            p = '' if not self._proxy else " with proxy"
            return await retry(
                HttpTimeoutError(code=HTTPStatus.REQUEST_TIMEOUT, message=f"{self._base_url} timeout{p}"))

        except (aiohttp.ClientOSError, aiohttp.ClientPayloadError) as e:
            return await retry(HttpConnectionError(code=HTTPStatus.BAD_REQUEST, message=str(e)))

        except HttpException as e:
            return await retry(e)

    async def _wait(self):
        while self._next_req > datetime.now():
            wait_for = self._next_req - datetime.now()
            await asyncio.sleep(wait_for.seconds + wait_for.microseconds / 1000000)
        self._next_req = datetime.now() + self._req_interval

    def _make_session(self) -> aiohttp.ClientSession:
        return aiohttp.ClientSession(
            base_url=self._base_url,
            headers=self._headers,
            timeout=self._timeout
        )

    async def _process_response(self, url, response: aiohttp.ClientResponse, max_attempts, **cgi):
        if response.status in {HTTPStatus.TOO_MANY_REQUESTS, HTTPStatus.GATEWAY_TIMEOUT} and max_attempts:
            parsed_url = urlparse(url)
            logging.warning(f"Another attempt to {parsed_url.netloc} due to 429.")
            await self._wait()
            return await self.fetch(url=url, max_attempts=max_attempts - 1, **cgi)

        result = await self._read_body(response)

        self.raise_for_status(response.status, response.reason, result)
        return result

    async def _read_body(self, response: aiohttp.ClientResponse):
        if response.content.at_eof() or response.content.exception() or \
                (response.content.total_bytes == 0 and not response.connection):
            return None

        json_content_type = response.content_type.startswith(CONTENT_TYPE_JSON)
        if json_content_type:
            result = await response.json()
            if self._debug:
                json.dumpf(result, 'response.json')
            return result

        result = await response.text()
        if self._debug:
            pathlib.Path('response.txt').write_text(result)

        form_url_encoded_content_type = response.content_type.startswith(CONTENT_TYPE_FORM_URLENCODED)
        if form_url_encoded_content_type:
            return parse.parse_qs(result)

        xml_content_type = response.content_type.startswith(CONTENT_TYPE_XML)
        if xml_content_type:
            return xmltodict.parse(result)
        return result

    def raise_for_status(self, status, reason, body=None):
        if status == HTTPStatus.OK:
            return

        if status in [HTTPStatus.FORBIDDEN, HTTPStatus.UNAUTHORIZED, HTTPStatus.PAYMENT_REQUIRED]:
            raise HttpUnauthorizedError(status, reason, body)

        if status in [HTTPStatus.NOT_FOUND]:
            raise HttpNotFoundError(status, reason, body)

        if HTTPStatus.BAD_REQUEST <= status < HTTPStatus.INTERNAL_SERVER_ERROR:
            raise HttpBadRequestError(status, reason, body)

        if HTTPStatus.INTERNAL_SERVER_ERROR <= status:
            raise HttpServerError(status, reason, body)

        raise HttpException(status, reason, body)
