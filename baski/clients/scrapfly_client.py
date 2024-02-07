import typing
from http import HTTPStatus

import aiohttp
from yarl import URL
from baski.http import HttpClient, HttpException


__all__ = ['ScrapflyClient']


class ScrapflyClient(HttpClient):

    def __init__(self, api_key, req_interval_sec=5.0, **kwargs):
        for f in ['base_url', 'timeout', 'proxy', 'req_interval_sec']:
            if f in kwargs:
                kwargs.pop(f)

        super().__init__(
            base_url="https://api.scrapfly.io",
            req_interval_sec=req_interval_sec,
            timeout=aiohttp.ClientTimeout(total=160)
        )

        self._api_key = api_key

    async def request(
            self,
            session: aiohttp.ClientSession,
            url: typing.AnyStr = None,
            method=aiohttp.hdrs.METH_GET,
            data: typing.Any = None,
            max_attempts=2,
            fail_fast=False,
            render_js=None,
            **cgi):
        tries = [('false', 'true', False), ('true', 'true', True)]
        if render_js is None:
            tries = [('false', 'false', False)] + tries
        result = {}
        for asp, js, last in tries:
            data = await super().request(
                session,
                URL("/scrape").update_query({
                    'url': str(URL(url).update_query(cgi)),
                    'key': self._api_key, 'asp': str(asp), 'render_js': str(js)
                }),
                method,
                data,
                max_attempts=0,
                fail_fast=fail_fast
            )
            result = data.get('result')
            if result['success']:
                return result
        self.raise_for_status(result.get("status_code", HTTPStatus.IM_A_TEAPOT), result.get('reason', 'Unknown reason'))
