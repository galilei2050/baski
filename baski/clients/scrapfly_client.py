import typing

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

        for asp, js, last in tries:
            try:
                data = await super().request(
                    session,
                    URL("/scrape").update_query({
                        'url': str(URL(url).update_query(cgi)),
                        'key': self._api_key, 'asp': str(asp), 'render_js': str(js)
                    }),
                    method,
                    data,
                    max_attempts=1,
                    fail_fast=fail_fast
                )
                result = data.get('result')
                if result['success']:
                    return result
                if not last:
                    continue
                self.raise_for_status(result["status_code"], result['reason'], result['content'])
            except HttpException as e:
                if last:
                    raise
