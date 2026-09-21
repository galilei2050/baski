import logging
from types import SimpleNamespace

import httpx
import pytest
from starlette.requests import Request

from baski.http.exception_handlers import http_exception_handler
from baski.server.logger import _JsonFormatter


def _request() -> Request:
    request = Request({"type": "http", "method": "GET", "path": "/", "headers": []})
    request.state.config = SimpleNamespace(debug=False)
    return request


@pytest.mark.asyncio
async def test_downstream_error_survives_the_json_formatter(caplog):
    request = httpx.Request("GET", "https://downstream.test/orders")
    response = httpx.Response(502, request=request, content=b'{"err":"boom"}')
    exc = httpx.HTTPStatusError("502", request=request, response=response)

    with caplog.at_level(logging.WARNING):
        await http_exception_handler(_request(), exc)

    record = next(r for r in caplog.records if r.getMessage() == "Downstream HTTP error")
    assert record.downstream["statusCode"] == 502
    assert '"statusCode": 502' in _JsonFormatter().format(record)
