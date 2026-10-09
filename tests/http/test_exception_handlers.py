import logging
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.requests import Request

from baski.http.exception_handlers import http_exception_handler
from baski.http.server import FastAPIServer
from baski.server.logger import _JsonFormatter


class AppError(Exception):
    pass


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


def test_unlisted_exception_gets_the_json_envelope():
    app = FastAPI()
    FastAPIServer.setup_exception_handlers(None, app)

    @app.get("/boom")
    async def boom() -> None:
        raise AppError("custom domain error")

    response = TestClient(app, raise_server_exceptions=False).get("/boom")
    assert response.headers["content-type"].startswith("application/json")
    assert response.json()["error"]["code"] == 500


def test_a_narrower_handler_still_wins():
    app = FastAPI()
    FastAPIServer.setup_exception_handlers(None, app)

    @app.get("/downstream")
    async def downstream() -> None:
        request = httpx.Request("GET", "https://downstream.test/orders")
        raise httpx.ConnectError("refused", request=request)

    response = TestClient(app, raise_server_exceptions=False).get("/downstream")
    assert response.json()["error"]["code"] == 502, "the catch-all must not shadow the connection handler"
