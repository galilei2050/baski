import time

from fastapi import FastAPI
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from ..dependencies import get_logger

__all__ = ["AccessLogMiddleware"]


class AccessLogMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: FastAPI) -> None:
        super().__init__(app)

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        if request.state.config.get("cloud"):
            return await call_next(request)

        start = time.perf_counter()
        response = await call_next(request)
        duration_ms = int((time.perf_counter() - start) * 1000)
        logger = get_logger(request)
        logger.info(f"{request.method} {request.url.path} {response.status_code} {duration_ms}ms")
        return response
