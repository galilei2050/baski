import asyncio
from http import HTTPStatus

from fastapi import FastAPI
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from ..dependencies import get_logger

__all__ = ["RequestTimeoutMiddleware"]


class RequestTimeoutMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: FastAPI, timeout: int = 30) -> None:
        super().__init__(app)
        self.timeout = timeout

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        try:
            return await asyncio.wait_for(call_next(request), timeout=self.timeout)
        except TimeoutError:
            logger = get_logger(request)
            client_host = request.client.host if request.client else "?"
            logger.warning(f"Request Timeout after {self.timeout}: {client_host} -> {request.method} {request.url}")
            return Response(status_code=HTTPStatus.GATEWAY_TIMEOUT)
