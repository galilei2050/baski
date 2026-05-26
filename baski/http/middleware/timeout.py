"""Request timeout middleware: abort handlers exceeding a per-request deadline."""

import asyncio
from http import HTTPStatus

from fastapi import FastAPI
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from ..dependencies import get_logger

__all__ = ["RequestTimeoutMiddleware"]


class RequestTimeoutMiddleware(BaseHTTPMiddleware):
    """Cancel a request and return 504 if it runs longer than ``timeout`` seconds."""

    def __init__(self, app: FastAPI, timeout: int = 30) -> None:
        """Initialize with the FastAPI app and per-request timeout in seconds."""
        super().__init__(app)
        self.timeout = timeout

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        """Run the handler with a timeout; return 504 if it expires."""
        try:
            return await asyncio.wait_for(call_next(request), timeout=self.timeout)
        except TimeoutError:
            logger = get_logger(request)
            client_host = request.client.host if request.client else "?"
            logger.warning(f"Request Timeout after {self.timeout}: {client_host} -> {request.method} {request.url}")
            return Response(status_code=HTTPStatus.GATEWAY_TIMEOUT)
