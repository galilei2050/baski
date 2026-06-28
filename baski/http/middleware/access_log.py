"""Access log middleware: emits one info line per request in local mode."""

import logging
import time

from fastapi import FastAPI
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

__all__ = ["AccessLogMiddleware"]

logger = logging.getLogger(__name__)


class AccessLogMiddleware(BaseHTTPMiddleware):
    """Log method, path, status, and duration for each request (local mode only)."""

    def __init__(self, app: FastAPI) -> None:
        """Initialize the middleware with the FastAPI app."""
        super().__init__(app)

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        """Run the request, then emit a single access log line in local mode."""
        if request.state.config.get("cloud"):
            return await call_next(request)

        start = time.perf_counter()
        response = await call_next(request)
        duration_ms = int((time.perf_counter() - start) * 1000)
        logger.info(f"{request.method} {request.url.path} {response.status_code} {duration_ms}ms")
        return response
