"""Log-context middleware: seed each request's ambient log labels (route + Cloud Trace)."""

from fastapi import FastAPI
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from ...server.logger import seed_request_context

__all__ = ["LogContextMiddleware"]


class LogContextMiddleware(BaseHTTPMiddleware):
    """Attach the request's route label and Cloud Trace context to every log it emits.

    Register outermost so access logging and the exception handlers it wraps carry the context.
    """

    def __init__(self, app: FastAPI, project_id: str | None = None) -> None:
        """Initialize with the FastAPI app and the GCP project id used to build trace links."""
        super().__init__(app)
        self.project_id = project_id

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        """Seed the request's ambient log context, then run the handler chain."""
        seed_request_context(request, project_id=self.project_id)
        return await call_next(request)
