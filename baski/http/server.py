"""FastAPI server template: lifespan-managed resources, handlers, routes, hypercorn launcher."""

import asyncio
import logging
import time
import types
from contextlib import asynccontextmanager
from functools import cached_property
from typing import Any

import google.cloud.firestore as firestore  # noqa: PLR0402 — `from google.cloud import X` form is broken for namespace pkg under mypy
import google.cloud.pubsub as pubsub  # noqa: PLR0402 — see above
import google.cloud.storage as storage  # noqa: PLR0402 — see above
import httpx
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware import gzip, trustedhost
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from google.api_core.exceptions import GoogleAPICallError
from google.auth import default as google_auth_default
from google.genai.errors import APIError as GenAIAPIError
from hypercorn.asyncio import serve
from hypercorn.config import Config as HypercornConfig
from pydantic import ValidationError
from pymongo import AsyncMongoClient
from pymongo.asynchronous import database
from pymongo.errors import PyMongoError

from ..env import get_env
from ..server.async_server import AsyncServer
from ..server.logger import seed_request_context
from .exception_handlers import (
    genai_api_exception_handler,
    google_api_exception_handler,
    http_connection_exception_handler,
    http_exception_handler,
    request_validation_exception_handler,
    runtime_exception_handler,
    timeout_exception_handler,
)
from .middleware import AccessLogMiddleware, RequestTimeoutMiddleware
from .mongo_logging import MongoQueryLogger

__all__ = ["FastAPIServer"]

logger = logging.getLogger(__name__)
_start_time = time.time()


class FastAPIServer(AsyncServer):
    """FastAPI-based AsyncServer wiring lifespan, middleware, exception handlers, and routes."""

    @cached_property
    def app(self) -> FastAPI:
        """Build (once) and return the FastAPI application."""

        @asynccontextmanager
        async def lifespan(_app: FastAPI) -> Any:  # noqa: ANN401 — asynccontextmanager generator return type is opaque to mypy
            async with self as resources:
                scheme = "http" if self.args["cloud"] else "https"
                logger.warning("Server ready: %s://0.0.0.0:%s", scheme, self.args["port"])
                config_refresh: asyncio.Task[None] | None = None
                if self.args["cloud"]:
                    config_refresh = asyncio.create_task(self.check_config_periodically())
                try:
                    yield resources
                finally:
                    if config_refresh is not None:
                        config_refresh.cancel()

        app = FastAPI(lifespan=lifespan, openapi_url="/docs/openapi.json")

        self.setup_exception_handlers(app)
        self.setup_middleware(app)
        self.setup_routes(app)
        return app

    @cached_property
    def http_client(self) -> httpx.AsyncClient:
        """Return the shared outbound httpx client (3 retries, 5 min timeout)."""
        return httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=3),
            timeout=httpx.Timeout(60 * 5),
        )

    @cached_property
    def firestore_client(self) -> firestore.AsyncClient:
        """Return the shared async Firestore client."""
        return firestore.AsyncClient()

    @cached_property
    def publisher_client(self) -> pubsub.PublisherClient:
        """Return the shared Pub/Sub publisher client with ordering enabled."""
        publisher_options = pubsub.types.PublisherOptions(enable_message_ordering=True)
        return pubsub.PublisherClient(publisher_options=publisher_options)

    @cached_property
    def mongo_client(self) -> AsyncMongoClient:
        """Return the shared async MongoDB client wired to the query logger."""
        return AsyncMongoClient(
            str(get_env("MONGODB_URI")),
            tz_aware=True,
            event_listeners=[MongoQueryLogger(logger=self.logger)],
        )

    @cached_property
    def storage_client(self) -> Any:  # noqa: ANN401 — google-cloud-storage Client lacks usable public types for mypy
        """Return the shared GCS storage client."""
        return storage.Client()

    @cached_property
    def default_database(self) -> database.AsyncDatabase:
        """Return the default MongoDB database from the connection URI."""
        return self.mongo_client.get_default_database()

    async def check_health(self, request: Request) -> None:
        """Override in subclass to add application-specific health checks."""

    async def __aenter__(self) -> dict[str, Any]:  # noqa: ANON002 — request.state context dict, intentionally polymorphic for subclasses
        """Enter the resource context; return the dict published as ``request.state``."""
        await self.http_client.__aenter__()
        return {
            "http_client": self.http_client,
            "firestore_client": self.firestore_client,
            "publisher_client": self.publisher_client,
            "mongo_client": self.mongo_client,
            "storage_client": self.storage_client,
            "config": self.config,
            "args": self.args,
        }

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        """Close all shared clients on shutdown."""
        await self.http_client.__aexit__(exc_type, exc_val, exc_tb)
        await self.mongo_client.close()
        self.firestore_client.close()
        self.publisher_client.transport.close()

    def setup_exception_handlers(self, app: FastAPI) -> None:
        """Register all global exception handlers on the FastAPI app."""
        # Starlette's add_exception_handler stub demands a (Request, Exception) signature, but
        # the handlers below narrow to specific exception subclasses — Starlette accepts that
        # at runtime. Suppress the stub's overconservative type via type: ignore on each call.
        app.add_exception_handler(RequestValidationError, request_validation_exception_handler)  # type: ignore[arg-type]
        app.add_exception_handler(ValidationError, request_validation_exception_handler)  # type: ignore[arg-type]

        for exception_class in [
            ArithmeticError,
            AssertionError,
            AttributeError,
            LookupError,
            ImportError,
            MemoryError,
            ReferenceError,
            ValueError,
            TypeError,
            OSError,
            RuntimeError,
            PyMongoError,
        ]:
            app.add_exception_handler(exception_class, runtime_exception_handler)

        for exception_class in [asyncio.TimeoutError]:
            app.add_exception_handler(exception_class, timeout_exception_handler)  # type: ignore[arg-type]

        for exception_class in [httpx.HTTPStatusError]:
            app.add_exception_handler(exception_class, http_exception_handler)  # type: ignore[arg-type]

        for exception_class in [httpx.ReadError, httpx.ConnectError]:
            app.add_exception_handler(exception_class, http_connection_exception_handler)  # type: ignore[arg-type]

        app.add_exception_handler(GoogleAPICallError, google_api_exception_handler)  # type: ignore[arg-type]
        app.add_exception_handler(GenAIAPIError, genai_api_exception_handler)  # type: ignore[arg-type]

    def setup_middleware(self, app: FastAPI) -> None:
        """Register all middleware (context seeding, timeout, access log, trusted host, gzip, CORS)."""
        project_id = self.config["project_id"]

        # Outermost middleware: seed the per-request ambient log context (route label + Cloud Trace
        # linkage) before anything else runs, so access logging and the exception handlers below all
        # emit it. The contextvar lives in this request's task and dies with it — no manual reset.
        @app.middleware("http")
        async def _seed_log_context(request: Request, call_next: Any) -> Response:  # noqa: ANN401 — Starlette call_next is an untyped ASGI callable
            seed_request_context(request, project_id=project_id)
            return await call_next(request)

        app.add_middleware(RequestTimeoutMiddleware, timeout=1800)  # type: ignore[arg-type]
        app.add_middleware(AccessLogMiddleware)  # type: ignore[arg-type]
        app.add_middleware(trustedhost.TrustedHostMiddleware, allowed_hosts=["*"])
        app.add_middleware(gzip.GZipMiddleware)
        app.add_middleware(
            CORSMiddleware,
            allow_origins=[
                "https://localhost:3000",
                "http://localhost:3000",
                "https://192.168.1.77:3000",
                "http://192.168.1.77:3000",
                "https://www.clarityautocare.com",
            ],
            allow_methods=["*"],
            allow_headers=["*"],
        )

    def setup_routes(self, app: FastAPI) -> None:
        """Register the built-in ``/api/ping``, ``/api/health``, and ``/api/status`` routes."""

        @app.get("/api/ping")
        @app.get("/api/health")
        async def root(request: Request) -> str:
            logger.info("I'm alive!")
            if "exception" in request.query_params:
                raise RuntimeError("Exception requested")
            uptime = int(time.time() - _start_time)
            return f"OK - running for {uptime} sec"

        @app.get("/api/status")
        async def health_check(request: Request) -> JSONResponse:
            logger.info("Health check")
            await self.default_database.command("ping")
            credentials, project_id = google_auth_default()
            if not credentials or not project_id:
                raise RuntimeError("Google Cloud credentials or project ID not found")
            await self.check_health(request)
            return JSONResponse({"status": "healthy"})

    async def execute(self) -> int:
        """Launch hypercorn against the FastAPI app and run until shutdown."""
        bind = f"0.0.0.0:{self.args['port']}"
        config_opts: dict[str, Any] = {
            "bind": [bind],
            "accesslog": None,
            # errorlog: Keep enabled (default) so unhandled exceptions are logged to stderr.
            # Custom exception handlers only cover specific types - exceptions from external
            # SDKs (e.g., google.api_core.exceptions) would otherwise result in silent 500s.
        }
        # Local development: Use HTTPS for mobile testing (cert.pem/key.pem from mkcert)
        if not self.args["cloud"]:
            config_opts["certfile"] = "cert.pem"
            config_opts["keyfile"] = "key.pem"
        config = HypercornConfig.from_mapping(**config_opts)
        await serve(self.app, config)  # type: ignore[arg-type]
        return 0
