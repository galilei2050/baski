import asyncio
import logging
import time
import types
from contextlib import asynccontextmanager
from functools import cached_property
from typing import Annotated, Any

import google.cloud.firestore as firestore
import google.cloud.pubsub as pubsub
import google.cloud.storage as storage
import httpx
from fastapi import Depends, FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware import gzip, trustedhost
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
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
from .dependencies import get_logger
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
    @cached_property
    def app(self) -> FastAPI:

        @asynccontextmanager
        async def lifespan(_app: FastAPI) -> Any:
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
        return httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=3),
            timeout=httpx.Timeout(60 * 5),
        )

    @cached_property
    def firestore_client(self) -> firestore.AsyncClient:
        return firestore.AsyncClient()

    @cached_property
    def publisher_client(self) -> pubsub.PublisherClient:
        publisher_options = pubsub.types.PublisherOptions(enable_message_ordering=True)
        return pubsub.PublisherClient(publisher_options=publisher_options)

    @cached_property
    def mongo_client(self) -> AsyncMongoClient:
        return AsyncMongoClient(
            str(get_env("MONGODB_URI")),
            tz_aware=True,
            event_listeners=[MongoQueryLogger(logger=self.logger)],
        )

    @cached_property
    def storage_client(self) -> Any:
        return storage.Client()

    @cached_property
    def default_database(self) -> database.AsyncDatabase:
        return self.mongo_client.get_default_database()

    async def check_health(self, request: Request) -> None:
        """Override in subclass to add application-specific health checks."""

    async def __aenter__(self) -> dict[str, Any]:
        await self.http_client.__aenter__()
        return {
            "http_client": self.http_client,
            "firestore_client": self.firestore_client,
            "publisher_client": self.publisher_client,
            "mongo_client": self.mongo_client,
            "storage_client": self.storage_client,
            "config": self.config,
            "args": self.args,
            "logging_client": self.logging_client,
        }

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        await self.http_client.__aexit__(exc_type, exc_val, exc_tb)
        await self.mongo_client.close()
        self.firestore_client.close()
        self.publisher_client.transport.close()

    def setup_exception_handlers(self, app: FastAPI) -> None:
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

        @app.get("/api/ping")
        @app.get("/api/health")
        async def root(
            request: Request,
            logger: Annotated[Any, Depends(get_logger)],
        ) -> str:
            logger.info("I'm alive!")
            if "exception" in request.query_params:
                raise RuntimeError("Exception requested")
            uptime = int(time.time() - _start_time)
            return f"OK - running for {uptime} sec"

        @app.get("/api/status")
        async def health_check(
            request: Request,
            logger: Annotated[Any, Depends(get_logger)],
        ) -> JSONResponse:
            logger.info("Health check")
            await self.default_database.command("ping")
            credentials, project_id = google_auth_default()
            if not credentials or not project_id:
                raise RuntimeError("Google Cloud credentials or project ID not found")
            await self.check_health(request)
            return JSONResponse({"status": "healthy"})

    def execute(self) -> int:
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
        asyncio.run(serve(self.app, config))  # type: ignore[arg-type]
        return 0
