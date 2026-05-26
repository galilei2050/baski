"""FastAPI dependency providers: pull request-scoped clients off ``request.state``."""

import google.cloud.firestore as firestore  # noqa: PLR0402 — `from google.cloud import X` form is broken for namespace pkg under mypy
import google.cloud.pubsub as pubsub  # noqa: PLR0402 — see above
import google.cloud.storage as storage  # noqa: PLR0402 — see above
import google.cloud.tasks_v2 as tasks_v2  # noqa: PLR0402 — see above
from fastapi import Depends, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from httpx import AsyncClient
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.asynchronous.mongo_client import AsyncMongoClient

from ..server.config import AppConfig
from ..server.logger import CloudLogger, LocalLogger, Logger

__all__ = [
    "get_auth_token",
    "get_cloud_tasks_client",
    "get_config",
    "get_dataset_bucket",
    "get_default_database",
    "get_firestore_client",
    "get_http_client",
    "get_logger",
    "get_mongo_client",
    "get_publisher_client",
    "get_storage_client",
]


security = HTTPBearer(auto_error=False)


def get_config(request: Request) -> AppConfig:
    """Return the application config attached to ``request.state``."""
    return request.state.config


def get_logger(request: Request) -> Logger:
    """Return a request-scoped logger (Cloud Logging in cloud mode, stdlib locally)."""
    if request.state.logging_client:
        return CloudLogger(
            logger_client=request.state.logging_client, request=request, project_id=request.state.config["project_id"]
        )
    return LocalLogger(request=request)


def get_mongo_client(request: Request) -> AsyncMongoClient:
    """Return the shared async MongoDB client."""
    return request.state.mongo_client


def get_default_database(request: Request) -> AsyncDatabase:
    """Return the default MongoDB database from the connection URI."""
    return request.state.mongo_client.get_default_database()


def get_firestore_client(request: Request) -> firestore.AsyncClient:
    """Return the shared async Firestore client."""
    return request.state.firestore_client


def get_publisher_client(request: Request) -> pubsub.PublisherClient:
    """Return the shared Pub/Sub publisher client."""
    return request.state.publisher_client


def get_http_client(request: Request) -> AsyncClient:
    """Return the shared outbound httpx client."""
    return request.state.http_client


def get_storage_client(request: Request) -> storage.Client:
    """Return the shared GCS storage client."""
    return request.state.storage_client


def get_dataset_bucket(request: Request) -> storage.Bucket:
    """Return the configured GCS dataset bucket."""
    return request.state.dataset_bucket


def get_cloud_tasks_client(request: Request) -> tasks_v2.CloudTasksAsyncClient:
    """Return the shared async Cloud Tasks client."""
    return request.state.cloud_tasks_client


def get_auth_token(credentials: HTTPAuthorizationCredentials = Depends(security)) -> str | None:
    """Extract the Bearer token from the Authorization header, if present."""
    if credentials:
        return credentials.credentials
    return None
