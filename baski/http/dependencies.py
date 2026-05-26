from fastapi import Depends, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from google.cloud import firestore, pubsub, storage, tasks_v2
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


# Dependency to get the application config
def get_config(request: Request) -> AppConfig:
    return request.state.config


def get_logger(request: Request) -> Logger:
    if request.state.logging_client:
        return CloudLogger(
            logger_client=request.state.logging_client, request=request, project_id=request.state.config["project_id"]
        )
    return LocalLogger(request=request)


# Dependency to get the MongoDB client
def get_mongo_client(request: Request) -> AsyncMongoClient:
    return request.state.mongo_client


# Dependency to get the default MongoDB database
def get_default_database(request: Request) -> AsyncDatabase:
    return request.state.mongo_client.get_default_database()


# Dependency to get the Firestore client
def get_firestore_client(request: Request) -> firestore.AsyncClient:
    return request.state.firestore_client


# Dependency to get the PubSub client
def get_publisher_client(request: Request) -> pubsub.PublisherClient:
    return request.state.publisher_client


# Dependency to get the HTTP client
def get_http_client(request: Request) -> AsyncClient:
    return request.state.http_client


# Dependency to get the Storage client
def get_storage_client(request: Request) -> storage.Client:
    return request.state.storage_client


# Dependency to get the dataset bucket
def get_dataset_bucket(request: Request) -> storage.Bucket:
    return request.state.dataset_bucket


# Dependency to get the Cloud Tasks async client
def get_cloud_tasks_client(request: Request) -> tasks_v2.CloudTasksAsyncClient:
    return request.state.cloud_tasks_client


# Dependency to extract token from a Bearer authorization header
def get_auth_token(credentials: HTTPAuthorizationCredentials = Depends(security)) -> str | None:
    if credentials:
        return credentials.credentials
    return None
