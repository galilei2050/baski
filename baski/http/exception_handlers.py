import asyncio
import json
from http import HTTPStatus
from json import JSONDecodeError
from typing import TYPE_CHECKING

from fastapi import Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from google.api_core.exceptions import GoogleAPICallError
from google.genai.errors import APIError as GenAIAPIError
from httpx import ConnectError, HTTPStatusError, ReadError, StreamError
from pydantic import ValidationError

from .dependencies import get_logger

if TYPE_CHECKING:
    from .config import AppConfig

__all__ = [
    "genai_api_exception_handler",
    "google_api_exception_handler",
    "http_connection_exception_handler",
    "http_exception_handler",
    "request_validation_exception_handler",
    "runtime_exception_handler",
    "timeout_exception_handler",
]


# Mapping downstream HTTP status codes to our service response codes
DOWNSTREAM_STATUS_MAPPING = {
    # Authentication/authorization issues - our service misconfiguration
    401: HTTPStatus.INTERNAL_SERVER_ERROR,
    403: HTTPStatus.INTERNAL_SERVER_ERROR,
    # Operational client errors - dependency issues
    404: HTTPStatus.SERVICE_UNAVAILABLE,
    # 429 not mapped — pass through as retryable
    # All 5xx errors - downstream service issues
    **dict.fromkeys(range(500, 600), HTTPStatus.SERVICE_UNAVAILABLE),
}


async def request_body(request: Request) -> dict | str | None:
    body = None
    if request.method in ["POST", "PUT", "PATCH"]:
        try:
            body = await request.json()
        except JSONDecodeError:
            body = await request.body()
            if body:
                body = body.decode("utf-8")
        except ValueError:
            pass
    if not body:
        return None
    encoded_size = len(body.encode("utf-8")) if isinstance(body, str) else len(json.dumps(body, default=str))
    if encoded_size < 1024 * 1024:
        return body
    return None


def timeout_exception_handler(request: Request, _exc: asyncio.TimeoutError) -> JSONResponse:
    logger = get_logger(request)
    logger.warning("Request timeout")
    return JSONResponse(
        content={"error": {"code": HTTPStatus.REQUEST_TIMEOUT, "message": "Timeout error during execution"}},
        status_code=HTTPStatus.REQUEST_TIMEOUT,
    )


async def http_exception_handler(request: Request, exc: HTTPStatusError) -> Response:
    config: AppConfig = request.state.config
    logger = get_logger(request)
    logger.warning(
        "Downstream HTTP error",
        labels={
            "downstream": {
                "url": str(exc.request.url),
                "method": exc.request.method,
                "statusCode": exc.response.status_code,
                "content": exc.response.content,
            },
            "body": await request_body(request),
        },
    )
    if config.debug:
        try:
            return Response(
                content=exc.response.content, status_code=exc.response.status_code, headers=exc.response.headers
            )
        except (JSONDecodeError, StreamError):
            pass

    # Map downstream status to appropriate response status
    response_status = DOWNSTREAM_STATUS_MAPPING.get(
        exc.response.status_code,
        exc.response.status_code,  # Pass through unmapped 4xx client errors
    )

    return JSONResponse(
        content={
            "error": {
                "code": response_status,
                "message": f"Http status {exc.response.status_code} from downstream http call",
            }
        },
        status_code=response_status,
    )


def http_connection_exception_handler(request: Request, exc: ReadError | ConnectError) -> JSONResponse:
    logger = get_logger(request)
    logger.warning(
        "Downstream HTTP connection error", labels={"exceptionType": type(exc).__name__, "exception": str(exc)}
    )

    return JSONResponse(
        content={
            "error": {
                "code": HTTPStatus.BAD_GATEWAY,
                "message": f"Connection error to downstream service: {type(exc).__name__}",
            }
        },
        status_code=HTTPStatus.BAD_GATEWAY,
    )


async def runtime_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    code = HTTPStatus.INTERNAL_SERVER_ERROR
    logger = get_logger(request)
    logger.error(
        "Runtime exception",
        labels={
            "body": await request_body(request),
        },
        exc_info=exc,
    )
    return JSONResponse(
        content={"error": {"code": code, "message": f"Exception {type(exc)} during execution"}}, status_code=code
    )


async def validation_exception_handler(request: Request, exc: ValidationError) -> JSONResponse:
    logger = get_logger(request)
    logger.error(
        "Pydantic validation error",
        exc_info=exc,
        labels={
            "errors": jsonable_encoder(exc.errors()),
            "body": await request_body(request),
        },
    )

    return JSONResponse(
        status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
        content={"detail": str(exc)},
    )


async def request_validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    logger = get_logger(request)

    logger.warning(
        "Validation error occurred",
        labels={"body": await request_body(request), "errors": jsonable_encoder(exc.errors())},
    )
    return JSONResponse(
        status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
        content={"detail": jsonable_encoder(exc.errors())},
    )


async def genai_api_exception_handler(request: Request, exc: GenAIAPIError) -> JSONResponse:
    logger = get_logger(request)
    logger.warning(
        "GenAI API error",
        labels={
            "body": await request_body(request),
            "genaiCode": exc.code,
            "genaiStatus": exc.status,
            "genaiMessage": exc.message,
            "genaiDetails": str(exc.details) if exc.details else None,
        },
    )

    response_status = DOWNSTREAM_STATUS_MAPPING.get(exc.code, exc.code)

    return JSONResponse(
        content={"error": {"code": response_status, "message": f"GenAI API error: {exc.status}"}},
        status_code=response_status,
    )


async def google_api_exception_handler(request: Request, exc: GoogleAPICallError) -> JSONResponse:
    logger = get_logger(request)
    logger.error(
        "Google API error",
        labels={
            "body": await request_body(request),
            "googleApiCode": exc.code,
            "googleApiMessage": exc.message,
            "grpcStatusCode": str(exc.grpc_status_code) if exc.grpc_status_code else None,
            "reason": exc.reason,
            "details": str(exc.details) if exc.details else None,
            "errors": str(exc.errors) if exc.errors else None,
        },
        exc_info=exc,
    )

    # Map Google API codes to HTTP response status
    code = int(exc.code) if exc.code else int(HTTPStatus.INTERNAL_SERVER_ERROR)
    response_status = int(DOWNSTREAM_STATUS_MAPPING.get(code, code))

    return JSONResponse(
        content={"error": {"code": response_status, "message": f"Google API error: {exc.message}"}},
        status_code=response_status,
    )
