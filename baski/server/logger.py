"""Structured logger abstractions: base, Google Cloud Logging, and stdlib fallback."""

import logging as python_logging
import sys
import traceback
from typing import ClassVar

from fastapi import Request
from fastapi.encoders import jsonable_encoder
from google.cloud import logging as google_logging

from ..primitives import json

__all__ = ["CloudLogger", "LocalLogger", "Logger"]


class Logger:
    """Base structured logger. Subclasses implement the level methods; this one no-ops."""

    BLOCKLISTED_HEADERS: ClassVar[list[str]] = [
        "authorization",
        "cookie",
        "x-api-key",
    ]

    def __init__(self, request: Request | None = None) -> None:
        """Capture request-derived labels and trace context for subsequent log calls."""
        if request:
            path_parts = [p for p in request.url.path.split("/") if p]
            self._name = "root" if not path_parts else "_".join(path_parts)

            # Extract trace context from headers
            trace_header = request.headers.get("x-cloud-trace-context", "")
            if trace_header:
                trace_parts = trace_header.split("/")
                self._trace_id = trace_parts[0] if trace_parts else None
                self._span_id = trace_parts[1].split(";")[0] if len(trace_parts) > 1 else None
            else:
                self._trace_id = None
                self._span_id = None

            labels = {
                "requestQueryParams": request.query_params,
                "requestUrl": str(request.url),
                "requestMethod": request.method,
                "handler": self._name,
            }
        else:
            self._name = "app"
            self._trace_id = None
            self._span_id = None
            labels = {"handler": self._name}

        self._labels = jsonable_encoder(labels)

    def debug(self, msg: str, labels: dict | None = None) -> None:  # noqa: ANON002 — structured-log fields, intentionally polymorphic
        """Emit a DEBUG-level structured log entry. No-op on the base class."""

    def info(self, msg: str, labels: dict | None = None) -> None:  # noqa: ANON002 — structured-log fields, intentionally polymorphic
        """Emit an INFO-level structured log entry. No-op on the base class."""

    def warning(self, msg: str, labels: dict | None = None) -> None:  # noqa: ANON002 — structured-log fields, intentionally polymorphic
        """Emit a WARNING-level structured log entry. No-op on the base class."""

    def error(self, msg: str, labels: dict | None = None, exc_info: BaseException | None = None) -> None:  # noqa: ANON002 — structured-log fields, intentionally polymorphic
        """Emit an ERROR-level structured log entry. No-op on the base class."""

    def exception(self, msg: str, labels: dict | None = None) -> None:  # noqa: ANON002 — structured-log fields, intentionally polymorphic
        """Log at ERROR level with the currently-handled exception. Call only from within an `except` block."""
        exc = sys.exc_info()[1]
        self.error(msg, labels=labels, exc_info=exc)


class CloudLogger(Logger):
    """Structured logger that writes to Google Cloud Logging via the python client."""

    def __init__(
        self,
        logger_client: google_logging.Client,
        request: Request | None = None,
        project_id: str | None = None,
    ) -> None:
        """Initialize with a Cloud Logging client, optional request, and project ID for trace links."""
        super().__init__(request)
        self._project_id = project_id

        self._logger: google_logging.Logger = logger_client.logger(
            name="app",
        )

    def _make_log_data(
        self,
        msg: str,
        severity: str,
        labels: dict | None = None,  # noqa: ANON002 — structured-log fields, intentionally polymorphic
        exc_info: BaseException | None = None,
    ) -> dict:  # noqa: ANON002 — Cloud Logging log_struct payload, intentionally polymorphic
        labels = self._labels | jsonable_encoder(labels or {})
        log_data = labels | {"message": msg, "severity": severity}

        # Add trace context for Google Cloud Logging
        if self._trace_id:
            log_data["trace"] = f"projects/{self._project_id}/traces/{self._trace_id}"
        if self._span_id:
            log_data["spanId"] = self._span_id

        if exc_info:
            log_data["excInfo"] = "".join(traceback.format_exception(type(exc_info), exc_info, exc_info.__traceback__))

        return log_data

    def debug(self, msg: str, labels: dict | None = None) -> None:  # noqa: ANON002 — structured-log fields, intentionally polymorphic
        """Write a DEBUG entry to Cloud Logging."""
        self._logger.log_struct(self._make_log_data(msg, "DEBUG", labels))

    def info(self, msg: str, labels: dict | None = None) -> None:  # noqa: ANON002 — structured-log fields, intentionally polymorphic
        """Write an INFO entry to Cloud Logging."""
        self._logger.log_struct(self._make_log_data(msg, "INFO", labels))

    def warning(self, msg: str, labels: dict | None = None) -> None:  # noqa: ANON002 — structured-log fields, intentionally polymorphic
        """Write a WARNING entry to Cloud Logging."""
        self._logger.log_struct(self._make_log_data(msg, "WARNING", labels))

    def error(self, msg: str, labels: dict | None = None, exc_info: BaseException | None = None) -> None:  # noqa: ANON002 — structured-log fields, intentionally polymorphic
        """Write an ERROR entry to Cloud Logging, optionally including a traceback."""
        self._logger.log_struct(self._make_log_data(msg, "ERROR", labels, exc_info))


class LocalLogger(Logger):
    """Structured logger that routes through the stdlib root logger for local runs."""

    def __init__(self, request: Request | None = None, *, skip_labels: bool = False) -> None:
        """Initialize; set ``skip_labels`` to omit the JSON label suffix for cleaner local output."""
        super().__init__(request)
        self._logger = python_logging.root
        self._skip_labels = skip_labels

    def _make_log_data(self, msg: str, labels: dict | None = None) -> str:  # noqa: ANON002 — structured-log fields, intentionally polymorphic
        labels = self._labels | jsonable_encoder(labels or {})
        if self._skip_labels:
            return msg
        # 17 chars aligns with "HH:MM:SS LEVEL   "
        return msg + "\n" + " " * 17 + json.dumps(labels)

    def debug(self, msg: str, labels: dict | None = None) -> None:  # noqa: ANON002 — structured-log fields, intentionally polymorphic
        """Write a DEBUG entry to the stdlib root logger."""
        self._logger.debug(self._make_log_data(msg, labels))

    def info(self, msg: str, labels: dict | None = None) -> None:  # noqa: ANON002 — structured-log fields, intentionally polymorphic
        """Write an INFO entry to the stdlib root logger."""
        self._logger.info(self._make_log_data(msg, labels))

    def warning(self, msg: str, labels: dict | None = None) -> None:  # noqa: ANON002 — structured-log fields, intentionally polymorphic
        """Write a WARNING entry to the stdlib root logger."""
        self._logger.warning(self._make_log_data(msg, labels))

    def error(self, msg: str, labels: dict | None = None, exc_info: BaseException | None = None) -> None:  # noqa: ANON002 — structured-log fields, intentionally polymorphic
        """Write an ERROR entry to the stdlib root logger, optionally including a traceback."""
        self._logger.error(self._make_log_data(msg, labels), exc_info=exc_info)
