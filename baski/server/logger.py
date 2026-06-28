"""Application logging on the standard library.

A JSON line per record in cloud mode (Cloud Run ingests stdout into Cloud Logging) and a
readable line locally.

Log with the ordinary idiom anywhere — ``logging.getLogger(__name__)``. Attach structured
fields per call with native ``extra={...}`` (each key lands at the top level of the JSON line,
i.e. Cloud Logging ``jsonPayload.<key>``), or set ambient context that every log in the current
request/task carries:

    with log_context(customer_id="123"):     # auto-reset on exit
        logging.getLogger(__name__).info("charged")   # -> field {"customer_id": "123"}

Context lives in a contextvar, so concurrent requests never see each other's labels.
"""

import contextvars
import logging
import sys
from collections.abc import Generator, Mapping
from contextlib import contextmanager
from types import MappingProxyType
from typing import Any

from fastapi import Request
from fastapi.encoders import jsonable_encoder

from ..primitives import json

__all__ = ["add_labels", "configure_logging", "log_context", "seed_request_context"]


# Read-only default — every writer (.set) installs a fresh dict, so the shared default is never mutated.
_labels: contextvars.ContextVar[Mapping[str, Any]] = contextvars.ContextVar("log_labels", default=MappingProxyType({}))


def add_labels(**labels: Any) -> None:  # noqa: ANN401 — arbitrary structured log fields
    """Merge labels into the current task's ambient log context (no auto-reset)."""
    _labels.set({**_labels.get(), **jsonable_encoder(labels)})


@contextmanager
def log_context(**labels: Any) -> Generator[None, None, None]:  # noqa: ANN401 — arbitrary structured log fields
    """Attach labels to every log emitted in this block; restore the prior context on exit."""
    token = _labels.set({**_labels.get(), **jsonable_encoder(labels)})
    try:
        yield
    finally:
        _labels.reset(token)


def seed_request_context(request: Request, *, project_id: str | None = None) -> None:
    """Seed ambient context from an HTTP request: route label and Cloud Trace linkage.

    Call once per request (e.g. from middleware) so every log in the request carries it.
    """
    path_parts = [p for p in request.url.path.split("/") if p]
    fields: dict[str, Any] = {
        "handler": "_".join(path_parts) if path_parts else "root",
        "requestUrl": str(request.url),
        "requestMethod": request.method,
        "requestQueryParams": dict(request.query_params),
    }
    trace = request.headers.get("x-cloud-trace-context", "")
    if trace and project_id:
        parts = trace.split("/")
        if parts[0]:
            fields["logging.googleapis.com/trace"] = f"projects/{project_id}/traces/{parts[0]}"
        if len(parts) > 1 and parts[1]:
            fields["logging.googleapis.com/spanId"] = parts[1].split(";")[0]
    add_labels(**fields)


# Attributes the stdlib sets on every LogRecord; anything else on a record came from extra={...}.
_RESERVED = frozenset(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {"message", "asctime", "taskName"}


def _fields(record: logging.LogRecord) -> dict[str, Any]:  # noqa: ANON002 — arbitrary structured log fields, intentionally polymorphic
    """The task's ambient labels plus this record's own ``extra={...}`` fields (per-call wins)."""
    extra = {key: value for key, value in record.__dict__.items() if key not in _RESERVED}
    return {**_labels.get(), **extra}


class _JsonFormatter(logging.Formatter):
    """One Cloud Logging JSON line per record: `severity`/trace keys promote, the rest land in jsonPayload."""

    def format(self, record: logging.LogRecord) -> str:
        entry = _fields(record)
        entry["severity"] = record.levelname
        message = record.getMessage()
        if record.exc_info:
            message = f"{message}\n{self.formatException(record.exc_info)}"
        entry["message"] = message
        # One record per line — Cloud Logging splits stdout on newlines, so the JSON must not be indented.
        return json.dumps(entry, indent=None)


class _PrettyFormatter(logging.Formatter):
    """Readable line plus any structured fields, for local runs."""

    def format(self, record: logging.LogRecord) -> str:
        line = super().format(record)
        fields = _fields(record)
        if fields:
            line += "\n" + " " * 17 + json.dumps(fields)  # aligns under "HH:MM:SS LEVEL   "
        return line


def configure_logging(*, cloud: bool, debug: bool) -> None:
    """Install one root stdout handler: JSON in cloud mode, readable locally."""
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(
        _JsonFormatter()
        if cloud
        else _PrettyFormatter(style="{", fmt="{asctime} {levelname:7} {message}", datefmt="%H:%M:%S")
    )
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(logging.DEBUG if debug else logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
