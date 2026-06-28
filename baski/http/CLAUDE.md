# Foundation Server Module

## Exception Handling & Status Code Mapping

### Downstream HTTP Status Code Mapping

When external API calls return errors, the global exception handler maps downstream status codes to appropriate service response codes.

**File**: `app/foundation/server/exception_handlers.py:20-32`

```python
DOWNSTREAM_STATUS_MAPPING = {
    # Authentication/authorization issues - our service misconfiguration
    401: HTTPStatus.INTERNAL_SERVER_ERROR,
    403: HTTPStatus.INTERNAL_SERVER_ERROR,

    # Operational client errors - dependency issues
    404: HTTPStatus.SERVICE_UNAVAILABLE,
    429: HTTPStatus.SERVICE_UNAVAILABLE,

    # All 5xx errors - downstream service issues
    **{status: HTTPStatus.SERVICE_UNAVAILABLE for status in range(500, 600)}
}
```

### Status Code Mapping Rationale

**429 (Rate Limited) → 503 (Service Unavailable)**

Rate limiting from downstream services is treated as an operational issue requiring immediate attention:
- Indicates excessive request volume
- May signal insufficient API quota
- Could reveal runaway jobs or bugs
- Triggers alerting for operations team
- Not a transient error to silently retry

The 503 response signals to clients that the service is temporarily unavailable due to dependency issues, while the actual 429 status is logged for debugging.

**404 (Not Found) → 503 (Service Unavailable)**

Downstream 404s are mapped to 503 because they represent dependency unavailability rather than client errors.

**401/403 → 500 (Internal Server Error)**

Authentication/authorization failures from downstream services indicate misconfiguration of our service credentials.

**5xx errors → 503 (Service Unavailable)**

All downstream server errors are uniformly treated as service unavailability.

### Exception Handlers

**HTTPStatusError Handler** (`exception_handlers.py:65-99`)

Handles errors from external HTTP calls made with httpx:
- Logs "Downstream HTTP error" with actual status code and response content
- Maps status code using `DOWNSTREAM_STATUS_MAPPING`
- Returns JSON error response with mapped status code
- In debug mode, passes through original response

**Connection Error Handler** (`exception_handlers.py:102-117`)

Handles `ReadError` and `ConnectError` from httpx:
- Logs "Downstream HTTP connection error"
- Returns 502 (Bad Gateway)

**Timeout Handler** (`exception_handlers.py:51-62`)

Handles `asyncio.TimeoutError`:
- Logs "Request timeout"
- Returns 408 (Request Timeout)

**Validation Error Handlers** (`exception_handlers.py:137-160`)

Handle Pydantic `ValidationError` and FastAPI `RequestValidationError`:
- Log validation errors with request body
- Return 422 (Unprocessable Entity)

**Runtime Exception Handler** (`exception_handlers.py:120-134`)

Catches all unhandled exceptions:
- Logs "Runtime exception" with full traceback
- Returns 500 (Internal Server Error)

### Exception Handler Registration

**File**: `app/foundation/server/fastapi_server.py:112-122`

```python
# HTTPStatusError from downstream HTTP calls
for exception_class in [httpx.HTTPStatusError]:
    app.add_exception_handler(exception_class, http_exception_handler)

# Connection errors
for exception_class in [httpx.ConnectError, httpx.ReadError]:
    app.add_exception_handler(exception_class, http_connection_exception_handler)

# Validation errors
app.add_exception_handler(ValidationError, validation_exception_handler)
app.add_exception_handler(RequestValidationError, request_validation_exception_handler)

# Runtime exceptions
app.add_exception_handler(Exception, runtime_exception_handler)
```

### Logging

Logging is plain stdlib (`baski.server.logger`). Get a logger with `logging.getLogger(__name__)`; attach per-call structured fields via native `extra={...}` (each key becomes a top-level `jsonPayload.<key>`). Ambient per-request/task context comes from `log_context(...)`/`add_labels(...)` (a contextvar). Each HTTP request is seeded once by the context-seeding middleware (`seed_request_context`, route label + Cloud Trace), so handlers and exception handlers just log — no per-request logger injection. Cloud mode (`configure_logging(cloud=True)`) emits one JSON line per record to stdout for Cloud Run to ingest; local mode is readable.

All exception handlers use structured logging:
- **Downstream HTTP errors**: Include `downstream.status_code` and `downstream.content`
- **Validation errors**: Include `errors` array and request `body`
- **Runtime exceptions**: Include full `exc_info` traceback
- Request body is logged for POST/PUT/PATCH requests (if < 1MB)

### Important Notes

- The actual downstream status code is always preserved in logs
- Status code mapping is transparent to clients
- Unmapped 4xx client errors pass through unchanged
- Debug mode bypasses mapping and returns original downstream responses
- All handlers are async to support async request body reading
