# Foundation Server Module

Updated: 2026-10-31

## Business decisions

None at this level.

## Technical decisions

None at this level.

## Exception Handling & Status Code Mapping

### Downstream HTTP Status Code Mapping

When external API calls return errors, the global exception handler maps downstream status codes to appropriate service response codes.

The mapping is `DOWNSTREAM_STATUS_MAPPING` in `baski/http/exception_handlers.py`.

### Status Code Mapping Rationale

**429 (Rate Limited) — not mapped**

A downstream 429 passes through unchanged, so callers see it as retryable.

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

Logging is plain stdlib (`baski.server.logger`). **Every module declares `logger = logging.getLogger(__name__)` at top scope and uses it directly — never inject a logger** (no `logger` params, fields, or `logger=...` call args). Request context rides a contextvar, so a passed logger buys nothing; `getLogger(__name__)` gives the named hierarchy `configure_logging` relies on. Attach per-call structured fields via native `extra={...}` (each key becomes a top-level `jsonPayload.<key>`). Ambient per-request/task context comes from `log_context(...)`/`add_labels(...)` (a contextvar). Each HTTP request is seeded once by `LogContextMiddleware` (added outermost in `setup_middleware` so access logging + exception handlers carry it; calls `seed_request_context` → route label + Cloud Trace), so handlers just log. Cloud mode (`configure_logging(cloud=True)`) emits one JSON line per record to stdout for Cloud Run to ingest; local mode is readable.

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
