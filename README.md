# baski

Shared foundation library for personal projects (`clarity-auto-care`, `nisse`, etc.). FastAPI server template, aiogram v3 Telegram framework, GCP integrations, common primitives.

## Install

```bash
uv add baski                              # from PyPI (not published yet)
uv add git+https://github.com/galilei2050/baski   # from GitHub
uv add /path/to/baski                     # local checkout
```

## What's in it

```
baski/
├── env, concurrent, on_exception, crypto, mongo_logging
├── primitives/    # datetime (UTC-aware), json (auto-parse dates), unique_id, dataclass helpers
├── pattern/       # Singleton, ClassFactory, exponential_backoff retry
├── middleware/    # AccessLog, RequestTimeout
├── server/        # FastAPIServer, AsyncServer base, structured Logger, dependencies, exception handlers
├── clients/       # serpapi, scrapin, spectr, playwright (httpx-based, async)
├── infra/         # Pulumi helpers: make_topic, create_subscription_with_push_and_dlq, make_scheduled_job
├── schema/        # Marshmallow Schema base + BigQueryDateTime / NotNullFloat / NotNullString
├── monitoring/    # PubSub telemetry publisher
└── telegram/      # aiogram v3: Receptionist, TypedHandler, FirebaseStorage, ChatHistory, filters, middleware
```

## Quick examples

**FastAPI server**

```python
from baski.server import FastAPIServer

class MyServer(FastAPIServer):
    name = "my-service"

if __name__ == "__main__":
    MyServer().start()
```

**Telegram bot (aiogram v3)**

```python
from aiogram import Router
from baski.server.aiogram_server import TelegramServer

router = Router()

@router.message()
async def echo(message):
    await message.answer(message.text)

class MyBot(TelegramServer):
    def routers(self):
        return [router]

if __name__ == "__main__":
    MyBot().start()
```

Polling locally, FastAPI webhook on `--cloud`. Both modes share the same hypercorn stack.

**Datetime / JSON**

```python
from baski.primitives import datetime, json

now = datetime.now()                     # UTC-aware
text = json.dumps({"ts": now})           # auto-serializes datetime → ISO 8601
data = json.loads(text)                  # auto-parses ISO strings back to datetime
```

**Pulumi infra**

```python
from baski.infra.queue import make_topic, create_subscription_with_push_and_dlq

topic, debug_sub = make_topic("orders")
create_subscription_with_push_and_dlq(
    topic_name="orders",
    subscription_name="consume",
    http_endpoint="https://api.example.com/consume/orders",
    service_account=my_sa,
    notification_channels=my_alert_channels,
)
```

## Development

```bash
uv sync                                  # install package + runtime deps
uv sync --group dev                      # add pytest
uv run pytest tests/
uv run ruff check baski/
```

Python 3.11+. Single flat dependency list — no extras to remember.
