"""aiogram v3 server with polling and webhook modes."""

import abc
import argparse
import asyncio
import json
from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager
from functools import cached_property
from http import HTTPStatus
from typing import Any
from urllib.parse import urlparse

from aiogram import Bot, Dispatcher, Router
from aiogram.exceptions import TelegramAPIError
from aiogram.fsm.storage.base import BaseStorage
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Update
from fastapi import FastAPI, Request, Response
from hypercorn.asyncio import serve
from hypercorn.config import Config as HypercornConfig

from ..clients.scheduler import CloudTasksConfig, CloudTasksScheduler
from ..env import get_env
from ..pattern import retry
from ..server.async_server import AsyncServer

__all__ = ["TelegramServer"]

_WORKER_PATH = "/tasks/update"


class TelegramServer(AsyncServer):
    """aiogram v3 server with two execution modes.

    - polling (default, local) — `dp.start_polling(bot)`.
    - webhook (`--cloud`) — FastAPI + hypercorn, same stack as `FastAPIServer`. POSTs to the
      webhook path are deserialized into `aiogram.types.Update` and dispatched via
      `dp.feed_webhook_update`. Webhook registration runs on FastAPI startup.

    Subclasses must implement `routers()`. Override `outer_middlewares` / `fsm_storage` /
    `add_webhook_routes` as needed.
    """

    @abc.abstractmethod
    def routers(self) -> Iterable[Router]:
        """Return the routers to be mounted on the dispatcher."""
        raise NotImplementedError

    def outer_middlewares(self) -> Iterable[Any]:
        """Return outer middlewares to attach to the message observer."""
        return []

    def fsm_storage(self) -> BaseStorage:
        """Return the FSM storage backend (defaults to in-memory)."""
        return MemoryStorage()

    def add_webhook_routes(self, app: FastAPI) -> None:
        """Hook for subclasses to add extra HTTP routes alongside the webhook endpoint."""

    def cloud_tasks_config(self) -> CloudTasksConfig:
        """Return Cloud Tasks settings for the inbound-update queue.

        Override to run in webhook mode — baski imposes no project, region, queue, or
        service-account convention; the app supplies them (DI seam, like `routers()`).
        """
        raise NotImplementedError("Override cloud_tasks_config() to run in webhook mode")

    @cached_property
    def _scheduler(self) -> CloudTasksScheduler:
        """Cloud Tasks enqueuer for inbound updates (webhook mode only)."""
        return CloudTasksScheduler(self.cloud_tasks_config())

    @cached_property
    def bot(self) -> Bot:
        """Construct the `Bot` instance from the `--token` CLI arg."""
        return Bot(token=str(self.args["token"]))

    @cached_property
    def dp(self) -> Dispatcher:
        """Construct the `Dispatcher`, wiring middlewares and routers."""
        dp = Dispatcher(storage=self.fsm_storage())
        for m in self.outer_middlewares():
            dp.message.outer_middleware(m)
        for router in self.routers():
            dp.include_router(router)
        return dp

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        """Register CLI arguments for webhook URL and bot token."""
        super().add_arguments(parser)
        parser.add_argument("--webhook-url", default=str(get_env("WEBHOOK_URL", "")), help="Public webhook URL")
        parser.add_argument("--token", default=str(get_env("TELEGRAM_TOKEN", "")), help="Telegram bot token")

    def execute(self) -> int:
        """Run the server in webhook or polling mode based on `--cloud`."""
        if self.args["cloud"]:
            return self._run_webhook()
        return self._run_polling()

    def _run_polling(self) -> int:
        async def main() -> None:
            await self.bot.delete_webhook(drop_pending_updates=False)
            await self.dp.start_polling(self.bot)

        asyncio.run(main())
        return 0

    def _run_webhook(self) -> int:  # noqa: PLR0915 — wires startup + webhook/worker/ping routes inline
        webhook_url = self.args["webhook_url"]
        parsed = urlparse(webhook_url)
        path = parsed.path or "/webhook"
        worker_url = f"{parsed.scheme}://{parsed.netloc}{_WORKER_PATH}"

        @asynccontextmanager
        async def lifespan(_: FastAPI) -> AsyncIterator[None]:
            info = await self.bot.get_webhook_info()
            if info.url != webhook_url:
                await retry(self.bot.set_webhook, exceptions=(TelegramAPIError,), url=webhook_url)
            # feed_webhook_update never fires the dispatcher startup/shutdown events that
            # start_polling emits, so router on_startup hooks (client init) must be driven here.
            await self.dp.emit_startup(bot=self.bot)
            yield
            await self.dp.emit_shutdown(bot=self.bot)
            await self.bot.session.close()

        app = FastAPI(lifespan=lifespan, openapi_url=None)

        @app.post(path)
        async def webhook(request: Request) -> Response:
            # Enqueue and answer 200 at once. Running the agent inline blows past Telegram's
            # ~60s webhook timeout, which re-delivers the same update_id and double-processes.
            # Forward Telegram's exact bytes; parse only to peek update_id for the dedup name.
            raw = await request.body()
            await self._scheduler.enqueue(
                endpoint=worker_url,
                task_name=f"tg-update-{json.loads(raw)['update_id']}",
                payload=raw,
            )
            return Response(status_code=HTTPStatus.OK)

        @app.post(_WORKER_PATH)
        async def process_task(request: Request) -> Response:
            update = Update.model_validate(await request.json(), context={"bot": self.bot})
            await self.dp.feed_webhook_update(self.bot, update)
            return Response(status_code=HTTPStatus.OK)

        @app.get("/")
        @app.get("/ping")
        async def ping() -> str:
            return "OK"

        self.add_webhook_routes(app)

        bind = f"0.0.0.0:{self.args['port']}"
        config = HypercornConfig.from_mapping(bind=[bind], accesslog=None)
        asyncio.run(serve(app, config))  # type: ignore[arg-type]
        return 0
