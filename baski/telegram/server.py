import abc
import argparse
import asyncio
import logging
from collections.abc import Iterable
from contextlib import asynccontextmanager
from functools import cached_property
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

from ..env import get_env
from ..pattern import retry
from ..server.async_server import AsyncServer

__all__ = ["TelegramServer"]


class TelegramServer(AsyncServer):
    """aiogram v3 server. Two execution modes:

    - polling (default, local) — `dp.start_polling(bot)`.
    - webhook (`--cloud`) — FastAPI + hypercorn, same stack as `FastAPIServer`. POSTs to the
      webhook path are deserialized into `aiogram.types.Update` and dispatched via
      `dp.feed_webhook_update`. Webhook registration runs on FastAPI startup.

    Subclasses must implement `routers()`. Override `outer_middlewares` / `fsm_storage` /
    `add_webhook_routes` as needed.
    """

    @abc.abstractmethod
    def routers(self) -> Iterable[Router]:
        raise NotImplementedError

    def outer_middlewares(self) -> Iterable[Any]:
        return []

    def fsm_storage(self) -> BaseStorage:
        return MemoryStorage()

    def add_webhook_routes(self, app: FastAPI) -> None:
        """Hook for subclasses to add extra HTTP routes alongside the webhook endpoint."""

    @cached_property
    def bot(self) -> Bot:
        return Bot(token=str(self.args["token"]))

    @cached_property
    def dp(self) -> Dispatcher:
        dp = Dispatcher(storage=self.fsm_storage())
        for m in self.outer_middlewares():
            dp.message.outer_middleware(m)
        for router in self.routers():
            dp.include_router(router)
        return dp

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        super().add_arguments(parser)
        parser.add_argument("--webhook-path", default=str(get_env("WEBHOOK_URL", "")), help="Public webhook URL")
        parser.add_argument("--token", default=str(get_env("TELEGRAM_TOKEN", "")), help="Telegram bot token")

    def stop(self) -> None:
        logging.warning("Got shutdown signal, stopping dispatcher")
        asyncio.get_event_loop().create_task(self.dp.stop_polling())
        super().stop()

    def execute(self) -> int:
        if self.args["cloud"]:
            return self._run_webhook()
        return self._run_polling()

    def _run_polling(self) -> int:
        async def main() -> None:
            await self.bot.delete_webhook(drop_pending_updates=False)
            await self.dp.start_polling(self.bot)

        self.loop.run_until_complete(main())
        return 0

    def _run_webhook(self) -> int:
        webhook_url = self.args["webhook_path"]
        path = urlparse(webhook_url).path or "/webhook"

        @asynccontextmanager
        async def lifespan(_: FastAPI):
            info = await self.bot.get_webhook_info()
            if info.url != webhook_url:
                await retry(self.bot.set_webhook, exceptions=(TelegramAPIError,), url=webhook_url)
            yield
            await self.bot.session.close()

        app = FastAPI(lifespan=lifespan, openapi_url=None)

        @app.post(path)
        async def webhook(request: Request) -> Response:
            update = Update.model_validate(await request.json(), context={"bot": self.bot})
            await self.dp.feed_webhook_update(self.bot, update)
            return Response(status_code=200)

        @app.get("/")
        @app.get("/ping")
        async def ping() -> str:
            return "OK"

        self.add_webhook_routes(app)

        bind = f"0.0.0.0:{self.args['port']}"
        config = HypercornConfig.from_mapping(bind=[bind], accesslog=None)
        return asyncio.run(serve(app, config))
