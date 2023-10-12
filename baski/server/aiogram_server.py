import abc
import argparse
import asyncio
import logging
import signal
import typing
import aiogram

from functools import cached_property
from urllib.parse import urlparse

from aiohttp import web
from aiogram.contrib.fsm_storage import memory
from aiogram.utils import exceptions, executor
from aiogram.dispatcher import storage

from ..telegram import middleware, receptionist
from ..env import get_env
from ..pattern import retry
from .async_server import AsyncServer


__all__ = ['TelegramServer']


async def ok(self, *args, **kwargs):
    return web.Response(body="OK\n")


class TelegramServer(AsyncServer):

    @abc.abstractmethod
    def register_handlers(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def filters(self) -> typing.List:
        return []

    @abc.abstractmethod
    def middlewares(self) -> typing.List:
        return [
            middleware.UnprocessedMiddleware()
        ]

    @abc.abstractmethod
    def fsm_storage(self) -> storage.BaseStorage:
        return memory.MemoryStorage()

    @abc.abstractmethod
    def web_routes(self) -> typing.List:
        return [
            web.get('/', ok),
        ]

    @cached_property
    def bot(self):
        return aiogram.Bot(token=str(self.args['token']))

    @cached_property
    def dp(self):
        dp = aiogram.Dispatcher(self.bot, storage=self.fsm_storage)
        for m in self.middlewares():
            dp.setup_middleware(m)
        for f in self.filters():
            dp.filters_factory.bind(f)
        return dp

    @cached_property
    def executor(self):
        return executor.Executor(dispatcher=self.dp)

    @cached_property
    def receptionist(self):
        return receptionist.Receptionist(self.dp, debug=self.args['debug'])

    def add_arguments(self, parser: argparse.ArgumentParser):
        super().add_arguments(parser)
        parser.add_argument('--webhook-path', help="Webhook path", default=str(get_env("WEBHOOK_URL", "")))
        parser.add_argument('--token', help="Telegram bot token", default=str(get_env("TELEGRAM_TOKEN", "")))

    def init(self, *args, **kwargs):
        super().init(*args, **kwargs)
        self.register_handlers()

    def should_wait_task(self, t: asyncio.Task):
        return not t.done()

    def stop(self):
        self.dp.stop_polling()
        super().stop()

    def check_tasks_and_stop(self):
        loop = self.loop
        running_tasks = [t.get_coro() for t in asyncio.all_tasks(loop) if self.should_wait_task(t)]
        if len(running_tasks) == 1 and '_run_app' in str(running_tasks[0]):
            logging.info("Stop main app")
            signal.raise_signal(signal.SIGINT)
            loop.call_later(1, self.check_tasks_and_stop)
            return
        super().check_tasks_and_stop()

    def execute(self):
        with self.loop_executor:
            try:
                if self.args['cloud']:
                    self.execute_webhook()
                else:
                    self.execute_pooling()
            finally:
                self.loop.close()

    def execute_pooling(self):
        self.loop.run_until_complete(self.bot.delete_webhook(drop_pending_updates=False))
        executor.start_polling(self.dp)

    def execute_webhook(self):
        parts = urlparse(self.args['webhook_path'])
        self.executor.set_webhook(parts.path)
        self.loop.add_signal_handler(signal.SIGINT, _raise_graceful_exit)
        web_app: web.Application = self.executor.web_app

        web_app.on_startup.append(self.register_webhook)

        web_app.add_routes(self.web_routes() + [
            web.get('/webhook', self.register_webhook),
            web.get('/ping', ok),
        ])

        web.run_app(app=web_app, port=self.args['port'], handle_signals=False, loop=self.loop)

    async def register_webhook(self, *args, **kwargs):
        webhook_url = self.args['webhook_path']
        webhook_info = await self.bot.get_webhook_info()
        if webhook_info.url == webhook_url:
            logging.info("Webhook already registered")
            return web.Response(body="registered\n")
        await retry(self.bot.set_webhook, exceptions=(exceptions.TelegramAPIError,), url=webhook_url)
        return web.Response(body="WebHook is registered\n")


def _raise_graceful_exit() -> None:
    from aiohttp.web_runner import GracefulExit
    raise GracefulExit()