import typing
from functools import cached_property
from aiogram.dispatcher import storage
from aiogram.contrib.fsm_storage import memory

from ..aiogram_server import TelegramServer
from .context import Context


class HelloBot(TelegramServer):

    @cached_property
    def context(self):
        return Context()

    def register_handlers(self):
        from . import simple_echo_handler, start_command
        start_command.register(self.receptionist, self.context)
        simple_echo_handler.register(self.receptionist, self.context)

    def web_routes(self) -> typing.List:
        return []

    def filters(self) -> typing.List:
        return []

    def middlewares(self) -> typing.List:
        return []

    def fsm_storage(self) -> storage.BaseStorage:
        return memory.MemoryStorage()


if __name__ == '__main__':
    HelloBot().run()
