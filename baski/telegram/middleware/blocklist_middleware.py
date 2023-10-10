from aiogram import types
from aiogram.dispatcher.handler import CancelHandler
from aiogram.dispatcher.middlewares import BaseMiddleware


__all__ = ['BlocklistMiddleware']


class BlocklistMiddleware(BaseMiddleware):

    def __init__(self, blocklist):
        super().__init__()
        self._blocklist = set(blocklist)

    async def on_process_message(self, message: types.Message, data: dict):
        if message.from_user.id in self._blocklist:
            await message.answer("You are blocked")
            raise CancelHandler()
