from aiogram import types
from aiogram.dispatcher import FSMContext

from .context import ContextHandler, Context
from ...telegram.receptionist import Receptionist
from ...telegram.error_handler import ErrorHandler


def register(receptionist: Receptionist, context: Context):
    receptionist.add_message_handler(StartCommandHandler(context), commands=['start'])
    receptionist.add_message_handler(DemoErrorHandler(context), commands=['error'])


class StartCommandHandler(ContextHandler):

    async def __call__(self, message: types.Message, state: FSMContext, **kwargs):
        await message.reply(f"Hello, {message.from_user.full_name}!")


class DemoErrorHandler(ErrorHandler, ContextHandler):

    def on_message(self, message: types.Message, *args, **kwargs):
        raise RuntimeError("I'm broken")

    def on_callback(self, callback_query: types.CallbackQuery, *args, **kwargs):
        pass
