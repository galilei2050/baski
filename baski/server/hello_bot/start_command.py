from aiogram import types
from aiogram.dispatcher import FSMContext

from .context import ContextHandler, Context
from baski.telegram import handlers
from ...telegram.receptionist import Receptionist


def register(receptionist: Receptionist, context: Context):
    receptionist.add_message_handler(StartCommandHandler(context), commands=['start'])
    receptionist.add_message_handler(DemoErrorHandler(context), commands=['error'])
    receptionist.add_message_handler(StateHandler(context), commands=['state'])


class DemoErrorHandler(handlers.LogErrorHandler, ContextHandler):

    def __init__(self, context: Context):
        ContextHandler.__init__(self, context)

    def on_message(self, message: types.Message, *args, **kwargs):
        raise RuntimeError("I'm broken")


class StartCommandHandler(ContextHandler):

    async def __call__(self, message: types.Message, state: FSMContext, **kwargs):
        await message.reply(f"Hello, {message.from_user.full_name}!")


class StateHandler(ContextHandler):

    async def __call__(self, message: types.Message, state: FSMContext, **kwargs):
        await state.set_state("some state")
        await message.reply(f"State is set!")
