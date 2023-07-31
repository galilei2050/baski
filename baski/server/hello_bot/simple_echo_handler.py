from aiogram import types

from ...telegram.receptionist import Receptionist
from .context import Context


def register(receptionist: Receptionist, context: Context):
    receptionist.add_message_handler(echo)


async def echo(message: types.Message, *args, **kwargs):
    await message.answer(f"You said {message.text}")
