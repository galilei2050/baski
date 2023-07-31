import logging
import typing
import abc
from aiogram import types
from aiogram.utils import exceptions


class ErrorHandler(metaclass=abc.ABCMeta):

    ignore_exceptions = (
        exceptions.ChatNotFound,
        exceptions.BotBlocked,
        exceptions.UserDeactivated,
        exceptions.InvalidUserId
    )

    warning_exceptions = (

    )

    @abc.abstractmethod
    def on_message(self, message: types.Message, *args, **kwargs):
        raise NotImplementedError()

    @abc.abstractmethod
    def on_callback(self, callback_query: types.CallbackQuery, *args, **kwargs):
        raise NotImplementedError()

    async def __call__(self, message: typing.Union[types.CallbackQuery, types.Message], *args, **kwargs):
        is_callback = isinstance(message, types.CallbackQuery)
        if is_callback:
            callback_query = message
            message = callback_query.message

        try:
            if is_callback:
                await self.on_callback(callback_query, *args, **kwargs)
            else:
                await self.on_message(message, *args, **kwargs)
        except self.ignore_exceptions as e:
            logging.info(f"{message.from_user.id} is not available: {e}")
        except self.warning_exceptions as e:
            logging.warning(f"From {message.from_user.id}: {e}")
        except Exception as e:
            logging.exception(f"Message from {message.from_user.id} error: {e}")
            await message.reply(**self.get_text_from_exception(e))

    def get_text_from_exception(self, exception: Exception):
        return I_AM_SORRY


I_AM_SORRY = {
    "text": "I'm sorry, something is broken inside. Unfortunately, "
            "I can't complete your request. You may try one more time."
}
