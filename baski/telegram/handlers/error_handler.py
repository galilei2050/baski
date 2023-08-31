import logging
import typing
import abc
import asyncio
from aiogram import types, dispatcher
from aiogram.utils import exceptions


__all__ = ['LogErrorHandler', 'SaySorryHandler']


class SaySorryHandler(object):

    def __call__(self, update: types.Update, exception: Exception, *args, **kwargs):
        text = self.get_text_from_exception(exception)
        message = _get_message_from_update(update)
        if message:
            return message.reply(**text)

    def get_text_from_exception(self, exception: Exception):
        return I_AM_SORRY


class LogErrorHandler(metaclass=abc.ABCMeta):

    ignore_exceptions = (
        exceptions.ChatNotFound,
        exceptions.BotBlocked,
        exceptions.UserDeactivated,
        exceptions.InvalidUserId
    )

    warning_exceptions = (
        asyncio.exceptions.CancelledError,
    )

    def __init__(self, ignore_exceptions=(), warning_exceptions=()):
        self.ignore_exceptions = self.ignore_exceptions + ignore_exceptions
        self.warning_exceptions = self.warning_exceptions + warning_exceptions

    async def __call__(
            self,
            message: typing.Union[types.CallbackQuery, types.Message],
            state: dispatcher.FSMContext,
            *args, **kwargs
    ):
        user_id = 'undefined'
        if isinstance(message, types.CallbackQuery):
            user_id = message.message.from_user.id
        elif isinstance(message, types.Message):
            user_id = message.from_user.id

        try:
            await super().__call__(message, state=state, *args, **kwargs)
        except self.ignore_exceptions as e:
            logging.info(f"From {user_id} ignore: {e}")
            raise
        except self.warning_exceptions as e:
            logging.warning(f"From {user_id}: {e}")
            raise
        except Exception as e:
            logging.exception(f"From {user_id} error: {e}")
            raise

    def get_text_from_exception(self, exception: Exception):
        return I_AM_SORRY


def _get_message_from_update(update: types.Update):
    if update.message:
        return update.message
    if update.callback_query:
        return update.callback_query.message
    raise RuntimeError(f"Can't get message from update: {update}")


I_AM_SORRY = {
    "text": "I'm sorry, something is broken inside. Unfortunately, "
            "I can't complete your request. You may try one more time."
}
