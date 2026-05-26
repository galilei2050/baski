import abc
import asyncio
import logging
from typing import Any, ClassVar

from aiogram import types
from aiogram.exceptions import TelegramForbiddenError, TelegramUnauthorizedError

__all__ = ["I_AM_SORRY", "LogErrorHandler", "SaySorryHandler"]


I_AM_SORRY = {
    "text": (
        "I'm sorry, something is broken inside. Unfortunately, "
        "I can't complete your request. You may try one more time."
    )
}


class SaySorryHandler:
    """Replies with an apology message. Register via `dp.errors.register(SaySorryHandler())`."""

    async def __call__(self, event: types.ErrorEvent, **kwargs: Any) -> Any:
        logging.warning(f"{event.exception}")
        message = _get_message_from_update(event.update)
        if message:
            return await message.reply(**self.get_text_from_exception(event.exception))
        return None

    def get_text_from_exception(self, exception: BaseException) -> dict:
        return I_AM_SORRY


class LogErrorHandler(metaclass=abc.ABCMeta):
    """Mixin that wraps a downstream handler with structured exception logging.

    Use with cooperative multiple inheritance — `super().__call__` must reach a handler that
    actually processes the event (typically `TypedHandler`):
        class MyHandler(LogErrorHandler, TypedHandler): ...
    """

    _DEFAULT_IGNORE: ClassVar[tuple[type[BaseException], ...]] = (
        TelegramForbiddenError,
        TelegramUnauthorizedError,
    )
    _DEFAULT_WARN: ClassVar[tuple[type[BaseException], ...]] = (asyncio.CancelledError,)

    def __init__(
        self,
        ignore_exceptions: tuple[type[BaseException], ...] = (),
        warning_exceptions: tuple[type[BaseException], ...] = (),
    ) -> None:
        self.ignore_exceptions = self._DEFAULT_IGNORE + ignore_exceptions
        self.warning_exceptions = self._DEFAULT_WARN + warning_exceptions

    async def __call__(self, event: types.Message | types.CallbackQuery, **kwargs: Any) -> Any:
        user_id: int | str = "undefined"
        if isinstance(event, types.CallbackQuery):
            user_id = event.from_user.id
        elif isinstance(event, types.Message) and event.from_user:
            user_id = event.from_user.id
        try:
            return await super().__call__(event, **kwargs)
        except self.ignore_exceptions as e:
            logging.info(f"From {user_id} ignore: {e}")
        except self.warning_exceptions as e:
            logging.warning(f"From {user_id}: {e}")
        except Exception as e:
            logging.exception(f"From {user_id} error: {e}")
            raise
        return None

    def get_text_from_exception(self, exception: BaseException) -> dict:
        return I_AM_SORRY


def _get_message_from_update(update: types.Update) -> types.Message | None:
    if update.message:
        return update.message
    if update.callback_query and isinstance(update.callback_query.message, types.Message):
        return update.callback_query.message
    return None
