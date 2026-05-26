"""Error-handling helpers for aiogram dispatchers."""

import asyncio
from typing import Any, ClassVar

from aiogram import types
from aiogram.exceptions import TelegramForbiddenError, TelegramUnauthorizedError

from ...server.logger import LocalLogger, Logger

__all__ = ["I_AM_SORRY", "LogErrorHandler", "SaySorryHandler"]


I_AM_SORRY = {
    "text": (
        "I'm sorry, something is broken inside. Unfortunately, "
        "I can't complete your request. You may try one more time."
    )
}


class SaySorryHandler:
    """Replies with an apology message. Register via `dp.errors.register(SaySorryHandler())`."""

    def __init__(self, logger: Logger | None = None) -> None:
        """Store logger; defaults to `LocalLogger`."""
        self._logger: Logger = logger or LocalLogger()

    async def __call__(
        self,
        event: types.ErrorEvent,
        **_: Any,  # noqa: ANN401 — aiogram middleware/observer forwarding
    ) -> Any:  # noqa: ANN401 — aiogram middleware/observer forwarding
        """Log the exception and reply to the originating message."""
        self._logger.warning(f"{event.exception}")
        message = _get_message_from_update(event.update)
        if message:
            return await message.reply(**self.get_text_from_exception(event.exception))
        return None

    def get_text_from_exception(self, _exception: BaseException) -> dict:  # noqa: ANON002 — aiogram message.reply kwargs payload, override-customised
        """Return reply payload for a given exception (override to customise)."""
        return I_AM_SORRY


class LogErrorHandler:
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
        logger: Logger | None = None,
    ) -> None:
        """Store exception categories and logger."""
        self.ignore_exceptions = self._DEFAULT_IGNORE + ignore_exceptions
        self.warning_exceptions = self._DEFAULT_WARN + warning_exceptions
        self._logger: Logger = logger or LocalLogger()

    async def __call__(
        self,
        event: types.Message | types.CallbackQuery,
        **kwargs: Any,  # noqa: ANN401 — aiogram middleware/observer forwarding
    ) -> Any:  # noqa: ANN401 — aiogram middleware/observer forwarding
        """Forward to `super().__call__` while logging and classifying exceptions."""
        user_id: int | str = "undefined"
        if event.from_user is not None:
            user_id = event.from_user.id
        try:
            # Cooperative mixin: real superclass provided by the concrete handler (e.g. TypedHandler).
            return await super().__call__(event, **kwargs)  # type: ignore[misc]
        except self.ignore_exceptions as e:
            self._logger.info(f"From {user_id} ignore: {e}")
        except self.warning_exceptions as e:
            self._logger.warning(f"From {user_id}: {e}")
        except Exception:
            self._logger.exception(f"From {user_id} error")
            raise
        return None

    def get_text_from_exception(self, _exception: BaseException) -> dict:  # noqa: ANON002 — aiogram message.reply kwargs payload, override-customised
        """Return reply payload for a given exception (override to customise)."""
        return I_AM_SORRY


def _get_message_from_update(update: types.Update) -> types.Message | None:
    msg = update.message or update.edited_message or update.channel_post or update.edited_channel_post
    if msg:
        return msg
    if update.callback_query and isinstance(update.callback_query.message, types.Message):
        return update.callback_query.message
    return None
