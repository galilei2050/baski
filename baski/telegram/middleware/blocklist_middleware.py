"""Middleware that drops messages from blocklisted user IDs."""

from collections.abc import Awaitable, Callable
from typing import Any

from aiogram import BaseMiddleware, types

__all__ = ["BlocklistMiddleware"]


class BlocklistMiddleware(BaseMiddleware):
    """Drops messages from blocklisted user IDs after replying "You are blocked".

    Register as outer middleware on the message observer: `dp.message.outer_middleware(...)`.
    """

    def __init__(self, blocklist: set[int] | list[int]) -> None:
        """Store the blocklisted user IDs."""
        super().__init__()
        self._blocklist = set(blocklist)

    async def __call__(
        self,
        handler: Callable[[types.TelegramObject, dict[str, Any]], Awaitable[Any]],  # noqa: ANON002 — aiogram middleware contract
        event: types.TelegramObject,
        data: dict[str, Any],  # noqa: ANON002 — aiogram middleware context dict
    ) -> Any:  # noqa: ANN401 — aiogram middleware/observer forwarding
        """Block or forward the event based on `from_user.id`."""
        if isinstance(event, types.Message) and event.from_user and event.from_user.id in self._blocklist:
            await event.answer("You are blocked")
            return None
        return await handler(event, data)
