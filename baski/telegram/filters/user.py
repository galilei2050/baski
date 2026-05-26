"""User-injecting filter backed by `UsersStorage`."""

from typing import Any, ClassVar

from aiogram import types
from aiogram.filters import BaseFilter

from ...primitives import datetime
from ..storage import TelegramUser, UsersStorage

__all__ = ["User"]


class User(BaseFilter):
    """Looks up the Telegram user in `UsersStorage`, injects `user` and `users` into the handler.

    Bind via `User.setup(users_storage)` once at startup. Always passes the filter.
    """

    users: ClassVar[UsersStorage | None] = None
    inject_user: bool = True

    @classmethod
    def setup(cls, users: UsersStorage) -> None:
        """Bind the shared `UsersStorage` used by all filter instances."""
        cls.users = users

    async def __call__(
        self,
        event: types.Message | types.CallbackQuery,
        **_: Any,  # noqa: ANN401 — aiogram middleware/observer forwarding
    ) -> bool | dict:  # noqa: ANON002 — aiogram filter context injected into handler kwargs
        """Resolve `TelegramUser` and inject `user`/`users` into the handler context."""
        if not self.inject_user or self.users is None:
            return True
        tg_user = event.from_user
        if tg_user is None:
            return {"user": None, "users": self.users}
        user = await self.users.get(tg_user.id) or TelegramUser(id=str(tg_user.id))
        user.sync_with(tg_user)
        user.last_in_message = datetime.now()
        self.users.set(user)
        return {"user": user, "users": self.users}
