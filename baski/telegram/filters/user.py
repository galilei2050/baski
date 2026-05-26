from datetime import datetime as _dt
from typing import Any, ClassVar

from aiogram import types
from aiogram.filters import BaseFilter

from ..storage import UsersStorage

__all__ = ["User"]


class User(BaseFilter):
    """Looks up the Telegram user in `UsersStorage`, injects `user` and `users` into the handler.

    Bind via `User.setup(users_storage)` once at startup. Always passes the filter.
    """

    users: ClassVar[UsersStorage | None] = None
    inject_user: bool = True

    @classmethod
    def setup(cls, users: UsersStorage) -> None:
        cls.users = users

    async def __call__(self, event: types.Message | types.CallbackQuery, **_: Any) -> bool | dict:
        if not self.inject_user or self.users is None:
            return True
        tg_user = event.from_user
        if tg_user is None:
            return {"user": None, "users": self.users}
        user = await self.users.get(tg_user.id)
        if user is None:
            return {"user": None, "users": self.users}
        user.sync_with(tg_user)
        user.last_in_message = _dt.now()
        self.users.set(user)
        return {"user": user, "users": self.users}
