from aiogram import filters, types
from ..storage import UsersStorage
from ...primitives import datetime


__ALL__ = ['User']


class User(filters.Filter):
    users = None

    def __init__(self, inject_user=True, *args, **kwargs):
        self.inject_user = inject_user

    @classmethod
    def setup(cls, users: UsersStorage):
        cls.users = users

    @classmethod
    def validate(cls, full_config):
        return {"inject_user": full_config.pop('inject_user', True)}

    async def check(self, obj: types.Message, *args, **kwargs):
        if not self.inject_user or not self.users:
            return True
        tg_user = None
        if isinstance(obj, types.Message):
            tg_user = obj.from_user
        elif isinstance(obj, types.CallbackQuery):
            tg_user = obj.from_user
        elif isinstance(obj, types.Update):
            tg_user = obj.message.from_user
        if tg_user is None:
            return {"user": None, "users": self.users}

        user = await self.users.get(tg_user.id) if tg_user else None
        if user is None:
            return {"user": user, "users": self.users}

        user.sync_with(tg_user)
        user.last_in_message = datetime.datetime.now()
        self.users.set(user)

        return {"user": user, "users": self.users}
