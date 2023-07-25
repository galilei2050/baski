import typing
import asyncio

from dataclasses import dataclass, field, asdict, fields, is_dataclass
from datetime import datetime
from aiogram import types
from google.cloud import firestore
from ...concurrent import as_task

__all__ = ['TelegramUser', 'UsersStorage']


@dataclass()
class TelegramUser:
    id: str = field(default=None)
    username: str = field(default=None)
    first_name: str = field(default=None)
    last_name: str = field(default=None)
    last_in_message: datetime = field(default=None)
    last_out_message: datetime = field(default=None)

    def sync_with(self, tg_user: types.User = None):
        if not tg_user:
            return False

        changed = False
        for f in ['id', 'first_name', 'last_name', 'username']:
            if getattr(self, f) != getattr(tg_user, f):
                setattr(self, f, getattr(tg_user, f))
                changed = True
        return changed


class UsersStorage(object):

    def __init__(
            self,
            collection=firestore.CollectionReference,
            klass=TelegramUser,
    ):
        assert is_dataclass(klass), "klass must be a dataclass"
        assert issubclass(klass, TelegramUser), "klass must be a TelegramUser"

        self._db = collection
        self._klass = klass
        self._tasks = []
        self._fields = {f.name for f in fields(klass)}

    async def commit(self):
        await asyncio.gather(*self._tasks)
        self._tasks = []

    async def delete(self, user_id):
        user_ref = self._db.document(str(user_id))
        await user_ref.delete()

    async def get(self, user_id) -> typing.Optional[TelegramUser]:
        user_ref = self._db.document(str(user_id))
        user_doc = await user_ref.get()
        if not user_doc.exists:
            return self._klass(id=user_id)
        data = {k: v for k, v in user_doc.to_dict().items() if k in self._fields}
        return self._klass(**data)

    def set(self, user: TelegramUser):
        user_ref = self._db.document(str(user.id))
        self._tasks.append(as_task(user_ref.set(asdict(user), merge=True)))
        self._tasks = [t for t in self._tasks[:] if not t.done()]
