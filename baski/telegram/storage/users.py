import asyncio
from dataclasses import asdict, dataclass, field, fields, is_dataclass
from datetime import datetime

from aiogram import types
from google.cloud import firestore

from ...concurrent import as_task
from ...primitives.dataclass import from_doc

__all__ = ["TelegramUser", "UsersStorage"]


@dataclass
class TelegramUser:
    id: str | None = field(default=None)
    username: str | None = field(default=None)
    first_name: str | None = field(default=None)
    last_name: str | None = field(default=None)
    last_in_message: datetime | None = field(default=None)
    last_out_message: datetime | None = field(default=None)

    def sync_with(self, tg_user: types.User | None = None) -> bool:
        if not tg_user:
            return False
        changed = False
        for f in ["id", "first_name", "last_name", "username"]:
            if getattr(self, f) != getattr(tg_user, f):
                setattr(self, f, getattr(tg_user, f))
                changed = True
        return changed


class UsersStorage:
    def __init__(
        self,
        collection: firestore.AsyncCollectionReference,
        klass: type[TelegramUser] = TelegramUser,
    ) -> None:
        if not is_dataclass(klass):
            raise TypeError("klass must be a dataclass")
        if not issubclass(klass, TelegramUser):
            raise TypeError("klass must be a TelegramUser")
        self._db = collection
        self._klass = klass
        self._tasks: list[asyncio.Task] = []
        self._fields = {f.name for f in fields(klass)}

    async def all(self) -> list[TelegramUser]:
        return [from_doc(self._klass, user) async for user in self._db.stream()]

    async def commit(self) -> None:
        await asyncio.gather(*self._tasks)
        self._tasks = []

    async def delete(self, user_id: str | int) -> None:
        user_ref = self._db.document(str(user_id))
        await user_ref.delete()

    async def get(self, user_id: str | int) -> TelegramUser | None:
        user_ref = self._db.document(str(user_id))
        user_doc = await user_ref.get()
        if not user_doc.exists:
            return None
        data = {k: v for k, v in (user_doc.to_dict() or {}).items() if k in self._fields}
        return self._klass(**data)

    def set(self, user: TelegramUser) -> None:
        user_ref = self._db.document(str(user.id))
        self._tasks.append(as_task(user_ref.set(asdict(user), merge=True)))
        self._tasks = [t for t in self._tasks[:] if not t.done()]
