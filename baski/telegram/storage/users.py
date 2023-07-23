import typing

from dataclasses import dataclass, field, asdict

from aiogram import types
from google.cloud import firestore


__all__ = ['TelegramUser', 'UsersStorage']


@dataclass()
class TelegramUser:
    id: str = field(default=None)
    username: str = field(default=None)
    first_name: str = field(default=None)
    last_name: str = field(default=None)

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
            db: firestore.AsyncClient,
            collection='telegram_users',
            factory=TelegramUser,
    ):
        self._db = db.collection(collection)
        self._factory = factory

    async def delete(self, user_id):
        user_ref = self._db.document(str(user_id))
        await user_ref.delete()

    async def get(self, user_id) -> typing.Optional[TelegramUser]:
        user_ref = self._db.document(str(user_id))
        user_doc = await user_ref.get()
        if not user_doc.exists:
            return self._factory(id=user_id)
        return self._factory(**user_doc.to_dict())

    async def set(self, user: TelegramUser):
        user_ref = self._db.document(str(user.id))
        await user_ref.set(asdict(user), merge=True)
