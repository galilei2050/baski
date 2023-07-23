import typing
from functools import cached_property

from aiogram.dispatcher.storage import BaseStorage
from google.cloud import firestore

__all__ = ['FirebaseStorage']


AIOGRAM_STATE = 'aiogram_state'
AIOGRAM_DATA = 'aiogram_data'
AIOGRAM_BUCKET = 'aiogram_bucket'


class FirebaseStorage(BaseStorage):

    def __init__(self, db: firestore.AsyncClient):
        self.db = db

    @cached_property
    def _state(self):
        return self.db.collection(AIOGRAM_STATE)

    @cached_property
    def _data(self):
        return self.db.collection(AIOGRAM_DATA)

    @cached_property
    def _bucket(self):
        return self.db.collection(AIOGRAM_BUCKET)

    async def set_state(self, *,
                        chat: typing.Union[str, int, None] = None,
                        user: typing.Union[str, int, None] = None,
                        state: typing.Optional[typing.AnyStr] = None):
        doc_ref = await self.get_doc_ref(chat, user, self._state)
        if state is None:
            await doc_ref.delete()
        else:
            await doc_ref.set({'state': self.resolve_state(state)})

    async def get_state(self, *,
                        chat: typing.Union[str, int, None] = None,
                        user: typing.Union[str, int, None] = None,
                        default: typing.Optional[str] = None) -> typing.Optional[str]:
        doc_ref = await self.get_doc_ref(chat, user, self._state)
        doc = await doc_ref.get()
        return doc.get('state') if doc.exists else self.resolve_state(default)

    async def set_data(self, *,
                       chat: typing.Union[str, int, None] = None,
                       user: typing.Union[str, int, None] = None,
                       data: typing.Dict = None):
        doc_ref = await self.get_doc_ref(chat, user, self._data)
        if not data:
            await doc_ref.delete()
        else:
            await doc_ref.set(data)

    async def get_data(self, *,
                       chat: typing.Union[str, int, None] = None,
                       user: typing.Union[str, int, None] = None,
                       default: typing.Optional[typing.Dict] = None) -> typing.Dict:
        doc_ref = await self.get_doc_ref(chat, user, self._data)
        doc = await doc_ref.get()
        return doc.to_dict() if doc.exists else default or {}

    async def update_data(self, *,
                          chat: typing.Union[str, int, None] = None,
                          user: typing.Union[str, int, None] = None,
                          data: typing.Dict = None, **kwargs):
        if not data or not isinstance(data, dict):
            return
        doc_ref = await self.get_doc_ref(chat, user, self._data)
        await doc_ref.set(data, merge=True)

    async def get_bucket(self, *,
                         chat: typing.Union[str, int, None] = None,
                         user: typing.Union[str, int, None] = None,
                         default: typing.Optional[dict] = None) -> typing.Dict:
        doc_ref = await self.get_doc_ref(chat, user, self._bucket)
        doc = await doc_ref.get()
        return doc.to_dict() if doc.exists else default or {}

    async def set_bucket(self, *,
                         chat: typing.Union[str, int, None] = None,
                         user: typing.Union[str, int, None] = None,
                         bucket: typing.Dict = None):
        doc_ref = await self.get_doc_ref(chat, user, self._bucket)
        if not bucket:
            await doc_ref.delete()
        else:
            await doc_ref.set(bucket)

    async def update_bucket(self, *,
                            chat: typing.Union[str, int, None] = None,
                            user: typing.Union[str, int, None] = None,
                            bucket: typing.Dict = None, **kwargs):
        if not bucket or not isinstance(bucket, dict):
            return
        doc_ref = await self.get_doc_ref(chat, user, self._data)
        await doc_ref.set(bucket, merge=True)

    async def get_doc_ref(self,
                          chat: typing.Union[str, int, None],
                          user: typing.Union[str, int, None],
                          collection: firestore.AsyncCollectionReference):
        chat, user = self.check_address(chat=chat, user=user)
        return collection.document(f'{chat}_{user}')

    async def close(self):
        """
        Nothing to close
        :return:
        """
        pass

    async def wait_closed(self):
        return True
