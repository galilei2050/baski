from collections.abc import Mapping
from functools import cached_property
from typing import Any

from aiogram.fsm.state import State
from aiogram.fsm.storage.base import BaseStorage, StorageKey
from google.cloud import firestore

__all__ = ["FirebaseStorage"]


AIOGRAM_STATE = "aiogram_state"
AIOGRAM_DATA = "aiogram_data"


class FirebaseStorage(BaseStorage):
    """aiogram v3 FSM storage backed by Firestore.

    State and data are stored in separate collections keyed by `bot_id:chat_id:user_id[:thread_id]`.
    The v2 `bucket` API is gone in v3 — only `state` and `data` are persisted.
    """

    def __init__(self, db: firestore.AsyncClient) -> None:
        self.db = db

    @cached_property
    def _state(self) -> firestore.AsyncCollectionReference:
        return self.db.collection(AIOGRAM_STATE)

    @cached_property
    def _data(self) -> firestore.AsyncCollectionReference:
        return self.db.collection(AIOGRAM_DATA)

    async def set_state(self, key: StorageKey, state: str | State | None = None) -> None:
        doc_ref = self._state.document(_doc_id(key))
        if state is None:
            await doc_ref.delete()
        else:
            await doc_ref.set({"state": state.state if isinstance(state, State) else state})

    async def get_state(self, key: StorageKey) -> str | None:
        doc_ref = self._state.document(_doc_id(key))
        doc = await doc_ref.get()
        return doc.get("state") if doc.exists else None

    async def set_data(self, key: StorageKey, data: Mapping[str, Any]) -> None:
        doc_ref = self._data.document(_doc_id(key))
        await doc_ref.set(dict(data))

    async def get_data(self, key: StorageKey) -> dict[str, Any]:
        doc_ref = self._data.document(_doc_id(key))
        doc = await doc_ref.get()
        return doc.to_dict() or {} if doc.exists else {}

    async def close(self) -> None:
        pass


def _doc_id(key: StorageKey) -> str:
    parts = [str(key.bot_id), str(key.chat_id), str(key.user_id)]
    if key.thread_id is not None:
        parts.append(str(key.thread_id))
    return ":".join(parts)
