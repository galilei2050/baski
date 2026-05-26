"""In-memory chat history backed by aiogram FSM storage."""

import datetime as _dt
import sys

from aiogram import types
from aiogram.fsm.context import FSMContext

__all__ = ["ChatHistory"]


USER_ROLE = "user"
AI_ROLE = "assistant"


class ChatHistory:
    """In-memory chat history wrapper around aiogram v3 `FSMContext`.

    v2's `FSMContextProxy` (with `setdefault` and dict-like writes) is gone in v3 — all reads/writes
    are explicit async calls. Construct via `await ChatHistory.create(state)`; `flush()` persists.

    Each mutating method updates an in-memory list; call `flush()` to write back to FSM storage.
    """

    def __init__(self, state: FSMContext, history: list[dict], length: int = 25) -> None:  # noqa: ANON002 — aiogram FSM storage payload (model_dump output)
        """Store FSM context, current history list, and max length."""
        self._state = state
        self._history = history
        self._length = length

    @classmethod
    async def create(cls, state: FSMContext, length: int = 25) -> "ChatHistory":
        """Load existing history from FSM and return a new instance."""
        data = await state.get_data()
        return cls(state=state, history=list(data.get("history", [])), length=length)

    async def flush(self) -> None:
        """Persist the in-memory history back to FSM storage."""
        await self._state.update_data(history=self._history)

    def __bool__(self) -> bool:
        """Return True if the history is non-empty."""
        return bool(self._history)

    def __getitem__(self, item: int | str) -> dict:  # noqa: ANON002 — aiogram message dump payload
        """Return the stored message dict by message_id."""
        for message in self._history:
            if str(message["message_id"]) == str(item):
                return message
        raise KeyError(f"Message with id {item} not found")

    def clear(self) -> None:
        """Drop all stored messages from memory (not yet flushed)."""
        self._history = []

    def from_user(self, message: types.Message) -> None:
        """Append a user-authored message."""
        self._add_to_history(_message_to_dict(message, USER_ROLE))

    def from_ai(self, message: types.Message) -> None:
        """Append an assistant-authored message."""
        self._add_to_history(_message_to_dict(message, AI_ROLE))

    def _add_to_history(self, obj: dict) -> None:  # noqa: ANON002 — aiogram message dump payload
        self._history = sorted([*self._history, obj][-self._length :], key=lambda x: x["date"])

    def before(self, message_id: int, n: int = sys.maxsize, fmt: str = "raw") -> list[dict]:  # noqa: ANON002 — aiogram message dump payloads
        """Return up to `n` messages with id lower than `message_id`."""
        return _format([msg for msg in self._history if msg["message_id"] < message_id][-n:], fmt)

    def last(self, n: int, fr: _dt.datetime | None = None, fmt: str = "raw") -> list[dict]:  # noqa: ANON002 — aiogram message dump payloads
        """Return up to `n` most-recent messages, optionally filtered by date."""
        history = self._history
        if fr is not None:
            date_from = fr.timestamp()
            history = [msg for msg in history if msg["date"] > date_from]
        return _format(history[-n:], fmt)

    def all(self, fmt: str = "raw") -> list[dict]:  # noqa: ANON002 — aiogram message dump payloads
        """Return the complete history in the requested format."""
        return _format(self._history, fmt)


_message_fields_to_store = {"message_id", "date", "from", "chat", "text"}


def _message_to_dict(message: types.Message, role: str) -> dict:  # noqa: ANON002 — aiogram Message.model_dump output
    dumped = message.model_dump(by_alias=True, mode="json")
    return {k: v for k, v in dumped.items() if k in _message_fields_to_store} | {
        "role": role,
        "date_dt": message.date,
    }


def _format(messages: list[dict], fmt: str) -> list[dict]:  # noqa: ANON002 — aiogram message dump payloads
    if fmt == "raw":
        return messages
    if fmt == "openai":
        return [{"role": msg["role"], "content": msg["text"]} for msg in messages]
    raise ValueError(f"Unknown format: {fmt}")
