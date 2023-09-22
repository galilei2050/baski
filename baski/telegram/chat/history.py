import datetime
import sys

from aiogram import types
from aiogram.dispatcher.storage import FSMContextProxy


__all__ = ['ChatHistory']


USER_ROLE = "user"
AI_ROLE="assistant"


class ChatHistory(object):

    def __init__(
            self,
            data_proxy: FSMContextProxy,
            length: int = 25,
            *args, **kwargs
    ):
        self._data_proxy = data_proxy
        self._length = length
        data_proxy.setdefault('history', [])

    def __bool__(self):
        return bool(self._data_proxy['history'])

    def __getitem__(self, item):
        for message in self._data_proxy['history']:
            if str(message['message_id']) == str(item):
                return message
        raise KeyError(f"Message with id {item} not found")

    def clear(self):
        self._data_proxy['history'] = []

    def from_user(self, message: types.Message):
        self._add_to_history(_message_to_dict(message, USER_ROLE))

    def from_ai(self, message: types.Message):
        self._add_to_history(_message_to_dict(message, AI_ROLE))

    def _add_to_history(self, obj):
        self._data_proxy['history'] = self._data_proxy['history'] + [obj]
        self._data_proxy['history'] = sorted(self._data_proxy['history'][-self._length:], key=lambda x: x['date'])

    def before(self, message_id, n=sys.maxsize, fmt="raw"):
        return _format(
            messages=[msg for msg in self._data_proxy['history']
                        if msg['message_id'] < message_id][-n:],
            fmt=fmt
        )

    def last(self, n, fr=None, fmt="raw"):
        history = self._data_proxy['history']
        if fr and isinstance(fr, datetime.datetime):
            date_from = fr.timestamp()
            history = [msg for msg in history if msg['date'] > date_from]
        return _format(history, fmt)

    def all(self, fmt="raw"):
        return _format(self._data_proxy['history'], fmt)


_message_fields_to_store = {
    'message_id',
    'date',
    'from',
    'chat',
    'text'
}


def _message_to_dict(message: types.Message, role):
    return {
        k: v for k, v in message.to_python().items()
        if k in _message_fields_to_store
    } | {'role': role, 'date_dt': message['date']}


def _format(messages: list, fmt):
    if fmt == 'raw':
        return messages
    if fmt == 'openai':
        return [{"role": msg['role'], "content": msg['text']} for msg in messages]
    raise ValueError(f"Unknown format: {fmt}")
