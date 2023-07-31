import aiogram
import logging
import inspect

from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters import Text, StateFilter, Filter, BoundFilter


class Receptionist(object):

    def __init__(
            self,
            dp: aiogram.Dispatcher):
        self._dp = dp
        self._error_handler = None
        self._setup_handlers()

    def _setup_handlers(self):
        self._dp.register_message_handler(self._clear_state, Text(startswith="/") & AnyStateFilter(self._dp))

    async def _clear_state(self, *args, state: FSMContext = None, **kwargs):
        '''
        Special handler to clear state, so that command execution overrides the current state.
        Example: we ask the user to enter a token, but he changed his mind and enters another command.
        '''
        try:
            await state.finish()
        except KeyError:
            logging.warning("State does not found in storage")
        StateFilter.ctx_state.set(None)
        await self._dp.process_update(aiogram.types.Update.get_current())

    def _check_callback(self, callback):
        spec = inspect.getfullargspec(callback)
        assert spec.varkw is not None, "Callback must have **kwargs argument"

    def add_message_handler(self, callback, *custom_filters, **kwargs):
        '''
        We add special handler to clear state, so that command execution overrides the current state.
        Example: we ask the user to enter a token, but he changed his mind and enters another command.
        '''
        self._check_callback(callback)
        callback = self._dp.async_task(callback)

        if 'commands' not in kwargs:
            self._dp.register_message_handler(callback,~Text(startswith="/"), *custom_filters, **kwargs)
            return

        self._dp.register_message_handler(callback, *custom_filters, **kwargs)

    def add_button_callback(self, callback, *custom_filters, **kwargs):
        self._check_callback(callback)
        callback = self._dp.async_task(callback)

        self._dp.register_callback_query_handler(callback, *custom_filters, **kwargs)


class AnyStateFilter(Filter):

    def __init__(self, dp):
        self.dp = dp

    def get_target(self, obj):
        if isinstance(obj, aiogram.types.CallbackQuery):
            return getattr(getattr(getattr(obj, 'message', None), 'chat', None), 'id', None), getattr(getattr(obj, 'from_user', None), 'id', None)
        return getattr(getattr(obj, 'chat', None), 'id', None), getattr(getattr(obj, 'from_user', None), 'id', None)

    async def check(self, obj) -> bool:
        chat, user = self.get_target(obj)
        if chat or user:
            state = await self.dp.storage.get_state(chat=chat, user=user)
            if state:
                return {'any_state': True}
        return False
