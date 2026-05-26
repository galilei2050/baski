from typing import Any

from aiogram import types
from aiogram.fsm.context import FSMContext

__all__ = ["TypedHandler"]


class TypedHandler:
    async def on_message(self, message: types.Message, state: FSMContext, **kwargs: Any) -> Any:
        raise NotImplementedError("message handler is not implemented")

    async def on_callback(self, callback_query: types.CallbackQuery, state: FSMContext, **kwargs: Any) -> Any:
        raise NotImplementedError("callback handler is not implemented")

    async def on_pre_checkout(
        self, pre_checkout_query: types.PreCheckoutQuery, state: FSMContext, **kwargs: Any
    ) -> Any:
        raise NotImplementedError("pre checkout handler is not implemented")

    async def __call__(
        self,
        event: types.Message | types.CallbackQuery | types.PreCheckoutQuery,
        state: FSMContext,
        **kwargs: Any,
    ) -> Any:
        if isinstance(event, types.Message):
            return await self.on_message(event, state=state, **kwargs)
        if isinstance(event, types.CallbackQuery):
            return await self.on_callback(event, state=state, **kwargs)
        if isinstance(event, types.PreCheckoutQuery):
            return await self.on_pre_checkout(event, state=state, **kwargs)
        raise TypeError(f"Unsupported event type: {type(event)}")
