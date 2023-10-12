import typing
from aiogram import types, dispatcher


class TypedHandler(object):

    async def on_message(
            self,
            message: types.Message,
            state: dispatcher.FSMContext,
            *args, **kwargs
    ):
        raise NotImplementedError("message handler is not implemented")

    async def on_callback(
            self,
            callback_query: types.CallbackQuery,
            state: dispatcher.FSMContext,
            *args, **kwargs
    ):
        raise NotImplementedError("callback handler is not implemented")

    async def on_pre_checkout(
            self,
            pre_checkout_query: types.PreCheckoutQuery,
            state: dispatcher.FSMContext,
            *args, **kwargs
    ):
        raise NotImplementedError("pre checkout handler is not implemented")

    async def __call__(
            self,
            message: typing.Union[types.CallbackQuery, types.Message],
            state: dispatcher.FSMContext,
            *args, **kwargs
    ):
        result = None
        if isinstance(message, types.Message):
            result = await self.on_message(message, *args, state=state, **kwargs)
        elif isinstance(message, types.CallbackQuery):
            result = await self.on_callback(message, *args, state=state, **kwargs)
        elif isinstance(message, types.PreCheckoutQuery):
            result = await self.on_pre_checkout(message, *args, state=state, **kwargs)
        else:
            raise TypeError(f"Unsupported message type: {type(message)}")
        return result if result else True
