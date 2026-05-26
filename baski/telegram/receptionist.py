from __future__ import annotations

from typing import TYPE_CHECKING, Any

from aiogram import Dispatcher, F, Router, types

from ..server.logger import LocalLogger, Logger

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from aiogram.fsm.context import FSMContext

__all__ = ["Receptionist"]


class Receptionist:
    """Thin wrapper over an aiogram v3 `Router`.

    Wires:
    - a "command resets state" outer-middleware on the message observer: any text starting with `/`
      clears the current FSM state before downstream handlers run.
    - non-command vs command split for `add_message_handler`: handlers without an explicit command
      filter only receive non-command messages.

    Call `mount(dp)` once after registering handlers to attach the router to a `Dispatcher`.
    """

    def __init__(self, debug: bool = False, logger: Logger | None = None) -> None:
        self._router = Router()
        self._debug = debug
        self._logger: Logger = logger or LocalLogger()
        self._router.message.outer_middleware(self._clear_state_on_command)

    @property
    def router(self) -> Router:
        return self._router

    def mount(self, dp: Dispatcher) -> None:
        dp.include_router(self._router)

    def add_error_handler(self, callback: Callable[..., Awaitable[Any]], *filters: Any) -> None:
        self._router.errors.register(callback, *filters)

    def add_pre_checkout_handler(self, callback: Callable[..., Awaitable[Any]], *filters: Any) -> None:
        self._router.pre_checkout_query.register(callback, *filters)

    def add_message_handler(
        self,
        callback: Callable[..., Awaitable[Any]],
        *filters: Any,
        is_command: bool = False,
    ) -> None:
        if is_command:
            self._router.message.register(callback, *filters)
        else:
            self._router.message.register(callback, ~F.text.startswith("/"), *filters)

    def add_button_callback(self, callback: Callable[..., Awaitable[Any]], *filters: Any) -> None:
        self._router.callback_query.register(callback, *filters)

    async def _clear_state_on_command(
        self,
        handler: Callable[[types.TelegramObject, dict[str, Any]], Awaitable[Any]],
        event: types.TelegramObject,
        data: dict[str, Any],
    ) -> Any:
        if isinstance(event, types.Message) and event.text and event.text.startswith("/"):
            state: FSMContext | None = data.get("state")
            if state is not None:
                try:
                    await state.clear()
                except KeyError:
                    self._logger.warning("State not found in storage")
        return await handler(event, data)
