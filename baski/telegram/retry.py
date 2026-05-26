"""Retry helpers for aiogram Telegram API calls."""

import functools
import random
from collections.abc import Awaitable, Callable, Iterable
from typing import Any

from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError, TelegramRetryAfter

from ..pattern import retry

__all__ = ["aiogram_retry", "aiogram_wait_time_function"]


def aiogram_wait_time_function(e: Exception, i: int, min_wait_ms: int, max_wait_ms: int) -> int:
    """Compute retry backoff respecting Telegram's `retry_after` hint."""
    if isinstance(e, TelegramRetryAfter):
        return e.retry_after * 1000 + 250
    return i * random.randrange(min_wait_ms, max_wait_ms)  # noqa: S311


async def aiogram_retry(
    do: Callable[..., Awaitable[Any]],
    *args: Any,  # noqa: ANN401 — aiogram middleware/observer forwarding
    exceptions: Iterable[type[BaseException]] | None = None,
    times: int = 50,
    **kwargs: Any,  # noqa: ANN401 — aiogram middleware/observer forwarding
) -> Any:  # noqa: ANN401 — aiogram middleware/observer forwarding
    """Run an aiogram coroutine with retries on transient Telegram errors."""
    exceptions = exceptions or (TelegramNetworkError, TelegramRetryAfter)
    bound = functools.partial(do, *args, **kwargs)
    try:
        return await retry(bound, exceptions, times, service_name="Telegram", wait_time_fn=aiogram_wait_time_function)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            return None
        raise
