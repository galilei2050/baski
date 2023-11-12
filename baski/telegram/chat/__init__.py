import functools
import typing
import aiogram
import random
from aiogram.utils.exceptions import RetryAfter, RestartingTelegram, NetworkError
from .history import *
from ...pattern import retry


def aiogram_wait_time_function(e, i, min_wait_ms, max_wait_ms):
    if isinstance(e, aiogram.utils.exceptions.RetryAfter):
        return e.timeout * 1000 + 250
    return i * random.randrange(min_wait_ms, max_wait_ms)


async def aiogram_retry(
        do: typing.Callable[[], typing.Awaitable],
        *args,
        exceptions: typing.Iterable = None,
        times=50,
        **kwargs):
    exceptions = exceptions or (RestartingTelegram, NetworkError, RetryAfter)
    do = functools.partial(do, *args, **kwargs)
    try:
        return await retry(do, exceptions, times, service_name="Telegram", wait_time_fn=aiogram_wait_time_function)
    except aiogram.utils.exceptions.MessageNotModified:
        pass
