import functools
import typing
from aiogram.utils.exceptions import RetryAfter, RestartingTelegram, NetworkError
from .history import *
from ...pattern import retry


async def aiogram_retry(
        do: typing.Callable[[], typing.Awaitable],
        *args,
        exceptions: typing.Iterable = None,
        times=50,
        **kwargs):
    exceptions = exceptions or (RestartingTelegram, NetworkError, RetryAfter)
    do = functools.partial(do, *args, **kwargs)
    return await retry(do, exceptions, times)
