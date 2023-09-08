import functools
import typing
from aiogram.utils.exceptions import TelegramAPIError, NetworkError
from .history import *
from ...pattern import retry


async def aiogram_retry(
        do: typing.Callable[[], typing.Awaitable],
        *args,
        exceptions: typing.Iterable = None,
        times=50,
        min_wait=0.1,
        max_wait=1.0,
        **kwargs):
    exceptions = exceptions or (TelegramAPIError, NetworkError)
    do = functools.partial(do, *args, **kwargs)
    return await retry(do, exceptions, times, min_wait, max_wait)
