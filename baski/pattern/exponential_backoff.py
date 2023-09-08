import asyncio
import logging
import random
import typing


async def retry(
        do: typing.Callable,
        exceptions: typing.Iterable,
        times=50,
        min_wait_ms=100,
        max_wait_ms=1000,
        **kwargs
):
    exceptions = tuple(exceptions)
    for i in range(1, times):
        try:
            return await do(**kwargs)
        except exceptions as e:
            wait_time = i * random.randrange(min_wait_ms, max_wait_ms)
            logging.warning(f"Got exception {type(e)}: '{e}'. retry after {wait_time/1000} seconds")
            await asyncio.sleep(wait_time/1000)
