import asyncio
import logging
import random
import typing


async def retry(
        do: typing.Callable,
        exceptions: typing.Iterable,
        times=50,
        min_wait=0.1,
        max_wait=1.0,
        **kwargs
):
    exceptions = tuple(exceptions)
    for i in range(times):
        try:
            return await do(**kwargs)
        except exceptions as e:
            wait_time = i * random.randrange(min_wait, max_wait)
            logging.warning(f"Got exception {e} and retry after {wait_time} seconds")
            await asyncio.wait(wait_time)
