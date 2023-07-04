import asyncio
import functools
import typing
from http import HTTPStatus

from tornado.web import HTTPError

from .config import AppConfig

__all__ = ['as_async', 'map_async']


@functools.lru_cache()
def concurrency():
    return AppConfig().concurrency


async def map_async(array: typing.List[typing.Any], async_fn: typing.Callable, *args, **kwargs) -> typing.List:
    backlog = list(array)
    results = []
    while backlog:
        tasks = [async_fn(item, *args, **kwargs) for item in backlog[:concurrency()]]
        backlog = backlog[concurrency():]

        done, pending = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED, timeout=kwargs.get('timeout', 60))
        if pending:
            raise HTTPError(HTTPStatus.INTERNAL_SERVER_ERROR, "Map async timeout")

        results.extend([p.result() for p in done if p.result() is not None])
    return results


async def as_async(f: typing.Callable, *args, **kwargs):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, functools.partial(f, *args, **kwargs))
