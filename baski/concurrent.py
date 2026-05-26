import asyncio
import functools
import typing
from http import HTTPStatus

from .server.config import AppConfig

__all__ = ["as_async", "as_task", "map_async"]


@functools.lru_cache
def concurrency() -> int:
    return AppConfig().concurrency


async def map_async(
    array: list[typing.Any],
    async_fn: typing.Callable,
    *args: typing.Any,
    timeout: float = 60,  # noqa: ASYNC109 — interface contract; wait timeout is internal, not a deadline forwarded to async_fn
    **kwargs: typing.Any,
) -> list:
    backlog = list(array)
    results = []
    while backlog:
        tasks = [asyncio.create_task(async_fn(item, *args, **kwargs)) for item in backlog[: concurrency()]]
        backlog = backlog[concurrency() :]

        done, pending = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED, timeout=timeout)
        if pending:
            raise RuntimeError(HTTPStatus.INTERNAL_SERVER_ERROR, "Map async timeout")

        results.extend([p.result() for p in done if p.result() is not None])
    return results


async def as_async(f: typing.Callable, *args: typing.Any, **kwargs: typing.Any) -> typing.Any:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, functools.partial(f, *args, **kwargs))


def as_task(coro: typing.Coroutine) -> asyncio.Task:
    return asyncio.get_event_loop().create_task(coro)
