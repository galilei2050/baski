"""Async helpers: bounded parallel map and sync-to-async adapters."""

import asyncio
import functools
import typing
from http import HTTPStatus

from .server.config import AppConfig

__all__ = ["map_async"]


@functools.lru_cache
def concurrency() -> int:
    """Return the configured max concurrency for map_async."""
    return AppConfig().concurrency


async def map_async(
    array: list[typing.Any],
    async_fn: typing.Callable,
    *args: typing.Any,  # noqa: ANN401 — forwarded transparently to async_fn
    timeout: float = 60,  # noqa: ASYNC109 — interface contract; wait timeout is internal, not a deadline forwarded to async_fn
    **kwargs: typing.Any,  # noqa: ANN401 — forwarded transparently to async_fn
) -> list:
    """Apply async_fn to each item with bounded concurrency, collecting non-None results."""
    backlog = list(array)
    results = []
    while backlog:
        tasks = [asyncio.create_task(async_fn(item, *args, **kwargs)) for item in backlog[: concurrency()]]
        backlog = backlog[concurrency() :]

        _, pending = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED, timeout=timeout)
        if pending:
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            raise RuntimeError(HTTPStatus.INTERNAL_SERVER_ERROR, "Map async timeout")

        # iterate tasks, not the done set: a set has no order and this is a map
        results.extend([r for r in (task.result() for task in tasks) if r is not None])
    return results
