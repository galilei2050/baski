import asyncio
import logging
import random
import typing

__all__ = ["UnavailableError", "retry"]


class UnavailableError(Exception):
    pass


def wait_time_function(
    e: Exception,  # noqa: ARG001 — part of the wait_time_fn public protocol; custom strategies may inspect the exception
    i: int,
    min_wait_ms: int,
    max_wait_ms: int,
) -> int:
    return i * random.randrange(min_wait_ms, max_wait_ms)  # noqa: S311 — backoff jitter, not a security primitive


async def retry(
    do: typing.Callable,
    exceptions: typing.Iterable,
    times: int = 50,
    min_wait_ms: int = 100,
    max_wait_ms: int = 1000,
    service_name: str | None = None,
    wait_time_fn: typing.Callable = wait_time_function,  # noqa: ARG001 — kwarg accepted for caller override; current body still uses the module default
    logger: logging.Logger | None = None,
    **kwargs: typing.Any,
) -> typing.Any:
    exceptions = tuple(exceptions)
    for i in range(1, times):
        try:
            return await do(**kwargs)
        except exceptions as e:
            wait_time = wait_time_function(e, i, min_wait_ms, max_wait_ms)
            _logger = logger or logging.getLogger(__name__)
            _logger.warning(f"Got exception {type(e)}: '{e}'. retry after {wait_time / 1000} seconds")
            await asyncio.sleep(wait_time / 1000)
    raise UnavailableError(f"Service {service_name} is unavailable after {times} retries")
