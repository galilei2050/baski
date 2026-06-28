"""Async retry helper with linear-bounded random backoff."""

import asyncio
import logging
import random
import typing

__all__ = ["UnavailableError", "retry"]

logger = logging.getLogger(__name__)


class UnavailableError(Exception):
    """Raised when retry exhausts all attempts."""


def wait_time_function(
    _e: Exception,
    i: int,
    min_wait_ms: int,
    max_wait_ms: int,
) -> int:
    """Default backoff: attempt index times a uniform random jitter in ms."""
    return i * random.randrange(min_wait_ms, max_wait_ms)  # noqa: S311 — backoff jitter, not a security primitive


async def retry(  # noqa: PLR0913 — knob-rich tuning API; grouping into a config object would hurt typical call ergonomics
    do: typing.Callable,
    exceptions: typing.Iterable,
    times: int = 50,
    min_wait_ms: int = 100,
    max_wait_ms: int = 1000,
    service_name: str | None = None,
    wait_time_fn: typing.Callable = wait_time_function,
    **kwargs: typing.Any,  # noqa: ANN401 — forwarded transparently to do()
) -> typing.Any:  # noqa: ANN401 — return value forwarded from arbitrary do()
    """Call do(**kwargs) up to times, sleeping between retries on listed exceptions."""
    exceptions = tuple(exceptions)
    for i in range(1, times + 1):
        try:
            return await do(**kwargs)
        except exceptions as e:
            wait_time = wait_time_fn(e, i, min_wait_ms, max_wait_ms)
            logger.warning(f"Got exception {type(e)}: '{e}'. retry after {wait_time / 1000} seconds")
            await asyncio.sleep(wait_time / 1000)
    raise UnavailableError(f"Service {service_name} is unavailable after {times} retries")
