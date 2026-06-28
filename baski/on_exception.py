"""Decorator that intercepts exceptions from async functions and routes them to a handler."""

import asyncio
import inspect
import logging
import typing
from functools import wraps

from .primitives.name import fn_name

__all__ = ["do_nothing", "do_nothing_sync", "on_exception"]


logger = logging.getLogger(__name__)


async def do_nothing(
    exception: None,
    *args: typing.Any,  # noqa: ANN401 — generic no-op handler that swallows any signature
    **kwargs: typing.Any,  # noqa: ANN401 — generic no-op handler that swallows any signature
) -> None:
    """Async no-op handler used as the default for on_exception."""


def do_nothing_sync(
    exception: None,
    *args: typing.Any,  # noqa: ANN401 — generic no-op handler that swallows any signature
    **kwargs: typing.Any,  # noqa: ANN401 — generic no-op handler that swallows any signature
) -> None:
    """Sync no-op handler counterpart to do_nothing."""


def _log_handled_exception(  # noqa: PLR0913 — internal helper; all params load-bearing and called from a single site
    exc: BaseException,
    name: str,
    args: tuple,
    kwargs: dict,  # noqa: ANON002 — wraps arbitrary user function
    warn_exceptions: tuple,
    skip_traceback_exceptions: tuple,
) -> None:
    logger.info(f"{name} called with {args}, {kwargs}")
    msg = f"{exc} while call {name}"
    if isinstance(exc, warn_exceptions):
        logger.warning(msg)
    elif isinstance(exc, skip_traceback_exceptions):
        logger.error(msg)
    else:
        logger.exception(msg)


async def _invoke_handler(  # noqa: PLR0913 — internal helper called from a single site; all params load-bearing
    do: typing.Callable,
    args: tuple,
    kwargs: dict,  # noqa: ANON002 — wraps arbitrary user function
    exception: BaseException,
    *,
    is_async: bool,
) -> typing.Any:  # noqa: ANN401 — return value forwarded from arbitrary user handler
    if is_async:
        return await do(*args, exception=exception, **kwargs)
    return do(*args, exception=exception, **kwargs)


def on_exception(  # noqa: PLR0913 — decorator factory; each option configures distinct behavior, grouping would hurt call-site readability
    do: typing.Callable = do_nothing,
    exceptions: type[BaseException] | tuple[type[BaseException], ...] = Exception,
    skip_traceback_exceptions: tuple = (),
    warn_exceptions: tuple = (),
    name: str | None = None,
) -> typing.Callable:
    """Wrap an async function so listed exceptions are logged and forwarded to do()."""

    def wrapper(fn: typing.Callable) -> typing.Callable:
        _name = name or fn_name(fn)
        if not inspect.iscoroutinefunction(fn):
            raise TypeError(f"on_exception supports async functions only, got {_name}")
        is_do_async = inspect.iscoroutinefunction(do)

        @wraps(fn)
        async def inner(
            *args: typing.Any,  # noqa: ANN401 — wraps arbitrary user function
            **kwargs: typing.Any,  # noqa: ANN401 — wraps arbitrary user function
        ) -> typing.Any:  # noqa: ANN401 — return type mirrors wrapped function
            try:
                ret_val = await fn(*args, **kwargs)
            except asyncio.CancelledError:
                logger.warning("Coroutine %s was cancelled.", _name)
                return None
            except exceptions as e:
                _log_handled_exception(
                    exc=e,
                    name=_name,
                    args=args,
                    kwargs=kwargs,
                    warn_exceptions=warn_exceptions,
                    skip_traceback_exceptions=skip_traceback_exceptions,
                )
                ret_val = await _invoke_handler(
                    do=do,
                    is_async=is_do_async,
                    args=args,
                    kwargs=kwargs,
                    exception=e,
                )
            if isinstance(ret_val, Exception):
                raise ret_val
            return ret_val

        return inner

    return wrapper
