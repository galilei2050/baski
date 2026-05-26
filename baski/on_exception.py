import asyncio
import inspect
import logging
import typing
from functools import wraps

from .primitives.name import fn_name

__all__ = ["do_nothing", "do_nothing_sync", "on_exception"]


# Default logger for cases where no logger is available
_default_logger = logging.getLogger(__name__)


async def do_nothing(exception: None, *args: typing.Any, **kwargs: typing.Any) -> None:
    pass


def do_nothing_sync(exception: None, *args: typing.Any, **kwargs: typing.Any) -> None:
    pass


def _log_handled_exception(
    exc: Exception,
    _logger: logging.Logger,
    name: str,
    args: tuple,
    kwargs: dict,
    warn_exceptions: tuple,
    skip_traceback_exceptions: tuple,
) -> None:
    _logger.info(f"{name} called with {args}, {kwargs}")
    msg = f"{exc} while call {name}"
    if isinstance(exc, warn_exceptions):
        _logger.warning(msg)
    elif isinstance(exc, skip_traceback_exceptions):
        _logger.error(msg)
    else:
        _logger.exception(msg)


async def _invoke_handler(
    do: typing.Callable,
    is_async: bool,
    args: tuple,
    kwargs: dict,
    exception: Exception,
) -> typing.Any:
    if is_async:
        return await do(*args, exception=exception, **kwargs)
    return do(*args, exception=exception, **kwargs)


def on_exception(
    do: typing.Callable = do_nothing,
    exceptions: type[BaseException] | tuple[type[BaseException], ...] = Exception,
    skip_traceback_exceptions: tuple = (),
    warn_exceptions: tuple = (),
    name: str | None = None,
    logger: typing.Any = None,
) -> typing.Callable:
    def wrapper(fn: typing.Callable) -> typing.Callable:
        _name = name or fn_name(fn)
        assert inspect.iscoroutinefunction(fn), "Only async functions supported"  # noqa: S101 — decorator-time invariant; trips at import, never on user input
        is_do_async = inspect.iscoroutinefunction(do)
        _logger = logger or _default_logger

        @wraps(fn)
        async def inner(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
            ret_val = None
            try:
                ret_val = await fn(*args, **kwargs)
            except asyncio.CancelledError:
                _logger.warning(f"Coroutine {_name} was cancelled. Live is different", _name)
            except (SystemExit, KeyboardInterrupt, GeneratorExit):
                raise
            except exceptions as e:
                _log_handled_exception(
                    exc=e,
                    _logger=_logger,
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
            finally:
                if isinstance(ret_val, Exception):
                    raise ret_val
                return ret_val  # noqa: B012 — intentional: handler may have replaced ret_val and we must surface it even when finally runs after a re-raise

        return inner

    return wrapper
