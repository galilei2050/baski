import asyncio
import inspect
import logging
import typing

from .primitives.name import fn_name

__all__ = ['do_nothing', 'do_nothing_sync', 'on_exception']


async def do_nothing(exception: None, *args, **kwargs):
    pass


def do_nothing_sync(exception: None, *args, **kwargs):
    pass


def on_exception(
        do: typing.Callable = do_nothing,
        exceptions=Exception,
        skip_traceback_exceptions=(),
        warn_exceptions=(),
        name=None
):
    def wrapper(fn: typing.Callable):
        _name = name or fn_name(fn)
        assert inspect.iscoroutinefunction(fn), "Only async functions supported"
        is_do_async = inspect.iscoroutinefunction(do)

        async def inner(*args, **kwargs):
            ret_val = None
            try:
                ret_val = await fn(*args, **kwargs)
            except asyncio.CancelledError:
                logging.warning(f"Coroutine {_name} was cancelled. Live is different", _name)
            except (SystemExit, KeyboardInterrupt, GeneratorExit):
                raise
            except exceptions as e:
                logging.info(f'{_name} called with {args}, {kwargs}')
                msg = f'{e} while call {_name}'
                if isinstance(e, warn_exceptions):
                    logging.warning(msg)
                elif isinstance(e, skip_traceback_exceptions):
                    logging.error(msg)
                else:
                    logging.exception(msg)

                if is_do_async:
                    ret_val = await do(*args, exception=e, **kwargs)
                else:
                    ret_val = do(*args, exception=e, **kwargs)
            finally:
                if isinstance(ret_val, Exception):
                    raise ret_val
                return ret_val

        return inner

    return wrapper
