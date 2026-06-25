"""Baski — shared foundational library: HTTP/Telegram server templates, logger, and reusable primitives."""

from .concurrent import map_async
from .env import get_env, is_cloud, is_debug, is_test, port, project_id, token
from .http import dependencies as dependencies
from .on_exception import do_nothing, do_nothing_sync, on_exception
from .primitives import datetime
from .primitives.dataclass import from_doc
from .primitives.json import JSONDecodeError, dump, dumpf, dumps, load, loadf, loads
from .primitives.name import fn_name, obj_name
from .primitives.unique_id import unique_id

__all__ = [
    "JSONDecodeError",
    "datetime",
    "dependencies",
    "do_nothing",
    "do_nothing_sync",
    "dump",
    "dumpf",
    "dumps",
    "fn_name",
    "from_doc",
    "get_env",
    "is_cloud",
    "is_debug",
    "is_test",
    "load",
    "loadf",
    "loads",
    "map_async",
    "obj_name",
    "on_exception",
    "port",
    "project_id",
    "token",
    "unique_id",
]
