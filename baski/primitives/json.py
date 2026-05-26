import json as true_json
from datetime import datetime
from json import JSONDecodeError
from pathlib import Path
from typing import IO, Any

import pytz

from .datetime import as_utc

__all__ = ["JSONDecodeError", "dump", "dumpf", "dumps", "load", "loadf", "loads"]


def load(fp: IO[str]) -> Any:
    return true_json.load(fp, object_hook=datetime_hook)


def loads(text: str) -> Any:
    return true_json.loads(text, object_hook=datetime_hook)


def loadf(file_path: str | Path) -> Any:
    return loads(Path(file_path).read_text(encoding="utf-8"))


def dump(data: Any, fp: IO[str]) -> None:
    return true_json.dump(data, fp, default=convert_date, indent=2, sort_keys=True)


def dumps(data: Any, indent: int = 2, sort_keys: bool = True) -> str:
    return true_json.dumps(data, default=convert_date, indent=indent, sort_keys=sort_keys)


def dumpf(data: Any, file_path: str | Path) -> None:
    Path(file_path).write_text(dumps(data))


def convert_date(o: Any) -> str | None:
    if isinstance(o, datetime):
        if o.tzinfo is None:
            return pytz.UTC.localize(o).isoformat()
        return pytz.UTC.normalize(o).isoformat()
    return None


date_formats: dict[int, list[str]] = {
    len("2021-01-01T00:00:00Z"): ["%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%SZ"],
    len("2021-01-01T00:00:00"): ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"],
    len("2021-01-01T00:00:00+00:00"): ["%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S%z"],
    len("2021-01-01T00:00:00.000Z"): ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%d %H:%M:%S.%fZ"],
    len("2021-01-01T00:00:00.000"): ["%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d %H:%M:%S.%f"],
    len("2021-01-01T00:00:00.000+00:00"): ["%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%d %H:%M:%S.%f%z"],
    len("2021-01-01T00:00:00.000000Z"): ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%d %H:%M:%S.%fZ"],
    len("2021-01-01T00:00:00.000000"): ["%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d %H:%M:%S.%f"],
    len("2021-01-01T00:00:00.000000+00:00"): ["%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%d %H:%M:%S.%f%z"],
    len("2021-01-01"): ["%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"],
}


def datetime_hook(doc: dict, *, add_tz: bool = False) -> dict:
    for k, v in doc.items():
        if not isinstance(v, str):
            continue
        possible_formats = date_formats.get(len(v), [])
        for fmt in possible_formats:
            try:
                d = datetime.strptime(v, fmt)  # noqa: DTZ007 — naive parse is intentional; tz attached below via as_utc when add_tz is set
                if add_tz and d.tzinfo is None:
                    d = as_utc(d)
                doc[k] = d
                break
            except (ValueError, OverflowError):
                pass
    return doc
