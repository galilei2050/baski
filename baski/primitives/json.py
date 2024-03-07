import json as true_json
from datetime import datetime
from json import JSONDecodeError
from pathlib import Path

import pytz
from dateutil.parser import parse

from .datetime import as_utc

__all__ = ['load', 'loadf', 'loads', 'dump', 'dumpf', 'dumps', 'JSONDecodeError']


def load(fp):
    return true_json.load(fp, object_hook=datetime_hook)


def loads(text):
    return true_json.loads(text, object_hook=datetime_hook)


def loadf(file_path):
    return loads(Path(file_path).read_text(encoding='utf-8'))


def dump(data, fp):
    return true_json.dump(data, fp, default=convert_date, indent=2, sort_keys=True)


def dumps(data):
    return true_json.dumps(data, default=convert_date, indent=2, sort_keys=True)


def dumpf(data, file_path):
    Path(file_path).write_text(dumps(data))


def convert_date(o):
    if isinstance(o, datetime):
        if o.tzinfo is None:
            return str(pytz.UTC.localize(o))
        return str(pytz.UTC.normalize(o))


date_formats = {
    len('2021-01-01T00:00:00Z'): ['%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%d %H:%M:%SZ'],
    len('2021-01-01T00:00:00'): ['%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S'],
    len('2021-01-01T00:00:00+00:00'): ['%Y-%m-%dT%H:%M:%S%z', '%Y-%m-%d %H:%M:%S%z'],
    len('2021-01-01T00:00:00.000Z'): ['%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%d %H:%M:%S.%fZ'],
    len('2021-01-01T00:00:00.000'): ['%Y-%m-%dT%H:%M:%S.%f'],
    len('2021-01-01T00:00:00.000+00:00'): ['%Y-%m-%dT%H:%M:%S.%f%z'],
    len('2021-01-01T00:00:00.000000Z'): ['%Y-%m-%dT%H:%M:%S.%fZ'],
    len('2021-01-01T00:00:00.000000'): ['%Y-%m-%dT%H:%M:%S.%f'],
    len('2021-01-01T00:00:00.000000+00:00'): ['%Y-%m-%dT%H:%M:%S.%f%z'],
    len('2021-01-01'): ['%Y-%m-%d', '%d.%m.%Y', '%d/%m/%Y'],
}


def datetime_hook(doc, add_tz=False):
    for k, v in doc.items():
        if not isinstance(v, str):
            continue
        possible_formats = date_formats.get(len(v), [])
        for fmt in possible_formats:
            try:
                d = datetime.strptime(v, fmt)
                if add_tz and d.tzinfo is None:
                    d = as_utc(d)
                doc[k] = d
                break
            except (ValueError, OverflowError):
                pass
    return doc
