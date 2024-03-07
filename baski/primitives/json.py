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


def datetime_hook(doc, add_tz=False):
    for k, v in doc.items():
        if not isinstance(v, str):
            continue
        if v.isnumeric():
            continue
        if len(v) < 8:
            continue
        # Try to parse the string as a date if it is not a number
        try:
            d, tokens = parse(v, fuzzy=False, fuzzy_with_tokens=True)
            for t in tokens:
                if t != 'T' and t.isalnum():
                    raise ValueError
            if add_tz and d.tzinfo is None:
                d = as_utc(d)
            doc[k] = d
        except (ValueError, OverflowError):
            pass
    return doc


def convert_date(o):
    if isinstance(o, datetime):
        if o.tzinfo is None:
            return str(pytz.UTC.localize(o))
        return str(pytz.UTC.normalize(o))
