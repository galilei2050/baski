import logging
import yaml

from collections import UserDict
from pathlib import Path
from google.api_core.exceptions import PermissionDenied
from google.cloud import firestore

from .pattern import Singleton


class Config(UserDict):

    def __init__(self, data=None, path=None):
        super().__init__()
        self._path = path or ''
        if not data:
            return
        assert isinstance(data, dict)
        for k, v in data.items():
            if isinstance(v, dict):
                self[k] = Config(v, '.'.join([self._path, k]) if self._path else k)
            else:
                self[k] = v

    def __missing__(self, key):
        return Config(path='.'.join([self._path, key]) if self._path else key)

    def __getitem__(self, key):
        if not isinstance(key, str):
            return super().__getitem__(key)
        if '.' not in key:
            return super().__getitem__(key)

        value = self
        for part in key.split('.'):
            value = value.get(part, {})
        return value

    def __getattr__(self, item):
        return self.__getitem__(item)

    def __setitem__(self, key, new):
        if not isinstance(key, str):
            return super().__setitem__(key, new)
        if '.' not in key:
            return super().__setitem__(key, new)

        value = self
        parts = key.split('.')
        for part in parts[:-1]:
            value = value.get(part, {})
        value[parts[-1]] = new

    def __str__(self):
        indent = "" if not self._path else "  " * len(self._path.split('.'))
        return "".join("\n{}{}: {}".format(indent, k, v) for k, v in self.items())


class AppConfig(metaclass=Singleton):

    def __init__(self, *args, **kwargs):
        self._cfg = Config(*args, **kwargs)

    def load_yml(self, file_path):
        file_path = Path(file_path)
        if not file_path.exists():
            return self
        with file_path.open() as config:
            data = yaml.load(config, Loader=yaml.Loader)
            self._cfg = Config(data)
        return self

    def load_db(self, db: firestore.Client):
        try:
            for doc in db.collection('config').stream():
                old = dict(self[doc.id])
                new = doc.to_dict()
                self[doc.id] = Config(data=old | new, path=doc.id)
        except PermissionDenied as e:
            logging.error(f'Failed load config from firestore: {e}')

    def __getitem__(self, item):
        return self._cfg.__getitem__(item)

    def __setitem__(self, key, value):
        self._cfg.__setitem__(key, value)

    def __getattr__(self, key):
        return self._cfg.__getattr__(key)

    def __str__(self):
        return str(self._cfg)

    def get(self, key, default=None):
        return self._cfg.get(key, default)

    def values(self):
        return self._cfg.values()

    def keys(self):
        return self._cfg.keys()

    def update(self, *args, **kwargs):
        self._cfg.update(*args, **kwargs)
