"""Application config: YAML + Firestore overlay, dot-path access, singleton wrapper."""

import logging
from collections import UserDict
from pathlib import Path
from typing import Any

import yaml
from google.api_core.exceptions import PermissionDenied
from google.cloud import firestore

from ..pattern import Singleton

__all__ = ["AppConfig", "Config"]

logger = logging.getLogger(__name__)


class Config(UserDict):
    """Nested dict with dot-path get/set and lazy missing-key behaviour."""

    def __init__(self, data: dict | None = None, path: str | None = None) -> None:  # noqa: ANON002 — arbitrary nested YAML config
        """Initialize from an optional dict; ``path`` is the dotted breadcrumb for diagnostics."""
        super().__init__()
        self._path = path or ""
        if not data:
            return
        for k, v in data.items():
            if isinstance(v, dict):
                self[k] = Config(v, f"{self._path}.{k}" if self._path else k)
            else:
                self[k] = v

    def __missing__(self, key: str) -> "Config":
        """Return an empty Config for missing keys so chained access does not raise."""
        return Config(path=f"{self._path}.{key}" if self._path else key)

    def __getitem__(self, key: Any) -> Any:  # noqa: ANN401 — UserDict contract accepts any hashable key/value
        """Look up ``key``; supports ``"a.b.c"`` dotted paths in addition to plain keys."""
        if not isinstance(key, str):
            return super().__getitem__(key)
        if "." not in key:
            return super().__getitem__(key)

        value = self
        for part in key.split("."):
            value = value.get(part, {})
        return value

    def __getattr__(self, item: str) -> Any:  # noqa: ANN401 — config values are arbitrary YAML scalars/structures
        """Allow attribute-style access (``cfg.foo``) by delegating to ``__getitem__``."""
        return self.__getitem__(item)

    def __setitem__(self, key: Any, new: Any) -> None:  # noqa: ANN401 — UserDict contract accepts any hashable key/value
        """Set ``key`` to ``new``; supports dotted paths to write into nested Configs."""
        if not isinstance(key, str):
            super().__setitem__(key, new)
            return
        if "." not in key:
            super().__setitem__(key, new)
            return

        value = self
        parts = key.split(".")
        for part in parts[:-1]:
            value = value.get(part, {})
        value[parts[-1]] = new

    def __str__(self) -> str:
        """Return a YAML-ish indented dump for human-readable logging."""
        indent = "" if not self._path else "  " * len(self._path.split("."))
        return "".join(f"\n{indent}{k}: {v}" for k, v in self.items())


class AppConfig(metaclass=Singleton):
    """Process-wide singleton wrapping a Config; loads YAML and Firestore overlay."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:  # noqa: ANN401 — forwards to UserDict-based Config which accepts arbitrary values
        """Initialize the underlying Config; args/kwargs forwarded to Config."""
        self._cfg = Config(*args, **kwargs)
        self.yml_loaded = False
        self.db_loaded = False

    def load_yml(self, file_path: str | Path) -> "AppConfig":
        """Load settings from a YAML file, replacing the current Config in place."""
        file_path = Path(file_path)
        if not file_path.exists():
            return self
        with file_path.open() as config:
            data = yaml.safe_load(config)
            self._cfg = Config(data)
        self.yml_loaded = True
        return self

    def load_db(self, db: firestore.Client) -> bool:
        """Merge Firestore ``config`` collection over the current settings; return True if anything changed."""
        changed = False
        try:
            for doc in db.collection("config").stream():
                old = dict(self.get(doc.id, {}))
                new = doc.to_dict() or {}
                if old != new:
                    changed = True
                self[doc.id] = Config(data={**old, **new}, path=doc.id)
            self.yml_loaded = True
            self.db_loaded = True
        except PermissionDenied:
            logger.exception("Failed to load config from Firestore")
        return changed

    def __getitem__(self, item: str) -> Any:  # noqa: ANN401 — config values are arbitrary YAML scalars/structures
        """Look up ``item`` on the underlying Config (supports dotted paths)."""
        return self._cfg.__getitem__(item)

    def __setitem__(self, key: str, value: Any) -> None:  # noqa: ANN401 — config values are arbitrary YAML scalars/structures
        """Set ``key`` on the underlying Config (supports dotted paths)."""
        self._cfg.__setitem__(key, value)

    def __getattr__(self, key: str) -> Any:  # noqa: ANN401 — config values are arbitrary YAML scalars/structures
        """Attribute-style access; delegates to the underlying Config."""
        return self._cfg.__getattr__(key)

    def __str__(self) -> str:
        """Return the underlying Config's human-readable dump."""
        return str(self._cfg)

    def get(self, key: str, default: Any = None) -> Any:  # noqa: ANN401 — config values are arbitrary YAML scalars/structures
        """Dict-like get with default."""
        return self._cfg.get(key, default)

    def values(self) -> Any:  # noqa: ANN401 — UserDict.values returns a view of arbitrary values
        """Dict-like values view."""
        return self._cfg.values()

    def keys(self) -> Any:  # noqa: ANN401 — UserDict.keys returns a view typed as Any here for parity
        """Dict-like keys view."""
        return self._cfg.keys()

    def update(self, *args: Any, **kwargs: Any) -> None:  # noqa: ANN401 — forwards to UserDict.update which accepts arbitrary values
        """Dict-like update; forwards to the underlying Config."""
        self._cfg.update(*args, **kwargs)
