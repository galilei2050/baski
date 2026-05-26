"""Thread-safe Singleton metaclass."""

import threading
from typing import Any, ClassVar

__all__ = ["Singleton"]


class Singleton(type):
    """Metaclass that returns the same instance per subclass on every construction."""

    _instances: ClassVar[dict] = {}  # noqa: ANON002 — keyed by user class; values are arbitrary user instances
    _lock: ClassVar[threading.Lock] = threading.Lock()

    def __call__(
        cls,
        *args: Any,  # noqa: ANN401 — forwarded to wrapped class __init__
        **kwargs: Any,  # noqa: ANN401 — forwarded to wrapped class __init__
    ) -> Any:  # noqa: ANN401 — return type is the user class instance
        """Return cached instance or create one under lock."""
        if cls in cls._instances:
            return cls._instances[cls]
        with cls._lock:
            if cls not in cls._instances:
                cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]
