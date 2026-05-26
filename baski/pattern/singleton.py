import threading
from typing import Any, ClassVar

__all__ = ["Singleton"]


class Singleton(type):
    _instances: ClassVar[dict] = {}
    _lock: ClassVar[threading.Lock] = threading.Lock()

    def __call__(cls, *args: Any, **kwargs: Any) -> Any:
        if cls in cls._instances:
            return cls._instances[cls]
        with cls._lock:
            if cls not in cls._instances:
                cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]
