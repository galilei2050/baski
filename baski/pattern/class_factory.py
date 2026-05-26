from abc import ABCMeta
from typing import Any

__all__ = ["ClassFactory"]


class ClassFactory(metaclass=ABCMeta):
    _heirs: dict[str, type]

    @classmethod
    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # Each direct child of ClassFactory owns its own registry so unrelated hierarchies don't collide.
        if ClassFactory in cls.__bases__:
            cls._heirs = {}
            return

        name = cls.__qualname__.lower()
        if name in cls._heirs:
            raise ValueError(f"{name} is not unique for {cls}")
        cls._heirs[name] = cls

    @classmethod
    def construct(cls, name: str, *args: Any, **kwargs: Any) -> Any:
        constructor = cls._heirs.get(name.lower())
        if constructor is None:
            raise LookupError(f"Class {name} is not implemented")
        return constructor(*args, **kwargs)
