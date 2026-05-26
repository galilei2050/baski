from abc import ABCMeta
from typing import Any, ClassVar

__all__ = ["ClassFactory"]


class ClassFactory(metaclass=ABCMeta):  # noqa: B024 — base for subclass auto-registration via __init_subclass__; abstract methods would defeat the purpose
    _heirs: ClassVar[dict[str, type]] = {}

    @classmethod
    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

        name = cls.__qualname__.lower()
        if name in cls._heirs:
            raise ValueError(f"{name} is not unique for {cls}")
        cls._heirs[name] = cls

    @classmethod
    def construct(cls, name: str, *args: Any, **kwargs: Any) -> Any:
        constructor = cls._heirs.get(name.lower(), None)
        if constructor is None:
            raise RuntimeError(404, f"Class {name} is not implemented")
        return constructor(*args, **kwargs)
