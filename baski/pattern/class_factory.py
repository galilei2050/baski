"""Base class that auto-registers subclasses for name-based construction."""

from abc import ABCMeta
from typing import Any

__all__ = ["ClassFactory"]


class ClassFactory(metaclass=ABCMeta):
    """Base class whose direct children each own a registry of their descendants."""

    _heirs: dict[str, type]

    @classmethod
    def __init_subclass__(
        cls,
        **kwargs: Any,  # noqa: ANN401 — forwarded to super().__init_subclass__
    ) -> None:
        """Register the subclass under its lowercased qualname."""
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
    def construct(
        cls,
        name: str,
        *args: Any,  # noqa: ANN401 — forwarded to subclass __init__
        **kwargs: Any,  # noqa: ANN401 — forwarded to subclass __init__
    ) -> Any:  # noqa: ANN401 — returns a subclass instance of unknown concrete type
        """Instantiate the registered subclass named name (case-insensitive)."""
        constructor = cls._heirs.get(name.lower())
        if constructor is None:
            raise LookupError(f"Class {name} is not implemented")
        return constructor(*args, **kwargs)
