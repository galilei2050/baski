"""Helpers for deriving qualified names of callables and objects."""

__all__ = ["fn_name", "obj_name"]


def fn_name(fn: object) -> str:
    """Return the callable's dotted name including module."""
    parts = [fn.__module__]
    if hasattr(fn, "__qualname__"):
        parts.append(fn.__qualname__)
    return ".".join(parts)


def obj_name(obj: object) -> str:
    """Return the object's dotted class name including module."""
    parts = [obj.__module__]
    if hasattr(obj, "__class__"):
        cls = obj.__class__
        if hasattr(cls, "__name__"):
            parts.append(cls.__name__)
    return ".".join(parts)
