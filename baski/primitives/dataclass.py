"""Helpers for building dataclasses from external sources."""

from dataclasses import fields, is_dataclass

from google.cloud import firestore

__all__ = ["from_doc"]


def from_doc[T](klass: type[T], doc: firestore.DocumentSnapshot) -> T:
    """Instantiate klass from a Firestore document, dropping unknown fields."""
    if not is_dataclass(klass):
        raise TypeError("klass must be a dataclass")
    klass_fields = {f.name for f in fields(klass)}
    data = {k: v for k, v in (doc.to_dict() or {}).items() if k in klass_fields}
    return klass(**data)
