"""Baski default marshmallow schema (ordered, unknown=EXCLUDE)."""

from marshmallow import EXCLUDE
from marshmallow import Schema as BaseSchema

__all__ = ["Schema"]


class Schema(BaseSchema):
    """Default marshmallow Schema with ordered output and unknown-key exclusion."""

    class Meta:
        """Marshmallow meta options."""

        ordered = True
        unknown = EXCLUDE
