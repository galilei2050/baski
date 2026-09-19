"""Baski default marshmallow schema (unknown=EXCLUDE)."""

from marshmallow import EXCLUDE
from marshmallow import Schema as BaseSchema

__all__ = ["Schema"]


class Schema(BaseSchema):
    """Default marshmallow Schema that drops unknown input keys instead of raising on them."""

    class Meta:
        """Marshmallow meta options."""

        unknown = EXCLUDE
