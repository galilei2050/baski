from marshmallow import EXCLUDE
from marshmallow import Schema as BaseSchema

__all__ = ["Schema"]


class Schema(BaseSchema):
    class Meta:
        ordered = True
        unknown = EXCLUDE
