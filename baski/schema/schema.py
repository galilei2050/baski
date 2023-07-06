from marshmallow import Schema as BaseSchema, EXCLUDE

__all__ = ['Schema']


class Schema(BaseSchema):

    class Meta:
        ordered = True
        unknown = EXCLUDE
