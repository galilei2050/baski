from abc import ABCMeta

from tornado.web import HTTPError


class ClassFactory(object, metaclass=ABCMeta):
    _heirs = {}

    @classmethod
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        name = cls.__qualname__.lower()
        if name in cls._heirs:
            raise ValueError(f"{name} is not unique for {cls}")
        cls._heirs[name] = cls

    @classmethod
    def construct(cls, name, *args, **kwargs):
        Constructor = cls._heirs.get(name.lower(), None)
        if Constructor is None:
            raise HTTPError(404, f"Class {name} is not implemented")
        return Constructor(*args, **kwargs)
