import abc
from dataclasses import dataclass, field


@dataclass
class Context:
    database: str = field(default='just example')


class ContextHandler(metaclass=abc.ABCMeta):

    def __init__(self, ctx: Context):
        self.ctx = ctx

    @abc.abstractmethod
    async def __call__(self, *args, **kwargs):
        raise NotImplementedError()
