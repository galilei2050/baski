import abc
import logging

from tornado.web import Application as WebApplication

from .async_server import AsyncServer
from ..http import OkHandler, ThreadHandler


class TornadoServer(AsyncServer):

    def __init__(self):
        super().__init__()
        self.web_app = None

    def init(self, *args, **kwargs):
        super().init(*args, **kwargs)
        handlers = self.web_handlers()
        handlers.append(['/ping', OkHandler])
        handlers.append(['/', OkHandler])
        handlers.append(['/threads', ThreadHandler])

        self.web_app = WebApplication(handlers=handlers, compress_response=True)
        self.web_app.listen(self.args['port'], backlog=4096, reuse_port=True)
        logging.info('Listen HTTP at %s', self.args['port'])

    @abc.abstractmethod
    def web_handlers(self):
        raise NotImplementedError()
