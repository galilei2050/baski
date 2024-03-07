import abc
import logging

from tornado.web import Application as WebApplication

from ..http import OkHandler, ThreadHandler
from .async_server import AsyncServer


class TornadoServer(AsyncServer):

    def __init__(self):
        super().__init__()
        self.web_app = None

    @abc.abstractmethod
    def web_handlers(self):
        raise NotImplementedError()

    def init(self, *args, **kwargs):
        super().init(*args, **kwargs)
        if self.config['cloud']:
            from tornado.log import access_log
            access_log.handlers.clear()
            access_log.setLevel(logging.ERROR)

        handlers = self.web_handlers()
        handlers.append(['/ping', OkHandler])
        handlers.append(['/', OkHandler])
        handlers.append(['/threads', ThreadHandler])

        self.web_app = WebApplication(handlers=handlers, compress_response=True)
        self.web_app.listen(self.args['port'], backlog=4096, reuse_port=True)
        logging.info('Listen HTTP at %s', self.args['port'])
