from tornado.web import RequestHandler

__all__ = ['OkHandler']


class OkHandler(RequestHandler):

    def get(self):
        self.write('OK')
