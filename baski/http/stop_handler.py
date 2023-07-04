import asyncio

from tornado.web import RequestHandler


class StopHandler(RequestHandler):

    def get(self):
        self.write("STOP")
        asyncio.get_event_loop().stop()
