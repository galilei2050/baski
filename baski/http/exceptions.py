__all__ = [
    'HttpConnectionError', 'HttpBadRequestError', 'HttpException',
    'HttpUnauthorizedError', 'HttpTimeoutError', 'HttpNotFoundError', 'HttpServerError'
]


class HttpException(RuntimeError):

    def __init__(self, code=None, message=None, body=None, *args):
        super(HttpException, self).__init__(*args)
        self.code = code
        self.message = message
        self.body = body

    def __str__(self):
        parts = []
        if self.code:
            parts.append(f"Bad HTTP code {self.code}")
        if self.message:
            parts.append(str(self.message))
        if not parts:
            return "Undefined http exception"
        return '. '.join(parts)


class HttpBadRequestError(HttpException):
    pass


class HttpServerError(HttpException):
    pass


class HttpUnauthorizedError(HttpException):
    pass


class HttpNotFoundError(HttpException):
    pass


class HttpTimeoutError(HttpException):
    pass


class HttpConnectionError(HttpException):
    pass
