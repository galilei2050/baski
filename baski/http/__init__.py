"""HTTP server template built on FastAPI plus shared dependencies and middleware."""

from . import dependencies as dependencies
from .server import FastAPIServer as FastAPIServer
