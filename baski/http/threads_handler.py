import asyncio
import traceback
from sys import _current_frames

from .request_handler import RequestHandler

__all__ = ['ThreadHandler']


class ThreadHandler(RequestHandler):

    def get(self):
        loop = asyncio.get_event_loop()
        lines = []
        lines.append("====================================================")
        lines.append("*** TASKS - START ***")

        for task in asyncio.all_tasks(loop):
            coro = task.get_coro()
            stack = task.get_stack()
            lines.append(f"\nTask: {coro}")
            for frame in stack:
                code = frame.f_code
                lines.append(f'File: "{code.co_filename}:{frame.f_lineno}", in {code.co_name}')
                for k, v in frame.f_locals.items():
                    lines.append(f"    {k} = {v}")
        lines.append("\n*** TASKS - END ***")

        lines.append("====================================================")

        lines.append("*** STACKTRACE - START ***")

        for threadId, stack in _current_frames().items():
            lines.append("\n# ThreadID: %s" % threadId)
            for filename, lineno, name, line in traceback.extract_stack(stack):
                lines.append(f'File: "{filename}:{lineno}", in {name}')
                if line:
                    lines.append("  %s" % (line.strip()))

        lines.append("\n*** STACKTRACE - END ***")
        lines.append("====================================================")
        self.write('\n'.join(lines))
        self.set_header("Content-Type", "text/plain; charset=UTF-8")
