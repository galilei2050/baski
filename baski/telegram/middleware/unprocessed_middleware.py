from aiogram import types
from aiogram.dispatcher.middlewares import BaseMiddleware

from .. import monitoring

I_DO_NOT_KNOW = """
I'm so sorry but don't know what to say to that 😔.\n
This should not ever happen. Please shoot a message to the developers' team at @galilei. 
Your help will be greatly appreciated
""".strip()


class UnprocessedMiddleware(BaseMiddleware):

    def __init__(self, telemetry=None):
        super().__init__()
        self.telemetry: monitoring.MessageTelemetry = telemetry

    async def on_post_process_message(self, message: types.Message, results, data: dict):
        if not data:
            await message.reply(I_DO_NOT_KNOW)
            if self.telemetry:
                self.telemetry.add_message(monitoring.UNKNOWN_MESSAGE_TYPE, message, message.from_user)
