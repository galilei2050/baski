from aiogram import types
from aiogram.dispatcher.middlewares import BaseMiddleware


I_DO_NOT_KNOW = """
I'm so sorry but don't know what to say to that 😔.\n
This should not ever happen. Please shoot a message to the developers' team at @galilei. 
Your help will be greatly appreciated
""".strip()


class UnprocessedMiddleware(BaseMiddleware):

    async def on_post_process_message(self, message: types.Message, results, data: dict):
        if not data:
            await message.reply(I_DO_NOT_KNOW)
