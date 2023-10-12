import io
import typing
import tempfile
import pathlib
from google.cloud import storage
from aiogram import types
from aiogram.dispatcher.middlewares import BaseMiddleware
from aiogram.utils.exceptions import TelegramAPIError

from ...pattern import retry
from ...concurrent import as_async
from ... primitives import datetime
from .. import monitoring


__all__ = ['UnprocessedMiddleware']


I_DO_NOT_KNOW = """
I'm so sorry but don't know what to say to that 😔.\n
This should not ever happen. Please shoot a message to the developers' team at @galilei. 
Your help will be greatly appreciated
""".strip()


class UnprocessedMiddleware(BaseMiddleware):

    def __init__(self, storage_client: storage.Client, storage_bucket,  telemetry=None):
        super().__init__()
        self.telemetry: monitoring.MessageTelemetry = telemetry
        self.bucket = storage.Bucket(storage_client, storage_bucket)

    async def on_post_process_message(self, message: types.Message, results, data: dict):
        if results:
            return
        await message.reply(I_DO_NOT_KNOW)
        if self.telemetry:
            self.telemetry.add_message(monitoring.UNKNOWN_MESSAGE_TYPE, message, message.from_user)
        await self._upload_media(message)

    async def _upload_media(self, message: types.Message):
        with tempfile.TemporaryDirectory() as tempdir:
            now = datetime.now()
            if message.document:
                await self._upload_content(
                    now=now,
                    message=message,
                    name=await self._download_media(message.document, tempdir),
                    mime_type=message.document.mime_type,
                    object_type='document',
                )

            if message.photo:
                await self._upload_content(
                    now=now,
                    message=message,
                    name=await self._download_media(message.photo[-1], tempdir),
                    mime_type='image/jpeg',
                    object_type='photo',
                )

    async def _upload_content(
            self,
            now: datetime.datetime,
            message: types.Message,
            name: typing.AnyStr,
            mime_type: typing.AnyStr,
            object_type: typing.AnyStr,
    ):
        local_file_path = pathlib.Path(name)
        with io.FileIO(name, 'rb') as read_buffer:
            bucket_path = f"{message.chat.id}/{now:%Y-%m-%d}_{object_type}_{message.message_id}_{local_file_path.name}"
            blob = self.bucket.blob(bucket_path)
            await as_async(blob.upload_from_file, file_obj=read_buffer, content_type=mime_type, num_retries=5)

    async def _download_media(
            self,
            telegram_object: typing.Union[types.Video | types.File | types.Voice],
            tempdir
    ) -> typing.AnyStr:

        buffer: io.FileIO = await retry(
            telegram_object.download,
            exceptions=(TelegramAPIError,),
            destination_dir=tempdir
        )
        buffer.flush()
        return buffer.name
