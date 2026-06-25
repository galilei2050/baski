"""Catch-all middleware for messages that no other handler processed."""

import functools
import io
import pathlib
import tempfile
from collections.abc import Awaitable, Callable
from typing import Any

import anyio
from aiogram import Bot, types
from aiogram.exceptions import TelegramAPIError
from google.cloud import storage

from ...pattern import retry
from ...primitives import datetime
from ..telemetry import UNKNOWN_MESSAGE_TYPE, MessageTelemetry

__all__ = ["I_DO_NOT_KNOW", "UnprocessedMiddleware"]


I_DO_NOT_KNOW = (
    "I'm so sorry but don't know what to say to that 😔.\n\n"
    "This should not ever happen. Please shoot a message to the developers' team at @galilei. "
    "Your help will be greatly appreciated"
)


class UnprocessedMiddleware:
    """Catch-all message handler.

    Register it as a handler on the LAST router so it only fires when no other handler matched.
    Replies with apology text, optionally logs telemetry, and uploads any attached media to GCS
    for inspection.

    Usage:
        unprocessed = UnprocessedMiddleware(bot=bot, storage_client=gcs, storage_bucket="my-bucket")
        last_router = Router()
        last_router.message.register(unprocessed)
        dp.include_router(last_router)
    """

    def __init__(
        self,
        bot: Bot,
        storage_client: storage.Client,
        storage_bucket: str,
        telemetry: MessageTelemetry | None = None,
    ) -> None:
        """Store bot, GCS bucket handle, and optional telemetry sink."""
        self._bot = bot
        self.telemetry = telemetry
        self.bucket = storage.Bucket(storage_client, storage_bucket)

    async def __call__(
        self,
        message: types.Message,
        **_: Any,  # noqa: ANN401 — aiogram middleware/observer forwarding
    ) -> None:
        """Reply with apology, record telemetry, and upload any media."""
        await message.reply(I_DO_NOT_KNOW)
        if self.telemetry and message.from_user:
            self.telemetry.add_message(UNKNOWN_MESSAGE_TYPE, message, message.from_user)
        await self._upload_media(message)

    async def _upload_media(self, message: types.Message) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            now = datetime.now()
            if message.document:
                await self._upload_content(
                    now=now,
                    message=message,
                    name=await self._download_media(message.document, tempdir),
                    mime_type=message.document.mime_type or "application/octet-stream",
                    object_type="document",
                )
            if message.photo:
                await self._upload_content(
                    now=now,
                    message=message,
                    name=await self._download_media(message.photo[-1], tempdir),
                    mime_type="image/jpeg",
                    object_type="photo",
                )

    async def _upload_content(  # noqa: PLR0913 — keyword-only fan-out to GCS upload
        self,
        *,
        now: datetime.datetime,
        message: types.Message,
        name: str,
        mime_type: str,
        object_type: str,
    ) -> None:
        local_file_path = pathlib.Path(name)
        with io.FileIO(name, "rb") as read_buffer:
            bucket_path = f"{message.chat.id}/{now:%Y-%m-%d}_{object_type}_{message.message_id}_{local_file_path.name}"
            blob = self.bucket.blob(bucket_path)
            await anyio.to_thread.run_sync(
                functools.partial(blob.upload_from_file, file_obj=read_buffer, content_type=mime_type, num_retries=5)
            )

    async def _download_media(
        self,
        telegram_object: types.PhotoSize | types.Document | types.Video | types.Voice,
        tempdir: str,
    ) -> str:
        destination = pathlib.Path(tempdir) / f"{telegram_object.file_unique_id}"
        await retry(
            self._bot.download,
            exceptions=(TelegramAPIError,),
            file=telegram_object,
            destination=str(destination),
            service_name="Telegram",
        )
        return str(destination)


# Optional: middleware variant that catches a single update flowing through and triggers
# the unprocessed flow if downstream handler returned None. Use only if you cannot put the
# catch-all on the last router (e.g. mixed sync/async handlers).
async def unprocessed_middleware_factory(
    unprocessed: UnprocessedMiddleware,
) -> Callable[[Callable[[types.Message, dict], Awaitable[Any]], types.Message, dict], Awaitable[Any]]:  # noqa: ANON002 — aiogram middleware contract
    async def middleware(
        handler: Callable[[types.Message, dict], Awaitable[Any]],  # noqa: ANON002 — aiogram middleware contract
        event: types.Message,
        data: dict,  # noqa: ANON002 — aiogram middleware context dict
    ) -> Any:  # noqa: ANN401 — aiogram middleware/observer forwarding
        result = await handler(event, data)
        if result is None:
            await unprocessed(event)
        return result

    return middleware
