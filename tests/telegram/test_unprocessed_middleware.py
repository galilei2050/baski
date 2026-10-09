import datetime
import types as pytypes
from unittest.mock import create_autospec

import pytest
from google.cloud import storage

from baski.telegram.middleware.unprocessed_middleware import UnprocessedMiddleware


@pytest.mark.asyncio
async def test_upload_call_matches_the_gcs_signature(tmp_path):
    middleware = UnprocessedMiddleware.__new__(UnprocessedMiddleware)
    blob = create_autospec(storage.Blob, instance=True)
    middleware.bucket = pytypes.SimpleNamespace(blob=lambda _path: blob)

    photo = tmp_path / "photo.jpg"
    photo.write_bytes(b"jpeg")
    message = pytypes.SimpleNamespace(chat=pytypes.SimpleNamespace(id=42), message_id=7)

    await middleware._upload_content(
        now=datetime.datetime(2026, 9, 21, tzinfo=datetime.UTC),
        message=message,
        name=str(photo),
        mime_type="image/jpeg",
        object_type="photo",
    )

    blob.upload_from_file.assert_called_once()
    assert blob.upload_from_file.call_args.kwargs["content_type"] == "image/jpeg"
