"""A url that serves a document must come back as text, not as a failure.

Measured over 663 recorded runs: 51 of 93 browse failures were Chromium refusing to render a file,
44 of them `.pdf`, and the hosts were the primary sources — cdtfa.ca.gov, chp.ca.gov, Stanford, BYU.
Every one was a source the agent asked for and never got, which is where it fell back to a
second-hand page or invented the number.

What is tested here is the decision — when to stop treating a url as a page — not pypdf's extraction,
which is the library's own business.
"""

import io

import pytest
from playwright.async_api import Error as PlaywrightError
from pypdf import PdfWriter

from baski.clients.playwright_client import PlaywrightClient, _fetch_document, _pdf_to_text


def _blank_pdf() -> bytes:
    """A real PDF whose page carries no text — what a scan looks like to an extractor."""
    writer = PdfWriter()
    writer.add_blank_page(width=200, height=200)
    buffer = io.BytesIO()
    writer.write(buffer)
    return buffer.getvalue()


def test_a_pdf_with_no_text_says_so_instead_of_returning_nothing() -> None:
    """A scanned PDF is images. An empty string would be filled in by the agent's imagination."""
    with pytest.raises(ValueError, match="no extractable text"):
        _pdf_to_text(_blank_pdf(), "https://example.test/scan.pdf")


@pytest.mark.asyncio
async def test_a_non_document_type_is_refused_rather_than_guessed(monkeypatch: pytest.MonkeyPatch) -> None:
    """An image is not readable; saying so beats handing the agent bytes as if they were prose."""

    class _Response:
        content = b"\x89PNG\r\n"
        headers = {"content-type": "image/png"}
        text = ""

        def raise_for_status(self) -> None:
            """The server answered fine; the TYPE is the problem."""

    class _Client:
        async def __aenter__(self) -> "_Client":
            return self

        async def __aexit__(self, *_: object) -> None:
            return None

        async def get(self, *_: object, **__: object) -> _Response:
            return _Response()

    monkeypatch.setattr("baski.clients.playwright_client.httpx.AsyncClient", lambda **_: _Client())
    with pytest.raises(ValueError, match="not readable"):
        await _fetch_document("https://example.test/x.png")


@pytest.mark.asyncio
async def test_a_download_is_fetched_instead_of_reported_as_a_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """The whole point: `Download is starting` stops being an error and becomes a fetch."""
    fetched: list[str] = []

    async def _document(url: str) -> str:
        fetched.append(url)
        return "[page 1]\nthe rate is 9.375%"

    class _Page:
        async def goto(self, *_: object, **__: object) -> None:
            raise PlaywrightError("Page.goto: Download is starting")

        async def content(self) -> str:
            return ""

        async def close(self) -> None:
            return None

    class _Context:
        async def new_page(self) -> _Page:
            return _Page()

    monkeypatch.setattr("baski.clients.playwright_client._fetch_document", _document)
    client = PlaywrightClient()
    client._context = _Context()  # type: ignore[assignment]  # noqa: SLF001 — no seam to inject a context

    text = await client.fetch_page_markdown("https://cdtfa.ca.gov/rates.pdf")

    assert text == "[page 1]\nthe rate is 9.375%"
    assert fetched == ["https://cdtfa.ca.gov/rates.pdf"]
