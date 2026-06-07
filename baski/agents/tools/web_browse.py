"""Tool for browsing websites and extracting page content as markdown."""

from http import HTTPStatus
from typing import Any, ClassVar

from httpx import HTTPStatusError, TimeoutException

from baski.clients.playwright_client import PlaywrightClient

from ..tool import Tool

_HTTP_FORBIDDEN = HTTPStatus.FORBIDDEN.value
_HTTP_NOT_FOUND = HTTPStatus.NOT_FOUND.value


class WebBrowseTool(Tool):
    """Tool for browsing and extracting content from any website."""

    name = "browse_website"
    one_line = "Browse and extract content from any website"
    description = (
        "Fetch and read content from any website URL. Returns the page content as markdown. "
        "Use this to read articles, documentation, company websites, or any web content."
    )
    input_schema: ClassVar[Any] = {
        "type": "object",
        "properties": {
            "url": {"type": "string", "description": "The full URL to browse (e.g., 'https://example.com')"}
        },
        "required": ["url"],
    }

    def __init__(self, playwright_client: PlaywrightClient) -> None:
        """Store the Playwright client for page fetching."""
        self.playwright_client = playwright_client

    async def execute(self, url: str) -> str:  # type: ignore[override]
        """Fetch URL and return page content as markdown."""
        try:
            return await self.playwright_client.fetch_page_markdown(url)
        except HTTPStatusError as e:
            return self._handle_http_error(url=url, e=e)
        except TimeoutException:
            return f"Website timed out. Try again later: {url}"

    def _handle_http_error(self, *, url: str, e: HTTPStatusError) -> str:
        """Convert HTTP status errors to descriptive strings."""
        status = e.response.status_code
        if status == _HTTP_FORBIDDEN:
            return f"Cannot access website (403 Forbidden). Website blocks automated access. URL: {url}"
        if status == _HTTP_NOT_FOUND:
            return f"Website not found (404). URL does not exist: {url}"
        return f"Website returned HTTP {status}: {url}"
