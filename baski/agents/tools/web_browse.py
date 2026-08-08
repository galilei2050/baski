"""Tool for browsing websites and extracting page content as markdown."""

from http import HTTPStatus

from httpx import HTTPStatusError, TimeoutException
from pydantic import BaseModel, Field

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

    class Input(BaseModel):
        """Arguments for a website fetch."""

        url: str = Field(description="The full URL to browse (e.g., 'https://example.com')")

    def __init__(self, playwright_client: PlaywrightClient, *, max_chars: int) -> None:
        """Store the Playwright client, and the size beyond which a page is cut.

        `max_chars` is required: an unbounded page is not a safe default. Whatever the site returns
        lands whole in the caller's context — one measured fetch was 457,780 characters, several
        times the entire working budget of the agent that asked for it. The number is the caller's
        to choose (it knows its own context budget); having one is not optional.
        """
        self.playwright_client = playwright_client
        self._max_chars = max_chars

    async def execute(self, url: str) -> str:  # type: ignore[override]
        """Fetch URL and return page content as markdown, cut to `max_chars` if it is longer."""
        try:
            return self._clip(await self.playwright_client.fetch_page_markdown(url))
        except HTTPStatusError as e:
            return self._handle_http_error(url=url, e=e)
        except TimeoutException:
            return f"Website timed out. Try again later: {url}"

    def _clip(self, page: str) -> str:
        """Cut an over-long page, and SAY it was cut.

        The marker is the point: a silently truncated page reads exactly like a complete one, so the
        agent answers from the top of an article believing it read all of it. Told the page was cut,
        it can fetch a more specific URL or tell the owner what it did not see.
        """
        if len(page) <= self._max_chars:
            return page
        return f"{page[: self._max_chars]}\n\n[Page cut: showing {self._max_chars} of {len(page)} characters.]"

    def _handle_http_error(self, *, url: str, e: HTTPStatusError) -> str:
        """Convert HTTP status errors to descriptive strings."""
        status = e.response.status_code
        if status == _HTTP_FORBIDDEN:
            return f"Cannot access website (403 Forbidden). Website blocks automated access. URL: {url}"
        if status == _HTTP_NOT_FOUND:
            return f"Website not found (404). URL does not exist: {url}"
        return f"Website returned HTTP {status}: {url}"
