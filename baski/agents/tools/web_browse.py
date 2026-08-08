"""Tool for browsing websites and extracting page content as markdown."""

import re
from http import HTTPStatus

from httpx import HTTPStatusError, TimeoutException
from pydantic import BaseModel, Field

from baski.clients.playwright_client import PlaywrightClient

from ..tool import Tool

_HTTP_FORBIDDEN = HTTPStatus.FORBIDDEN.value
_HTTP_NOT_FOUND = HTTPStatus.NOT_FOUND.value
_HEADING = re.compile(r"^#{1,3} +(.+)$", re.MULTILINE)
_MAX_HEADINGS = 40  # a contents list longer than this is a page whose structure isn't helping anyway
_MIN_HEADINGS = 2  # one heading (or none) describes nothing worth spending tokens on


def _contents(page: str) -> str:
    """The whole page's markdown headings with the character each one starts at.

    Built over the entire page even when only a window is returned — the structure is what lets the
    agent pick the section it needs instead of hoping it was in the first window.
    """
    found = [(m.start(), m.group(1).strip()) for m in _HEADING.finditer(page)][:_MAX_HEADINGS]
    if len(found) < _MIN_HEADINGS:
        return ""
    return "Contents of the whole page:\n" + "\n".join(f"  {at} · {title}" for at, title in found)


class WebBrowseTool(Tool):
    """Read any website as markdown, a window at a time. Lifecycle: as long as the agent holding it.

    Stateful on purpose — it holds the page behind the last fetch so reading on in it costs no
    second browser load.
    """

    name = "browse_website"
    one_line = "Browse and extract content from any website"
    description = (
        "Fetch and read content from any website URL. Returns the page content as markdown. "
        "Use this to read articles, documentation, company websites, or any web content. "
        "A long page arrives one window at a time, preceded by the contents of the WHOLE page with "
        "each section's offset — pass that offset back as `offset` to read the section you need."
    )

    class Input(BaseModel):
        """Arguments for a website fetch."""

        url: str = Field(description="The full URL to browse (e.g., 'https://example.com')")
        offset: int = Field(
            default=0,
            description="Character to start reading at. Take it from the contents list of an earlier fetch.",
        )

    def __init__(self, playwright_client: PlaywrightClient, *, max_chars: int) -> None:
        """Store the Playwright client, and how much of a page one read returns.

        `max_chars` is required: an unbounded page is not a safe default. Whatever the site returns
        lands whole in the caller's context — one measured fetch was 457,780 characters, several
        times the entire working budget of the agent that asked for it. The number is the caller's
        to choose (it knows its own context budget); having one is not optional.
        """
        self.playwright_client = playwright_client
        self._max_chars = max_chars
        self._last: tuple[str, str] | None = None  # the page behind the last fetch (see `_fetch`)

    async def execute(self, url: str, offset: int = 0) -> str:  # type: ignore[override]
        """Fetch URL and return the page as markdown — one window of it if it is long."""
        try:
            return self._window(await self._fetch(url), offset)
        except HTTPStatusError as e:
            return self._handle_http_error(url=url, e=e)
        except TimeoutException:
            return f"Website timed out. Try again later: {url}"

    async def _fetch(self, url: str) -> str:
        """The page, loading it unless it is the one just read.

        Reading a long page takes several calls at different offsets, and each browser load costs
        seconds; holding the page behind the last fetch turns the follow-ups into slicing. One entry,
        not a cache: the access pattern is "read on in the page I am in", and a growing map of every
        page a long conversation ever opened would hold megabytes for a hit rate near zero.
        """
        if self._last is not None and self._last[0] == url:
            return self._last[1]
        page = await self.playwright_client.fetch_page_markdown(url)
        self._last = (url, page)
        return page

    def _window(self, page: str, offset: int) -> str:
        """One readable slice of a long page, with the whole page's contents above it.

        A page is never silently cut down to what fits: cutting blind means neither the tool nor the
        agent knows whether the part that mattered was in the part that was kept. So what a long page
        returns is a WINDOW plus the table of contents of the ENTIRE page — every section and the
        offset it starts at. Nothing is hidden, only deferred: the agent reads the structure, sees
        that "Pricing" starts at 45 000, and asks for that offset instead of guessing from the lede.
        """
        end = min(offset + self._max_chars, len(page))
        if offset == 0 and end == len(page):
            return page
        header = f"[Characters {offset}-{end} of {len(page)}.]"
        contents = _contents(page)
        more = (
            f"[Cut at {end}. Read on with offset={end}, or jump to a section's offset above.]"
            if end < len(page)
            else "[End of page.]"
        )
        return "\n\n".join(part for part in (header, contents, page[offset:end], more) if part)

    def _handle_http_error(self, *, url: str, e: HTTPStatusError) -> str:
        """Convert HTTP status errors to descriptive strings."""
        status = e.response.status_code
        if status == _HTTP_FORBIDDEN:
            return f"Cannot access website (403 Forbidden). Website blocks automated access. URL: {url}"
        if status == _HTTP_NOT_FOUND:
            return f"Website not found (404). URL does not exist: {url}"
        return f"Website returned HTTP {status}: {url}"
