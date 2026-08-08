"""Tool for browsing websites and extracting page content as markdown."""

import gzip
import re
from functools import cached_property
from http import HTTPStatus
from typing import NamedTuple

from httpx import HTTPStatusError, TimeoutException
from pydantic import BaseModel, ConfigDict, Field

from baski.clients.playwright_client import PlaywrightClient

from ..tool import Tool

_HTTP_FORBIDDEN = HTTPStatus.FORBIDDEN.value
_HTTP_NOT_FOUND = HTTPStatus.NOT_FOUND.value
_HEADING = re.compile(r"^#{1,3} +(.+)$", re.MULTILINE)
_MAX_HEADINGS = 40  # a contents list longer than this is a page whose structure isn't helping anyway
_MIN_HEADINGS = 2  # one heading (or none) describes nothing worth spending tokens on


class _Section(NamedTuple):
    """One markdown heading and the span of page it owns, up to the next heading."""

    start: int
    end: int
    title: str


class _Selection(NamedTuple):
    """What asking a page for sections by name yields: their text, and the names it does not have."""

    text: str
    missing: list[str]


class Page(BaseModel):
    """A fetched page and the sections it is made of. Lifecycle: one read of one url.

    The sections are always those of the WHOLE page, even when the reader only gets a window of it —
    the structure is what lets an agent name the part it needs instead of hoping it was in the part
    it got. Everything about a page lives here, so no caller has to carry its text around to ask it
    something.
    """

    model_config = ConfigDict(frozen=True)

    text: str

    @cached_property
    def sections(self) -> list[_Section]:
        """Each markdown heading and the span it owns, up to the next one."""
        found = [(m.start(), m.group(1).strip()) for m in _HEADING.finditer(self.text)][:_MAX_HEADINGS]
        if not found:
            return []
        ends = [start for start, _ in found[1:]] + [len(self.text)]
        return [_Section(start, end, title) for (start, title), end in zip(found, ends, strict=True)]

    def contents(self) -> str:
        """The list the reader picks section names out of; empty when there is no structure to show."""
        if len(self.sections) < _MIN_HEADINGS:
            return ""
        lines = "\n".join(f"  {s.title} ({s.end - s.start} chars)" for s in self.sections)
        return f"Sections of the whole page — ask for any of them by name:\n{lines}"

    def named(self, wanted: list[str]) -> _Selection:
        """The named sections' text in page order, plus the names this page does not have."""
        by_title = {s.title.casefold(): s for s in self.sections}
        found = sorted({by_title[name.casefold()] for name in wanted if name.casefold() in by_title})
        return _Selection(
            text="\n\n".join(self.text[s.start : s.end] for s in found),
            missing=[name for name in wanted if name.casefold() not in by_title],
        )

    def __len__(self) -> int:
        """How long the page is, in characters — what every cut is measured against."""
        return len(self.text)


class WebBrowseTool(Tool):
    """Read any website as markdown, by section. Lifecycle: as long as the agent holding it.

    Stateful on purpose — it holds the pages it has read, gzipped, so asking for another section of
    one costs no second browser load.
    """

    name = "browse_website"
    one_line = "Browse and extract content from any website"
    description = (
        "Fetch and read content from any website URL. Returns the page content as markdown. "
        "Use this to read articles, documentation, company websites, or any web content. "
        "A long page arrives as its opening plus a list of the sections of the WHOLE page — ask for "
        "the ones you need BY NAME in `sections`, several at once, rather than reading on blindly."
    )

    class Input(BaseModel):
        """Arguments for a website fetch."""

        url: str = Field(description="The full URL to browse (e.g., 'https://example.com')")
        sections: list[str] = Field(
            default_factory=list,
            description="Section names from an earlier fetch's list. Ask for every one you need in ONE call.",
        )
        offset: int = Field(
            default=0,
            description="Character to read on from. Only for a long page that listed no sections to name.",
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
        self._pages: dict[str, bytes] = {}  # url → gzipped page (see `_fetch`)

    async def execute(self, url: str, sections: list[str] | None = None, offset: int = 0) -> str:  # type: ignore[override]
        """Fetch URL and return the page as markdown — the named sections, or one window of it."""
        try:
            page = await self._fetch(url)
            return self._named(page, sections) if sections else self._window(page, offset)
        except HTTPStatusError as e:
            return self._handle_http_error(url=url, e=e)
        except TimeoutException:
            return f"Website timed out. Try again later: {url}"

    async def _fetch(self, url: str) -> Page:
        """The page, loading it only if it is not already held.

        Reading a long page takes several calls — its opening, then the sections named from the list
        — and each browser load costs seconds, so holding the page turns the follow-ups into slicing.
        Gzipped, measured 2.9x on 730 production pages: a whole month of one agent's browsing packs
        into 1.9 MB, so nothing is evicted and there is nothing to bound.
        """
        if packed := self._pages.get(url):
            return Page(text=gzip.decompress(packed).decode())
        text = await self.playwright_client.fetch_page_markdown(url)
        self._pages[url] = gzip.compress(text.encode())
        return Page(text=text)

    def _named(self, page: Page, wanted: list[str]) -> str:
        """The named sections, in page order, in one result.

        Naming beats paging on both counts that matter. A name survives a re-fetch that shifts every
        offset, and several non-adjacent sections come back in ONE call — where paging costs a turn
        each, and a turn re-reads the whole conversation prefix.
        """
        found = page.named(wanted)
        absent = f"No section named {', '.join(repr(m) for m in found.missing)}."
        if not found.text:
            return f"{absent}\n\n{page.contents()}"

        parts = [found.text[: self._max_chars]]
        if len(found.text) > self._max_chars:
            parts.append(
                f"[Sections cut at {self._max_chars} of {len(found.text)} characters — ask for fewer at once.]"
            )
        if found.missing:
            parts.append(f"[{absent}]\n{page.contents()}")
        return "\n\n".join(parts)

    def _window(self, page: Page, offset: int) -> str:
        """The opening of a long page (or a slice from `offset`), with the whole page's sections listed.

        A page is never silently cut down to what fits: cutting blind means neither the tool nor the
        agent knows whether the part that mattered was in the part that was kept. So a long page
        returns a WINDOW plus the sections of the ENTIRE page. Nothing is hidden, only deferred — the
        agent reads the structure, sees that "Pricing" exists, and asks for it by name.
        """
        end = min(offset + self._max_chars, len(page))
        if offset == 0 and end == len(page):
            return page.text
        contents = page.contents()
        header = f"[Characters {offset}-{end} of {len(page)}.]"
        more = (
            "[Ask for the sections you need by name."
            + ("" if contents else f" This page lists none — read on with offset={end}.")
            + "]"
            if end < len(page)
            else "[End of page.]"
        )
        return "\n\n".join(part for part in (header, contents, page.text[offset:end], more) if part)

    def _handle_http_error(self, *, url: str, e: HTTPStatusError) -> str:
        """Convert HTTP status errors to descriptive strings."""
        status = e.response.status_code
        if status == _HTTP_FORBIDDEN:
            return f"Cannot access website (403 Forbidden). Website blocks automated access. URL: {url}"
        if status == _HTTP_NOT_FOUND:
            return f"Website not found (404). URL does not exist: {url}"
        return f"Website returned HTTP {status}: {url}"
