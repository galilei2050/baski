"""A long page must arrive readable, and must never hide from the agent what it did not send.

One measured production fetch returned 457,780 characters into an agent whose whole working budget
was 32,000 tokens — and returned it looking exactly like any other complete page. Cutting blind is
not the fix either: neither the tool nor the agent can know whether the part that mattered was in
the part that was kept. So a long page returns a window plus the contents of the WHOLE page.
"""

from typing import cast

import pytest

from baski.agents.tools.web_browse import WebBrowseTool
from baski.clients.playwright_client import PlaywrightClient


class _Page:
    """A Playwright client that returns a page of a fixed size."""

    def __init__(self, text: str) -> None:
        self.text = text
        self.loads = 0

    async def fetch_page_markdown(self, url: str) -> str:
        self.loads += 1
        return self.text


def _tool(text: str, max_chars: int) -> WebBrowseTool:
    return WebBrowseTool(cast("PlaywrightClient", _Page(text)), max_chars=max_chars)


@pytest.mark.asyncio
async def test_a_short_page_is_returned_untouched() -> None:
    assert await _tool("the whole article", max_chars=100).execute("https://x") == "the whole article"


@pytest.mark.asyncio
async def test_a_long_page_returns_one_window_and_says_where_it_stopped() -> None:
    result = await _tool("x" * 5000, max_chars=1000).execute("https://x")

    assert "[Characters 0-1000 of 5000.]" in result
    assert "offset=1000" in result, "the agent must be told how to read on"
    assert len(result) < 1200, "the window must actually bound what enters the context"


@pytest.mark.asyncio
async def test_the_contents_cover_the_whole_page_not_just_the_window() -> None:
    page = "# Intro\n" + "a" * 500 + "\n## Pricing\n" + "b" * 500 + "\n## FAQ\n" + "c" * 500

    result = await _tool(page, max_chars=200).execute("https://x")

    assert "Pricing" in result and "FAQ" in result, "sections past the window must still be visible"
    assert "b" * 500 not in result, "their text is deferred, not sent"


@pytest.mark.asyncio
async def test_an_offset_reads_the_section_the_agent_asked_for() -> None:
    page = "# Intro\n" + "a" * 100 + "\n## Pricing\n" + "the number is 42\n"
    pricing_at = page.index("## Pricing")

    result = await _tool(page, max_chars=100).execute("https://x", offset=pricing_at)

    assert "the number is 42" in result
    assert "[End of page.]" in result, "the last window must say it is the last"


@pytest.mark.asyncio
async def test_reading_on_in_the_same_page_does_not_load_it_again() -> None:
    site = _Page("x" * 5000)
    tool = WebBrowseTool(cast("PlaywrightClient", site), max_chars=1000)

    await tool.execute("https://x")
    await tool.execute("https://x", offset=1000)
    await tool.execute("https://other")

    assert site.loads == 2, "one load for the page being read, one for the new url"


def test_the_window_size_has_no_default() -> None:
    with pytest.raises(TypeError):
        WebBrowseTool(cast("PlaywrightClient", _Page("")))  # type: ignore[call-arg]  # that is the point
