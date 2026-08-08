"""A long page must arrive readable, and must never hide from the agent what it did not send.

One measured production fetch returned 457,780 characters into an agent whose whole working budget
was 32,000 tokens — and returned it looking exactly like any other complete page. Cutting blind is
not the fix either: neither the tool nor the agent can know whether the part that mattered was in
the part that was kept. So a long page returns its opening plus the sections of the WHOLE page, and
the agent asks for the ones it needs by name.
"""

from typing import cast

import pytest

from baski.agents.tools.web_browse import WebBrowseTool
from baski.clients.playwright_client import PlaywrightClient

_PAGE = "# Intro\n" + "a" * 300 + "\n## Pricing\nthe number is 42\n" + "\n## FAQ\n" + "c" * 300


class _Page:
    """A Playwright client that returns a page of a fixed size, and counts its loads."""

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
async def test_a_long_page_returns_its_opening_and_says_where_it_stopped() -> None:
    result = await _tool("x" * 5000, max_chars=1000).execute("https://x")

    assert "[Characters 0-1000 of 5000.]" in result
    assert "offset=1000" in result, "with no sections to name, the agent must be told how to read on"
    assert len(result) < 1200, "the window must actually bound what enters the context"


@pytest.mark.asyncio
async def test_the_section_list_covers_the_whole_page_not_just_the_window() -> None:
    result = await _tool(_PAGE, max_chars=100).execute("https://x")

    assert "Pricing" in result and "FAQ" in result, "sections past the window must still be visible"
    assert "the number is 42" not in result, "their text is deferred, not sent"


@pytest.mark.asyncio
async def test_named_sections_come_back_together_in_one_call() -> None:
    result = await _tool(_PAGE, max_chars=1000).execute("https://x", sections=["FAQ", "Pricing"])

    assert "the number is 42" in result and "c" * 300 in result, "both sections, one call"
    assert "a" * 300 not in result, "and nothing that was not asked for"


@pytest.mark.asyncio
async def test_a_name_that_is_not_there_says_so_and_lists_what_is() -> None:
    result = await _tool(_PAGE, max_chars=1000).execute("https://x", sections=["Refunds"])

    assert "'Refunds'" in result, "the agent must learn its guess missed, not get silence"
    assert "Pricing" in result, "and be shown what it can ask for instead"


@pytest.mark.asyncio
async def test_reading_on_in_the_same_page_does_not_load_it_again() -> None:
    site = _Page(_PAGE)
    tool = WebBrowseTool(cast("PlaywrightClient", site), max_chars=100)

    await tool.execute("https://x")
    await tool.execute("https://x", sections=["Pricing"])

    assert site.loads == 1, "the follow-up read slices the page already held"


def test_the_window_size_has_no_default() -> None:
    with pytest.raises(TypeError):
        WebBrowseTool(cast("PlaywrightClient", _Page("")))  # type: ignore[call-arg]  # that is the point
