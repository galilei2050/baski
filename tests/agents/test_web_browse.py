"""A fetched page must never be able to blow past the context budget of the agent that asked for it.

One measured production fetch returned 457,780 characters into an agent whose whole working budget
was 32,000 tokens — and returned it looking exactly like any other complete page.
"""

from typing import cast

import pytest

from baski.agents.tools.web_browse import WebBrowseTool
from baski.clients.playwright_client import PlaywrightClient


class _Page:
    """A Playwright client that returns a page of a fixed size."""

    def __init__(self, text: str) -> None:
        self.text = text

    async def fetch_page_markdown(self, url: str) -> str:
        return self.text


def _tool(text: str, max_chars: int) -> WebBrowseTool:
    return WebBrowseTool(cast("PlaywrightClient", _Page(text)), max_chars=max_chars)


@pytest.mark.asyncio
async def test_a_short_page_is_returned_untouched() -> None:
    assert await _tool("the whole article", max_chars=100).execute("https://x") == "the whole article"


@pytest.mark.asyncio
async def test_an_over_long_page_is_cut_and_says_so() -> None:
    result = await _tool("x" * 5000, max_chars=1000).execute("https://x")

    assert result.startswith("x" * 1000)
    assert "[Page cut: showing 1000 of 5000 characters.]" in result
    assert len(result) < 1100, "the cut must actually bound what enters the context"


def test_the_cap_has_no_default() -> None:
    with pytest.raises(TypeError):
        WebBrowseTool(cast("PlaywrightClient", _Page("")))  # type: ignore[call-arg]  # that is the point
