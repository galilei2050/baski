"""A page that will not load must fail once, quickly — not three times, slowly.

The person waiting is in a chat. Measured over 663 recorded runs: 93 fetches failed and exactly one
of those urls ever loaded anywhere else, so a retry after a timeout buys nothing measurable. It cost
plenty: each of the three attempts ran three waits that each claimed the full timeout, and the worst
recorded failure took 464 seconds — while the slowest SUCCESSFUL fetch in the same corpus took 122.
"""

import asyncio

import pytest
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from baski.clients.playwright_client import PlaywrightClient


class _Page:
    """Stands in for a Playwright page whose navigation always fails the same way."""

    def __init__(self, error: Exception) -> None:
        self.error = error
        self.attempts = 0

    async def goto(self, url: str, **_: object) -> None:
        """Count the attempt, then fail the way the real page would."""
        self.attempts += 1
        raise self.error

    async def wait_for_load_state(self, *_: object, **__: object) -> None:
        """Never reached — goto fails first."""

    async def wait_for_selector(self, *_: object, **__: object) -> None:
        """Never reached — goto fails first."""


async def _goto(page: _Page, timeout: int = 90000) -> None:
    """Drive the client's navigation path against the stand-in page."""
    client = PlaywrightClient(timeout=timeout)
    await client._safe_goto(page, "https://example.test/slow")  # type: ignore[arg-type]  # noqa: SLF001


@pytest.mark.asyncio
async def test_timeout_is_not_retried() -> None:
    """A timeout is deterministic for this url: one attempt, then surface it."""
    page = _Page(PlaywrightTimeoutError("Page.goto: Timeout 90000ms exceeded."))
    with pytest.raises(PlaywrightTimeoutError):
        await _goto(page)
    assert page.attempts == 1


@pytest.mark.asyncio
async def test_dropped_connection_is_retried() -> None:
    """A dropped connection is the one class a second attempt could survive."""
    page = _Page(PlaywrightError("net::ERR_ABORTED at https://example.test/slow"))
    with pytest.raises(PlaywrightError):
        await _goto(page)
    assert page.attempts == 3


@pytest.mark.asyncio
async def test_whole_attempt_shares_one_deadline() -> None:
    """A page that hangs is abandoned at the configured timeout, not at three times it."""

    class _Hanging(_Page):
        async def goto(self, url: str, **_: object) -> None:
            self.attempts += 1
            await asyncio.sleep(10)

    page = _Hanging(PlaywrightError("unused"))
    with pytest.raises(TimeoutError, match="did not load within"):
        await _goto(page, timeout=50)  # 50 ms, so the test costs nothing to run
    assert page.attempts == 1
