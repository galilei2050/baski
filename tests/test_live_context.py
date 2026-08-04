"""Where a tool's per-turn content is delivered, and why it matters.

Render order is tools → system → messages, so a system prompt that differs by one byte invalidates
every cached message behind it. Content a tool rewrites while the agent works therefore has to ride
AFTER the cache breakpoint. Measured on a consumer before this split: one core-memory edit mid-run
threw away ~6.7k cached tokens and re-wrote them at the write rate, on 12% of multi-turn runs.
"""

from __future__ import annotations

import pytest

from pydantic import BaseModel

from baski.agents.tool import Tool
from baski.agents.toolset import ToolSet

pytestmark = pytest.mark.asyncio


class _Stable(Tool):
    """A tool whose guidance is a constant — safe inside the cached system block."""

    name = "stable"
    one_line = "stable guidance"
    description = "stable"

    class Input(BaseModel):
        """No arguments."""

    async def execute(self) -> str:
        return ""

    async def system_prompt(self) -> str:
        return "STABLE GUIDANCE"


class _Live(_Stable):
    """A tool that rewrites its own guidance as the agent works — e.g. an editable memory block."""

    name = "live"
    one_line = "live block"
    live_system_prompt = True

    async def system_prompt(self) -> str:
        return "LIVE BLOCK"


@pytest.fixture
def toolset() -> ToolSet:
    ts = ToolSet()
    ts.add(_Stable())
    ts.add(_Live())
    return ts


async def test_live_content_is_kept_out_of_the_system_prompt(toolset: ToolSet) -> None:
    """The whole point: a tool that rewrites itself must not sit in the block the cache keys on."""
    system = await toolset.system_prompt()

    assert "STABLE GUIDANCE" in system
    assert "LIVE BLOCK" not in system


async def test_live_content_is_still_delivered(toolset: ToolSet) -> None:
    """Excluding it from the system prompt must not drop it — that would silently lose the agent's
    core memory rather than merely re-cache it."""
    assert await toolset.live_system_prompt() == "LIVE BLOCK"


async def test_a_toolset_with_nothing_live_produces_no_trailing_block(toolset: ToolSet) -> None:
    """An empty string means the Agent appends no message at all; a blank one would cost tokens and
    push a stray empty turn into every request."""
    only_stable = ToolSet()
    only_stable.add(_Stable())

    assert await only_stable.live_system_prompt() == ""


async def test_the_roster_still_lists_every_tool(toolset: ToolSet) -> None:
    """Routing a tool's guidance elsewhere must not hide the tool itself — the model still has to
    know it can be called."""
    system = await toolset.system_prompt()

    assert "(stable)" in system
    assert "(live)" in system
