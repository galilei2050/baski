"""Transport-agnostic agent step events.

The agent loop emits these as it runs; a listener (callback) renders them. The
agent knows nothing about who is listening — Telegram, a CLI, a test, a trace.
Keep this module free of any transport concept (no chat id, no aiogram, no HTTP).
"""

from collections.abc import Awaitable, Callable
from enum import StrEnum

from pydantic import BaseModel


class EventType(StrEnum):
    """Tag for the agent step events."""

    TURN_STARTED = "turn_started"
    THINKING = "thinking"
    TEXT_DELTA = "text_delta"
    MESSAGE = "message"
    TOOL_STARTED = "tool_started"
    TOOL_FINISHED = "tool_finished"
    JUDGED = "judged"
    COMPLETED = "completed"


class TurnStarted(BaseModel):
    """A new agentic turn has begun (one model call + its tool calls)."""

    type: EventType = EventType.TURN_STARTED
    turn: int


class Thinking(BaseModel):
    """The model produced extended-thinking text this turn (may be long)."""

    type: EventType = EventType.THINKING
    text: str


class TextDelta(BaseModel):
    """A chunk of assistant text as it streams from the model (token-level, before the turn ends)."""

    type: EventType = EventType.TEXT_DELTA
    text: str


class Message(BaseModel):
    """The model produced user-facing text mid-run (narration before its tool calls)."""

    type: EventType = EventType.MESSAGE
    text: str


class ToolStarted(BaseModel):
    """A tool is about to execute."""

    type: EventType = EventType.TOOL_STARTED
    name: str
    tool_input: dict[str, object]


class ToolFinished(BaseModel):
    """A tool finished executing."""

    type: EventType = EventType.TOOL_FINISHED
    name: str
    ok: bool
    duration_ms: int


class Judged(BaseModel):
    """The completeness judge graded the answer at the loop's exit (may trigger a retry)."""

    type: EventType = EventType.JUDGED
    finished: bool
    missing: list[str]
    feedback: str  # the verdict as an actionable instruction (what to do next); "" when finished
    attempt: int


class Completed(BaseModel):
    """The agent finished and produced its final answer (or None)."""

    type: EventType = EventType.COMPLETED
    response: str | None


AgentEvent = TurnStarted | Thinking | TextDelta | Message | ToolStarted | ToolFinished | Judged | Completed

# A listener is any async callable that consumes one event. Compose several by
# wrapping them in one function; the agent takes a single listener for simplicity.
Listener = Callable[[AgentEvent], Awaitable[None]]


async def noop(_event: AgentEvent) -> None:
    """Default listener — drops every event."""
