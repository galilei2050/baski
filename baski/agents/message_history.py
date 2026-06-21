"""Conversation history: the Protocol the Agent depends on, plus an in-memory implementation."""

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Protocol, Self

from anthropic.types import (
    CacheControlEphemeralParam,
    ContentBlock,
    MessageParam,
    TextBlockParam,
    ToolResultBlockParam,
    Usage,
)
from pydantic import BaseModel

from baski.server.logger import Logger

from .pricing import effective_input_tokens

EPHEMERAL_CACHE = CacheControlEphemeralParam(type="ephemeral")


_UNCACHEABLE_BLOCKS = {"thinking", "redacted_thinking"}  # these param types have no cache_control field


def _block_type(block: object) -> str | None:
    """The Anthropic `type` discriminator, whether the block is an SDK model or a plain dict."""
    if isinstance(block, BaseModel):
        return getattr(block, "type", None)
    return block.get("type") if isinstance(block, dict) else None


def mark_cached(message: MessageParam) -> MessageParam:
    """Copy `message` with a prompt-cache breakpoint on its last cacheable content block.

    Copy-safe (never mutates the input — a mutated stored turn would persist `cache_control`). Skips
    trailing thinking blocks, which carry no `cache_control` field.
    """
    content = message["content"]
    if not isinstance(content, list) or not content:
        return message
    blocks = list(content)
    idx = next((i for i in reversed(range(len(blocks))) if _block_type(blocks[i]) not in _UNCACHEABLE_BLOCKS), -1)
    if idx < 0:
        return message
    block = blocks[idx]
    blocks[idx] = (block.model_dump() if isinstance(block, BaseModel) else dict(block)) | {
        "cache_control": EPHEMERAL_CACHE
    }
    return MessageParam(role=message["role"], content=blocks)


def context_status(last_input_tokens: int, max_tokens: int) -> MessageParam | None:
    """The `[Context: N% used]` footer block (volatile — rides after the cache breakpoint), or None."""
    if not last_input_tokens:
        return None
    pct = int(last_input_tokens / max_tokens * 100)
    text = f"[Context: {pct}% used — {max_tokens - last_input_tokens:,} tokens remaining]"
    return MessageParam(role="user", content=[TextBlockParam(type="text", text=text)])


@dataclass
class Turn:
    """A single agentic turn grouping all messages exchanged in one round."""

    id: int
    messages: list[MessageParam] = field(default_factory=list)


class MessageHistory(Protocol):
    """The transcript interface the Agent drives — it owns no persistence of its own.

    The Agent reads `turns`/`max_tokens`, appends through the context manager + `add_*`, formats
    with `format_for_api`, and trims with `truncate`. `InMemoryMessageHistory` is the default,
    volatile implementation. A caller that needs durability supplies its OWN implementation of this
    Protocol (e.g. Mongo-backed) rather than subclassing the in-memory one and patching it — that is
    what kept in-memory trims (`truncate`, `delete_turns`) from ever reaching the durable store.
    """

    @property
    def turns(self) -> Sequence[Turn]:
        """Committed turns, oldest first — read-only; the implementation owns all mutation.

        Read-only (a `Sequence`, no setter) so callers mutate only through the contract methods
        (`__enter__`/`add_*`/`truncate`/`delete_turns`); also covariant, so an implementation may
        store a `list` of a `Turn` subclass.
        """
        ...

    def __len__(self) -> int:
        """Number of committed turns."""
        ...

    def __enter__(self) -> Self:
        """Open a new turn to collect the messages of one agentic round."""
        ...

    def __exit__(self, *args: object) -> None:
        """Commit the open turn to the transcript if it has any messages."""
        ...

    def add_assistant(self, content_blocks: list[ContentBlock]) -> None:
        """Append the assistant's message (text/tool_use/thinking blocks) to the open turn."""
        ...

    def add_tool_results(self, results: list[ToolResultBlockParam]) -> None:
        """Append the tool_result blocks for this round to the open turn."""
        ...

    def add_user_text(self, text: str) -> None:
        """Append a plain user-text message to the open turn."""
        ...

    def format_for_api(self) -> list[MessageParam]:
        """Render the transcript as the message list for the Anthropic API (cache breakpoint on the last turn)."""
        ...

    def context_status(self) -> MessageParam | None:
        """Volatile context-usage footer, appended AFTER the cache breakpoint (or None if no data yet)."""
        ...

    def truncate(self, usage: Usage) -> None:
        """Drop oldest turns when the latest input-token usage exceeds the budget."""
        ...

    def initial_context_too_large(self, input_tokens: int) -> bool:
        """True when the transcript is empty yet the first request already exceeds half the budget.

        Owns the budget so it isn't exposed: the agent raises on this rather than running a session
        whose system/tool/pinned prefix is so large no history would ever fit.
        """
        ...

    async def delete_turns(self, turn_ids: list[int]) -> int:
        """Remove whole turns by id; async so a durable implementation can persist the removal."""
        ...


class InMemoryMessageHistory(MessageHistory):
    """Ordered conversation history with automatic context-window truncation.

    Volatile — turns live only for the process. Correct as a pure in-memory transcript; durable
    transcripts are separate implementations of `MessageHistory`, never subclasses of this one.
    """

    def __init__(
        self,
        logger: Logger,
        max_tokens: int = 64_000,
        truncate_threshold: float = 0.9,
        truncate_percentage: float = 0.3,
    ) -> None:
        """Initialize with configurable token limits and truncation policy."""
        self._turns: list[Turn] = []
        self.max_tokens = max_tokens
        self.truncate_threshold = truncate_threshold
        self.truncate_percentage = truncate_percentage
        self.logger = logger
        self._next_turn_id: int = 0
        self._current_turn: Turn | None = None
        self._last_input_tokens: int = 0

    @property
    def turns(self) -> Sequence[Turn]:
        """Committed turns, oldest first — read-only; mutation goes through the contract methods."""
        return self._turns

    def __len__(self) -> int:
        """Return the number of recorded turns."""
        return len(self._turns)

    def __enter__(self) -> Self:
        """Begin a new turn, assigning the next sequential ID."""
        self._next_turn_id += 1
        self._current_turn = Turn(id=self._next_turn_id)
        return self

    def __exit__(self, *args: object) -> None:
        """Commit the current turn to history if it contains messages."""
        if self._current_turn and self._current_turn.messages:
            self._turns.append(self._current_turn)
        self._current_turn = None

    @property
    def _turn(self) -> Turn:
        """Return the active turn, raising if used outside the context manager."""
        if self._current_turn is None:
            raise RuntimeError("No active turn; use the history as a context manager first")
        return self._current_turn

    def add_assistant(self, content_blocks: list[ContentBlock]) -> None:
        """Append an assistant message with the given content blocks to the current turn."""
        self._turn.messages.append(
            MessageParam(
                role="assistant",
                content=content_blocks,
            )
        )

    def add_tool_results(self, results: list[ToolResultBlockParam]) -> None:
        """Append a user message containing tool results to the current turn."""
        self._turn.messages.append(
            MessageParam(
                role="user",
                content=results,
            )
        )

    def add_user_text(self, text: str) -> None:
        """Append a plain-text user message to the current turn."""
        self._turn.messages.append(
            MessageParam(
                role="user",
                content=[TextBlockParam(type="text", text=text)],
            )
        )

    def format_for_api(self) -> list[MessageParam]:
        """Return messages ready for API with [Turn N] markers, cache breakpoint on the last turn."""
        result = []
        for turn in self._turns:
            result.append(MessageParam(role="user", content=[TextBlockParam(type="text", text=f"[Turn {turn.id}]")]))
            result.extend(turn.messages)

        if result:
            result[-1] = mark_cached(result[-1])
        return result

    def context_status(self) -> MessageParam | None:
        """The context-usage footer, rendered by the shared helper from this history's counters."""
        return context_status(self._last_input_tokens, self.max_tokens)

    def initial_context_too_large(self, input_tokens: int) -> bool:
        """True when the transcript is empty yet the first request already exceeds half the budget."""
        return not self._turns and input_tokens > self.max_tokens // 2

    async def delete_turns(self, turn_ids: list[int]) -> int:
        """Remove entire turns by ID."""
        ids_to_remove = set(turn_ids)
        original = len(self._turns)
        self._turns = [t for t in self._turns if t.id not in ids_to_remove]
        removed = original - len(self._turns)
        self.logger.info(
            "Messages deleted by agent",
            labels={
                "turnIds": sorted(ids_to_remove),
                "turnsRemoved": removed,
            },
        )
        return removed

    def truncate(self, usage: Usage) -> None:
        """Remove the oldest turns when threshold exceeded."""
        context_tokens = effective_input_tokens(usage)
        self._last_input_tokens = context_tokens
        if context_tokens < int(self.max_tokens * self.truncate_threshold) or not self._turns:
            return

        turns_to_remove = max(int(len(self._turns) * self.truncate_percentage), 1)
        initial_count = len(self._turns)
        self._turns = self._turns[turns_to_remove:]

        self.logger.info(
            "Truncated message history",
            labels={
                "inputTokens": context_tokens,
                "maxTokens": self.max_tokens,
                "threshold": self.max_tokens * self.truncate_threshold,
                "turnsRemoved": initial_count - len(self._turns),
                "turnsBefore": initial_count,
                "turnsAfter": len(self._turns),
            },
        )
