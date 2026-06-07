"""Conversation history management with context-window truncation."""

from dataclasses import dataclass, field
from typing import Self

from anthropic.types import ContentBlock, MessageParam, TextBlockParam, ToolResultBlockParam, Usage

from baski.server.logger import Logger


@dataclass
class Turn:
    """A single agentic turn grouping all messages exchanged in one round."""

    id: int
    messages: list[MessageParam] = field(default_factory=list)


class MessageHistory:
    """Ordered conversation history with automatic context-window truncation."""

    def __init__(
        self,
        logger: Logger,
        max_tokens: int = 64_000,
        truncate_threshold: float = 0.9,
        truncate_percentage: float = 0.3,
    ) -> None:
        """Initialize with configurable token limits and truncation policy."""
        self.turns: list[Turn] = []
        self.max_tokens = max_tokens
        self.truncate_threshold = truncate_threshold
        self.truncate_percentage = truncate_percentage
        self.logger = logger
        self._next_turn_id: int = 0
        self._current_turn: Turn | None = None
        self._last_input_tokens: int = 0

    def __len__(self) -> int:
        """Return the number of recorded turns."""
        return len(self.turns)

    def __enter__(self) -> Self:
        """Begin a new turn, assigning the next sequential ID."""
        self._next_turn_id += 1
        self._current_turn = Turn(id=self._next_turn_id)
        return self

    def __exit__(self, *args: object) -> None:
        """Commit the current turn to history if it contains messages."""
        if self._current_turn and self._current_turn.messages:
            self.turns.append(self._current_turn)
        self._current_turn = None

    @property
    def _turn(self) -> Turn:
        """Return the active turn, raising if used outside the context manager."""
        if self._current_turn is None:
            raise RuntimeError("No active turn; use MessageHistory as a context manager first")
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
        """Return messages ready for API with [Turn N] markers on user messages."""
        result = []
        for turn in self.turns:
            result.append(MessageParam(role="user", content=[TextBlockParam(type="text", text=f"[Turn {turn.id}]")]))
            result.extend(turn.messages)

        if self._last_input_tokens:
            pct = int(self._last_input_tokens / self.max_tokens * 100)
            result.append(
                MessageParam(
                    role="user",
                    content=[
                        TextBlockParam(
                            type="text",
                            text=(
                                f"[Context: {pct}% used"
                                f" — {self.max_tokens - self._last_input_tokens:,} tokens remaining]"
                            ),
                        )
                    ],
                )
            )

        return result

    def delete_turns(self, turn_ids: list[int]) -> int:
        """Remove entire turns by ID."""
        ids_to_remove = set(turn_ids)
        original = len(self.turns)
        self.turns = [t for t in self.turns if t.id not in ids_to_remove]
        removed = original - len(self.turns)
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
        self._last_input_tokens = usage.input_tokens
        if usage.input_tokens < int(self.max_tokens * self.truncate_threshold) or not self.turns:
            return

        turns_to_remove = max(int(len(self.turns) * self.truncate_percentage), 1)
        initial_count = len(self.turns)
        self.turns = self.turns[turns_to_remove:]

        self.logger.info(
            "Truncated message history",
            labels={
                "inputTokens": usage.input_tokens,
                "maxTokens": self.max_tokens,
                "threshold": self.max_tokens * self.truncate_threshold,
                "turnsRemoved": initial_count - len(self.turns),
                "turnsBefore": initial_count,
                "turnsAfter": len(self.turns),
            },
        )
