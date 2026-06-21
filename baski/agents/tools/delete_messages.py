"""Tool for deleting conversation turns from message history."""

from pydantic import BaseModel, Field

from ..message_history import MessageHistory
from ..tool import Tool


class DeleteMessagesTool(Tool):
    """Tool for agent to delete specific messages from conversation history."""

    name = "prune_transcript"
    one_line = "Drop old conversation turns from the context window (does NOT touch any memory tier)"
    description = """Prune turns from the conversation transcript to free context space.
This only trims the live context window — it does NOT delete from memory (use a memory tool for that).

Two ways to prune (combine if useful):
- keep_last=N — keep only the last N turns, drop everything older. Use this to clear stale backlog
  in one call (e.g. on a topic change) instead of listing dozens of ids.
- turn_ids=[…] — drop specific turns by their [Turn N] ids visible in messages.

Use after working_note to drop turns whose content you've already preserved.
Old search results may contain outdated info — prune to avoid misleading context."""

    class Input(BaseModel):
        """Arguments for pruning conversation turns — give keep_last, turn_ids, or both."""

        keep_last: int | None = Field(default=None, description="Keep only the last N turns; drop all older ones")
        turn_ids: list[int] = Field(default_factory=list, description="Specific [Turn N] ids to drop")

    def __init__(self, message_history: MessageHistory) -> None:
        """Store reference to the shared message history."""
        self.message_history = message_history

    async def execute(self, keep_last: int | None = None, turn_ids: list[int] | None = None) -> str:  # type: ignore[override]
        """Drop the turns selected by keep_last and/or turn_ids; return the removal count."""
        ids = set(turn_ids or [])
        if keep_last is not None:
            ids.update(turn.id for turn in self.message_history.turns[: -keep_last or None])
        removed = await self.message_history.delete_turns(sorted(ids))
        return f"Deleted {removed} turn(s). Remaining: {len(self.message_history)} turns."

    async def system_prompt(self) -> str:
        """Instructions telling the agent to free context after storing knowledge."""
        return (
            "CONTEXT MANAGEMENT: after saving knowledge, prune the source turns with "
            "prune_transcript to free context space.\n"
            "Workflow: search → working_note → prune_transcript for the turns you just saved."
        )
