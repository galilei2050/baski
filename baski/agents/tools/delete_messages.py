"""Tool for deleting conversation turns from message history."""

from pydantic import BaseModel, Field

from ..message_history import MessageHistory
from ..tool import Tool


class DeleteMessagesTool(Tool):
    """Tool for agent to delete specific messages from conversation history."""

    name = "prune_transcript"
    one_line = "Drop old conversation turns from the context window (does NOT touch any memory tier)"
    description = """Prune turns from the conversation transcript by their [Turn N] IDs visible in messages.
This only trims the live context window — it does NOT delete from memory (use a memory tool for that).

Use after working_note to drop turns whose content you've already preserved.
Old search results may contain outdated info — prune to avoid misleading context.
Tool-result turns are the largest — prioritize pruning those."""

    class Input(BaseModel):
        """Arguments for deleting conversation turns."""

        turn_ids: list[int] = Field(description="Turn IDs to delete (visible as [Turn N] in messages)")

    def __init__(self, message_history: MessageHistory) -> None:
        """Store reference to the shared message history."""
        self.message_history = message_history

    async def execute(self, turn_ids: list[int]) -> str:  # type: ignore[override]
        """Delete specified turns and return removal count."""
        removed = await self.message_history.delete_turns(turn_ids)
        return f"Deleted {removed} message(s). Remaining: {len(self.message_history)} messages."

    async def system_prompt(self) -> str:
        """Instructions telling the agent to free context after storing knowledge."""
        return (
            "CONTEXT MANAGEMENT: after saving knowledge, prune the source turns with "
            "prune_transcript to free context space.\n"
            "Workflow: search → working_note → prune_transcript for the turns you just saved."
        )
