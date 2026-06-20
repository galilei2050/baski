"""Tool for deleting conversation turns from message history."""

from pydantic import BaseModel, Field

from ..message_history import MessageHistory
from ..tool import Tool


class DeleteMessagesTool(Tool):
    """Tool for agent to delete specific messages from conversation history."""

    name = "delete_messages"
    one_line = "Delete old messages to free context window"
    description = """Delete turns from conversation history by [Turn N] IDs visible in messages.

Use after store_memory to remove turns whose content is already preserved.
Old search results may contain outdated info — delete to avoid misleading context.
Tool result turns are the largest — prioritize deleting those."""

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

    def system_prompt(self) -> str:
        """Instructions telling the agent to free context after storing knowledge."""
        return (
            "CONTEXT MANAGEMENT: After storing knowledge, delete the source turns with "
            "delete_messages to free context space.\n"
            "Workflow: search → store_memory → delete_messages for the turns you just stored."
        )
