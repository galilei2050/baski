"""Tool for deleting conversation turns from message history."""

from typing import Any, ClassVar

from ..message_history import MessageHistory
from ..tool import Tool


class DeleteMessagesTool(Tool):
    """Tool for agent to delete specific messages from conversation history."""

    name = "delete_messages"
    one_line = "Delete old messages to free context window"
    description = """Delete turns from conversation history by [Turn N] IDs visible in messages.

Use after store_knowledge to remove turns whose content is already preserved.
Old search results may contain outdated info — delete to avoid misleading context.
Tool result turns are the largest — prioritize deleting those."""

    input_schema: ClassVar[Any] = {
        "type": "object",
        "properties": {
            "turn_ids": {
                "type": "array",
                "items": {"type": "integer"},
                "description": "Turn IDs to delete (visible as [Turn N] in messages)",
            }
        },
        "required": ["turn_ids"],
    }

    def __init__(self, message_history: MessageHistory) -> None:
        """Store reference to the shared message history."""
        self.message_history = message_history

    async def execute(self, turn_ids: list[int]) -> str:  # type: ignore[override]
        """Delete specified turns and return removal count."""
        removed = self.message_history.delete_turns(turn_ids)
        return f"Deleted {removed} message(s). Remaining: {len(self.message_history)} messages."
