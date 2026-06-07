"""Tool for storing knowledge gathered during agent research."""

from pathlib import Path
from typing import Any, ClassVar

from anthropic.types import MessageParam, TextBlockParam

from ..tool import Tool


class KnowledgeTool(Tool):
    """Tool for agent to store knowledge gathered during conversation."""

    name = "store_knowledge"
    one_line = "Lightweight tool to preserve context (USE FREQUENTLY)"
    description = """Store facts you discover during research to preserve them when conversation context gets truncated.

CRITICAL: This is a lightweight, fast operation. Use it extensively.

WHEN TO USE (frequently):
- Immediately after extracting facts from any source
- When you find conflicting information between sources
- After every tool call that returns useful data
- When noticing patterns, conflicts, or insights
- After completing research on a specific topic (team, traction, market)
- BEFORE you lose access to the information

WHAT TO STORE:
- Key facts, data points, and metrics from any source
- Relationships and connections between entities
- Observations, patterns, and discrepancies
- Source attributions for critical claims
- Context that might be needed later

WHY THIS EXISTS:
- Tool outputs disappear after a few turns
- You need facts available to synthesize final reports later
- Knowledge costs ~4k tokens vs 30k+ for keeping full sources

FACT FORMULATION FORMAT: [SOURCE] entity + fact + temporal context

GOOD EXAMPLES:
- [WEB] Project X launched v2.0 in January 2024 with 3 new integrations
- [LINKEDIN] Jane Smith - Senior Engineer at Acme Corp since March 2022
- [DOCS] API rate limit is 1000 req/min for free tier, 10k for enterprise
- [GOOGLE_PLAY] App has 100k+ downloads, 4.2 rating (523 reviews)
- [CONFLICT] Docs say max 5 users, but pricing page says 10 users on free plan
- [OBSERVATION] Library supports Python 3.10+ only, project uses 3.9

BAD EXAMPLES:
- "100k+ downloads" (which app? when checked?)
- "Supports 3 languages" (which product? which languages?)
- "Worked at Google" (who? how long?)
- "Good documentation" (based on what? which sections?)

Use aggressively - store first, synthesize later. More is better than perfect."""

    input_schema: ClassVar[Any] = {
        "type": "object",
        "properties": {
            "facts": {
                "type": "array",
                "items": {"type": "string"},
                "description": "List of facts with [SOURCE] prefix. Example: '[DECK] Company founded 2020'",
            }
        },
        "required": ["facts"],
    }

    def __init__(self) -> None:
        """Initialize an empty knowledge store."""
        self.knowledge: list[str] = []

    async def execute(self, facts: list[str]) -> str:  # type: ignore[override]
        """Store facts and return confirmation."""
        self.knowledge.extend(facts)
        stored = len(facts)
        total = len(self.knowledge)
        return f"Stored {stored} fact(s). Total: {total}. Now delete source turns with Tool: delete_messages."

    def dump(self) -> None:
        """Dump knowledge to markdown file (CLI only)."""
        output_file = Path("knowledge.md")
        content = "\n".join(f"- {k}" for k in self.knowledge)
        output_file.write_text(content)

    def get_knowledge(self) -> list[str]:
        """Get all stored knowledge."""
        return self.knowledge.copy()

    def clear(self) -> None:
        """Clear all stored knowledge."""
        self.knowledge.clear()

    def format_as_user_message(self) -> MessageParam:
        """Format all knowledge as user message for API."""
        parts = [f"IMPORTANT: Use {self.name} tool immediately after learning ANY new information to prevent loss.", ""]

        if not self.knowledge:
            parts.append("No knowledge stored yet.")
        else:
            parts.append("Knowledge:")
            parts.extend(f"- {k}" for k in self.knowledge)

        return MessageParam(
            role="user",
            content=[TextBlockParam(type="text", text="\n".join(parts))],
        )
