"""Short-term memory tool — stores facts gathered during a single agent run."""

from pathlib import Path
from typing import Annotated

from anthropic.types import MessageParam, TextBlockParam
from pydantic import BaseModel, BeforeValidator, Field

from ..tool import Tool


def _coerce_facts(value: object) -> object:
    """Split a newline-joined string into lines before validation.

    The model often sends `facts` as one string instead of the array the schema asks
    for. Splitting here stops `extend` from iterating the string character-by-character
    and flooding the context with thousands of single-letter "facts".
    """
    if isinstance(value, str):
        return [line.strip() for line in value.splitlines() if line.strip()]
    return value


class ShortTermMemory(Tool):
    """Tool for agent to store knowledge gathered during conversation."""

    name = "working_note"
    one_line = "WORKING MEMORY: jot a fact to this reply's scratchpad, cleared after the reply (USE FREQUENTLY)"
    description = """WORKING MEMORY — a scratchpad for THIS reply only; it is cleared once the reply ends.
Stash facts the moment you find them so they survive when the conversation context gets truncated mid-task.

NOT durable: to keep knowledge across future conversations, save it to long-term memory instead.

CRITICAL: lightweight and fast — use it extensively.

WHEN TO USE (frequently):
- Immediately after extracting facts from any source or tool call
- When you find conflicting information between sources
- BEFORE you lose access to the information

WHAT TO STORE:
- Key facts, data points, metrics, relationships, observations, source attributions
- Anything you'll need later this reply to synthesize the answer

WHY THIS EXISTS:
- Tool outputs disappear after a few turns; a note costs ~4k tokens vs 30k+ for keeping full sources

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

    class Input(BaseModel):
        """Arguments for storing short-term memory facts."""

        facts: Annotated[list[str], BeforeValidator(_coerce_facts)] = Field(
            description="List of facts with [SOURCE] prefix. Example: '[DECK] Company founded 2020'"
        )

    def __init__(self) -> None:
        """Initialize an empty knowledge store."""
        self.knowledge: list[str] = []

    async def execute(self, facts: list[str]) -> str:  # type: ignore[override]
        """Store facts and return confirmation."""
        self.knowledge.extend(facts)
        stored = len(facts)
        total = len(self.knowledge)
        return (
            f"Noted {stored} fact(s) to working memory. Total: {total}. Now prune source turns with prune_transcript."
        )

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

    async def user_message(self) -> MessageParam:
        """Format all knowledge as the per-turn user block injected at the top of the prompt."""
        parts = [
            f"WORKING MEMORY — this reply's scratchpad (cleared after the reply). Use {self.name} the "
            "moment you learn anything, so it survives context truncation.",
            "",
        ]

        if not self.knowledge:
            parts.append("(empty)")
        else:
            parts.append("Notes:")
            parts.extend(f"- {k}" for k in self.knowledge)

        return MessageParam(
            role="user",
            content=[TextBlockParam(type="text", text="\n".join(parts))],
        )

    async def system_prompt(self) -> str:
        """Instructions telling the agent to preserve facts proactively."""
        return (
            f"WORKING MEMORY: use {self.name} proactively to preserve facts before context truncation; "
            f"it is cleared after each reply. For durable knowledge that must survive across conversations, "
            f"use your long-term memory tool instead."
        )
