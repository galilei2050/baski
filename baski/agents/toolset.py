"""Registry and executor for all agent tools."""

import asyncio
import logging
import time

from anthropic.types import MessageParam, ToolParam, ToolResultBlockParam, ToolUseBlock
from pydantic import ValidationError

from .tool import Tool


def _format_validation_error(tool_name: str, exc: ValidationError) -> str:
    """Render a pydantic error as a short, per-field message the model can act on."""
    lines = [f"Invalid input for tool '{tool_name}'. Fix these and call it again:"]
    for err in exc.errors():
        field = ".".join(str(p) for p in err["loc"]) or "(top level)"
        detail = err["msg"]
        if "input" in err:
            detail += f" (received: {err['input']!r})"
        lines.append(f"- {field}: {detail}")
    return "\n".join(lines)


class ToolSet:
    """Registry and parallel executor for a named set of agent tools."""

    def __init__(self, logger: logging.Logger) -> None:
        """Initialize an empty toolset with a logger for error reporting."""
        self._tools: dict[str, Tool] = {}
        self.logger = logger
        self.last_timings: dict[str, int] = {}

    def __contains__(self, tool_name: str) -> bool:
        """Return True if a tool with the given name is registered."""
        return tool_name in self._tools

    def add(self, tool: Tool) -> None:
        """Add a tool to the toolset."""
        self._tools[tool.name] = tool

    def get(self, tool_name: str) -> Tool | None:
        """Get a tool from the toolset by name."""
        return self._tools.get(tool_name)

    def remove(self, tool_name: str) -> None:
        """Remove a tool from the toolset by name."""
        self._tools.pop(tool_name, None)

    async def system_prompt(self) -> str:
        """The tool roster plus each tool's own prompt contribution, for the system prompt.

        Async and aggregated every turn (like `user_messages`), so tools whose guidance is live
        (e.g. owner preferences from a store) contribute fresh content each turn.
        """
        if not self._tools:
            return "No tools available"

        roster = ["You have tools:"]
        for i, tool in enumerate(self._tools.values(), 1):
            roster.append(f"{i}. {tool.one_line} ({tool.name})")

        sections = ["\n".join(roster)]
        sections.extend([c for tool in self._tools.values() if (c := await tool.system_prompt())])
        return "\n\n".join(sections)

    async def user_messages(self) -> list[MessageParam]:
        """Per-turn user blocks every tool injects at the top of the prompt (skip Nones)."""
        return [m for tool in self._tools.values() if (m := await tool.user_message()) is not None]

    def format_for_api(self) -> list[ToolParam]:
        """Convert tools to Claude API format."""
        return [tool.to_dict() for tool in self._tools.values()]

    async def _execute_single(self, tool_call: ToolUseBlock) -> ToolResultBlockParam:
        """Execute a single tool call with timing."""
        tool_name = tool_call.name
        tool_input = tool_call.input

        if tool_name not in self._tools:
            self.logger.error("Tool not found", extra={"toolName": tool_name})
            self.last_timings[tool_call.id] = 0
            return ToolResultBlockParam(
                type="tool_result",
                tool_use_id=tool_call.id,
                content=f"Error: Tool {tool_name} not found",
                is_error=True,
            )

        tool = self._tools[tool_name]

        try:
            kwargs = tool.Input.model_validate(tool_input).model_dump()
        except ValidationError as exc:
            # Skip execute; hand the parsed error back so the model retries. Sibling calls still run.
            content = _format_validation_error(tool_name, exc)
            self.logger.warning("Tool input invalid", extra={"toolName": tool_name, "error": content})
            self.last_timings[tool_call.id] = 0
            return ToolResultBlockParam(
                type="tool_result",
                tool_use_id=tool_call.id,
                content=content,
                is_error=True,
            )

        start = time.monotonic()

        try:
            result = await tool.execute(**kwargs)
        except Exception as exc:
            # A tool raising must not kill the whole agent run. Hand the error back to
            # the model as a failed tool_result so it can recover or report it.
            self.logger.exception("Tool execution failed", extra={"toolName": tool_name, "error": str(exc)})
            self.last_timings[tool_call.id] = int((time.monotonic() - start) * 1000)
            return ToolResultBlockParam(
                type="tool_result",
                tool_use_id=tool_call.id,
                content=f"Error executing tool {tool_name}: {exc}",
                is_error=True,
            )

        self.last_timings[tool_call.id] = int((time.monotonic() - start) * 1000)
        return ToolResultBlockParam(type="tool_result", tool_use_id=tool_call.id, content=result)

    async def execute(self, tool_calls: list[ToolUseBlock]) -> list[ToolResultBlockParam]:
        """Execute tool calls in parallel and return formatted results."""
        self.last_timings = {}
        results = await asyncio.gather(*[self._execute_single(tc) for tc in tool_calls])
        return list(results)
