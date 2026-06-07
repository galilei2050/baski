"""Registry and executor for all agent tools."""

import asyncio
import time

from anthropic.types import ToolParam, ToolResultBlockParam, ToolUseBlock

from baski.server import Logger

from .tool import Tool


class ToolBox:
    """Registry and parallel executor for a named set of agent tools."""

    def __init__(self, logger: Logger) -> None:
        """Initialize an empty toolbox with a logger for error reporting."""
        self._tools: dict[str, Tool] = {}
        self.logger = logger
        self.last_timings: dict[str, int] = {}

    def __contains__(self, tool_name: str) -> bool:
        """Return True if a tool with the given name is registered."""
        return tool_name in self._tools

    def add(self, tool: Tool) -> None:
        """Add a tool to the toolbox."""
        self._tools[tool.name] = tool

    def get(self, tool_name: str) -> Tool | None:
        """Get a tool from the toolbox by name."""
        return self._tools.get(tool_name)

    def remove(self, tool_name: str) -> None:
        """Remove a tool from the toolbox by name."""
        self._tools.pop(tool_name, None)

    def short_description(self) -> str:
        """Short description of tools in toolbox for LLM awareness."""
        if not self._tools:
            return "No tools available"

        descriptions = []
        for i, tool in enumerate(self._tools.values(), 1):
            descriptions.append(f"{i}. {tool.one_line} ({tool.name})")

        return "\n".join(descriptions)

    def format_for_api(self) -> list[ToolParam]:
        """Convert tools to Claude API format."""
        return [tool.to_dict() for tool in self._tools.values()]

    async def _execute_single(self, tool_call: ToolUseBlock) -> ToolResultBlockParam:
        """Execute a single tool call with timing."""
        tool_name = tool_call.name
        tool_input = tool_call.input

        if tool_name not in self._tools:
            self.logger.error("Tool not found", labels={"toolName": tool_name})
            self.last_timings[tool_call.id] = 0
            return ToolResultBlockParam(
                type="tool_result", tool_use_id=tool_call.id, content=f"Error: Tool {tool_name} not found"
            )

        tool = self._tools[tool_name]
        start = time.monotonic()

        result = await tool.execute(**tool_input)
        self.last_timings[tool_call.id] = int((time.monotonic() - start) * 1000)
        return ToolResultBlockParam(type="tool_result", tool_use_id=tool_call.id, content=result)

    async def execute(self, tool_calls: list[ToolUseBlock]) -> list[ToolResultBlockParam]:
        """Execute tool calls in parallel and return formatted results."""
        self.last_timings = {}
        results = await asyncio.gather(*[self._execute_single(tc) for tc in tool_calls])
        return list(results)
