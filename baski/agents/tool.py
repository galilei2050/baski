"""Abstract base class for all agent tools."""

from abc import ABC, abstractmethod
from typing import Any, ClassVar

from anthropic.types import ToolParam


class Tool(ABC):
    """Base class for all agent tools."""

    name: ClassVar[str]
    one_line: ClassVar[str]
    description: ClassVar[str]
    input_schema: ClassVar[Any]

    def __hash__(self) -> int:
        """Hash by tool name for use in sets and dicts."""
        return hash(self.name)

    def __eq__(self, other: object) -> bool:
        """Tools are equal if they share the same name."""
        return isinstance(other, Tool) and self.name == other.name

    def to_dict(self) -> ToolParam:
        """Convert tool to Claude API format."""
        return ToolParam(
            name=self.name,
            description=self.description,
            input_schema=self.input_schema,
        )

    @abstractmethod
    async def execute(self, **kwargs: object) -> str:
        """Execute the tool with the given keyword arguments and return a string result."""
