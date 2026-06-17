"""Abstract base class for all agent tools."""

from abc import ABC, abstractmethod
from typing import Any, ClassVar

from anthropic.types import ToolParam
from pydantic import BaseModel


class Tool(ABC):
    """Base class for all agent tools.

    Every tool declares its argument contract as a nested `Input(BaseModel)`. The JSON
    schema sent to the API is derived from it, and the agent validates every call
    against it before running `execute`. A tool with no `Input` fails at
    class-definition time — the contract is enforced when the class is built, not at
    runtime.
    """

    name: ClassVar[str]
    one_line: ClassVar[str]
    description: ClassVar[str]
    Input: ClassVar[type[BaseModel]]
    input_schema: ClassVar[Any]

    def __init_subclass__(cls, **kwargs: object) -> None:
        """Require a nested `Input` model and derive `input_schema` from it."""
        super().__init_subclass__(**kwargs)
        if getattr(cls, "Input", None) is None:
            raise TypeError(f"{cls.__name__} must define a nested `Input(BaseModel)` describing its arguments")
        cls.input_schema = cls.Input.model_json_schema()

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

    def system_prompt(self) -> str:
        """Extra instructions this tool adds to the agent system prompt. None by default."""
        return ""

    @abstractmethod
    async def execute(self, **kwargs: object) -> str:
        """Execute the tool with the given keyword arguments and return a string result."""
