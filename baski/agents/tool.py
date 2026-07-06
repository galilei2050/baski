"""Abstract base class for all agent tools."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, ClassVar

from anthropic.types import MessageParam, ToolParam
from pydantic import BaseModel


@dataclass
class ToolResult:
    """A richer `execute` return: the text result plus what the call cost and any agents it spawned.

    A tool with nothing extra to report just returns a `str` (cost 0, no sub-traces). A delegating
    tool returns its child's spend and trace id here, so the agent folds the cost into the turn and
    the parent trace can walk into the child. Cost/trace are properties of the RESULT — returned, not
    threaded through a shared sink or an in/out argument.
    """

    content: str
    cost: float = 0.0  # USD this call cost (a nested agent's total, a paid API's charge)
    sub_trace_ids: list[str] = field(default_factory=list)  # traces of agents this call spawned


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

    async def system_prompt(self) -> str:
        """Extra instructions this tool adds to the agent system prompt, or "" for none.

        Async and re-read every turn (symmetric with `user_message`), so a tool whose guidance
        depends on live state — e.g. owner preferences loaded from a store — can return current
        content each turn rather than a value frozen at build time.
        """
        return ""

    async def user_message(self) -> MessageParam | None:
        """A user-role block this tool injects at the top of every turn, or None.

        The seam for always-present, per-turn context a tool owns — short-term memory's
        fact list, a long-term memory index, a skill's loaded body. The Agent collects
        these from every tool in the set on every turn; default None means the tool injects
        nothing. Async so a tool can query its live source (e.g. a store) each turn, picking
        up changes the agent made mid-run. If the content is stable within a run, keep it
        date-only to preserve the prompt cache.
        """
        return None

    @abstractmethod
    async def execute(self, **kwargs: object) -> "str | ToolResult":
        """Execute the tool and return its text result.

        Return a plain `str` for a normal tool. Return a `ToolResult` to also report what the call
        cost and any sub-agent trace ids (e.g. a delegating tool returns its child's cost + trace).
        """
