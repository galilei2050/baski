"""Anthropic API pricing for cost calculation."""

from dataclasses import dataclass

from anthropic.types import Usage
from pydantic import BaseModel

# Pricing in USD per million tokens
# Source: https://www.anthropic.com/pricing
MODEL_PRICING = {
    "claude-opus-4-8": {
        "input": 5.00,  # $5 per million input tokens
        "output": 25.00,  # $25 per million output tokens
    },
    "claude-opus-4-6": {
        "input": 5.00,  # $5 per million input tokens
        "output": 25.00,  # $25 per million output tokens
    },
    "claude-opus-4-5": {
        "input": 5.00,  # $5 per million input tokens
        "output": 25.00,  # $25 per million output tokens
    },
    "claude-sonnet-4-5": {
        "input": 3.00,  # $3 per million input tokens
        "output": 15.00,  # $15 per million output tokens
    },
    "claude-haiku-4-5": {
        "input": 1.00,  # $1 per million input tokens
        "output": 5.00,  # $5 per million output tokens
    },
}


# Cache multipliers vs base input price (5-minute ephemeral write, read).
_CACHE_WRITE_MULTIPLIER = 1.25
_CACHE_READ_MULTIPLIER = 0.10


def effective_input_tokens(usage: Usage) -> int:
    """Real context-window size: `input_tokens` (uncached only, under caching) plus both cache buckets."""
    return usage.input_tokens + (usage.cache_read_input_tokens or 0) + (usage.cache_creation_input_tokens or 0)


def calculate_cost(model: str, usage: Usage) -> float:
    """Calculate cost in USD for one API response, pricing each cache bucket at its own rate."""
    pricing = MODEL_PRICING.get(model, MODEL_PRICING["claude-sonnet-4-5"])

    input_cost = (usage.input_tokens / 1_000_000) * pricing["input"]
    output_cost = (usage.output_tokens / 1_000_000) * pricing["output"]
    cache_write_cost = (
        ((usage.cache_creation_input_tokens or 0) / 1_000_000) * pricing["input"] * _CACHE_WRITE_MULTIPLIER
    )
    cache_read_cost = ((usage.cache_read_input_tokens or 0) / 1_000_000) * pricing["input"] * _CACHE_READ_MULTIPLIER

    return input_cost + output_cost + cache_write_cost + cache_read_cost


class ExecutionLogFields(BaseModel):
    """Structured log labels for agent execution summary."""

    total_input_tokens: int
    total_output_tokens: int
    total_tokens: int
    turn_count: int
    tool_call_count: int
    total_cost: str


@dataclass
class ExecutionStats:
    """Tracks token usage, costs, and turn counts across an agent execution."""

    model: str
    input_tokens: int = 0
    output_tokens: int = 0
    last_input_tokens: int = 0  # input of the most recent API call — the current context-window size
    turn_count: int = 0
    tool_calls: int = 0
    cost: float = 0.0

    def collect(self, usage: Usage) -> None:
        """Accumulate token usage and cost from a single API response.

        `last_input_tokens` is the real context-window size (incl. cached prefix); the cost prices
        the cache read/write buckets at their own rates.
        """
        self.input_tokens += usage.input_tokens
        self.output_tokens += usage.output_tokens
        self.last_input_tokens = effective_input_tokens(usage)
        self.turn_count += 1
        self.cost += calculate_cost(self.model, usage)

    def for_logs(self) -> ExecutionLogFields:
        """Build a structured log-fields model from current execution stats."""
        return ExecutionLogFields(
            total_input_tokens=self.input_tokens,
            total_output_tokens=self.output_tokens,
            total_tokens=self.input_tokens + self.output_tokens,
            turn_count=self.turn_count,
            tool_call_count=self.tool_calls,
            total_cost=f"${self.cost:.6f}",
        )
