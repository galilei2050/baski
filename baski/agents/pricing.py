"""What a model costs, and the running tally of what one execution spent."""

from dataclasses import dataclass
from typing import NamedTuple

from anthropic.types import Usage
from pydantic import BaseModel


class ModelPrice(NamedTuple):
    """USD per million tokens, one rate per billed bucket.

    Four explicit rates rather than multipliers off `input`, because the multipliers are Anthropic's
    and nobody else's: an open model served through a gateway may bill a cached read at 0.30x
    (moonshotai/kimi-k2-thinking) or not bill it at all (openai/gpt-oss-120b, zai/glm-4.7-flash),
    and none of them charge a premium to write the cache. Deriving those from `input` understated one
    measured run threefold.
    """

    input: float
    output: float
    cache_write: float
    cache_read: float


# Anthropic's 5-minute ephemeral cache: a written token costs 1.25x base input, a read one 0.10x.
_CACHE_WRITE_MULTIPLIER = 1.25
_CACHE_READ_MULTIPLIER = 0.10


def anthropic_price(base_input: float, output: float) -> ModelPrice:
    """An Anthropic model's four rates, derived from the two the price list publishes."""
    return ModelPrice(
        input=base_input,
        output=output,
        cache_write=base_input * _CACHE_WRITE_MULTIPLIER,
        cache_read=base_input * _CACHE_READ_MULTIPLIER,
    )


# Source: https://www.anthropic.com/pricing
MODEL_PRICING: dict[str, ModelPrice] = {
    "claude-opus-5": anthropic_price(5.00, 25.00),
    "claude-sonnet-5": anthropic_price(3.00, 15.00),
    "claude-opus-4-8": anthropic_price(5.00, 25.00),
    "claude-opus-4-6": anthropic_price(5.00, 25.00),
    "claude-opus-4-5": anthropic_price(5.00, 25.00),
    "claude-sonnet-4-5": anthropic_price(3.00, 15.00),
    "claude-haiku-4-5": anthropic_price(1.00, 5.00),
}


def effective_input_tokens(usage: Usage) -> int:
    """Real context-window size: `input_tokens` (uncached only, under caching) plus both cache buckets."""
    return usage.input_tokens + (usage.cache_read_input_tokens or 0) + (usage.cache_creation_input_tokens or 0)


def calculate_cost(price: ModelPrice, usage: Usage) -> float:
    """Cost in USD for one API response, each bucket at its own published rate.

    Takes the price rather than the model name so a model the table has never heard of — anything
    behind a gateway — is priced by whoever chose it, instead of being guessed at or rejected. The
    table stays the convenient source for Anthropic's own models.
    """
    return (
        usage.input_tokens * price.input
        + usage.output_tokens * price.output
        + (usage.cache_creation_input_tokens or 0) * price.cache_write
        + (usage.cache_read_input_tokens or 0) * price.cache_read
    ) / 1_000_000


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

    model: str  # recorded on the trace, so a stored run says WHAT was called
    price: ModelPrice  # and at what rate — supplied by whoever chose the model, not looked up here
    input_tokens: int = 0
    output_tokens: int = 0
    # The two cache buckets, kept apart because they are priced 12.5x apart: a written token costs
    # 1.25x base input, a read one 0.10x. Without them a stored run's cost cannot be recomputed or
    # explained — `input_tokens` alone counts only what missed the cache.
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0
    last_input_tokens: int = 0  # input of the most recent API call — the current context-window size
    turn_count: int = 0
    tool_calls: int = 0
    cost: float = 0.0  # this run and everything it delegated to — what the answer cost in total
    own_cost: float = 0.0  # only this agent's own API calls, so rows from many agents can be summed

    def collect(self, usage: Usage) -> None:
        """Accumulate token usage and cost from a single API response.

        `last_input_tokens` is the real context-window size (incl. cached prefix); the cost prices
        the cache read/write buckets at their own rates.
        """
        self.input_tokens += usage.input_tokens
        self.output_tokens += usage.output_tokens
        self.cache_read_tokens += usage.cache_read_input_tokens or 0
        self.cache_write_tokens += usage.cache_creation_input_tokens or 0
        self.last_input_tokens = effective_input_tokens(usage)
        self.turn_count += 1
        call_cost = calculate_cost(self.price, usage)
        self.cost += call_cost
        self.own_cost += call_cost

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
