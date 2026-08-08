"""A run must be billed at its own model's rate, and an unpriced model must say so.

The regression these guard: `MODEL_PRICING.get(model, MODEL_PRICING["claude-sonnet-4-5"])` charged
every `claude-opus-5` run at Sonnet's price. Nothing failed and nothing looked wrong — a month of
production spend simply read 1.67x low in the cost line under every answer.
"""

import pytest
from anthropic.types import Usage

from baski.agents.pricing import ExecutionStats, calculate_cost


def test_opus_is_not_billed_at_sonnet_rates() -> None:
    usage = Usage(input_tokens=1_000_000, output_tokens=1_000_000)

    assert calculate_cost("claude-opus-5", usage) == pytest.approx(30.0)  # $5 in + $25 out
    assert calculate_cost("claude-sonnet-5", usage) == pytest.approx(18.0)  # $3 in + $15 out


def test_an_unpriced_model_raises_instead_of_guessing() -> None:
    with pytest.raises(KeyError, match="claude-not-a-model"):
        ExecutionStats(model="claude-not-a-model")


def test_each_cache_bucket_is_priced_at_its_own_rate() -> None:
    usage = Usage(
        input_tokens=0, output_tokens=0, cache_creation_input_tokens=1_000_000, cache_read_input_tokens=1_000_000
    )

    # $5 base: a written token costs 1.25x it, a read one 0.10x
    assert calculate_cost("claude-opus-5", usage) == pytest.approx(6.25 + 0.50)


def test_stats_keep_the_cache_buckets_and_separate_own_from_delegated_spend() -> None:
    stats = ExecutionStats(model="claude-opus-5")

    stats.collect(Usage(input_tokens=10, output_tokens=20, cache_read_input_tokens=30, cache_creation_input_tokens=40))
    own = stats.own_cost
    stats.cost += 1.5  # what a delegating tool spent, as `Agent._execute_tools` adds it

    assert (stats.cache_read_tokens, stats.cache_write_tokens) == (30, 40)
    assert stats.own_cost == own, "a sub-agent's spend must not land in this agent's own cost"
    assert stats.cost == pytest.approx(own + 1.5)
