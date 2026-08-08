"""A run must be billed at its own model's rate, and every bucket at that model's own rate.

Two regressions these guard. `MODEL_PRICING.get(model, MODEL_PRICING["claude-sonnet-4-5"])` charged
every `claude-opus-5` run at Sonnet's price — nothing failed, a month of production spend simply read
1.67x low. And deriving the cache rates from `input` by Anthropic's 1.25x/0.10x multipliers priced one
measured gateway run at 3.04x what the provider actually charged: an open model may bill a cached read
at 0.30x of input, or not bill it at all.
"""

import pytest
from anthropic.types import Usage

from baski.agents.pricing import MODEL_PRICING, ExecutionStats, ModelPrice, anthropic_price, calculate_cost

# openai/gpt-oss-120b as the Vercel catalogue prices it: cached reads are not billed at all.
_GATEWAY = ModelPrice(input=0.10, output=0.50, cache_write=0.0, cache_read=0.0)


def test_opus_is_not_billed_at_sonnet_rates() -> None:
    usage = Usage(input_tokens=1_000_000, output_tokens=1_000_000)

    assert calculate_cost(MODEL_PRICING["claude-opus-5"], usage) == pytest.approx(30.0)  # $5 in + $25 out
    assert calculate_cost(MODEL_PRICING["claude-sonnet-5"], usage) == pytest.approx(18.0)  # $3 in + $15 out


def test_each_cache_bucket_is_priced_at_its_own_rate() -> None:
    usage = Usage(
        input_tokens=0, output_tokens=0, cache_creation_input_tokens=1_000_000, cache_read_input_tokens=1_000_000
    )

    # $5 base: a written token costs 1.25x it, a read one 0.10x
    assert calculate_cost(anthropic_price(5.00, 25.00), usage) == pytest.approx(6.25 + 0.50)


def test_a_model_that_does_not_bill_cache_reads_is_not_charged_for_them() -> None:
    """The failure this replaces: charging a gateway's cached reads at 0.10x of input, three times over."""
    usage = Usage(input_tokens=4_665, output_tokens=2_782, cache_read_input_tokens=37_952)

    charged = 0.001857  # what the gateway's own balance actually moved by for this run
    assert calculate_cost(_GATEWAY, usage) == pytest.approx(charged, abs=1e-6)


def test_stats_keep_the_cache_buckets_and_separate_own_from_delegated_spend() -> None:
    stats = ExecutionStats(model="claude-opus-5", price=MODEL_PRICING["claude-opus-5"])

    stats.collect(Usage(input_tokens=10, output_tokens=20, cache_read_input_tokens=30, cache_creation_input_tokens=40))
    own = stats.own_cost
    stats.cost += 1.5  # what a delegating tool spent, as `Agent._execute_tools` adds it

    assert (stats.cache_read_tokens, stats.cache_write_tokens) == (30, 40)
    assert stats.own_cost == own, "a sub-agent's spend must not land in this agent's own cost"
    assert stats.cost == pytest.approx(own + 1.5)
