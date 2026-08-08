"""A stored run must say which tool spent the money and which one filled the context.

Without this the only way to ask "what is `browse_website` costing us" is to download and parse
every gzipped trace in the bucket — the summary in Mongo could not answer it at all.
"""

from typing import cast

from anthropic.types import ToolUseBlock
from pymongo.asynchronous.database import AsyncDatabase

from baski.agents.trace import TraceCollector, TraceCollectorConfig


def _collector() -> TraceCollector:
    """A collector recording in memory; nothing here reaches the database or the bucket."""
    return TraceCollector(
        config=TraceCollectorConfig(
            user_request="find me a hotel",
            model="claude-opus-5",
            system_prompt="",
            bucket_name="unused",
            database=cast("AsyncDatabase", None),
            agent_name="researcher",
        )
    )


def _record(trace: TraceCollector, calls: list[tuple[str, str, str, bool]], costs: dict[str, float]) -> None:
    """Play one turn through the real recording path: tool calls, then their results."""
    trace.start_turn([])
    trace._turn.tool_calls = [ToolUseBlock(id=i, name=n, input={}, type="tool_use") for i, n, _, _ in calls]
    trace.record_tool_results(
        [{"type": "tool_result", "tool_use_id": i, "content": out, "is_error": err} for i, _, out, err in calls],
        timings={i: 100 for i, _, _, _ in calls},
        sub_trace_ids={},
        costs=costs,
    )
    trace.end_turn()


def test_repeated_calls_of_one_tool_fold_into_one_line() -> None:
    trace = _collector()

    _record(
        trace,
        [("t1", "browse_website", "x" * 5000, False), ("t2", "browse_website", "y" * 3000, False)],
        costs={},
    )

    (browse,) = trace._tool_usage()
    assert (browse.name, browse.calls, browse.errors) == ("browse_website", 2, 0)
    assert browse.output_chars == 8000, "the volume a tool puts into context is the point of the record"
    assert browse.duration_ms == 200


def test_a_delegating_tool_reports_what_its_agent_spent_and_ranks_first() -> None:
    trace = _collector()

    _record(
        trace,
        [("t1", "google_search", "hits", False), ("t2", "retrieval", "a cited answer", False)],
        costs={"t2": 0.42},
    )

    retrieval, search = trace._tool_usage()
    assert (retrieval.name, retrieval.cost) == ("retrieval", 0.42), "dearest tool first"
    assert search.cost == 0.0, "a plain tool spends nothing of its own"


def test_a_failed_call_is_counted_as_an_error_not_dropped() -> None:
    trace = _collector()

    _record(trace, [("t1", "browse_website", "403 Forbidden", True)], costs={})

    (browse,) = trace._tool_usage()
    assert (browse.calls, browse.errors) == (1, 1)
