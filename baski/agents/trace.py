"""Agent trace collection and persistence to GCS and MongoDB."""

import asyncio
import gzip
import logging
import time
import uuid
from pathlib import Path
from typing import NamedTuple, cast

import anyio
from anthropic.types import Message, MessageParam, TextBlock, ThinkingBlock, ToolResultBlockParam, ToolUseBlock
from google.api_core.exceptions import GoogleAPIError
from google.cloud import storage
from pydantic import BaseModel, ConfigDict, SkipValidation, field_serializer
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.errors import PyMongoError

from baski.primitives import datetime, json

from .execute_result import AgentExecuteResult
from .pricing import ExecutionStats

logger = logging.getLogger(__name__)

TRACES_PREFIX = "traces/"


# Strong references to detached trace-persistence tasks: asyncio keeps only a weak ref to a
# bare task, so without this a fire-and-forget task can be garbage-collected mid-flight.
_pending: set[asyncio.Task] = set()


def _track(task: asyncio.Task) -> None:
    """Hold a strong ref to a detached task until it finishes, then drop it."""
    _pending.add(task)
    task.add_done_callback(_pending.discard)


class TraceCollectorConfig(NamedTuple):
    """Configuration for TraceCollector initialization."""

    user_request: str
    model: str
    agent_name: str  # which agent this run belongs to — required, never defaulted (see `_save_to_db`)
    system_prompt: str
    bucket_name: str
    database: AsyncDatabase
    local_traces_dir: str | None = None  # write the full trace here instead of GCS; None → GCS


class SerializedMessageContent(BaseModel):
    """One content item within a serialized message."""

    model_config = ConfigDict(extra="allow")


class SerializedMessage(BaseModel):
    """A message serialized for GCS trace storage."""

    role: str
    content: list[SerializedMessageContent | str]


class ToolResultRecord(BaseModel):
    """A single tool execution result for trace storage."""

    tool_name: str
    tool_id: str
    output: str
    is_error: bool
    duration_ms: int
    cost: float = 0.0  # USD the tool itself spent (a delegating sub-agent, a paid API); 0 for a plain tool
    sub_trace_ids: list[str] = []  # traces of any agents this tool spawned (sub-agent delegation)


class ToolUsageRecord(BaseModel):
    """What one tool cost a run, summed over its calls — the per-tool line of the spend summary.

    A list of these, not a name→count map: a new measure is a new FIELD here, readable beside the
    ones already stored and aggregable with `$unwind` + `$group`, whereas widening a map means a
    second parallel map per measure and rows of two different shapes in one collection.
    """

    name: str
    calls: int
    errors: int
    cost: float  # USD the tool itself spent — a delegating sub-agent, a paid API; 0 for a plain tool
    duration_ms: int
    # What the tool put into the agent's context. It is paid twice — written into the prompt cache
    # once at 1.25x input price, then read back at 0.10x on every remaining turn — so this is the
    # measure that says which tool is really driving the token bill, not the call count.
    output_chars: int


class TurnRecord(BaseModel):
    """Record of a single agentic turn for trace storage."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    turn_number: int
    # Already-valid anthropic params, kept only to serialize. SkipValidation stops pydantic
    # from re-validating and turning each `content` list into a single-use ValidatorIterator.
    messages: SkipValidation[list[MessageParam]]
    thinking: list[str] = []
    text_response: str | None = None
    tool_calls: list[ToolUseBlock] = []
    tool_results: list[ToolResultRecord] = []
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_creation_tokens: int = 0
    duration_ms: int = 0
    stop_reason: str = ""

    @field_serializer("messages")
    @classmethod
    def serialize_messages(cls, messages: list[MessageParam]) -> list[SerializedMessage]:
        """Serialize messages to JSON-safe format."""
        serialized = []
        for msg in messages:
            content = msg.get("content", [])
            items = content if isinstance(content, list) else [content]
            serialized.append(
                SerializedMessage(
                    role=msg["role"],
                    content=cast(
                        "list[SerializedMessageContent | str]",
                        [item.model_dump() if isinstance(item, BaseModel) else item for item in items],
                    ),
                )
            )
        return serialized


class TraceRecord(BaseModel):
    """Full execution trace record for GCS storage."""

    id: str
    created_at: str
    user_request: str
    model: str
    system_prompt: str
    turns: list[TurnRecord]
    result: AgentExecuteResult | None
    error: str | None


class TraceCollector:
    """Collects turn-by-turn trace data and persists to GCS and MongoDB."""

    def __init__(self, config: TraceCollectorConfig) -> None:
        """Initialize a new trace collector with a fresh UUID."""
        self.id = str(uuid.uuid4())
        self._user_request = config.user_request
        self._model = config.model
        self._agent_name = config.agent_name
        self._system_prompt = config.system_prompt
        self._bucket_name = config.bucket_name
        self._database = config.database
        self._local_traces_dir = config.local_traces_dir
        self._turns: list[TurnRecord] = []
        self._result: AgentExecuteResult | None = None
        self._error: str | None = None
        self._created_at = datetime.now().isoformat()
        self._current_turn: TurnRecord | None = None
        self._turn_start: float = 0
        self._persist_task: asyncio.Task | None = None

    def start_turn(self, messages: list[MessageParam]) -> None:
        """Begin recording a new turn."""
        self._current_turn = TurnRecord(
            turn_number=len(self._turns) + 1,
            messages=messages,
        )
        self._turn_start = time.monotonic()

    @property
    def _turn(self) -> TurnRecord:
        """Return the active turn, raising if no turn is in progress."""
        if self._current_turn is None:
            raise RuntimeError("No active turn; call start_turn first")
        return self._current_turn

    def record_response(self, message: Message, api_duration_ms: int) -> None:
        """Record the API response content into the current turn."""
        turn = self._turn
        turn.input_tokens = message.usage.input_tokens
        turn.output_tokens = message.usage.output_tokens
        turn.cache_read_tokens = message.usage.cache_read_input_tokens or 0
        turn.cache_creation_tokens = message.usage.cache_creation_input_tokens or 0
        turn.stop_reason = message.stop_reason or ""

        for block in message.content:
            if isinstance(block, ThinkingBlock):
                turn.thinking.append(block.thinking)
            elif isinstance(block, TextBlock):
                if turn.text_response is None:
                    turn.text_response = block.text
                else:
                    turn.text_response += "\n" + block.text
            elif isinstance(block, ToolUseBlock):
                turn.tool_calls.append(block)
            else:
                logger.warning("Unknown content block type", extra={"blockType": type(block).__name__})

        _ = api_duration_ms  # recorded at turn level via end_turn timing

    def record_tool_results(
        self,
        tool_results: list[ToolResultBlockParam],
        timings: dict[str, int],
        sub_trace_ids: dict[str, list[str]],
        costs: dict[str, float],
    ) -> None:
        """Record tool execution results into the current turn."""
        turn = self._turn
        for result in tool_results:
            tool_use_id = result["tool_use_id"]
            content = result.get("content", "")
            is_error = result.get("is_error", False)

            tool_name = ""
            for tc in turn.tool_calls:
                if tc.id == tool_use_id:
                    tool_name = tc.name
                    break

            turn.tool_results.append(
                ToolResultRecord(
                    tool_name=tool_name,
                    tool_id=tool_use_id,
                    output=content if isinstance(content, str) else str(content),
                    is_error=is_error,
                    duration_ms=timings.get(tool_use_id, 0),
                    cost=costs.get(tool_use_id, 0.0),
                    sub_trace_ids=sub_trace_ids.get(tool_use_id, []),
                )
            )

    def end_turn(self) -> None:
        """Finalize the current turn and add it to the trace."""
        turn = self._turn
        turn.duration_ms = int((time.monotonic() - self._turn_start) * 1000)
        self._turns.append(turn)
        self._current_turn = None

    def finalize(
        self, stats: ExecutionStats, result: AgentExecuteResult | None = None, error: str | None = None
    ) -> None:
        """Schedule trace persistence (full trace to GCS or LOCAL_TRACES_DIR, summary to Mongo).

        Detached task; fire-and-forget by default — the reply path never blocks on or fails from
        trace IO. Call `wait()` (Agent does when `await_trace` is set) to block until it finishes.
        """
        if result is not None:
            self._result = result
        self._error = error
        task = asyncio.create_task(self._persist(stats))
        self._persist_task = task
        _track(task)

    async def wait(self) -> None:
        """Block until scheduled persistence finishes (no-op if finalize wasn't called)."""
        if self._persist_task is not None:
            await self._persist_task

    def _build_record(self) -> "TraceRecord":
        """Assemble the full trace record — the single source for both persist modes."""
        return TraceRecord(
            id=self.id,
            created_at=self._created_at,
            user_request=self._user_request,
            model=self._model,
            system_prompt=self._system_prompt,
            turns=self._turns,
            result=self._result,
            error=self._error,
        )

    async def _persist(self, stats: ExecutionStats) -> None:
        """Save the full trace (local file or GCS) and the Mongo summary concurrently."""
        async with anyio.create_task_group() as tg:
            tg.start_soon(self._save_full_trace)
            tg.start_soon(self._save_to_db, stats)

    async def _save_full_trace(self) -> None:
        """Persist the full record — to `local_traces_dir` when set (debug/probe), else GCS."""
        if self._local_traces_dir is not None:
            path = Path(self._local_traces_dir) / f"{self.id}.json"
            await anyio.to_thread.run_sync(self._write_local, path)
        else:
            await self._upload_to_gcs()

    def _write_local(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self._build_record().model_dump(), sort_keys=False))

    async def _upload_to_gcs(self) -> None:
        compressed = gzip.compress(json.dumps(self._build_record().model_dump(), sort_keys=False).encode())
        blob_name = f"{TRACES_PREFIX}{self.id}.json.gz"

        def _upload() -> None:
            storage.Client().bucket(self._bucket_name).blob(blob_name).upload_from_string(
                compressed, content_type="application/gzip"
            )

        upload_error: str | None = None
        try:
            await anyio.to_thread.run_sync(_upload)
        except (GoogleAPIError, OSError) as e:
            upload_error = str(e)

        if upload_error is not None:
            logger.error("Failed to upload trace", extra={"traceId": self.id, "error": upload_error})
        else:
            logger.info("Trace uploaded", extra={"traceId": self.id})

    def _tool_usage(self) -> list[ToolUsageRecord]:
        """Fold this run's tool calls into one record per tool, dearest first."""
        totals: dict[str, ToolUsageRecord] = {}
        for turn in self._turns:
            for result in turn.tool_results:
                row = totals.setdefault(
                    result.tool_name,
                    ToolUsageRecord(name=result.tool_name, calls=0, errors=0, cost=0.0, duration_ms=0, output_chars=0),
                )
                row.calls += 1
                row.errors += int(result.is_error)
                row.cost += result.cost
                row.duration_ms += result.duration_ms
                row.output_chars += len(result.output)
        return sorted(totals.values(), key=lambda r: (-r.cost, -r.output_chars))

    async def _save_to_db(self, stats: ExecutionStats) -> None:
        """Save the trace summary to MongoDB — enough of it to answer "where did the money go".

        The summary is the only index over runs; the full trace is a gzipped blob in GCS that has to
        be downloaded and parsed. So it carries what a spend question needs: WHO ran (`agent_name` —
        one collection holds the main loop, every sub-agent and the nightly maintenance pass, and the
        model alone does not tell them apart), WHAT it delegated to (`sub_trace_ids`, so one answer's
        whole tree is walkable without opening a blob), and both cache buckets (priced 12.5x apart,
        so `cost` is otherwise unexplainable and `input_tokens` alone is only the part that missed
        the cache). `cost` covers this run AND everything it delegated to; `own_cost` covers only
        this agent's own calls, so a query may sum rows without counting a child twice.

        `tools` is the per-tool line of the same question — see `ToolUsageRecord`.
        """
        doc = {
            "_id": self.id,
            "created_at": self._created_at,
            "user_request": self._user_request[:128],
            "agent_name": self._agent_name,
            "model": self._model,
            "input_tokens": stats.input_tokens,
            "output_tokens": stats.output_tokens,
            "cache_read_tokens": stats.cache_read_tokens,
            "cache_write_tokens": stats.cache_write_tokens,
            "turn_count": stats.turn_count,
            "tool_call_count": stats.tool_calls,
            "sub_trace_ids": sorted({s for turn in self._turns for tr in turn.tool_results for s in tr.sub_trace_ids}),
            "tools": [t.model_dump() for t in self._tool_usage()],
            "cost": stats.cost,
            "own_cost": stats.own_cost,
            "error": self._error,
        }
        try:
            await self._database["traces"].insert_one(doc)
        except PyMongoError:
            logger.exception("Failed to save trace to DB", extra={"traceId": self.id})
            return
        logger.info("Trace saved to DB", extra={"traceId": self.id})
