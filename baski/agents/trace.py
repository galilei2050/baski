"""Agent trace collection and persistence to GCS and MongoDB."""

import gzip
import time
import uuid
from typing import NamedTuple

from anthropic.types import Message, MessageParam, TextBlock, ThinkingBlock, ToolResultBlockParam, ToolUseBlock
from google.api_core.exceptions import GoogleAPIError
from google.cloud import storage
from pydantic import BaseModel, ConfigDict, field_serializer
from pymongo.asynchronous.database import AsyncDatabase

from baski.concurrent import as_async
from baski.primitives import datetime, json
from baski.server import Logger

from .execute_result import AgentExecuteResult
from .pricing import ExecutionStats

TRACES_PREFIX = "traces/"


class TraceCollectorConfig(NamedTuple):
    """Configuration for TraceCollector initialization."""

    user_request: str
    model: str
    system_prompt: str
    bucket_name: str
    database: AsyncDatabase
    logger: Logger


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


class TurnRecord(BaseModel):
    """Record of a single agentic turn for trace storage."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    turn_number: int
    messages: list[MessageParam]
    thinking: list[str] = []
    text_response: str | None = None
    tool_calls: list[ToolUseBlock] = []
    tool_results: list[ToolResultRecord] = []
    input_tokens: int = 0
    output_tokens: int = 0
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
                    content=[item.model_dump() if isinstance(item, BaseModel) else item for item in items],
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
        self._system_prompt = config.system_prompt
        self._bucket_name = config.bucket_name
        self._database = config.database
        self._logger = config.logger
        self._turns: list[TurnRecord] = []
        self._result: AgentExecuteResult | None = None
        self._error: str | None = None
        self._created_at = datetime.now().isoformat()
        self._current_turn: TurnRecord | None = None
        self._turn_start: float = 0

    def start_turn(self, messages: list[MessageParam]) -> None:
        """Begin recording a new turn."""
        self._current_turn = TurnRecord(
            turn_number=len(self._turns) + 1,
            messages=messages,
        )
        self._turn_start = time.monotonic()

    def record_response(self, message: Message, api_duration_ms: int) -> None:
        """Record the API response content into the current turn."""
        turn = self._current_turn
        turn.input_tokens = message.usage.input_tokens
        turn.output_tokens = message.usage.output_tokens
        turn.stop_reason = message.stop_reason

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
                self._logger.warning("Unknown content block type", labels={"blockType": type(block).__name__})

        _ = api_duration_ms  # recorded at turn level via end_turn timing

    def record_tool_results(self, tool_results: list[ToolResultBlockParam], timings: dict[str, int]) -> None:
        """Record tool execution results into the current turn."""
        turn = self._current_turn
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
                )
            )

    def end_turn(self) -> None:
        """Finalize the current turn and add it to the trace."""
        self._current_turn.duration_ms = int((time.monotonic() - self._turn_start) * 1000)
        self._turns.append(self._current_turn)
        self._current_turn = None

    async def finalize(
        self, stats: ExecutionStats, result: AgentExecuteResult | None = None, error: str | None = None
    ) -> None:
        """Persist trace to GCS and MongoDB."""
        if result is not None:
            self._result = result
        self._error = error

        await self._upload_to_gcs()
        await self._save_to_db(stats)

    async def _upload_to_gcs(self) -> None:
        upload_error: str | None = None
        try:
            record = TraceRecord(
                id=self.id,
                created_at=self._created_at,
                user_request=self._user_request,
                model=self._model,
                system_prompt=self._system_prompt,
                turns=self._turns,
                result=self._result,
                error=self._error,
            )
            json_bytes = json.dumps(record.model_dump(), indent=None, sort_keys=False).encode()
            compressed = gzip.compress(json_bytes)

            blob_name = f"{TRACES_PREFIX}{self.id}.json.gz"

            def _upload() -> None:
                client = storage.Client()
                bucket = client.bucket(self._bucket_name)
                blob = bucket.blob(blob_name)
                blob.upload_from_string(compressed, content_type="application/gzip")

            await as_async(_upload)
        except (GoogleAPIError, OSError) as e:
            upload_error = str(e)

        if upload_error is not None:
            self._logger.error("Failed to upload trace", labels={"traceId": self.id, "error": upload_error})
        else:
            self._logger.info("Trace uploaded", labels={"traceId": self.id})

    async def _save_to_db(self, stats: ExecutionStats) -> None:
        """Save lightweight trace summary to MongoDB."""
        doc = {
            "_id": self.id,
            "created_at": self._created_at,
            "user_request": self._user_request[:128],
            "model": self._model,
            "input_tokens": stats.input_tokens,
            "output_tokens": stats.output_tokens,
            "turn_count": stats.turn_count,
            "tool_call_count": stats.tool_calls,
            "cost": stats.cost,
            "error": self._error,
        }
        await self._database["traces"].insert_one(doc)
        self._logger.info("Trace saved to DB", labels={"traceId": self.id})
