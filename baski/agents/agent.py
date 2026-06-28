"""Core agentic loop with tool execution and conversation management."""

import asyncio
import logging
import time
from http import HTTPStatus
from typing import NamedTuple

from anthropic import APIConnectionError, APIStatusError, AsyncAnthropic
from anthropic.types import ContentBlock, Message, MessageParam, TextBlock, TextBlockParam, ThinkingBlock, ToolUseBlock
from pymongo.asynchronous.database import AsyncDatabase

from baski.primitives import datetime

from .events import Completed, Listener, TextDelta, Thinking, ToolFinished, ToolStarted, TurnStarted, noop
from .events import Message as MessageEvent
from .execute_result import AgentExecuteResult
from .message_history import EPHEMERAL_CACHE, MessageHistory
from .pricing import ExecutionStats
from .toolset import ToolSet
from .trace import TraceCollector, TraceCollectorConfig

DEFAULT_MODEL = "claude-opus-4-8"

AGENT_LOOP_GUIDANCE = (
    "Always use the most appropriate tool. Use parallel tool calls when they are independent\n"
    "Provide brief explanation of your reasoning for when you use tools and what are next steps."
)


class AgentRefusalError(RuntimeError):
    """The model returned stop_reason='refusal'.

    Anthropic's safety classifier halted generation with no usable output. Raised
    immediately so the caller can surface it instead of silently falling back.
    """


class AgentProviderUnavailableError(RuntimeError):
    """The model provider is unavailable — a 5xx/529 (overloaded) status or a connection failure.

    Raised once retries are exhausted, so the caller can tell a provider-side outage apart from a
    bug on our side (4xx) and surface it differently instead of as a generic error.
    """


def _is_retriable(e: APIStatusError | APIConnectionError) -> bool:
    """A transient provider failure worth retrying — a connection blip or a 5xx/529 (overloaded) status.

    A 4xx (bad request, auth, rate limit) is our problem, not the provider's; retrying won't help.
    """
    if isinstance(e, APIConnectionError):
        return True
    return e.status_code >= HTTPStatus.INTERNAL_SERVER_ERROR


class ParsedResponse(NamedTuple):
    """Parsed content blocks from an Anthropic API response."""

    tool_calls: list[ToolUseBlock]
    text_blocks: list[TextBlock]


class TurnResult(NamedTuple):
    """Result of a single agentic turn."""

    message_to_user: str | None
    has_tool_calls: bool


class AgentConfig(NamedTuple):
    """Configuration parameters for Agent initialization.

    The caller assembles the collaborators (`toolset`, `message_history`) and passes
    them in — the Agent constructs nothing. Per-turn knowledge injection (short-term
    facts, memory indexes, skill bodies) flows through each tool's `user_message()`,
    collected by the toolset — no tool needs a dedicated config field.
    """

    logger: logging.Logger
    toolset: ToolSet
    message_history: MessageHistory
    anthropic_client: AsyncAnthropic
    database: AsyncDatabase
    bucket_name: str
    system_prompt: str
    model: str = DEFAULT_MODEL
    await_trace: bool = False  # block reply on trace persistence (tests/probe read it right after)
    local_traces_dir: str | None = None  # write full traces here instead of GCS (tests/probe); None → GCS


class Agent:
    """Stateful agent with agentic loop handling tool execution and conversation management."""

    def __init__(self, config: AgentConfig, on_event: Listener = noop, **params: object) -> None:
        """Initialize agent with config, a step-event listener, and optional API overrides.

        `on_event` is an async listener that receives step events (turn start, thinking,
        tool start/finish, completion) as the loop runs — the seam for live progress UIs.
        The agent stays transport-agnostic; the listener owns rendering.
        """
        self.logger = config.logger
        self.database = config.database
        self.bucket_name = config.bucket_name
        self.anthropic_client = config.anthropic_client
        self.message_history = config.message_history
        self.toolset = config.toolset
        self._await_trace = config.await_trace
        self._local_traces_dir = config.local_traces_dir
        self.on_event = on_event
        # Not in message_history.turns, so truncate/prune_transcript can't reach it.
        self._pinned: list[MessageParam] = []

        self._system_prompt = config.system_prompt

        # Cache tools and system on separate breakpoints, so editing the system (e.g. core memory)
        # doesn't evict the stabler tool-schema cache. (System breakpoint set per-turn in _run_turn.)
        tools = self.toolset.format_for_api()
        if tools:
            tools[-1]["cache_control"] = EPHEMERAL_CACHE

        self.params: dict[str, object] = {
            "model": config.model,
            "tools": tools,
            "tool_choice": {"type": "auto", "disable_parallel_tool_use": False},
            "thinking": {"type": "adaptive"},
            "max_tokens": 128_000,
        } | params

    def add_pinned(self, message: MessageParam) -> None:
        """Pin a message to the top of every turn, protected from truncation/deletion."""
        self._pinned.append(message)

    def add_pinned_text(self, text: str) -> None:
        """Pin a plain user-text message (e.g. a one-shot task framed for the model)."""
        self.add_pinned(MessageParam(role="user", content=[TextBlockParam(type="text", text=text)]))

    async def _system(self) -> str:
        """Assemble the system prompt fresh — tool contributions (e.g. owner preferences) can change per turn."""
        return f"{self._system_prompt}\n\n{await self.toolset.system_prompt()}\n\n{AGENT_LOOP_GUIDANCE}"

    async def _stream_message(self, messages: list[MessageParam]) -> Message:
        """Stream one response, emitting each text delta to the listener; return the assembled message."""
        async with self.anthropic_client.messages.stream(
            messages=messages,
            **self.params,  # type: ignore[arg-type]  # params is a dynamic dict merged with caller overrides
        ) as stream:
            async for text in stream.text_stream:
                await self.on_event(TextDelta(text=text))
            return await stream.get_final_message()

    async def _call_api(self, messages: list[MessageParam]) -> Message:
        """Streaming API call with one retry on a transient provider error (2s backoff).

        Raises AgentRefusalError on a `refusal` stop_reason — caught here, before the message
        reaches history, so the refused content never gets fed back in. A provider outage
        (5xx/529 or a connection failure) becomes AgentProviderUnavailableError once the retry
        is exhausted; a 4xx (our own bug) re-raises as-is.
        """
        max_retries = 1
        retry_count = 0

        while retry_count <= max_retries:
            try:
                message = await self._stream_message(messages)
            except (APIStatusError, APIConnectionError) as e:
                if not _is_retriable(e):
                    raise
                if retry_count < max_retries:
                    retry_count += 1
                    self.logger.warning(
                        "Anthropic API error, retrying",
                        extra={"json_fields": {"retryCount": retry_count, "maxRetries": max_retries, "error": str(e)}},
                    )
                    await asyncio.sleep(2)
                    continue
                raise AgentProviderUnavailableError("Anthropic is unavailable") from e

            if message.stop_reason == "refusal":
                raise AgentRefusalError("Model returned stop_reason='refusal'")
            return message

        raise RuntimeError("Unreachable: retry loop exited without return or raise")

    def _parse_response(self, content_blocks: list[ContentBlock]) -> ParsedResponse:
        """Separate tool_use and text blocks, log thinking blocks."""
        tool_calls: list[ToolUseBlock] = []
        text_blocks: list[TextBlock] = []

        for block in content_blocks:
            if isinstance(block, ToolUseBlock):
                tool_calls.append(block)
            elif isinstance(block, ThinkingBlock):
                self.logger.info("Thinking", extra={"json_fields": {"thinking": block.thinking}})
            elif isinstance(block, TextBlock):
                text_blocks.append(block)
            else:
                self.logger.warning(
                    "Unknown content block type", extra={"json_fields": {"blockType": type(block).__name__}}
                )

        return ParsedResponse(tool_calls=tool_calls, text_blocks=text_blocks)

    async def _build_messages(self) -> list[MessageParam]:
        """Build the full message list for an API call: pinned context, then the history."""
        now = datetime.now()
        time_message = MessageParam(
            role="user",
            content=[TextBlockParam(type="text", text=f"Current time: {now.strftime('%A, %B %d, %Y %I:%M %p %Z')}")],
        )
        # Stable prefix (pinned + history, cache breakpoint on the last turn) first; volatile blocks
        # after it so they don't invalidate the cache: context footer, per-turn user_message()
        # injections, then time (changes every minute).
        history = self.message_history.format_for_api()
        status = self.message_history.context_status()
        return [
            *self._pinned,
            *history,
            *([status] if status else []),
            *await self.toolset.user_messages(),
            time_message,
        ]

    async def _execute_tools(
        self, tool_calls: list[ToolUseBlock], stats: ExecutionStats, trace: TraceCollector
    ) -> None:
        """Execute tool calls and record results in history and trace."""
        stats.tool_calls += len(tool_calls)
        self.logger.info(
            "Tools execution",
            extra={"json_fields": {"toolCount": len(tool_calls), "tools": [tc.name for tc in tool_calls]}},
        )
        for tc in tool_calls:
            await self.on_event(
                ToolStarted(name=tc.name, tool_input=dict(tc.input) if isinstance(tc.input, dict) else {})
            )

        tool_results = await self.toolset.execute(tool_calls)
        self.message_history.add_tool_results(tool_results)
        trace.record_tool_results(tool_results, self.toolset.last_timings)

        name_by_id = {tc.id: tc.name for tc in tool_calls}
        for result in tool_results:
            tool_id = result["tool_use_id"]
            await self.on_event(
                ToolFinished(
                    name=name_by_id.get(tool_id, ""),
                    ok=not result.get("is_error", False),
                    duration_ms=self.toolset.last_timings.get(tool_id, 0),
                )
            )

    def _collect_text(self, text_blocks: list[TextBlock]) -> str | None:
        """Join text blocks into a single user-facing message string."""
        if not text_blocks:
            return None
        message_to_user = "\n".join([b.text for b in text_blocks])
        self.logger.info("Text received", extra={"json_fields": {"message_to_user": message_to_user}})
        return message_to_user

    async def _emit_step_events(self, message: Message, parsed: ParsedResponse) -> str | None:
        """Surface this turn's thinking and pre-tool narration to the listener; return the text.

        Narration before tool calls is user-facing; the final turn's text (no tool calls) is
        the answer and is delivered via Completed, not here.
        """
        for block in message.content:
            if isinstance(block, ThinkingBlock):
                await self.on_event(Thinking(text=block.thinking))
        text = self._collect_text(parsed.text_blocks)
        if text and parsed.tool_calls:
            await self.on_event(MessageEvent(text=text))
        return text

    async def _run_turn(self, stats: ExecutionStats, trace: TraceCollector) -> TurnResult:
        """Run a single agentic turn: build messages, call API, parse response, execute tools."""
        messages = await self._build_messages()
        trace.start_turn(messages)

        start = time.monotonic()
        # Reassembled each turn — tool guidance can be live.
        self.params["system"] = [TextBlockParam(type="text", text=await self._system(), cache_control=EPHEMERAL_CACHE)]
        message = await self._call_api(messages)
        api_duration_ms = int((time.monotonic() - start) * 1000)

        stats.collect(message.usage)
        await self.on_event(TurnStarted(turn=stats.turn_count))
        if self.message_history.initial_context_too_large(stats.last_input_tokens):
            raise RuntimeError("Initial context is too large. Try to provide more LLM context.")

        self.message_history.truncate(message.usage)
        parsed = self._parse_response(message.content)
        trace.record_response(message, api_duration_ms)

        message_to_user = await self._emit_step_events(message, parsed)

        with self.message_history:
            self.message_history.add_assistant(message.content)
            if parsed.tool_calls:
                await self._execute_tools(parsed.tool_calls, stats, trace)

        trace.end_turn()
        return TurnResult(message_to_user=message_to_user, has_tool_calls=bool(parsed.tool_calls))

    def _request_label(self) -> str:
        """Short label for traces/logs: the newest user text, from history then pinned."""
        history_messages = [m for turn in self.message_history.turns for m in turn.messages]
        for message in reversed(self._pinned + history_messages):
            if message["role"] != "user":
                continue
            content = message["content"]
            if isinstance(content, list):
                for block in content:
                    if isinstance(block, dict) and block.get("type") == "text":
                        return str(block.get("text", ""))
        return ""

    async def execute(self) -> AgentExecuteResult:
        """Drive the agentic loop over the current pinned context + history until done.

        The caller sets up the request before calling: pin a task with `add_pinned_text`
        (task mode), or add the message to the injected `message_history` (chat mode, which
        also lets the loop continue a prior conversation). Re-callable — context is
        preserved across calls. Step events go to the constructor's `on_event` listener.
        """
        label = self._request_label()
        self.logger.info("Agent execution started", extra={"json_fields": {"userRequest": label[:100]}})

        stats = ExecutionStats(model=str(self.params["model"]))
        trace = TraceCollector(
            config=TraceCollectorConfig(
                user_request=label,
                model=str(self.params["model"]),
                system_prompt=await self._system(),
                bucket_name=self.bucket_name,
                database=self.database,
                logger=self.logger,
                local_traces_dir=self._local_traces_dir,
            )
        )

        turn = TurnResult(message_to_user=None, has_tool_calls=False)
        try:
            while True:
                turn = await self._run_turn(stats, trace)
                if not turn.has_tool_calls:
                    break
        except Exception as e:
            trace.finalize(stats, error=str(e))
            raise

        await self.on_event(Completed(response=turn.message_to_user))

        self.logger.info(
            "Agent execution complete",
            extra={"json_fields": {"userRequest": label[:100], "traceId": trace.id, **stats.for_logs().model_dump()}},
        )

        result = AgentExecuteResult(
            trace_id=trace.id,
            response=turn.message_to_user,
            total_input_tokens=stats.input_tokens,
            total_output_tokens=stats.output_tokens,
            turn_count=stats.turn_count,
            tool_call_count=stats.tool_calls,
            total_cost=stats.cost,
            context_tokens=stats.last_input_tokens,
        )

        trace.finalize(stats, result)
        if self._await_trace:
            await trace.wait()

        return result
