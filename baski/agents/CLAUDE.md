# Agent Framework

Agentic framework for building LLM agents with tool use, built on Anthropic's Claude API.
Ported from clarity-auto-care; the generic core lives here so other projects (e.g. nisse) reuse it.

## Core Components

- `Agent` (`agent.py`): the agentic loop. Constructs nothing — the caller passes collaborators via `AgentConfig` plus an `on_event` listener.
- `ToolSet` (`toolset.py`): tool registry + parallel executor; validates each call against the tool's `Input` model; `system_prompt()` aggregates the roster + per-tool contributions.
- `Tool` (`tool.py`): abstract base. A tool declares `name`/`one_line`/`description`, a nested `Input(BaseModel)`, and `async execute(**kwargs) -> str`; optional `system_prompt()`.
- `MessageHistory` (`message_history.py`): transcript with context-window truncation.
- `TraceCollector` (`trace.py`): turn-by-turn traces → GCS + Mongo `traces`.
- `AgentExecuteResult` (`execute_result.py`): pydantic result (trace id, tokens, counts, cost).

## Built-in Tools (`tools/`)

The caller wires these into the `ToolSet` (not auto-injected):
- `ShortTermMemory` (`store_memory`): per-run fact scratchpad; also passed as `AgentConfig.short_term_memory`.
- `DeleteMessagesTool` (`delete_messages`): drop turns by id; built with the shared `MessageHistory`.
- `GoogleSearchTool`, `YelpSearchTool`, `GooglePlayTool`, `AppleAppStoreTool` (SerpApi), `WebBrowseTool` (Playwright).

Project-specific tools stay in the consuming project, NOT here. Canonical assembly: nisse `app/assistant/assistant.py`.

## Dependencies on baski

`baski.primitives.{datetime,json}`, `baski.concurrent.as_async`, `baski.server.Logger`, `baski.clients.*`.

## Notes

- Tracing is mandatory: every `execute()` requires a Mongo `database` and a GCS `bucket_name`.
- Extended thinking is `adaptive`; parallel tool use enabled; `max_tokens=128_000`.
- The clarity FastAPI trace-browser router was NOT ported — deployment-specific. Add a thin router in the consuming project if a trace UI is needed.
