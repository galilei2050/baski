# Agent Framework

Agentic framework for building LLM agents with tool use, built on Anthropic's Claude API.
Ported from clarity-auto-care; the generic core lives here so other projects (e.g. nisse) reuse it.

## Core Components

- `Agent`: Main agent loop — streaming responses, parallel tool execution, conversation flow. Creates `ToolBox` internally. Auto-injects `KnowledgeTool` and `DeleteMessagesTool`.
- `ToolBox`: Registry + parallel executor (`asyncio.gather`) for a set of tools.
- `Tool`: Abstract base — define `name`, `one_line`, `description`, `input_schema`, implement `async execute(**kwargs) -> str`.
- `MessageHistory`: Conversation history with context-window truncation (64k tokens default, truncates at 90%).
- `TraceCollector` (`trace.py`): Persists turn-by-turn traces to GCS (gzipped JSON) + a Mongo `traces` summary collection.
- `AgentExecuteResult`: Pydantic result (trace id, tokens, turn/tool counts, cost).

## Built-in Tools (`tools/`)

Auto-injected by `Agent`:
- `KnowledgeTool` (`store_knowledge`): preserve facts across context truncation.
- `DeleteMessagesTool` (`delete_messages`): drop turns by id to free context.

Opt-in, generic (pass via `AgentConfig.tools`):
- `GoogleSearchTool`, `YelpSearchTool`, `GooglePlayTool`, `AppleAppStoreTool` — via `baski.clients.serpapi_client`.
- `WebBrowseTool` — via `baski.clients.playwright_client`.

Project-specific tools (DB lookups, formatters) stay in the consuming project, NOT here.

## Dependencies on baski

Imports map to baski foundation: `baski.primitives.datetime`, `baski.primitives.json`,
`baski.concurrent.as_async`, `baski.server.Logger`, `baski.clients.*`.

## Usage

```python
from baski.agents import Agent, AgentConfig
from baski.agents.tools import GoogleSearchTool

config = AgentConfig(
    logger=logger,
    tools=[GoogleSearchTool(serpapi_client=serpapi_client)],
    anthropic_client=anthropic_client,
    database=database,          # pymongo AsyncDatabase — trace summaries
    bucket_name="my-traces",    # GCS bucket for full traces (parameterized, no hardcoded default)
    # model defaults to DEFAULT_MODEL ("claude-opus-4-8"); override per-agent if needed
)
result = await Agent(config=config).execute("user request")
```

## Notes

- Tracing is mandatory: every `execute()` requires a Mongo `database` and a GCS `bucket_name`.
- Extended thinking is `adaptive`; parallel tool use enabled; `max_tokens=128_000`.
- The clarity FastAPI trace-browser router (`router.py` / `get_trace_flow.py`) was NOT ported — it is deployment-specific. Add a thin router in the consuming project if a trace UI is needed.
