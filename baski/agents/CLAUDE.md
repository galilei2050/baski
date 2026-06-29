# Agent Framework

Agentic framework for building LLM agents with tool use, built on Anthropic's Claude API.
Ported from clarity-auto-care; the generic core lives here so other projects (e.g. nisse) reuse it.

## Core Components

- `Agent` (`agent.py`): the agentic loop. Constructs nothing — the caller passes collaborators via `AgentConfig` plus an `on_event` listener.
- `ToolSet` (`toolset.py`): tool registry + parallel executor; validates each call against the tool's `Input` model; `system_prompt()` aggregates the roster + per-tool contributions.
- `Tool` (`tool.py`): abstract base. A tool declares `name`/`one_line`/`description`, a nested `Input(BaseModel)`, and `async execute(**kwargs) -> str`; optional `system_prompt()` (static guidance into the system prompt) and `user_message()` (a per-turn user block injected at the top — short-term facts, a memory index, a skill body; default None). The Agent collects `user_message()` from every tool, so injection needs no dedicated config field.
- `MessageHistory` (`message_history.py`): **Protocol** the Agent drives; `InMemoryMessageHistory` is the default volatile implementation (token-budget truncation). Durability = a separate implementation of the Protocol (e.g. nisse's Mongo-backed one), never a subclass of the in-memory one. `delete_turns` is `async` so a durable impl can persist the removal. `turns` is exposed **read-only** (a covariant `Sequence[Turn]`, so an impl may store a `Turn` subclass) — mutate only through the contract methods. The token budget is NOT exposed; the agent asks `initial_context_too_large(input_tokens)` instead.
- `TraceCollector` (`trace.py`): turn-by-turn traces → GCS + Mongo `traces`.
- `AgentExecuteResult` (`execute_result.py`): pydantic result (trace id, tokens, counts, cost, `judge_verdicts`).
- `Judge` (`judge.py`): LLM-as-judge run at the loop's **exit** — a **required** `AgentConfig.judge` (a `Judge` protocol). The single loop in `execute()` keeps going while the model calls tools; the first tool-free turn is graded for COMPLETENESS, and on `finished=false` the verdict's feedback is fed back as a user turn so the same loop redoes the work, capped by `judge_max_retries`. `GeminiJudge` is the cross-family (Gemini/Vertex, ADC — no key) impl; it holds one client, so construct once and share. Each check emits a `Judged` event (for live UIs) and the ordered verdicts ride `AgentExecuteResult.judge_verdicts`. Grades completeness, not truth (transcript-checkable).

## Built-in Tools (`tools/`)

The caller wires these into the `ToolSet` (not auto-injected):
- `ShortTermMemory` (`working_note`): WORKING MEMORY — per-reply fact scratchpad; injects its facts via `user_message()`.
- `DeleteMessagesTool` (`prune_transcript`): drop turns by id; built with the shared `MessageHistory`.
- `GoogleSearchTool`, `YelpSearchTool`, `GooglePlayTool`, `AppleAppStoreTool` (SerpApi), `WebBrowseTool` (Playwright).

Project-specific tools stay in the consuming project, NOT here. Canonical assembly: nisse `app/assistant/assistant.py`.

## Dependencies on baski

`baski.primitives.{datetime,json}`, `baski.concurrent.as_async`, `logging.getLogger(__name__)` (stdlib; see http/CLAUDE.md "Logging"), `baski.clients.*`.

## Notes

- Tracing is mandatory: every `execute()` requires a Mongo `database` and a GCS `bucket_name`.
- Extended thinking is `adaptive`; parallel tool use enabled; `max_tokens=128_000`.
- **Prompt caching**: cache breakpoints set in `agent.py` (tools, system) and `format_for_api` (history, via `mark_cached`). Invariant when adding anything to the prompt — volatile/per-turn content (time, context footer, `user_message()` injections) must go AFTER the last breakpoint; keep it in `_build_messages`' trailing section, never inside `format_for_api`. `mark_cached` must stay copy-safe (mutating a stored turn would persist `cache_control`).
- Size context only via `pricing.effective_input_tokens` — caching hides cached tokens from `usage.input_tokens`, so a `truncate` reading it raw never trims.
- The clarity FastAPI trace-browser router was NOT ported — deployment-specific. Add a thin router in the consuming project if a trace UI is needed.
