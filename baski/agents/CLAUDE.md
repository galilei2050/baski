# Agent Framework

Agentic framework for building LLM agents with tool use, built on Anthropic's Claude API.
Ported from clarity-auto-care; the generic core lives here so other projects (e.g. nisse) reuse it.

## Core Components

- `Agent` (`agent.py`): the agentic loop. Constructs nothing — the caller passes collaborators via `AgentConfig` plus an `on_event` listener.
- `ToolSet` (`toolset.py`): tool registry + parallel executor; validates each call against the tool's `Input` model; `system_prompt()` aggregates the roster + per-tool contributions.
- `Tool` (`tool.py`): abstract base. A tool declares `name`/`one_line`/`description`, a nested `Input(BaseModel)`, and `async execute(**kwargs) -> str | ToolResult`; optional `system_prompt()` (static guidance into the system prompt) and `user_message()` (a per-turn user block injected at the top — short-term facts, a memory index, a skill body; default None). The Agent collects `user_message()` from every tool, so injection needs no dedicated config field. **`ToolResult`** (`tool.py`): the richer return — `content` plus `cost` (USD this call spent) and `sub_trace_ids` (traces of agents it spawned). A plain `str` return means cost 0, no sub-traces; a delegating tool returns its child's cost + trace so the agent folds cost into the turn and the trace tree links up.
- `MessageHistory` (`message_history.py`): **Protocol** the Agent drives; `InMemoryMessageHistory` is the default volatile implementation (token-budget truncation). Durability = a separate implementation of the Protocol (e.g. nisse's Mongo-backed one), never a subclass of the in-memory one. `delete_turns` is `async` so a durable impl can persist the removal. `turns` is exposed **read-only** (a covariant `Sequence[Turn]`, so an impl may store a `Turn` subclass) — mutate only through the contract methods. The token budget is NOT exposed; the agent asks `initial_context_too_large(input_tokens)` instead.
- `TraceCollector` (`trace.py`): turn-by-turn traces → GCS + Mongo `traces`. Sub-agent linkage: a delegating tool returns `ToolResult(sub_trace_ids=[child.trace_id], cost=child.total_cost)`; `ToolSet` records those onto the parent's `ToolResultRecord.sub_trace_ids` and `last_costs`. One root trace id then walks the whole delegation tree (main → researcher → retrieval), and `total_cost` covers the tree (the agent adds `sum(last_costs)` to each turn), not just the LLM — no ContextVar, no per-tool wiring beyond the return. **The Mongo summary must be able to answer "where did the money go" on its own** — the full trace is a gzipped blob nobody queries. So it carries `agent_name` (from the required `AgentConfig.name`), `sub_trace_ids`, both cache-token buckets, `own_cost` beside the tree `cost`, and `tools` — one `ToolUsageRecord` per tool with calls/errors/cost/duration and `output_chars`, the volume it put into context. A delegating sub-agent is a tool, so it appears there by name with what it spent. Add a new measure as a FIELD on `ToolUsageRecord`, never as a second parallel map.
- `AgentExecuteResult` (`execute_result.py`): pydantic result (trace id, tokens, counts, cost, `judge_verdicts`).
- `Judge` (`judge.py`): LLM-as-judge run at the loop's **exit** — a **required** `AgentConfig.judge` (a `Judge` protocol). The single loop in `execute()` keeps going while the model calls tools; the first tool-free turn is graded for COMPLETENESS — the judge sees the current time, a compact transcript (`MessageHistory.format_for_judge()`: user/assistant text + `[tool] name(args)` markers, tool *outputs* omitted to stay a completeness check, not a fact-check) and the agent's own system prompt + tool-injected rules as the owner's standards (`execute()` calls `evaluate(transcript, answer, rules)` with `rules = await self._system()`). On `finished=false` the verdict's feedback is fed back as a user turn so the same loop redoes the work, capped by `judge_max_retries`. `GeminiJudge` is the cross-family (Gemini/Vertex, ADC — no key) impl; it holds one client, so construct once and share. Each check emits a `Judged` event (for live UIs) and the ordered verdicts ride `AgentExecuteResult.judge_verdicts`. Grades completeness, not truth (transcript-checkable).

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
- **Turn budget** (`AgentConfig.max_turns`, default 40): a hard ceiling on the agentic loop. Every turn a volatile `[Turn n/max …]` line is injected (in `_build_messages`' trailing section, after the cache breakpoint) so the agent paces itself; the final allowed turn runs with `tool_choice=none` (`_run_turn` → `disable_tools`) so a runaway investigation always terminates in a synthesized answer, not more tool calls. Give a sub-agent a tighter value to keep research shallow. Independent of `judge_max_retries`.
- Extended thinking is `adaptive`; parallel tool use enabled; `max_tokens=128_000`.
- **Prompt caching**: cache breakpoints set in `agent.py` (tools, system) and `format_for_api` (history, via `mark_cached`). Invariant when adding anything to the prompt — volatile/per-turn content (time, context footer, `user_message()` injections) must go AFTER the last breakpoint; keep it in `_build_messages`' trailing section, never inside `format_for_api`. `mark_cached` must stay copy-safe (mutating a stored turn would persist `cache_control`).
- Size context only via `pricing.effective_input_tokens` — caching hides cached tokens from `usage.input_tokens`, so a `truncate` reading it raw never trims.
- **A model missing from `MODEL_PRICING` raises** (at `ExecutionStats` construction, before the run spends anything) instead of falling back to a default rate. The fallback it replaces billed every `claude-opus-5` call at Sonnet's price and understated a month of spend by 1.67x, with nothing in the number to show it was a guess. Adding a model to the agent means adding its price.
- **A tool's output must be bounded by the caller, not by the site** — `WebBrowseTool` takes a required `max_chars` and appends a visible "page cut" marker, since a silently truncated page reads exactly like a complete one. One measured fetch was 457,780 characters into a 32k-token agent.
- The clarity FastAPI trace-browser router was NOT ported — deployment-specific. Add a thin router in the consuming project if a trace UI is needed.
