"""Result model returned by Agent.execute()."""

from pydantic import BaseModel, Field


class AgentExecuteResult(BaseModel):
    """Result from Agent.execute() containing response and execution metrics."""

    trace_id: str = Field(..., description="Trace ID for debugging — references the full trace in GCS")
    response: str | None = Field(None, description="Final text response from the agent to the user")
    total_input_tokens: int = Field(..., description="Total input tokens consumed across all API calls")
    total_output_tokens: int = Field(..., description="Total output tokens consumed across all API calls")
    turn_count: int = Field(..., description="Number of API calls (turns) in the agentic loop")
    tool_call_count: int = Field(..., description="Total number of tool calls across all turns")
    total_cost: float = Field(..., description="Total cost in USD for this execution")
    context_tokens: int = Field(..., description="Input tokens on the last API call — the current context-window size")
