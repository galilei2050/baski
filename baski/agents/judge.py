"""LLM-as-judge: a second model grades whether the agent FINISHED the user's request.

A provider-agnostic `Judge` the `Agent` calls at the exit of its loop, plus a Gemini/Vertex
implementation. Cross-family by design — a judge from a different model family has decorrelated
blind spots, so it catches completion failures the executor is prone to wave through. It grades
COMPLETENESS of the deliverable, not factual truth (transcript-checkable without domain knowledge).
"""

import logging
from typing import Protocol

from google import genai
from google.genai import types as ai_types
from pydantic import BaseModel

logger = logging.getLogger(__name__)

DEFAULT_JUDGE_MODEL = "gemini-3-flash-preview"  # cheap, fast, different family from the Opus executor

_DEFAULT_INSTRUCTIONS = """\
You grade whether an assistant FULLY completed the user's request. Judge COMPLETENESS of the \
deliverable, not factual correctness.

Mark finished=false if the answer:
- omits any sub-deliverable the user explicitly asked for (e.g. prices, links, clock times, a table \
or model that was requested) and it is absent or vague,
- stops at research or options without assembling the concrete artifact requested,
- ends by asking the user a question or offering to continue instead of delivering.

List exactly what is missing, and write one block of actionable feedback to finish it now."""

_SCHEMA = {
    "type": "object",
    "properties": {
        "finished": {"type": "boolean"},
        "missing": {"type": "array", "items": {"type": "string"}},
        "feedback": {"type": "string"},
    },
    "required": ["finished", "missing", "feedback"],
}


class Verdict(BaseModel):
    """Whether an answer fully delivers the request, and — if not — what's missing and how to fix it."""

    finished: bool
    missing: list[str]
    feedback: str


class Judge(Protocol):
    """Grades an answer against the request. The Agent calls this at the exit of its loop."""

    async def evaluate(self, request: str, answer: str) -> Verdict:
        """Return a completeness verdict on `answer` for `request`."""
        ...


def retry_prompt(verdict: Verdict) -> str:
    """The feedback turn fed back into the loop to make the agent finish (used when not finished)."""
    missing = "; ".join(verdict.missing) or verdict.feedback
    return (
        f"[Completeness check] Your previous answer is not finished. Missing: {missing}. "
        f"{verdict.feedback} Deliver the complete result now — no questions, no offers to continue."
    )


class GeminiJudge(Judge):
    """Gemini/Vertex completeness judge. Holds one client for the process (built once, reused per call).

    Lifecycle: long-lived — construct once and share across conversations. ADC auth, no API key.
    """

    def __init__(
        self,
        *,
        project: str,
        location: str = "global",
        model: str = DEFAULT_JUDGE_MODEL,
        instructions: str = _DEFAULT_INSTRUCTIONS,
    ) -> None:
        """Build the shared Vertex client once."""
        self._client = genai.Client(vertexai=True, project=project, location=location)
        self._model = model
        self._instructions = instructions

    async def evaluate(self, request: str, answer: str) -> Verdict:
        """Ask the judge model whether `answer` fully delivers what `request` asked for."""
        prompt = f"{self._instructions}\n\nUSER REQUEST:\n{request}\n\nASSISTANT ANSWER:\n{answer}"
        response = await self._client.aio.models.generate_content(
            model=self._model,
            contents=prompt,
            config=ai_types.GenerateContentConfig(response_mime_type="application/json", response_schema=_SCHEMA),
        )
        if response.text is None:
            raise RuntimeError("Judge model returned no content")
        verdict = Verdict.model_validate_json(response.text)
        logger.info("Judge verdict", extra={"finished": verdict.finished, "missing": verdict.missing})
        return verdict
