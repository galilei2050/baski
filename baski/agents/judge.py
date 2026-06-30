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
from pydantic import BaseModel, Field

from baski.primitives import datetime

logger = logging.getLogger(__name__)

DEFAULT_JUDGE_MODEL = "gemini-3.5-flash"  # cheap, fast, GA, different family from the Opus executor

_DEFAULT_INSTRUCTIONS = """\
You grade whether an assistant's answer is DONE. Be CONSERVATIVE: a redo regenerates the entire answer
(expensive, and the owner sees a near-duplicate), so demand a redo ONLY for a MATERIAL problem. When in
doubt, pass (finished=true).

Not every message is a task. A casual remark, reaction, acknowledgement, greeting or thanks is fully
answered by a brief, relevant reply — when the user asked for nothing, demand NO tools, research, or
deliverable, and treat a friendly "tell me if you want more" as a fine sign-off, not a punt. Pass it.

Mark finished=false ONLY if the answer:
- omits a concrete sub-deliverable the request explicitly asked for (e.g. prices, links, a table/model) \
and it is absent,
- stops at research or options without assembling the artifact that was requested,
- presents specific figures or money-making "opportunities" with NO sourcing at all (a fabricated \
arbitrage, a number from nowhere, a plausible-sounding niche with nothing behind it),
- answers an investigative / advisory / comparative ask ("какие способы…", "что мне сделать чтобы…", \
"подумай…", "сравни…") with generic, obvious-tier options and NO concrete sourced specifics — when the \
ask plainly warranted reading sources and comparing (depth must match the ask, below), or
- withholds the requested work to ask permission or punt it back ("want me to…?") instead of just doing \
it — but an answer that already delivers the work in full and merely ends with a trailing offer or \
courtesy is DONE; do NOT redo just to strip that line.

The `[tool]` lines show every tool the assistant ran WITH its arguments, but NOT the outputs — so do not
treat a tool call as automatic proof of enough work; match DEPTH to the ask. A casual remark, a closed fact
lookup, or a current-events question is DONE by a direct answer or a search or two — demand no more. An open
investigative / advisory / comparative ask warrants reading sources and comparing, so a generic answer with
no concrete sourced specifics is incomplete — but one already carrying named sources, real figures, or a
genuine comparison IS done; never redo for more depth or for style.

You grade COMPLETENESS, not truth — you CANNOT verify the actual values. A concrete or recent claim backed
by a relevant `[tool]` call or a source cited in the answer is grounded: NEVER call it fabricated, made-up,
or "from the future." Your own training cutoff is NOT the current date (given above) — tool-sourced or cited
data dated later than what you happen to know is REAL, not a hallucination and not a date error. Flag
fabrication ONLY for a concrete factual/numeric claim with NO supporting tool call AND NO cited source.

A substantively complete, grounded answer is DONE even if its wording, formatting, length, structure, or
persona/voice are imperfect — do NOT redo for style. An honest "I can't do X without Y" is also DONE, NOT
a punt: when the assistant made a real attempt and then asks for input it genuinely cannot get itself (an
inaccessible source, a missing constraint or decision only the owner can supply), that IS the complete,
correct answer — pass it. The owner's rules below inform your read of the substance, but only a MATERIAL
violation (a missing asked deliverable, an ungrounded claim) warrants a redo.

When finished=false, list exactly what is materially missing, and write `feedback` as a DIRECT
INSTRUCTION to the assistant — imperative, what to DO next (e.g. "Добавь ссылки для брони и время по
часам", "Убери неподтверждённый тезис про арбитраж или подтверди его поиском"), not a description of the
problem. Write `missing` and `feedback` in the SAME LANGUAGE as the user request (shown to the owner)."""


class Verdict(BaseModel):
    """Whether an answer fully delivers the request, and — if not — what's missing and how to fix it.

    Doubles as the judge's structured-output schema — Gemini reads these field descriptions, so they
    are part of the prompt, not just docs.
    """

    finished: bool = Field(
        description="True if the latest reply fully handles the user's message, or there was no task to do. "
        "False ONLY for a material gap."
    )
    missing: list[str] = Field(
        description="Concrete deliverables the user asked for that are absent. Empty when finished. "
        "Same language as the user request."
    )
    feedback: str = Field(
        description="A direct, imperative instruction telling the assistant what to do next to finish. "
        "Empty when finished. Same language as the user request."
    )


class Judge(Protocol):
    """Grades an answer against the conversation. The Agent calls this at the exit of its loop."""

    async def evaluate(self, transcript: str, answer: str, rules: str) -> Verdict:
        """Return a completeness verdict on `answer` (the latest reply), read in `transcript`'s context."""
        ...


def retry_prompt(verdict: Verdict) -> str:
    """The feedback turn fed back into the loop to make the agent finish — the judge's instruction."""
    return (
        f"[Completeness check] Your answer isn't finished. {verdict.feedback} "
        f"Deliver the complete result now — no questions, no offers to continue."
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

    async def evaluate(self, transcript: str, answer: str, rules: str) -> Verdict:
        """Ask the judge whether `answer` finishes the request, read in the conversation's context."""
        now = datetime.now()
        prompt = (
            f"Current time: {now.strftime('%A, %B %d, %Y %I:%M %p %Z')}\n\n"
            f"{self._instructions}\n\n"
            f"<owner_rules>\n{rules}\n</owner_rules>\n\n"
            f"<conversation>\n{transcript}\n</conversation>\n\n"
            f"<reply_to_grade>\n{answer}\n</reply_to_grade>\n\n"
            "Grade <reply_to_grade> for completeness, read in the context of <conversation>."
        )
        response = await self._client.aio.models.generate_content(
            model=self._model,
            contents=prompt,
            config=ai_types.GenerateContentConfig(response_mime_type="application/json", response_schema=Verdict),
        )
        if response.text is None:
            raise RuntimeError("Judge model returned no content")
        verdict = Verdict.model_validate_json(response.text)
        logger.info("Judge verdict", extra={"finished": verdict.finished, "missing": verdict.missing})
        return verdict
