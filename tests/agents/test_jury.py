"""A panel of judges, not one judge wearing another as a coat.

An agent has more than one reason to send an answer back, and they are different kinds of thing: a
model's judgement of completeness, and arithmetic over the run's own tool calls. Composition has to
be visible where the panel is assembled, not buried in a decorator.
"""

import pytest

from baski.agents.judge import Judge, JudgeUnavailableError, Jury, Verdict


class _Says(Judge):
    """A judge with a fixed opinion."""

    def __init__(self, *, finished: bool, missing: str = "", feedback: str = "") -> None:
        self._verdict = Verdict(finished=finished, missing=[missing] if missing else [], feedback=feedback)

    async def evaluate(self, transcript: str, answer: str, rules: str) -> Verdict:
        """Return the opinion it was built with."""
        return self._verdict


class _Unavailable(Judge):
    """A judge that is down, which is not the same as a judge that approves."""

    async def evaluate(self, transcript: str, answer: str, rules: str) -> Verdict:
        """Fail the way a quota or a 5xx does."""
        raise JudgeUnavailableError("quota")


async def _verdict(*judges: Judge) -> Verdict:
    """Run the panel over one answer."""
    return await Jury(list(judges)).evaluate(transcript="q", answer="a", rules="r")


@pytest.mark.asyncio
async def test_one_objection_is_enough_to_send_the_answer_back() -> None:
    verdict = await _verdict(_Says(finished=True), _Says(finished=False, missing="cite what you read"))

    assert verdict.finished is False


@pytest.mark.asyncio
async def test_every_objection_is_kept() -> None:
    """The worker fixes what it is told; a dropped objection is a turn spent on half the problem."""
    verdict = await _verdict(
        _Says(finished=False, missing="the county count", feedback="find the CBP figure"),
        _Says(finished=False, missing="two unopened sources", feedback="open them"),
    )

    assert verdict.missing == ["the county count", "two unopened sources"]
    assert verdict.feedback == "find the CBP figure\nopen them"


@pytest.mark.asyncio
async def test_a_unanimous_pass_passes() -> None:
    verdict = await _verdict(_Says(finished=True), _Says(finished=True))

    assert verdict.finished is True
    assert verdict.missing == []


@pytest.mark.asyncio
async def test_a_judge_that_is_down_does_not_read_as_approval() -> None:
    """The loop already decides what to do with an outage; swallowing it here would hide one."""
    with pytest.raises(JudgeUnavailableError):
        await _verdict(_Says(finished=True), _Unavailable())


def test_an_empty_panel_is_a_caller_bug() -> None:
    """A jury of nobody would pass every answer — silently, which is the worst way to pass."""
    with pytest.raises(ValueError, match="at least one judge"):
        Jury([])
