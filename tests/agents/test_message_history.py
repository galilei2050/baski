"""The judge must see the whole tool call, or it grades a guess.

`format_for_judge` clipped every call's arguments at 200 characters. A real `create_estimate` call
serializes to ~1100, so the judge saw a payload with the last key gone and the JSON unclosed, drew the
only conclusion available — the work is unfinished — and its feedback went back to the agent as an
instruction. Three production runs re-called the tool and opened a second work order for one phone call
(ShopMonkey #4231+#4232, #4249+#4250, #4266+#4267). Two of those verdicts said outright that the payload
they were shown was truncated.
"""

from anthropic.types import ToolUseBlock

from baski.agents.message_history import InMemoryMessageHistory


def test_judge_sees_the_last_argument_of_a_long_call() -> None:
    note = "Customer reports a grinding noise from the front left wheel under braking. " * 12
    call = ToolUseBlock(
        id="toolu_01",
        type="tool_use",
        name="create_estimate",
        input={"complaint": note, "vehicle": "2015 Honda Civic", "note": note, "authorized": True},
    )
    history = InMemoryMessageHistory()
    with history as turn:
        turn.add_user_text("Book the estimate")
        turn.add_assistant([call])

    transcript = history.format_for_judge()

    assert len(transcript) > 1_000, "a call this size must not be summarized away"
    assert '"authorized": true' in transcript, "the last argument is what the judge checks was passed"
    assert transcript.endswith("})"), "an unclosed payload reads to the judge as a malformed call"
