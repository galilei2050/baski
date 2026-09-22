import asyncio

import pytest

from baski.pattern.exponential_backoff import UnavailableError, retry


@pytest.fixture
def slept(monkeypatch):
    recorded = []

    async def record(seconds):
        recorded.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", record)
    return recorded


@pytest.mark.asyncio
async def test_exhausted_retry_does_not_sleep_before_raising(slept):
    async def always_fails():
        raise ConnectionError("down")

    with pytest.raises(UnavailableError):
        await retry(always_fails, exceptions=(ConnectionError,), times=3, wait_time_fn=lambda _e, i, _a, _b: i * 1000)

    assert slept == [1.0, 2.0], "three attempts means two waits between them, not three"


@pytest.mark.asyncio
async def test_success_after_a_failure_still_waits_once(slept):
    attempts = []

    async def fails_once():
        attempts.append(1)
        if len(attempts) == 1:
            raise ConnectionError("down")
        return "ok"

    result = await retry(
        fails_once, exceptions=(ConnectionError,), times=3, wait_time_fn=lambda _e, i, _a, _b: i * 1000
    )

    assert result == "ok"
    assert slept == [1.0]
