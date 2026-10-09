import asyncio

import pytest

from baski.concurrent import concurrency, map_async
from baski.server.config import AppConfig


@pytest.fixture(autouse=True)
def _concurrency_of_five():
    AppConfig()["concurrency"] = 5
    concurrency.cache_clear()
    yield
    concurrency.cache_clear()


@pytest.mark.asyncio
async def test_results_follow_the_input_order():
    async def finish_in_reverse(i: int) -> int:
        await asyncio.sleep((5 - i) / 100)
        return i

    assert await map_async([0, 1, 2, 3, 4], finish_in_reverse) == [0, 1, 2, 3, 4]


@pytest.mark.asyncio
async def test_order_holds_across_batches():
    async def echo(i: int) -> int:
        await asyncio.sleep(0.01)
        return i

    assert await map_async(list(range(12)), echo) == list(range(12))


@pytest.mark.asyncio
async def test_timeout_cancels_what_is_still_running():
    cancelled = []

    async def hang(i: int) -> None:
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled.append(i)
            raise

    with pytest.raises(RuntimeError, match="Map async timeout"):
        await map_async([1, 2], hang, timeout=0.01)

    assert sorted(cancelled) == [1, 2]


@pytest.mark.asyncio
async def test_none_results_are_dropped():
    async def only_even(i: int) -> int | None:
        return i if i % 2 == 0 else None

    assert await map_async([0, 1, 2, 3], only_even) == [0, 2]
