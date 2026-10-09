import asyncio

import pytest

from baski.on_exception import on_exception


@on_exception()
async def slow() -> str:
    await asyncio.sleep(10)
    return "done"


@on_exception()
async def boom() -> str:
    raise ValueError("handled by the decorator")


@pytest.mark.asyncio
async def test_wait_for_still_times_out():
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(slow(), timeout=0.01)


@pytest.mark.asyncio
async def test_cancelled_task_reports_itself_cancelled():
    task = asyncio.create_task(slow())
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert task.cancelled()


@pytest.mark.asyncio
async def test_listed_exception_is_still_handled():
    assert await boom() is None
