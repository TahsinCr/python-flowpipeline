"""Share event-loop ownership and cleanup operations across async components."""

import asyncio
import inspect
from collections.abc import Awaitable, Coroutine
from typing import Any, TypeVar

T = TypeVar("T")


def create_task_safely(
    coroutine: Coroutine[Any, Any, T],
    *,
    name: str | None = None,
    loop: asyncio.AbstractEventLoop | None = None,
) -> asyncio.Task[T]:
    """Close a fresh coroutine if its event loop rejects task creation."""
    try:
        if loop is None:
            return asyncio.create_task(coroutine, name=name)
        return loop.create_task(coroutine, name=name)
    except BaseException:
        coroutine.close()
        raise


def ensure_future_safely(operation: Awaitable[T]) -> asyncio.Future[T]:
    """Close a coroutine if its event loop rejects scheduling it."""
    try:
        return asyncio.ensure_future(operation)
    except BaseException:
        if inspect.iscoroutine(operation):
            operation.close()
        raise


def bind_running_loop(
    current: asyncio.AbstractEventLoop | None, owner: str
) -> asyncio.AbstractEventLoop:
    """Bind to the active loop or reject use from another loop."""
    loop = asyncio.get_running_loop()
    if current is not None and current is not loop:
        raise RuntimeError(f"{owner} belongs to a different event loop")
    return loop


async def finish_cleanup(operation: Awaitable[T]) -> T:
    task = ensure_future_safely(operation)
    if task is asyncio.current_task():
        raise RuntimeError("Cleanup cannot await itself")
    cancellation: asyncio.CancelledError | None = None
    if not task.done():
        completed = asyncio.Event()
        task.add_done_callback(lambda _task: completed.set())
        while not task.done():
            try:
                await completed.wait()
            except asyncio.CancelledError as exc:
                cancellation = exc
    try:
        result = task.result()
    except BaseException as error:
        if cancellation is not None:
            raise cancellation from error
        raise
    if cancellation is not None:
        raise cancellation
    return result
