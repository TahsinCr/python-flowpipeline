[← Documentation Map](README.md) · [Previous: 02 · Pipelines and Middleware](pipelines-and-middleware.md) · [Next: 04 · API Reference →](api-reference.md)

# 03 · AsyncStream, Backpressure, and Lifecycle

This guide covers `AsyncStream`'s lazy pull model, operator fusion, backpressure trade-offs across `map`, `filter`, `batch`, `batch_within`, and `buffer`, and leak-free shutdown guarantees.

---

## Table of Contents

- [The Problem: Unbounded Push Buffering and Resource Leaks](#the-problem-unbounded-push-buffering-and-resource-leaks)
- [Lazy Pull Model and Operator Fusion](#lazy-pull-model-and-operator-fusion)
- [Operators and Engineering Trade-Offs](#operators-and-engineering-trade-offs)
  - [`map` and `filter`](#map-and-filter)
  - [`batch(size, partial=True)`](#batchsize-partialtrue)
  - [`batch_within(max_items, max_delay, partial=True)`](#batch_withinmax_items-max_delay-partialtrue)
  - [`buffer(capacity)` — When It Helps vs When It Hurts](#buffercapacity--when-it-helps-vs-when-it-hurts)
- [Single-Consumer Ownership and Lifecycle (`async with`)](#single-consumer-ownership-and-lifecycle-async-with)
- [Error Propagation and Cleanup Ordering](#error-propagation-and-cleanup-ordering)

---

## The Problem: Unbounded Push Buffering and Resource Leaks

When an async producer yields faster than the consumer can process, unbounded buffering causes memory spikes (OOM). Worse, breaking out of an `async for` loop early or hitting an error without calling `aclose()` frequently leaves async generators, network sockets, and prefetch background tasks dangling.

`AsyncStream` uses **demand-driven pull execution** so producers only advance as downstream consumes, and bounds any explicit buffering strictly.

[Back to top](#table-of-contents)

---

## Lazy Pull Model and Operator Fusion

Constructing `AsyncStream(source).map(...).filter(...)` starts **zero background tasks**:

```mermaid
flowchart LR
    Src["AsyncIterable Source"] --> Read["__anext__()"]
    Read --> Op1["operator 1: map"]
    Op1 --> Op2["operator 2: filter"]
    Op2 --> Consumer["Consumer (async for)"]
```

- Consecutive `map` and `filter` transformations fuse into a single tuple of operators attached directly to the root stream, avoiding intermediate wrapper tasks or queues.
- Both synchronous callables (`lambda x: x * 2`) and awaitables (`async def enrich(x): ...`) work in `map` and `filter`. (`filter` requires exact `bool` returns; non-boolean truthy values raise `TypeError`.)

[Back to top](#table-of-contents)

---

## Operators and Engineering Trade-Offs

### `map` and `filter`

- Zero background tasks; executed sequentially inside the consumer's read step.
- Synchronous transformations returning basic non-awaitable types (`int`, `bool`, `str`, `None`, etc.) skip generic awaitable inspection on the hot path.

### `batch(size, partial=True)`

- Accumulates up to `size` items into a `tuple` with zero background tasks.
- `partial` controls whether any remaining shorter tuple is yielded when the source completes (`True` by default).

### `batch_within(max_items, max_delay, partial=True)`

Designed for workflows like bulk database writes where you want full batches under load without stalling indefinitely when traffic trickles in:

- Spawns one producer task writing into a single demand-gated pending slot, keeping buffered state bounded to one active batch plus one in-flight source item.
- Schedules **one monotonic timer (`loop.call_at`) per batch**, not per item.

### `buffer(capacity)` — When It Helps vs When It Hurts

`buffer(capacity)` decouples producer and consumer via `asyncio.Queue(maxsize=capacity)`:

- When full, `await queue.put(item)` blocks the producer (**backpressure**); items are never dropped.
- **When to use:** Your source awaits network/disk I/O *and* your consumer also performs async work, allowing the next item to prefetch concurrently.
- **When to avoid:** If your source is already fast in-memory data and the consumer is fast, adding an `asyncio.Queue` task handoff on every item costs more than it saves.

```python
import asyncio
from flowpipeline import AsyncStream


async def fetch_pages():
    for page in range(1, 5):
        await asyncio.sleep(0.01)  # Simulated I/O latency
        yield page


async def main() -> None:
    stream = AsyncStream(fetch_pages()).buffer(2).batch_within(3, 0.05)
    async with stream:
        batches = [b async for b in stream]
    assert sum(len(b) for b in batches) == 4


asyncio.run(main())
```

[Back to top](#table-of-contents)

---

## Single-Consumer Ownership and Lifecycle (`async with`)

`AsyncStream` enforces strict **single-consumer**, **single-task** ownership:

- A stream can only be consumed once; a second `aiter()` call raises `RuntimeError("AsyncStream supports one consumption only")`.
- Concurrent `__anext__` calls across tasks (e.g., `asyncio.gather(anext(s), anext(s))`) raise `RuntimeError`.
- Each stream binds to its owning event loop and rejects cross-loop access.
- **Always wrap the terminal stream in `async with stream:`:** Even on early `break` or exceptions, exiting the block awaits `stream.aclose()`, which cancels any active read, stops producer tasks, and closes upstream iterators (`aclose()`) cleanly.
- Calling `await stream.aclose()` from inside the same task currently awaiting `__anext__` raises `RuntimeError` to prevent self-deadlock.

[Back to top](#table-of-contents)

---

## Error Propagation and Cleanup Ordering

- Under `buffer()` or `batch_within()`, if the source fails after producing earlier items, **queued items are delivered first**, and the source exception is raised once the queue drains.
- Natural exhaustion (`StopAsyncIteration`) triggers cleanup automatically.
- If `aclose()` raises while handling an upstream error, the primary error is preserved with the cleanup error chained (`__cause__`), unless cleanup raises a non-`Exception` `BaseException` such as `CancelledError`.
- Repeated cancellation during `aclose()` cannot abandon producer shutdown (`finish_cleanup` awaits cleanup completion).

[Back to top](#table-of-contents)

---

**Previous:** [02 · Pipelines, Middleware, and Observability](pipelines-and-middleware.md) · **Next:** [04 · API Reference →](api-reference.md)
