[← Documentation Map](README.md) · [Next: 02 · Pipelines, Middleware, and Observability →](pipelines-and-middleware.md)

# 01 · Getting Started and Architecture

This guide covers installing `flowpipeline`, the real-world problems it solves compared to ad-hoc Python chains, its core architectural trade-offs, and minimal working examples.

---

## Table of Contents

- [Installation](#installation)
- [The Problem: Why Ad-Hoc Function Chains Break Down](#the-problem-why-ad-hoc-function-chains-break-down)
- [Architectural Decisions ("Why Is It Designed This Way?")](#architectural-decisions-why-is-it-designed-this-way)
  - [Same-Type Context Transformation (`Pipeline[T]`)](#same-type-context-transformation-pipelinet)
  - [Copy-on-Change Snapshot Compilation](#copy-on-change-snapshot-compilation)
  - [Strict Sync vs Async Separation](#strict-sync-vs-async-separation)
  - [When Not to Use FlowPipeline](#when-not-to-use-flowpipeline)
- [First Synchronous Pipeline](#first-synchronous-pipeline)
- [First Async Pipeline + Stream](#first-async-pipeline--stream)

---

## Installation

`flowpipeline` is pure Python, requires **Python 3.10 or newer**, and has zero runtime dependencies.

```bash
# Install from PyPI
python -m pip install flowpipeline

# Editable install from the repository root
python -m pip install -e .

# Include test and lint/type tools
python -m pip install -e ".[dev]"
```

[Back to top](#table-of-contents)

---

## The Problem: Why Ad-Hoc Function Chains Break Down

In small scripts, multi-step processing starts out simple:

```python
def process(payload: dict) -> dict:
    payload = validate(payload)
    payload = enrich(payload)
    payload = redact(payload)
    return payload
```

As an application matures, four issues emerge quickly:

1. **Cross-cutting boilerplate spreads everywhere:** Adding per-step latency metrics, correlation IDs, caching short-circuits, or identifying **exact step failures** turns clean functions into nested `try/except` and timer clutter.
2. **Step ordering becomes brittle:** Inserting a new plugin step *"after `validate` but before `enrich`"* requires editing shared control-flow code instead of declaring ordering constraints.
3. **Runtime toggling races with in-flight calls:** Disabling or hot-swapping a step while requests are running can expose half-mutated step lists to concurrent callers.
4. **Async generators leak on early exit:** Stopping early inside an `async for` loop or running background prefetch queues often leaves tasks and upstream network iterators unclosed when exceptions or `break` occur.

`flowpipeline` solves these problems explicitly while staying lightweight and dependency-free.

[Back to top](#table-of-contents)

---

## Architectural Decisions ("Why Is It Designed This Way?")

### Same-Type Context Transformation (`Pipeline[T]`)

Both `Pipeline[T]` and `AsyncPipeline[T]` enforce a **`T -> T`** transformation contract:

- Each stage takes a `context: T` object (such as a dataclass, `dict`, or primitive) and returns an updated `T`.
- **Why not arbitrary `A -> B -> C` stages inside `Pipeline`?** Because if a stage changes type at every step, calling `pipeline.disable("enrich")` or inserting middleware dynamically would break downstream type expectations. Keeping `T -> T` means any disabled stage simply passes `T` forward safely.
- When you need to project or reshape values between different types (`Event -> Record -> Batch`), `AsyncStream.map(...)` handles type changes cleanly.

### Copy-on-Change Snapshot Compilation

Every configuration change (`add`, `remove`, `replace`, `enable`, `disable`) acquires the registry lock (`RLock`), validates ordering constraints, recomputes the topological plan, and atomically swaps `_execution`:

- `run()` reads the current immutable plan without blocking configuration mutations.
- Updates made while calls are in flight affect **only subsequent `run()` calls**; active runs finish on a consistent snapshot.

### Strict Sync vs Async Separation

- `Pipeline` accepts only synchronous callbacks and never spawns a hidden `asyncio` event loop or background threads. Passing an `async def` callback raises `TypeError` immediately at registration.
- `AsyncPipeline` requires awaitable-returning stages and middleware, binds to the first `asyncio` loop that executes `run()`, and raises `RuntimeError` if later invoked from a different loop.

### When Not to Use FlowPipeline

- **Not a distributed workflow engine:** If steps must survive process crashes with durable checkpoints across machines, use Temporal, Prefect, or Celery.
- **Trivial static calls:** If you only have 2–3 fixed pure functions with no middleware, metrics, or runtime reconfiguration, calling them directly in Python is simpler and faster.

[Back to top](#table-of-contents)

---

## First Synchronous Pipeline

```python
from dataclasses import dataclass, replace
from flowpipeline import Pipeline, PipelineStats


@dataclass(frozen=True)
class RequestContext:
    text: str
    normalized: bool = False


stats = PipelineStats()
pipeline = Pipeline[RequestContext](observer=stats)


@pipeline.stage("trim")
def trim(ctx: RequestContext) -> RequestContext:
    return replace(ctx, text=ctx.text.strip())


@pipeline.stage("lowercase", after="trim")
def lowercase(ctx: RequestContext) -> RequestContext:
    return replace(ctx, text=ctx.text.lower(), normalized=True)


@pipeline.middleware("fallback")
def handle_empty(ctx: RequestContext, next_):
    if not ctx.text.strip():
        return RequestContext(text="empty", normalized=True)
    return next_(ctx)


result = pipeline.run(RequestContext("  Hello Flow  "))
assert result == RequestContext(text="hello flow", normalized=True)
assert stats.snapshot.runs.successful == 1
assert stats.snapshot.stages["lowercase"].calls == 1
```

[Back to top](#table-of-contents)

---

## First Async Pipeline + Stream

Pass `AsyncPipeline.run` directly to `AsyncStream.map(...)`:

```python
import asyncio
from flowpipeline import AsyncPipeline, AsyncStream


async def incoming_events():
    for i in range(1, 6):
        await asyncio.sleep(0)
        yield i


async def main() -> None:
    pipeline = AsyncPipeline[int]()

    @pipeline.stage("square")
    async def square(value: int) -> int:
        return value * value

    stream = AsyncStream(incoming_events()).map(pipeline.run).filter(lambda x: x > 5).batch(2)

    async with stream:
        batches = [batch async for batch in stream]

    assert batches == [(9, 16), (25,)]


asyncio.run(main())
```

[Back to top](#table-of-contents)

---

**Previous:** [Documentation Map](README.md) · **Next:** [02 · Pipelines, Middleware, and Observability →](pipelines-and-middleware.md)
