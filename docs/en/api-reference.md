[← Documentation Map](README.md) · [Previous: 03 · AsyncStream and Backpressure](async-streams-and-backpressure.md)

# 04 · FlowPipeline API Reference

Complete reference for every public class, method, dataclass, protocol, and exception exported by `flowpipeline`.

---

## Table of Contents

- [Package Exports](#package-exports)
- [`Pipeline[T]` and `AsyncPipeline[T]`](#pipelinet-and-asyncpipelinet)
- [`AsyncStream[T]`](#asyncstreamt)
- [Observability Types](#observability-types)
- [Exception (`PipelineExecutionError`)](#exception-pipelineexecutionerror)

---

## Package Exports

```python
from flowpipeline import (
    AsyncPipeline,
    AsyncStream,
    ExecutionMetrics,
    Pipeline,
    PipelineExecutionError,
    PipelineObservation,
    PipelineObserver,
    PipelineStats,
    PipelineStatsSnapshot,
    __version__,
)
```

[Back to top](#table-of-contents)

---

## `Pipeline[T]` and `AsyncPipeline[T]`

Both classes share identical configuration, ordering, and observability APIs, differing only in sync vs async callback execution.

### Constructor

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `observer` | `PipelineObserver \| None` | `None` | Fast **synchronous** callable notified after each stage, middleware, and run. Cannot be a coroutine (`TypeError`). |
| `correlation_provider` | `Callable[[], str \| None] \| None` | `None` | **Synchronous** callable supplying `PipelineObservation.correlation_id`. |

### Properties and Methods

| Member | Signature / Type | Description |
| --- | --- | --- |
| `stage_names` | `tuple[str, ...]` | All stage names in execution order, including disabled entries. |
| `middleware_names` | `tuple[str, ...]` | All middleware names from outermost to innermost, including disabled entries. |
| `add` | `(name: str, callback, *, before: str \| None = None, after: str \| None = None) -> None` | Atomically register a named stage. |
| `stage` | `(name: str, *, before: str \| None = None, after: str \| None = None) -> Callable` | Decorator variant returning the original callable unchanged. |
| `replace` | `(name: str, callback) -> None` | Swap callback while preserving ordering and enabled flag. |
| `enable` / `disable` | `(name: str) -> None` | Enable or disable a stage for future runs. |
| `remove` | `(name: str) -> callback` | Delete a stage and references to it; returns the prior callable. |
| `add_middleware` | `(name: str, callback, *, before=None, after=None) -> None` | Register up to **64** around-middleware callables accepting `(context, next_)`. |
| `middleware` | `(name: str, *, before=None, after=None) -> Callable` | Decorator variant for middleware. |
| `replace_middleware` | `(name: str, callback) -> None` | Swap middleware callback without altering position. |
| `enable_middleware` / `disable_middleware` | `(name: str) -> None` | Enable or disable middleware for future runs. |
| `remove_middleware` | `(name: str) -> callback` | Remove middleware and return its callback. |
| `run` | `Pipeline.run(context: T) -> T` / `AsyncPipeline.run(context: T) -> Awaitable[T]` | Execute a compiled immutable snapshot of the pipeline. |

[Back to top](#table-of-contents)

---

## `AsyncStream[T]`

Implements `AsyncIterator[T]` with `async with` and `async for` support.

| Member | Signature / Type | Description |
| --- | --- | --- |
| `AsyncStream(source)` | `source: AsyncIterable[T]` | Wrap an async iterable (`TypeError` otherwise). |
| `closed` | `bool` | Whether reads are stopped and the stream is closed. |
| `map(callback)` | `(Callable[[T], U \| Awaitable[U]]) -> AsyncStream[U]` | Transform each item lazily without spawning tasks. |
| `filter(predicate)` | `(Callable[[T], bool \| Awaitable[bool]]) -> AsyncStream[T]` | Keep items where predicate returns exact `bool` `True`. |
| `batch(size, *, partial=True)` | `size: int > 0, partial: bool` | Yield fixed-size `tuple` batches lazily without spawning tasks. |
| `batch_within(max_items, max_delay, *, partial=True)` | `max_items: int > 0, max_delay: float > 0` | Yield a batch once full or `max_delay` seconds after the batch's first item arrives. |
| `buffer(capacity)` | `capacity: int > 0` | Bounded `asyncio.Queue(maxsize=capacity)` prefetch + backpressure. |
| `aclose()` | `async def aclose() -> None` | Cancel active read, stop producer tasks, and close upstream sources cleanly. |

[Back to top](#table-of-contents)

---

## Observability Types

| Type | Fields / Methods | Description |
| --- | --- | --- |
| `PipelineObservation` | `kind` (`"run" \| "stage" \| "middleware"`), `name: str \| None`, `elapsed: float`, `status` (`"success" \| "error" \| "cancelled"`), `correlation_id: str \| None`, `error: BaseException \| None` | Immutable (`frozen=True, slots=True`) record for a single execution unit. |
| `PipelineObserver` | `__call__(observation: PipelineObservation) -> None` | Synchronous observer protocol. |
| `ExecutionMetrics` | `calls`, `successful`, `failed`, `cancelled`, `total_seconds`, `max_seconds` | Aggregate counts and durations (`frozen=True, slots=True`). |
| `PipelineStatsSnapshot` | `runs: ExecutionMetrics`, `stages: Mapping[str, ExecutionMetrics]`, `middleware: Mapping[str, ExecutionMetrics]` | Read-only (`MappingProxyType`) snapshot decoupled from future updates/resets. |
| `PipelineStats` | `PipelineStats()`, `.snapshot`, `.reset()` | Built-in thread-safe metrics collector. |

[Back to top](#table-of-contents)

---

## Exception (`PipelineExecutionError`)

Subclasses `RuntimeError`. Raised on standard `Exception` failures in stages, middleware, runs, or observers (`__cause__` preserves the underlying exception).

| Field | Type | Description |
| --- | --- | --- |
| `name` | `str \| None` | Name of failing stage/middleware (`None` at `"run"` phase). |
| `phase` | `Literal["stage", "middleware", "run", "observer"]` | Execution phase where the failure occurred. |
| `context` | `object` | Payload active at failure time. |
| `observer_errors` | `tuple[Exception, ...]` | Any observer exceptions thrown while recording an already-failing execution. |

[Back to top](#table-of-contents)

---

**Previous:** [03 · AsyncStream, Backpressure, and Lifecycle](async-streams-and-backpressure.md) · **Map:** [Documentation Map](README.md)
