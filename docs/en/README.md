# FlowPipeline English Documentation

`flowpipeline` is a zero-dependency Python library for ordered processing pipelines (`Pipeline`, `AsyncPipeline`) and backpressure-controlled async streams (`AsyncStream`). It lets you organize transformations with composable middleware, built-in metrics (`PipelineStats`), and leak-free cleanup in both synchronous and `asyncio` code.

> **Scope note:** `flowpipeline` handles **in-process** transformation and stream coordination inside a single Python process. It does not replace distributed job queues (Celery, Temporal) or cross-process message brokers (Kafka).

---

## Table of Contents

- [Where Should You Start?](#where-should-you-start)
- [Documentation Map](#documentation-map)
- [Choosing the Right Tool](#choosing-the-right-tool)
- [Ecosystem Role](#ecosystem-role)

---

## Where Should You Start?

- If you are new to the library, start with [01 · Getting Started and Architecture](getting-started.md) to install `flowpipeline`, understand why plain function chains break down at scale, and run your first sync and async examples.
- To order stages (`before` / `after`), layer middleware around execution, preserve error causes (`PipelineExecutionError`), and capture metrics (`PipelineStats`), read [02 · Pipelines, Middleware, and Observability](pipelines-and-middleware.md).
- To process large or I/O-backed streams incrementally with `batch()`, `batch_within()`, and bounded `buffer()` prefetching, see [03 · AsyncStream, Backpressure, and Lifecycle](async-streams-and-backpressure.md).
- To look up exact constructor parameters, signatures, and exception fields, jump to [04 · API Reference](api-reference.md).

[Back to top](#table-of-contents)

---

## Documentation Map

| Guide | Contents |
| --- | --- |
| **[01 · Getting Started and Architecture](getting-started.md)** | Installation, "Why FlowPipeline?", same-type `Pipeline[T]` contract, sync/async boundaries, and architectural limits. |
| **[02 · Pipelines, Middleware, and Observability](pipelines-and-middleware.md)** | Topological stage ordering (`before` / `after`), immutable compiled plans, middleware continuation rules, `PipelineExecutionError`, and `PipelineStats`. |
| **[03 · AsyncStream, Backpressure, and Lifecycle](async-streams-and-backpressure.md)** | Lazy pull semantics, operator fusion, `batch` / `batch_within` / `buffer` trade-offs, single-consumer ownership, and `async with` cleanup. |
| **[04 · API Reference](api-reference.md)** | Public exports, constructor signatures, methods, observation dataclasses, exception fields, and hard limits. |

[Back to top](#table-of-contents)

---

## Choosing the Right Tool

| Need | Recommended Tool | Why? |
| --- | --- | --- |
| Sequential transformations over one synchronous input | `Pipeline[T]` | Runs directly on the calling thread with zero hidden event loops or worker threads. |
| Dynamically swapping/disabling named steps and wrapping them with timing/logging | `Pipeline` + `middleware` + `PipelineStats` | Keeps call sites unchanged while supporting `replace()`, `disable()`, `before`/`after` ordering, and `PipelineObserver`. |
| I/O-bound coroutine steps inside an `asyncio` application | `AsyncPipeline[T]` | Propagates cancellation (`asyncio.CancelledError`) directly into the active stage and isolates concurrent runs. |
| Processing infinite or I/O-driven async sequences with bounded memory | `AsyncStream[T]` | Pull-driven execution: no tasks spawned for `map`/`filter`, bounded memory for `buffer` and `batch_within`. |
| Chaining 2–3 tiny static pure functions | Plain Python `f(g(x))` | If runtime reconfiguration, middleware, or observability are unnecessary, plain function calls have zero abstraction overhead. |

[Back to top](#table-of-contents)

---

## Ecosystem Role

The Flow suite consists of three independent, zero-dependency packages that compose cleanly when needed:

```mermaid
flowchart LR
    FE["flowevent\nEvent Delivery & Bridges"] -->|"process incoming events"| FP["flowpipeline\nPipelines & AsyncStream"]
    FP -->|"guard stages with retry / rate limits"| FC["flowcoord\nResilience & Concurrency"]
```

- Need in-process pub/sub, routing, and bridges? Use **FlowEvent**.
- Need keyed limiters, single-flight, retries, circuit breakers, scopes, or task supervision? Use **FlowCoord**.
- Need composable stages, middleware, and lazy backpressure streams? Use **FlowPipeline**.

[Back to top](#table-of-contents)

---

**Next:** [01 · Getting Started and Architecture →](getting-started.md)
