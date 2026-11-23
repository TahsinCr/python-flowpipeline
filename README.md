[![CI status][ci-shield]][ci]
[![PyPI version][pypi-shield]][pypi]
[![Python 3.10+][python-shield]][python]
[![MIT License][license-shield]][license]
**English** | [Türkçe][readme-tr]

# FlowPipeline

[Repository][repository] · [Changelog][changelog] · [MIT License][license]

Composable processing stages and async streams for Python applications. Define a sequence of transformations and run it synchronously or asynchronously.

## Contents

- [Getting started](#getting-started)
- [Core concepts](#core-concepts)
- [Pipelines and middleware](#pipelines)
- [Async pipelines and streams](#streams)
- [Development and license](#development)

<a id="getting-started"></a>

## Getting started

### Install

Install from the project root with Python 3.10 or later.

~~~sh
python -m pip install .
~~~

### Run named stages in order

Each stage receives the previous stage’s result. Register functions with a decorator or add existing callables by name.

~~~python
from flowpipeline import Pipeline

pipeline = Pipeline[str]()


@pipeline.stage("trim")
def trim(value: str) -> str:
    return value.strip()


pipeline.add("normalize", str.lower)
assert pipeline.run("  Ready  ") == "ready"
~~~

<a id="core-concepts"></a>

## Core concepts

Choose `Pipeline` for synchronous stages and `AsyncPipeline` for coroutine stages. Use `AsyncStream` when input arrives as a stream or needs incremental processing with bounded buffering.

<a id="pipelines"></a>

## Pipelines and middleware

### Wrap execution and collect measurements

Middleware can run code around the remaining stages. Here it supplies a default value before normalizing the input, while the observer records the run.

~~~python
from flowpipeline import Pipeline, PipelineStats

stats = PipelineStats()
pipeline = Pipeline[str](observer=stats)
pipeline.add("normalize", str.lower)


@pipeline.middleware("default")
def supply_default(value, next_):
    return next_(value or "UNKNOWN")


assert pipeline.run("") == "unknown"
assert stats.snapshot.runs.successful == 1
~~~

<a id="streams"></a>

## Async pipelines and streams

### Process items and consume them in batches

A stream consumes input incrementally. It can apply an async pipeline to each item, filter the results, and group the remaining items into batches.

~~~python
import asyncio

from flowpipeline import AsyncPipeline, AsyncStream


async def source():
    for value in range(4):
        yield value


async def main() -> None:
    pipeline = AsyncPipeline[int]()

    @pipeline.stage("double")
    async def double(value: int) -> int:
        return value * 2

    stream = AsyncStream(source()).map(pipeline.run).filter(lambda value: value > 0).batch(2)
    async with stream:
        assert [batch async for batch in stream] == [(2, 4), (6,)]


asyncio.run(main())
~~~

Use `batch(size)` to group by count. Use `batch_within(size, delay)` when a partial batch needs a deadline. Add `buffer(capacity)` when the producer and consumer can benefit from running independently. Buffering adds a queue handoff, which can cost more than it saves for an in-memory source and a fast consumer.

<a id="development"></a>

## Development and license

~~~sh
python -m pip install -e ".[dev]"
python run_tests.py
~~~

This project is licensed under the [MIT License][license].

## Related projects

[FlowEvent][repo-flowevent] · [FlowCoord][repo-flowcoord]

[ci-shield]: https://img.shields.io/github/actions/workflow/status/TahsinCr/python-flowpipeline/ci.yml?branch=main&event=push&label=CI&style=for-the-badge&logo=github&logoColor=white
[pypi-shield]: https://img.shields.io/pypi/v/flowpipeline?label=PyPI&style=for-the-badge&logo=pypi&logoColor=white
[python-shield]: https://img.shields.io/badge/Python-3.10%2B-3776AB.svg?style=for-the-badge&logo=python&logoColor=white
[license-shield]: https://img.shields.io/badge/license-MIT-blue.svg?style=for-the-badge
[ci]: https://github.com/TahsinCr/python-flowpipeline/actions/workflows/ci.yml?query=branch%3Amain+event%3Apush
[pypi]: https://pypi.org/project/flowpipeline/
[python]: https://www.python.org/downloads/
[readme-tr]: https://github.com/TahsinCr/python-flowpipeline/blob/main/README-TR.md
[repository]: https://github.com/TahsinCr/python-flowpipeline
[changelog]: https://github.com/TahsinCr/python-flowpipeline/blob/main/CHANGELOG.md
[license]: https://github.com/TahsinCr/python-flowpipeline/blob/main/LICENSE
[repo-flowevent]: https://github.com/TahsinCr/python-flowevent
[repo-flowcoord]: https://github.com/TahsinCr/python-flowcoord
