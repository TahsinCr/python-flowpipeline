"""Timed batching, partial results, and early cleanup."""

from __future__ import annotations

import asyncio
import unittest

from flowpipeline.stream import AsyncStream


class TimedBatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_stream_batch_within_flushes_by_time_and_preserves_final_partial(self):
        closed = asyncio.Event()

        async def source():
            try:
                yield 1
                await asyncio.sleep(0.03)
                yield 2
                yield 3
            finally:
                closed.set()

        stream = AsyncStream(source()).batch_within(10, 0.01)
        self.assertEqual([batch async for batch in stream], [(1,), (2, 3)])
        self.assertTrue(closed.is_set())
        for invalid in (0, -1, float("inf"), True):
            with self.assertRaises((TypeError, ValueError)):
                AsyncStream(source()).batch_within(2, invalid)

    async def test_timed_batch_surfaces_source_error_after_partial_batch(self):
        async def source():
            yield 4
            raise LookupError("source failed")

        batches = AsyncStream(source()).batch_within(2, 10)
        iterator = batches.__aiter__()
        self.assertEqual(await iterator.__anext__(), (4,))
        with self.assertRaisesRegex(LookupError, "source failed"):
            await iterator.__anext__()

    async def test_timed_batch_early_exit_closes_its_source(self):
        closed = asyncio.Event()

        async def source():
            try:
                for value in range(10):
                    yield value
            finally:
                closed.set()

        async with AsyncStream(source()).batch_within(2, 10) as batches:
            async for _batch in batches:
                break
        self.assertTrue(closed.is_set())
