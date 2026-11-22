from __future__ import annotations

import unittest

from flowpipeline.stream import AsyncStream


class StreamFilterTests(unittest.IsolatedAsyncioTestCase):
    async def test_sync_predicate_must_return_bool(self) -> None:
        async def source():
            yield 1

        stream = AsyncStream(source()).filter(lambda _event: "true")  # type: ignore[arg-type]
        with self.assertRaisesRegex(TypeError, "return bool"):
            await stream.__anext__()

    async def test_async_predicate_must_return_bool(self) -> None:
        async def source():
            yield 1

        async def predicate(_event: int) -> bool:
            return "false"  # type: ignore[return-value]

        stream = AsyncStream(source()).filter(predicate)
        with self.assertRaisesRegex(TypeError, "return bool"):
            await stream.__anext__()
