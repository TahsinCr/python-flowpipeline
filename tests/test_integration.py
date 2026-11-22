import asyncio
import unittest

from flowpipeline import AsyncPipeline, AsyncStream, PipelineStats


class StreamPipelineIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_stream_pipeline_batches_and_closes_owned_tasks(self):
        stats = PipelineStats()
        pipeline = AsyncPipeline[int](observer=stats)

        @pipeline.stage("double")
        async def double(value):
            return value * 2

        async def source():
            for value in range(6):
                yield value

        stream = (
            AsyncStream(source())
            .map(pipeline.run)
            .filter(lambda value: value % 4 == 0)
            .buffer(2)
            .batch(2)
        )
        output = []
        async with stream:
            async for batch in stream:
                output.extend(batch)

        self.assertEqual(output, [0, 4, 8])
        self.assertEqual(stats.snapshot.runs.calls, 6)
        self.assertFalse(
            [
                task
                for task in asyncio.all_tasks()
                if task.get_name().startswith("FlowPipeline") and not task.done()
            ]
        )
