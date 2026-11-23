"""Synchronous and asynchronous data pipelines and streams."""

from .async_ import AsyncPipeline
from .contracts import PipelineExecutionError
from .observability import (
    ExecutionMetrics,
    PipelineObservation,
    PipelineObserver,
    PipelineStats,
    PipelineStatsSnapshot,
)
from .stream import AsyncStream
from .sync import Pipeline

__version__ = "1.0.0"

__all__ = [
    "__version__",
    "AsyncPipeline",
    "AsyncStream",
    "ExecutionMetrics",
    "Pipeline",
    "PipelineExecutionError",
    "PipelineObservation",
    "PipelineObserver",
    "PipelineStats",
    "PipelineStatsSnapshot",
]
