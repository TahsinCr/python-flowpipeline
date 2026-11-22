"""Private bounds and duration checks."""

from __future__ import annotations

import math
from operator import index


def validate_positive_capacity(value: int, *, name: str) -> int:
    """Validate a bounded collection/schedule capacity."""

    if isinstance(value, bool) or not isinstance(value, int) or (value := index(value)) <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def validate_finite_duration(value: float, *, name: str, allow_zero: bool = False) -> float:
    """Validate a finite duration, optionally accepting zero."""
    qualifier = "non-negative" if allow_zero else "positive"
    message = f"{name} must be {qualifier} and finite"
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(message)
    try:
        timeout = float(value)
    except OverflowError as exc:
        if allow_zero:
            raise ValueError(message) from exc
        raise ValueError(message) from None
    if not math.isfinite(timeout) or timeout < 0 or (timeout == 0 and not allow_zero):
        raise ValueError(message)
    return timeout
