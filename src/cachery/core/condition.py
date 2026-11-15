"""Cache-write conditions: callables deciding whether a computed result is worth caching.

A condition receives the computed result and returns ``True`` to allow the write.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from typing import TypeAlias

__all__ = ["NOT_NONE", "Condition", "time_condition"]

Condition: TypeAlias = Callable[[object], bool]


def _not_none(result: object) -> bool:
    return result is not None


#: Cache the result only if it is not ``None``.
NOT_NONE: Condition = _not_none


def time_condition(
    start: dt.time,
    end: dt.time,
    *,
    tz: dt.tzinfo | None = None,
) -> Condition:
    """Only cache results computed inside the daily ``[start, end)`` wall-clock window.

    Args:
        start: window start time (inclusive).
        end: window end time (exclusive; may be earlier than ``start`` for windows
            crossing midnight).
        tz: optional timezone used to compute "now"; defaults to local time.
    """

    def condition(result: object) -> bool:
        now = dt.datetime.now(tz).time()
        if start <= end:
            return start <= now < end
        return now >= start or now < end

    return condition
