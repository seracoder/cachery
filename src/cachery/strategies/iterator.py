"""``@cache.iterator(ttl=...)`` — cache async (or sync) generator results.

The generator is consumed once into a list, cached, and re-yielded from the
list on hits, so iteration is not re-executed per element.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from cachery.strategies._base import CacheStrategy, _call_value, _materialize

__all__ = ["IteratorStrategy"]


class IteratorStrategy(CacheStrategy):
    name = "iterator"
    wraps_as_iterator = True

    async def on_miss(self, key: str, ttl: float | None, call: Callable[[], Any]) -> Any:
        del key, ttl
        result = await _materialize(await _call_value(call))
        if isinstance(result, list):
            return result
        return [result]
