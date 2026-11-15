"""``@cache.hit(ttl=..., cache_hits=..., update_after=...)`` — expire-after-N-hits.

A per-key hit counter (kept in the backend, so it works across processes)
refreshes the value in the background after ``update_after`` hits and forces a
synchronous recompute once ``cache_hits`` is exceeded.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from cachery.core.engine import Engine
from cachery.core.keys import KeyBuilder
from cachery.core.ttl import TTLValue
from cachery.strategies._base import CacheStrategy, fresh, missed, schedule_background

__all__ = ["HitStrategy"]


class HitStrategy(CacheStrategy):
    name = "hit"

    def __init__(
        self,
        engine: Engine,
        key_builder: KeyBuilder,
        *,
        ttl: TTLValue = None,
        cache_hits: int | None = None,
        update_after: int | None = None,
        condition: Callable[[Any], bool] | None = None,
    ) -> None:
        super().__init__(engine, key_builder, ttl=ttl, condition=condition)
        self.cache_hits = cache_hits
        self.update_after = update_after

    async def load(self, key: str, ttl: float | None, call: Any) -> tuple[bool, Any]:
        hit, value = await self.engine.load_value(key)
        if not hit:
            return missed()
        if ttl is None:
            return fresh(value)
        counter_key = f"{key}:__hits__"
        count = await self.engine.incr(counter_key)
        if count == 1:
            await self.engine.expire(counter_key, ttl)
        if self.cache_hits is not None and count > self.cache_hits:
            await self.engine.delete(key)
            await self.engine.delete(counter_key)
            return missed()
        if self.update_after is not None and count >= self.update_after:
            # Reset the counter so the refresh amortises: one background
            # recompute per update_after hits, not one on every hit after.
            await self.engine.delete(counter_key)
            schedule_background(f"hit:{key}", lambda: self._recompute(key, ttl, call))
        return fresh(value)
