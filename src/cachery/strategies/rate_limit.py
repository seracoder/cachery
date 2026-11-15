"""``@cache.rate_limit(limit=..., period=...)`` — quota the wrapped call per window.

Calls inside the quota execute (and are cached with ``ttl``); calls beyond the
quota raise :class:`cachery.core.exceptions.RateLimitError` without executing.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

from cachery.core.engine import Engine, is_disabled
from cachery.core.exceptions import RateLimitError
from cachery.core.keys import KeyBuilder
from cachery.core.ttl import TTLValue, resolve_ttl
from cachery.strategies._base import CacheStrategy

__all__ = ["RateLimitStrategy"]


class RateLimitStrategy(CacheStrategy):
    """Flow control, not caching: the quota counts *executions* of the wrapped
    function; the cache layer is bypassed entirely."""

    name = "rate_limit"

    def __init__(
        self,
        engine: Engine,
        key_builder: KeyBuilder,
        *,
        ttl: TTLValue = None,
        limit: int = 10,
        period: TTLValue = "1m",
        condition: Callable[[Any], bool] | None = None,
    ) -> None:
        super().__init__(engine, key_builder, ttl=ttl, condition=condition)
        self.limit = limit
        self.period = period

    async def run(self, args: tuple[Any, ...], kwargs: dict[str, Any], call: Any) -> Any:
        if is_disabled():
            return await call()
        key = self.key_builder.build(args, kwargs)
        period = await resolve_ttl(self.period, *args, **kwargs) or 60.0
        window = int(time.time() // period)
        counter_key = f"{key}:__rl__:{window}"
        count = await self.engine.incr(counter_key)
        if count == 1:
            await self.engine.expire(counter_key, period)
        if count > self.limit:
            raise RateLimitError(f"Rate limit exceeded for {key!r}: {self.limit} calls per {period:.0f}s")
        return await call()
