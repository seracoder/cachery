"""``@cache.circuit_breaker(errors_rate=..., period=..., ttl=...)``.

Tracks the error rate of the wrapped call over a sliding window (per-key
counters in the backend). Pure flow control — no caching layer. Once at least
``min_calls`` samples exist and errors
exceed ``errors_rate`` percent, the circuit opens for ``ttl`` seconds and
calls fail fast with :class:`cachery.core.exceptions.CircuitBreakerOpen`.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

from cachery.core.engine import Engine, is_disabled
from cachery.core.exceptions import CircuitBreakerOpen
from cachery.core.keys import KeyBuilder
from cachery.core.ttl import TTLValue, resolve_ttl
from cachery.strategies._base import CacheStrategy

__all__ = ["CircuitBreakerStrategy"]


class CircuitBreakerStrategy(CacheStrategy):
    name = "circuit_breaker"

    def __init__(
        self,
        engine: Engine,
        key_builder: KeyBuilder,
        *,
        ttl: TTLValue = None,
        errors_rate: int = 50,
        period: TTLValue = "1m",
        min_calls: int = 3,
        condition: Callable[[Any], bool] | None = None,
    ) -> None:
        super().__init__(engine, key_builder, ttl=ttl, condition=condition)
        self.errors_rate = errors_rate
        self.period = period
        self.min_calls = min_calls

    async def run(self, args: tuple[Any, ...], kwargs: dict[str, Any], call: Any) -> Any:
        if is_disabled():
            return await call()
        key = self.key_builder.build(args, kwargs)
        period = await resolve_ttl(self.period, *args, **kwargs) or 60.0
        open_ttl = await resolve_ttl(self.ttl, *args, **kwargs) or 60.0
        open_key = f"{key}:__cbopen__"
        if await self.engine.exists(open_key):
            raise CircuitBreakerOpen(f"Circuit open for {key!r}; retrying after {open_ttl:.0f}s")
        window = int(time.time() // period)
        total_key = f"{key}:__cbt__:{window}"
        err_key = f"{key}:__cbe__:{window}"
        try:
            result = await call()
        except Exception:
            total = await self.engine.incr(total_key)
            errors = await self.engine.incr(err_key)
            for ck in (total_key, err_key):
                await self.engine.expire(ck, period * 2)
            if total >= self.min_calls and errors / total * 100 >= self.errors_rate:
                await self.engine.set(open_key, b"1", ttl=open_ttl)
            raise
        await self.engine.incr(total_key)
        await self.engine.expire(total_key, period * 2)
        return result
