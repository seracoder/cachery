"""``@cache.failover(ttl=..., exceptions=...)`` — serve stale values when the source fails."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from cachery.core.engine import Engine
from cachery.core.keys import KeyBuilder
from cachery.core.ttl import TTLValue
from cachery.strategies._base import CacheStrategy, _call_value, _materialize

__all__ = ["FailoverStrategy"]

_DEFAULT_STALE_TTL = 86400.0


class FailoverStrategy(CacheStrategy):
    name = "failover"

    def __init__(
        self,
        engine: Engine,
        key_builder: KeyBuilder,
        *,
        ttl: TTLValue = None,
        exceptions: tuple[type[BaseException], ...] = (Exception,),
        stale_ttl: TTLValue = None,
        condition: Callable[[Any], bool] | None = None,
    ) -> None:
        super().__init__(engine, key_builder, ttl=ttl, condition=condition)
        self.exceptions = exceptions
        self.stale_ttl = stale_ttl

    async def on_miss(self, key: str, ttl: float | None, call: Any) -> Any:
        stale_key = f"{key}:__failover__"
        try:
            result = await _materialize(await _call_value(call))
        except self.exceptions:
            hit, stale = await self.engine.load_value(stale_key)
            if hit:
                return stale
            raise
        stale_ttl = self.stale_ttl
        if stale_ttl is not None:
            from cachery.core.ttl import resolve_ttl  # noqa: PLC0415 - cold path

            stale_seconds = await resolve_ttl(stale_ttl)
        else:
            stale_seconds = max(ttl or 0.0, _DEFAULT_STALE_TTL)
        await self.engine.set(stale_key, result, stale_seconds)
        return result
