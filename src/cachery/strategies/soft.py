"""``@cache.soft(ttl=..., soft_ttl=...)`` — soft-TTL stampede & failure protection.

The value physically lives ``ttl + soft_ttl``; after the fresh window (``ttl``)
elapses the stale value keeps being served while exactly one recompute runs
(single-flight via the backend), instead of every caller stampeding the source.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from cachery.core.engine import Engine
from cachery.core.keys import KeyBuilder
from cachery.core.ttl import TTLValue
from cachery.strategies._base import (
    CacheStrategy,
    fresh,
    missed,
    pack_envelope,
    schedule_background,
    unpack_envelope,
    wall_clock,
)

__all__ = ["SoftStrategy"]


class SoftStrategy(CacheStrategy):
    name = "soft"

    def __init__(
        self,
        engine: Engine,
        key_builder: KeyBuilder,
        *,
        ttl: TTLValue = None,
        soft_ttl: TTLValue = None,
        condition: Callable[[Any], bool] | None = None,
    ) -> None:
        super().__init__(engine, key_builder, ttl=ttl, condition=condition)
        self.soft_ttl = soft_ttl

    async def load(self, key: str, ttl: float | None, call: Any) -> tuple[bool, Any]:
        hit, payload = await self.engine.load_value(key)
        if not hit:
            return missed()
        envelope = unpack_envelope(payload)
        if envelope is None:  # plain value written elsewhere
            return fresh(payload)
        value, created_at, stored_ttl = envelope.value, envelope.created_at, envelope.ttl
        if stored_ttl <= 0:  # written with no TTL → always fresh
            return fresh(value)
        age = wall_clock() - created_at
        if age < 0:  # clock skew across hosts sharing a backend
            age = 0.0
        if age <= stored_ttl:
            return fresh(value)
        # stale window: serve stale, refresh exactly once (single-flight)
        lock_key = f"__softlock__:{key}"
        acquired = await self.engine.set(lock_key, b"1", ttl=stored_ttl, exist=False)
        if acquired:
            schedule_background(f"soft:{key}", lambda: self._recompute(key, stored_ttl, call))
        return fresh(value)

    async def store(self, key: str, value: Any, ttl: float | None) -> None:
        soft = await self._soft_seconds(ttl)
        if ttl is None or soft is None:
            await self.engine.set(key, pack_envelope(value, wall_clock(), 0.0), None)
            return
        await self.engine.set(key, pack_envelope(value, wall_clock(), ttl), ttl + soft)

    async def _soft_seconds(self, ttl: float | None) -> float | None:
        from cachery.core.ttl import resolve_ttl  # noqa: PLC0415

        expr = self.soft_ttl if self.soft_ttl is not None else (ttl or 0.0) / 3
        if isinstance(expr, (int, float)):
            return float(expr)
        result = await resolve_ttl(expr)
        return result if isinstance(result, (int, float)) else None
