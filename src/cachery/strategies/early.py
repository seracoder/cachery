"""``@cache.early(ttl=..., early_ttl=...)`` — stampede protection via background refresh.

A hit within ``early_ttl`` of expiry returns the still-fresh value while a
background recompute is scheduled, so the expiry cliff never happens under
concurrency. The envelope records creation time; the backend TTL stays the
hard ``ttl``.
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
    jittered,
    missed,
    pack_envelope,
    schedule_background,
    unpack_envelope,
    wall_clock,
)

__all__ = ["EarlyStrategy"]


class EarlyStrategy(CacheStrategy):
    name = "early"

    def __init__(
        self,
        engine: Engine,
        key_builder: KeyBuilder,
        *,
        ttl: TTLValue = None,
        early_ttl: TTLValue = None,
        jitter: float = 0.1,
        condition: Callable[[Any], bool] | None = None,
    ) -> None:
        super().__init__(engine, key_builder, ttl=ttl, condition=condition)
        self.early_ttl = early_ttl
        #: Relative jitter on the refresh threshold (0.1 = ±10%) so many keys
        #: expiring together don't all refresh in the same event-loop tick.
        self.jitter = jitter

    async def load(self, key: str, ttl: float | None, call: Any) -> tuple[bool, Any]:
        hit, payload = await self.engine.load_value(key)
        if not hit:
            return missed()
        envelope = unpack_envelope(payload)
        if envelope is None:  # plain value written by something else
            return fresh(payload)
        value, created_at, stored_ttl = envelope.value, envelope.created_at, envelope.ttl
        if stored_ttl <= 0:  # written with no expiry → always fresh
            return fresh(value)
        early = await self._early_seconds(ttl if ttl is not None else stored_ttl)
        if early is None:  # unresolvable early window → serve, never refresh
            return fresh(value)
        if early <= 0:
            return fresh(value)
        age = wall_clock() - created_at
        if age < 0:  # clock skew across hosts sharing a backend
            age = 0.0
        threshold = jittered(stored_ttl - early, self.jitter * (stored_ttl - early))
        if threshold <= 0 or age >= threshold:
            schedule_background(f"early:{key}", lambda: self._recompute(key, ttl, call))
        return fresh(value)

    async def _early_seconds(self, ttl: float) -> float | None:
        from cachery.core.ttl import resolve_ttl  # noqa: PLC0415

        expr = self.early_ttl if self.early_ttl is not None else ttl / 3
        if isinstance(expr, (int, float)):
            return float(expr)
        result = await resolve_ttl(expr)
        return result if isinstance(result, (int, float)) else None

    async def store(self, key: str, value: Any, ttl: float | None) -> None:
        await self.engine.set(key, pack_envelope(value, wall_clock(), ttl or 0.0), ttl)
