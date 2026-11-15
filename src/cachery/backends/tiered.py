"""Tiered backend: L1 (memory) + L2 (redis/disk) with read/write-through.

Reads check L1 first; on an L1 miss the L2 hit is promoted into L1 with the
remaining TTL (capped by ``l1_ttl_cap``). Writes go to L2 first, then L1.

Staleness bound: promotion TTL is always finite — the L2's remaining TTL when
the backend exposes ``ttl_of`` (Memory/Disk/Redis all do), else ``l1_ttl_cap``,
else a conservative default. L1 can therefore never outlive L2 by more than
that bound, even when another process deletes the L2 entry directly.
"""

from __future__ import annotations

import time
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from typing import Any, cast

from cachery.backends.protocol import Backend, BaseBackend

__all__ = ["TieredBackend"]

#: Fallback promotion TTL when the L2 exposes no ``ttl_of`` and no cap is set.
_DEFAULT_PROMOTION_TTL = 60.0


async def _remaining_ttl(backend: Any, key: str) -> float | None:
    """Optional capability: ``await backend.ttl_of(key)`` → seconds or None."""
    ttl_of = cast(
        "Callable[[str], Awaitable[float | None]] | None",
        getattr(backend, "ttl_of", None),
    )
    if not callable(ttl_of):
        return None
    result = await ttl_of(key)
    if isinstance(result, (int, float)) and not isinstance(result, bool):
        return float(result)
    return None


class TieredBackend(BaseBackend):
    def __init__(
        self,
        l1: Backend,
        l2: Backend,
        *,
        l1_ttl_cap: float | None = None,
    ) -> None:
        self.l1 = l1
        self.l2 = l2
        self.l1_ttl_cap = l1_ttl_cap

    def _promotion_ttl(self, remaining: float | None, write_ttl: float | None) -> float:
        """L1 TTL for a promoted/written value — always finite."""
        if remaining is not None:
            ttl = remaining
        elif write_ttl is not None:
            ttl = write_ttl
        else:
            ttl = self.l1_ttl_cap if self.l1_ttl_cap is not None else _DEFAULT_PROMOTION_TTL
        if self.l1_ttl_cap is not None:
            ttl = min(ttl, self.l1_ttl_cap)
        return max(ttl, 0.001)

    # -- reads: L1 → L2 → promote -----------------------------------------------

    async def get(self, key: str) -> Any | None:
        value = await self.l1.get(key)
        if value is not None:
            return value
        value = await self.l2.get(key)
        if value is None:
            return None
        remaining = await _remaining_ttl(self.l2, key)
        await self.l1.set(key, value, ttl=self._promotion_ttl(remaining, None))
        return value

    async def get_many(self, *keys: str) -> tuple[Any | None, ...]:
        out: list[Any | None] = []
        missing: list[int] = []
        for i, key in enumerate(keys):
            value = await self.l1.get(key)
            out.append(value)
            if value is None:
                missing.append(i)
        if missing:
            l2_values = await self.l2.get_many(*(keys[i] for i in missing))
            for pos, value in zip(missing, l2_values, strict=False):
                if value is not None:
                    out[pos] = value
                    remaining = await _remaining_ttl(self.l2, keys[pos])
                    await self.l1.set(keys[pos], value, ttl=self._promotion_ttl(remaining, None))
        return tuple(out)

    # -- writes: L2 then L1 ---------------------------------------------------------

    async def set(self, key: str, value: Any, ttl: float | None = None, exist: bool | None = None) -> bool:
        ok = await self.l2.set(key, value, ttl, exist)
        if ok:
            await self.l1.set(key, value, ttl=self._promotion_ttl(None, ttl))
        return ok

    async def set_many(self, pairs: Mapping[str, Any], ttl: float | None = None) -> None:
        await self.l2.set_many(pairs, ttl)
        await self.l1.set_many(pairs, self._promotion_ttl(None, ttl))

    async def delete(self, key: str) -> bool:
        await self.l1.delete(key)
        return bool(await self.l2.delete(key))

    async def delete_many(self, *keys: str) -> int:
        await self.l1.delete_many(*keys)
        return int(await self.l2.delete_many(*keys))

    async def delete_match(self, pattern: str) -> int:
        await self.l1.delete_match(pattern)
        return int(await self.l2.delete_match(pattern))

    async def exists(self, key: str) -> bool:
        return bool(await self.l1.exists(key)) or bool(await self.l2.exists(key))

    async def expire(self, key: str, ttl: float) -> bool:
        await self.l1.delete(key)  # simplest correct: drop L1, keep L2 authoritative
        return bool(await self.l2.expire(key, ttl))

    async def incr(self, key: str, delta: int = 1) -> int:
        result = int(await self.l2.incr(key, delta))
        await self.l1.delete(key)  # counters stay L2-authoritative
        return result

    async def ttl_of(self, key: str) -> float | None:
        l1_ttl = await _remaining_ttl(self.l1, key)
        if l1_ttl is not None:
            return l1_ttl
        return await _remaining_ttl(self.l2, key)

    async def scan(self, pattern: str, batch_size: int = 100) -> AsyncIterator[str]:
        seen: set[str] = set()
        async for key in self.l1.scan(pattern, batch_size):
            seen.add(key)
            yield key
        async for key in self.l2.scan(pattern, batch_size):
            if key not in seen:
                yield key

    async def clear(self) -> int:
        await self.l1.clear()
        return int(await self.l2.clear())

    async def ping(self) -> bytes:
        await self.l1.ping()
        return await self.l2.ping()

    async def close(self) -> None:
        await self.l1.close()
        await self.l2.close()

    def now(self) -> float:
        return time.time()
