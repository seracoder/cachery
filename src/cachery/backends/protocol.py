"""The structural ``Backend`` contract.

Implement these methods on *any* class — no inheritance from cachery, no
imports required — and ``cache.setup(backend=your_object)`` accepts it. A
convenience :class:`BaseBackend` ABC derives the batch operations from the
single-key primitives; subclass it only if you want the defaults.

All values passed through this contract are **already serialized bytes**
(serialization happens in the layer above), though nothing breaks if
an implementation stores ``Any``.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Mapping
from typing import Any, Protocol, runtime_checkable

__all__ = ["Backend", "BaseBackend"]


@runtime_checkable
class Backend(Protocol):
    """Duck-typed backend contract — implement it, don't inherit it."""

    async def get(self, key: str) -> Any | None: ...

    async def set(
        self,
        key: str,
        value: Any,
        ttl: float | None = None,
        exist: bool | None = None,
    ) -> bool:
        """Store ``value`` under ``key``.

        ``exist``: ``None`` = always write; ``False`` = only if absent (NX);
        ``True`` = only if present (XX). Returns whether the write happened.
        """
        raise NotImplementedError

    async def get_many(self, *keys: str) -> tuple[Any | None, ...]: ...

    async def set_many(self, pairs: Mapping[str, Any], ttl: float | None = None) -> None: ...

    async def delete(self, key: str) -> bool: ...

    async def delete_many(self, *keys: str) -> int: ...

    async def delete_match(self, pattern: str) -> int: ...

    async def exists(self, key: str) -> bool: ...

    async def expire(self, key: str, ttl: float) -> bool: ...

    async def incr(self, key: str, delta: int = 1) -> int: ...

    def scan(self, pattern: str, batch_size: int = 100) -> AsyncIterator[str]:
        """Yield keys matching a glob-style ``pattern`` (``*``, ``?``, ``[seq]``)."""
        ...

    async def clear(self) -> int: ...

    async def ping(self) -> bytes: ...

    async def close(self) -> None: ...


_SCAN_DELETE_BATCH = 100


class BaseBackend(ABC):
    """Optional convenience base: batch ops derived from single-key primitives.

    Override any of the derived methods when your store has a native batch API
    (as :class:`~cachery.backends.redis.RedisBackend` does).
    """

    @abstractmethod
    async def get(self, key: str) -> Any | None: ...

    @abstractmethod
    async def set(
        self,
        key: str,
        value: Any,
        ttl: float | None = None,
        exist: bool | None = None,
    ) -> bool: ...

    @abstractmethod
    async def delete(self, key: str) -> bool: ...

    @abstractmethod
    async def exists(self, key: str) -> bool: ...

    @abstractmethod
    async def expire(self, key: str, ttl: float) -> bool: ...

    @abstractmethod
    async def incr(self, key: str, delta: int = 1) -> int: ...

    @abstractmethod
    def scan(self, pattern: str, batch_size: int = 100) -> AsyncIterator[str]: ...

    @abstractmethod
    async def clear(self) -> int: ...

    @abstractmethod
    async def ping(self) -> bytes: ...

    async def close(self) -> None:
        return None

    # -- derived batch operations -------------------------------------------

    async def get_many(self, *keys: str) -> tuple[Any | None, ...]:
        import asyncio  # noqa: PLC0415 - stdlib, import cost negligible here

        results = await asyncio.gather(*(self.get(k) for k in keys))
        return tuple(results)

    async def set_many(self, pairs: Mapping[str, Any], ttl: float | None = None) -> None:
        import asyncio  # noqa: PLC0415

        await asyncio.gather(*(self.set(k, v, ttl) for k, v in pairs.items()))

    async def delete_many(self, *keys: str) -> int:
        import asyncio  # noqa: PLC0415

        results = await asyncio.gather(*(self.delete(k) for k in keys))
        return sum(1 for ok in results if ok)

    async def delete_match(self, pattern: str) -> int:
        deleted = 0
        batch: list[str] = []
        async for key in self.scan(pattern):
            batch.append(key)
            if len(batch) >= _SCAN_DELETE_BATCH:
                deleted += await self.delete_many(*batch)
                batch = []
        if batch:
            deleted += await self.delete_many(*batch)
        return deleted
