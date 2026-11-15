"""``@cache.locked(ttl=...)`` — single-flight: one caller recomputes, others wait.

Two layers of protection:

- **In-process**: a per-key ``asyncio`` future. Concurrent callers in this
  process await the winner's in-flight recompute directly (no polling, no
  duplicated work, and a legitimate ``None`` result is delivered to waiters
  like any other value).
- **Cross-process**: a backend lock (``set(exist=False)``). Concurrent
  *processes* still single-flight; a process that dies mid-recompute has its
  lock reclaimed when the TTL lapses.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Any

from cachery.core.engine import Engine
from cachery.core.keys import KeyBuilder
from cachery.core.ttl import TTLValue
from cachery.strategies._base import CacheStrategy

__all__ = ["LockedStrategy"]

_POLL_INTERVAL = 0.05
#: How long a cross-process waiter polls the backend before computing itself.
_WAITER_PATIENCE = 30.0

_local_locks: dict[str, asyncio.Future[Any]] = {}


class LockedStrategy(CacheStrategy):
    name = "locked"

    def __init__(
        self,
        engine: Engine,
        key_builder: KeyBuilder,
        *,
        ttl: TTLValue = None,
        condition: Callable[[Any], bool] | None = None,
    ) -> None:
        super().__init__(engine, key_builder, ttl=ttl, condition=condition)

    async def on_miss(self, key: str, ttl: float | None, call: Any) -> Any:
        # In-process single flight: join the winner's in-flight recompute.
        # Awaiting the shared future (not a stored value) makes waiter
        # semantics exact — a cached None is delivered like any other value.
        existing = _local_locks.get(key)
        if existing is not None and not existing.done():
            return await asyncio.shield(existing)
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Any] = loop.create_future()
        _local_locks[key] = future
        try:
            result = await self._recompute_locked(key, ttl, call)
            if not future.done():
                future.set_result(result)
            return result
        except BaseException as e:
            if not future.done():
                future.set_exception(e)
            raise
        finally:
            _local_locks.pop(key, None)

    async def _recompute_locked(self, key: str, ttl: float | None, call: Any) -> Any:
        """Backend lock for cross-process single-flight; waiters poll the value."""
        lock_key = f"__slock__:{key}"
        budget = ttl or 10.0
        if await self.engine.set(lock_key, b"1", ttl=budget, exist=False):
            try:
                return await self._recompute(key, ttl, call)
            finally:
                await self.engine.delete(lock_key)
        # Another process holds the lock: wait for it to store *any* value.
        deadline = time.monotonic() + min(_WAITER_PATIENCE, budget)
        while time.monotonic() < deadline:
            await asyncio.sleep(_POLL_INTERVAL)
            hit, value = await self.engine.load_value(key)
            if hit:
                return value
        # Patience exhausted — compute ourselves rather than fail the caller.
        return await self._recompute(key, ttl, call)
