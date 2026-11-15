"""In-memory backend — a fully supported, standalone backend.

``cache.setup("mem://")`` is a complete configuration for single-process apps,
scripts, tests and local dev — not merely the L1 half of a tiered setup.

Design:
- Bounded by ``size`` with a pluggable **eviction policy** (cachetools parity):
  ``lru`` (default), ``lfu``, ``fifo``, ``mru``, ``rr`` (random).
- Expiration via an *incremental reaper*: a min-heap of ``(expire_at, key)``
  that each operation opportunistically drains of due entries — O(log n)
  amortized, never a periodic full-dict scan (the tail-latency problem with
  periodic-sweep designs), and no background thread or loop required.
- Thread-safe: a single lock guards state, so the same backend works from both
  an event loop and the sync bridge's background thread.
"""

from __future__ import annotations

import fnmatch
import heapq
import itertools
import random
import threading
import time
from collections import OrderedDict
from collections.abc import AsyncIterator, Mapping
from typing import Any

from cachery.backends.protocol import BaseBackend

__all__ = ["MemoryBackend"]

_REAP_BUDGET = 64

#: Supported eviction policies (cachetools-compatible names).
POLICIES = ("lru", "lfu", "fifo", "mru", "rr")


class MemoryBackend(BaseBackend):
    def __init__(self, size: int | None = 1024, policy: str = "lru") -> None:
        if size is not None and size <= 0:
            raise ValueError("size must be positive or None (unbounded)")
        policy = policy.lower()
        if policy not in POLICIES:
            raise ValueError(f"Unknown eviction policy {policy!r}; expected one of {', '.join(POLICIES)}")
        self.size = size
        self.policy = policy
        self._lock = threading.Lock()
        self._data: OrderedDict[str, tuple[Any, float | None]] = OrderedDict()
        self._heap: list[tuple[float, str]] = []
        self._freq: dict[str, int] = {}
        self._counter = itertools.count()

    # -- internals -----------------------------------------------------------

    def _now(self) -> float:
        return time.monotonic()

    def _reap(self, now: float) -> None:
        """Drop due entries from the heap (amortized; bounded per operation)."""
        for _ in range(_REAP_BUDGET):
            if not self._heap or self._heap[0][0] > now:
                break
            expire_at, key = heapq.heappop(self._heap)
            entry = self._data.get(key)
            if entry is not None and entry[1] == expire_at:
                del self._data[key]
                self._freq.pop(key, None)

    def _live_entry(self, key: str, now: float) -> Any | None:
        entry = self._data.get(key)
        if entry is None:
            return None
        _value, expire_at = entry
        if expire_at is not None and expire_at <= now:
            del self._data[key]
            self._freq.pop(key, None)
            return None
        return entry

    def _touch(self, key: str) -> None:
        """Record a use according to the eviction policy."""
        if self.policy in ("lru", "mru"):
            self._data.move_to_end(key)
        elif self.policy == "lfu":
            self._freq[key] = self._freq.get(key, 0) + 1

    def _evict_if_needed(self) -> None:
        if self.size is None:
            return
        while len(self._data) > self.size:
            victim = self._choose_victim()
            if victim is None:  # pragma: no cover - defensive
                return
            del self._data[victim]
            self._freq.pop(victim, None)

    def _choose_victim(self) -> str | None:
        if not self._data:
            return None
        if self.policy == "mru":
            return next(reversed(self._data))
        if self.policy == "rr":
            return random.choice(list(self._data))
        if self.policy == "lfu":
            # O(n) scan: eviction is infrequent and n is bounded by ``size``.
            return min(self._data, key=lambda k: self._freq.get(k, 0))
        # lru / fifo: oldest insertion (recency order for lru)
        return next(iter(self._data))

    # -- Backend contract ------------------------------------------------------

    async def get(self, key: str) -> Any | None:
        with self._lock:
            now = self._now()
            self._reap(now)
            entry = self._live_entry(key, now)
            if entry is None:
                return None
            self._touch(key)
            return entry[0]

    async def set(self, key: str, value: Any, ttl: float | None = None, exist: bool | None = None) -> bool:
        with self._lock:
            now = self._now()
            self._reap(now)
            exists = self._live_entry(key, now) is not None
            if exist is True and not exists:
                return False
            if exist is False and exists:
                return False
            self._store(key, value, ttl, now)
            return True

    def _store(self, key: str, value: Any, ttl: float | None, now: float) -> None:
        expire_at = now + ttl if ttl is not None else None
        self._data[key] = (value, expire_at)
        if self.policy == "lfu":
            self._freq[key] = self._freq.get(key, 0) + 1
        elif self.policy == "fifo":
            pass  # insertion order already preserved by OrderedDict
        else:
            self._data.move_to_end(key)
        if expire_at is not None:
            heapq.heappush(self._heap, (expire_at, key))
        self._evict_if_needed()

    async def get_many(self, *keys: str) -> tuple[Any | None, ...]:
        out: list[Any | None] = []
        for key in keys:
            out.append(await self.get(key))
        return tuple(out)

    async def set_many(self, pairs: Mapping[str, Any], ttl: float | None = None) -> None:
        for key, value in pairs.items():
            await self.set(key, value, ttl)

    async def delete(self, key: str) -> bool:
        with self._lock:
            deleted = self._data.pop(key, None) is not None
            self._freq.pop(key, None)
            return deleted

    async def delete_many(self, *keys: str) -> int:
        deleted = 0
        for key in keys:
            if await self.delete(key):
                deleted += 1
        return deleted

    async def delete_match(self, pattern: str) -> int:
        with self._lock:
            keys = [k for k in self._data if fnmatch.fnmatchcase(k, pattern)]
            for k in keys:
                del self._data[k]
                self._freq.pop(k, None)
            return len(keys)

    async def exists(self, key: str) -> bool:
        with self._lock:
            now = self._now()
            self._reap(now)
            return self._live_entry(key, now) is not None

    async def expire(self, key: str, ttl: float) -> bool:
        with self._lock:
            now = self._now()
            self._reap(now)
            entry = self._live_entry(key, now)
            if entry is None:
                return False
            expire_at = now + ttl
            self._data[key] = (entry[0], expire_at)
            heapq.heappush(self._heap, (expire_at, key))
            return True

    async def incr(self, key: str, delta: int = 1) -> int:
        with self._lock:
            now = self._now()
            self._reap(now)
            entry = self._live_entry(key, now)
            current = entry[0] if entry is not None else 0
            if not isinstance(current, int) or isinstance(current, bool):
                if entry is not None:
                    raise ValueError(f"Cannot incr non-integer value under {key!r}")
                current = 0
            new_value = current + delta
            expire_at = entry[1] if entry is not None else None
            self._data[key] = (new_value, expire_at)
            self._touch(key)
            return new_value

    async def ttl_of(self, key: str) -> float | None:
        """Remaining seconds before expiry (``None`` = no expiry / absent)."""
        with self._lock:
            now = self._now()
            self._reap(now)
            entry = self._live_entry(key, now)
            if entry is None:
                return None
            expire_at = entry[1]
            return None if expire_at is None else max(expire_at - now, 0.0)

    async def cull(self) -> int:
        """Drop every expired entry now (the incremental reaper is partial)."""
        with self._lock:
            now = self._now()
            expired = [k for k, (_v, exp) in self._data.items() if exp is not None and exp <= now]
            for k in expired:
                del self._data[k]
                self._freq.pop(k, None)
            self._heap = [(exp, k) for exp, k in self._heap if self._data.get(k, (None, None))[1] == exp]
            heapq.heapify(self._heap)
            return len(expired)

    def currsize(self) -> int:
        return len(self._data)

    async def scan(self, pattern: str, batch_size: int = 100) -> AsyncIterator[str]:
        with self._lock:
            now = self._now()
            self._reap(now)
            matching = [k for k in self._data if fnmatch.fnmatchcase(k, pattern)]
        for i in range(0, len(matching), batch_size):
            for key in matching[i : i + batch_size]:
                yield key

    async def clear(self) -> int:
        with self._lock:
            n = len(self._data)
            self._data.clear()
            self._heap.clear()
            self._freq.clear()
            return n

    async def ping(self) -> bytes:
        return b"PONG"

    async def close(self) -> None:
        await self.clear()

    # -- synchronous primitives --------------------------------------------------
    # The memory store is inherently synchronous; exposing ``_sync_*`` lets the
    # decorator's sync fast path run the whole cache flow inline instead of
    # paying a cross-thread hop through the event-loop bridge.

    def _sync_get(self, key: str) -> Any | None:
        with self._lock:
            now = self._now()
            self._reap(now)
            entry = self._live_entry(key, now)
            if entry is None:
                return None
            self._touch(key)
            return entry[0]

    def _sync_set(self, key: str, value: Any, ttl: float | None = None, exist: bool | None = None) -> bool:
        with self._lock:
            now = self._now()
            self._reap(now)
            exists = self._live_entry(key, now) is not None
            if exist is True and not exists:
                return False
            if exist is False and exists:
                return False
            self._store(key, value, ttl, now)
            return True

    def _sync_delete(self, key: str) -> bool:
        with self._lock:
            deleted = self._data.pop(key, None) is not None
            self._freq.pop(key, None)
            return deleted
