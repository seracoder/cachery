"""Deterministic fakes for unit tests."""

from __future__ import annotations

import fnmatch
from collections.abc import AsyncIterator
from typing import Any

from cachery.backends.protocol import BaseBackend

__all__ = ["FakeBackend"]


class FakeBackend(BaseBackend):
    """Fully deterministic, synchronous-state backend for tests.

    Records every operation for assertions: ``backend.ops`` is a list of
    ``(op, key)`` tuples, ``backend.fail_on`` is a set of keys that raise.
    """

    def __init__(self) -> None:
        self.data: dict[str, tuple[Any, float | None]] = {}
        self.ops: list[tuple[str, str]] = []
        self.fail_on: set[str] = set()
        self.now = 1000.0

    def _check(self, key: str) -> None:
        if key in self.fail_on:
            raise ConnectionError(f"injected failure for {key!r}")

    def _live(self, key: str) -> Any | None:
        entry = self.data.get(key)
        if entry is None:
            return None
        value, expire_at = entry
        if expire_at is not None and expire_at <= self.now:
            return None
        return value

    async def get(self, key: str) -> Any | None:
        self.ops.append(("get", key))
        self._check(key)
        return self._live(key)

    async def set(self, key: str, value: Any, ttl: float | None = None, exist: bool | None = None) -> bool:
        self.ops.append(("set", key))
        self._check(key)
        exists = self._live(key) is not None
        if (exist is True and not exists) or (exist is False and exists):
            return False
        self.data[key] = (value, self.now + ttl if ttl is not None else None)
        return True

    async def delete(self, key: str) -> bool:
        self.ops.append(("delete", key))
        self._check(key)
        return self.data.pop(key, None) is not None

    async def exists(self, key: str) -> bool:
        self.ops.append(("exists", key))
        self._check(key)
        return self._live(key) is not None

    async def expire(self, key: str, ttl: float) -> bool:
        self.ops.append(("expire", key))
        self._check(key)
        entry = self.data.get(key)
        if entry is None or self._live(key) is None:
            return False
        self.data[key] = (entry[0], self.now + ttl)
        return True

    async def incr(self, key: str, delta: int = 1) -> int:
        self.ops.append(("incr", key))
        self._check(key)
        current = self._live(key)
        value = (current if isinstance(current, int) else 0) + delta
        entry = self.data.get(key)
        self.data[key] = (value, entry[1] if entry else None)
        return value

    async def scan(self, pattern: str, batch_size: int = 100) -> AsyncIterator[str]:
        self.ops.append(("scan", pattern))
        for key in list(self.data):
            if fnmatch.fnmatchcase(key, pattern) and self._live(key) is not None:
                yield key

    async def clear(self) -> int:
        self.ops.append(("clear", "*"))
        n = len(self.data)
        self.data.clear()
        return n

    async def ping(self) -> bytes:
        self.ops.append(("ping", ""))
        return b"PONG"

    async def close(self) -> None:
        return None
