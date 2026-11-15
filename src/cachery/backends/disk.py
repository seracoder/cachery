"""Disk backend on stdlib ``sqlite3``.

WAL mode + ``synchronous=NORMAL`` for a good durability/speed tradeoff; a
single ``(key, value, expire_at)`` table. All sqlite calls run in a small
executor so the async contract never blocks the event loop. Deliberately not
wrapping ``diskcache`` — one less dependency to babysit, and full control over
the schema (native ``scan``/``delete_match``, which diskcache's sharding
breaks).
"""

from __future__ import annotations

import asyncio
import fnmatch
import os
import sqlite3
import threading
import time
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from cachery.backends.protocol import BaseBackend

__all__ = ["DiskBackend"]

_SCHEMA = """
CREATE TABLE IF NOT EXISTS cachery (
    key       TEXT PRIMARY KEY,
    value     BLOB NOT NULL,
    expire_at REAL
)
"""
_SIZE_UNITS = {"": 1, "kb": 1024, "mb": 1024**2, "gb": 1024**3}


def _parse_size(raw: str | int | None) -> int | None:
    if raw is None:
        return None
    if isinstance(raw, int):
        return raw
    text = raw.strip().lower()
    digits = "".join(ch for ch in text if ch.isdigit())
    unit = "".join(ch for ch in text if ch.isalpha())
    if unit not in _SIZE_UNITS or not digits:
        raise ValueError(f"Cannot parse size {raw!r}; expected e.g. '2gb' or 2048")
    return int(digits) * _SIZE_UNITS[unit]


class DiskBackend(BaseBackend):
    def __init__(
        self,
        path: str = "cachery.sqlite3",
        *,
        size_limit: str | int | None = None,
        purge_on_start: bool = True,
    ) -> None:
        self.path = str(path)
        self.size_limit = _parse_size(size_limit)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute(_SCHEMA)
        self._conn.commit()
        if purge_on_start:
            self._sync_purge_expired()

    # -- sync core (called via executor) --------------------------------------

    def _sync_purge_expired(self) -> None:
        with self._lock:
            self._conn.execute(
                "DELETE FROM cachery WHERE expire_at IS NOT NULL AND expire_at <= ?",
                (time.time(),),
            )
            self._conn.commit()

    def _sync_get(self, key: str) -> Any | None:
        with self._lock:
            row = self._conn.execute("SELECT value, expire_at FROM cachery WHERE key = ?", (key,)).fetchone()
        if row is None:
            return None
        value, expire_at = row
        if expire_at is not None and expire_at <= time.time():
            return None
        return value

    def _sync_set(self, key: str, value: Any, ttl: float | None, exist: bool | None) -> bool:
        expire_at = time.time() + ttl if ttl is not None else None
        with self._lock:
            exists = (
                self._conn.execute(
                    "SELECT 1 FROM cachery WHERE key = ? AND (expire_at IS NULL OR expire_at > ?)",
                    (key, time.time()),
                ).fetchone()
                is not None
            )
            if exist is True and not exists:
                return False
            if exist is False and exists:
                return False
            self._conn.execute(
                "INSERT INTO cachery (key, value, expire_at) VALUES (?, ?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value, "
                "expire_at = excluded.expire_at",
                (key, value, expire_at),
            )
            self._conn.commit()
        self._enforce_size_limit_sync()
        return True

    def _enforce_size_limit_sync(self) -> None:
        if self.size_limit is None:
            return
        with self._lock:
            row = self._conn.execute(
                "SELECT key, LENGTH(value) FROM cachery ORDER BY expire_at IS NULL, expire_at ASC"
            ).fetchall()
            total = sum(length for _k, length in row)
            if total <= self.size_limit:
                return
            # Evict soonest-to-expire entries first until within the budget.
            for key, length in row:
                if total <= self.size_limit:
                    break
                self._conn.execute("DELETE FROM cachery WHERE key = ?", (key,))
                total -= int(length)
            self._conn.commit()

    def _sync_delete(self, key: str) -> bool:
        with self._lock:
            cur = self._conn.execute("DELETE FROM cachery WHERE key = ?", (key,))
            self._conn.commit()
            return cur.rowcount > 0

    def _sync_exists(self, key: str) -> bool:
        return self._sync_get(key) is not None

    def _sync_expire(self, key: str, ttl: float) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM cachery WHERE key = ? AND (expire_at IS NULL OR expire_at > ?)",
                (key, time.time()),
            ).fetchone()
            if row is None:
                return False
            self._conn.execute(
                "UPDATE cachery SET expire_at = ? WHERE key = ?",
                (time.time() + ttl, key),
            )
            self._conn.commit()
            return True

    def _sync_incr(self, key: str, delta: int) -> int:
        with self._lock:
            row = self._conn.execute("SELECT value, expire_at FROM cachery WHERE key = ?", (key,)).fetchone()
            now = time.time()
            if row is not None and (row[1] is None or row[1] > now):
                current = row[0]
                if not isinstance(current, (int, bytes)) or isinstance(current, bool):
                    raise ValueError(f"Cannot incr non-integer value under {key!r}")
                current = int(current)
                new_value = current + delta
                expire_at = row[1]
            else:
                new_value = delta
                expire_at = None
            self._conn.execute(
                "INSERT INTO cachery (key, value, expire_at) VALUES (?, ?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, new_value, expire_at),
            )
            self._conn.commit()
            return new_value

    def _sync_keys(self, pattern: str) -> list[str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT key FROM cachery WHERE expire_at IS NULL OR expire_at > ?",
                (time.time(),),
            ).fetchall()
        return [k for (k,) in rows if fnmatch.fnmatchcase(k, pattern)]

    def _sync_clear(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT COUNT(*) FROM cachery").fetchone()
            n = int(row[0]) if row else 0
            self._conn.execute("DELETE FROM cachery")
            self._conn.commit()
            return n

    # -- async contract (executor bridge) --------------------------------------

    async def _run(self, fn: Any, *args: Any) -> Any:
        return await asyncio.to_thread(fn, *args)

    def _sync_ttl_of(self, key: str) -> float | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT expire_at FROM cachery WHERE key = ? AND (expire_at IS NULL OR expire_at > ?)",
                (key, time.time()),
            ).fetchone()
        if row is None:
            return None
        expire_at = row[0]
        return None if expire_at is None else max(expire_at - time.time(), 0.0)

    async def ttl_of(self, key: str) -> float | None:
        """Remaining seconds before expiry (``None`` = no expiry / absent)."""
        remaining: float | None = await self._run(self._sync_ttl_of, key)
        return remaining

    async def get(self, key: str) -> Any | None:
        return await self._run(self._sync_get, key)

    async def set(self, key: str, value: Any, ttl: float | None = None, exist: bool | None = None) -> bool:
        result: bool = await self._run(self._sync_set, key, value, ttl, exist)
        return result

    async def delete(self, key: str) -> bool:
        result: bool = await self._run(self._sync_delete, key)
        return result

    async def exists(self, key: str) -> bool:
        result: bool = await self._run(self._sync_exists, key)
        return result

    async def expire(self, key: str, ttl: float) -> bool:
        result: bool = await self._run(self._sync_expire, key, ttl)
        return result

    async def incr(self, key: str, delta: int = 1) -> int:
        result: int = await self._run(self._sync_incr, key, delta)
        return result

    async def scan(self, pattern: str, batch_size: int = 100) -> AsyncIterator[str]:
        keys = await self._run(self._sync_keys, pattern)
        for i in range(0, len(keys), batch_size):
            for key in keys[i : i + batch_size]:
                yield key

    async def clear(self) -> int:
        result: int = await self._run(self._sync_clear)
        return result

    async def ping(self) -> bytes:
        await self._run(lambda: None)
        return b"PONG"

    async def close(self) -> None:
        def _close() -> None:
            with self._lock:
                self._conn.close()

        await self._run(_close)

    def storage_size(self) -> int:
        try:
            return os.path.getsize(self.path)
        except OSError:
            return 0

    @classmethod
    def from_url_params(cls, path: str, **params: Any) -> DiskBackend:
        return cls(path=path, **params)


def default_path() -> Path:
    return Path("cachery.sqlite3")
