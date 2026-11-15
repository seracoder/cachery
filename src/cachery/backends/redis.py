"""Redis backend on ``redis.asyncio`` — optional extra ``redis``.

RESP3, pipelined batch operations, sha256 (not md5) default digest. The client
library is imported lazily so the core stays dependency-free; a clear
``ImportError`` tells the user which extra to install.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from typing import Any

from cachery.backends.protocol import BaseBackend

__all__ = ["RedisBackend"]

_DELETE_BATCH = 500


def _import_redis() -> Any:
    try:
        import redis.asyncio as redis_asyncio  # noqa: PLC0415 - optional extra

        return redis_asyncio
    except ImportError as e:
        raise ImportError("The Redis backend requires the 'redis' extra: pip install cachery[redis]") from e


class RedisBackend(BaseBackend):
    def __init__(
        self,
        url: str = "redis://localhost:6379/0",
        *,
        client_side: bool = False,
        client: Any | None = None,
    ) -> None:
        if client is not None:
            self._client = client
            self._owns_client = False
            return
        redis_asyncio = _import_redis()
        kwargs: dict[str, Any] = {"decode_responses": False}
        if client_side:
            kwargs["protocol"] = 3
            kwargs["client_name"] = "cachery"
        # A dedicated pool per instance: redis-py otherwise shares one global
        # pool per URL across event loops, which breaks when each backend is
        # used from a different loop (as under pytest-asyncio).
        self._client = redis_asyncio.Redis.from_url(url, **kwargs)
        self._owns_client = True

    @property
    def _redis(self) -> Any:
        return self._client

    # -- single-key ops ---------------------------------------------------------

    async def get(self, key: str) -> Any | None:
        return await self._redis.get(key)

    async def set(self, key: str, value: Any, ttl: float | None = None, exist: bool | None = None) -> bool:
        kwargs: dict[str, Any] = {}
        if exist is True:
            kwargs["xx"] = True
        elif exist is False:
            kwargs["nx"] = True
        if ttl is not None:
            # redis-py rejects ex=0 and sub-second values silently truncate;
            # px= keeps millisecond precision for short TTLs.
            if ttl < 1.0:
                result = await self._redis.set(key, value, px=int(max(ttl * 1000, 1)), **kwargs)
            else:
                result = await self._redis.set(key, value, ex=ttl, **kwargs)
        else:
            result = await self._redis.set(key, value, **kwargs)
        return bool(result)

    async def delete(self, key: str) -> bool:
        return bool(await self._redis.delete(key))

    async def exists(self, key: str) -> bool:
        return bool(await self._redis.exists(key))

    async def expire(self, key: str, ttl: float) -> bool:
        # Redis EXPIRE truncates to whole seconds; sub-second TTLs would
        # round to 0 (= immediate expiry semantics per redis-py >= 5) — use
        # PEXPIRE for millisecond precision instead.
        return bool(await self._redis.pexpire(key, int(ttl * 1000)))

    async def ttl_of(self, key: str) -> float | None:
        """Remaining seconds (``None`` = no expiry; ``-1.0`` sentinel mapped to None;
        ``-2.0`` absent)."""
        ttl = await self._redis.pttl(key)
        if ttl is None:
            return None
        t = int(ttl)
        if t < 0:
            return None
        return t / 1000.0

    async def incr(self, key: str, delta: int = 1) -> int:
        return int(await self._redis.incrby(key, delta))

    # -- batch ops (pipelined) -----------------------------------------------------

    async def get_many(self, *keys: str) -> tuple[Any | None, ...]:
        if not keys:
            return ()
        return tuple(await self._redis.mget(keys))

    async def set_many(self, pairs: Mapping[str, Any], ttl: float | None = None) -> None:
        if not pairs:
            return
        async with self._redis.pipeline(transaction=False) as pipe:
            for key, value in pairs.items():
                if ttl is not None:
                    pipe.set(key, value, ex=ttl)
                else:
                    pipe.set(key, value)
            await pipe.execute()

    async def delete_many(self, *keys: str) -> int:
        if not keys:
            return 0
        return int(await self._redis.delete(*keys))

    async def delete_match(self, pattern: str) -> int:
        deleted = 0
        batch: list[str] = []
        async for key in self.scan(pattern):
            batch.append(key)
            if len(batch) >= _DELETE_BATCH:
                deleted += int(await self._redis.delete(*batch))
                batch = []
        if batch:
            deleted += int(await self._redis.delete(*batch))
        return deleted

    # -- scan/clear/ping/close -------------------------------------------------------

    async def scan(self, pattern: str, batch_size: int = 100) -> AsyncIterator[str]:
        cursor: int | bytes = 0
        while True:
            cursor, keys = await self._redis.scan(cursor=cursor, match=pattern, count=batch_size)
            for key in keys:
                yield key.decode() if isinstance(key, bytes) else key
            if not cursor or int(cursor) == 0:
                break

    async def clear(self) -> int:
        """Deletes only cachery-visible keys (never FLUSHALL)."""
        n = 0
        batch: list[str] = []
        async for key in self.scan("*"):
            batch.append(key)
            if len(batch) >= _DELETE_BATCH:
                n += int(await self._redis.delete(*batch))
                batch = []
        if batch:
            n += int(await self._redis.delete(*batch))
        return n

    async def ping(self) -> bytes:
        result: bytes = await self._redis.ping()
        return result

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()
