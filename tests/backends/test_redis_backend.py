"""Redis backend unit tests against a fake async client (no server needed).

The full contract suite runs against a real Redis in the ``integration``-marked
tests (CI spins one up via a service container; locally set
CACHEERY_TEST_REDIS_URL / CACHERY_TEST_REDIS_URL).
"""

from __future__ import annotations

from typing import Any

import pytest

from cachery.backends.redis import RedisBackend
from cachery.backends.registry import create_backend


class FakeRedis:
    """Minimal async fake mirroring the redis-py surface the backend uses."""

    def __init__(self) -> None:
        self.data: dict[str, Any] = {}
        self.ttls: dict[str, float] = {}
        self.scan_calls = 0

    async def get(self, key: str) -> Any | None:
        return self.data.get(key)

    async def set(
        self,
        key: str,
        value: Any,
        ex: float | None = None,
        xx: bool = False,
        nx: bool = False,
    ) -> bool:
        exists = key in self.data
        if (xx and not exists) or (nx and exists):
            return False
        self.data[key] = value
        self.ttls[key] = ex if ex is not None else float("inf")
        return True

    async def delete(self, *keys: str) -> int:
        n = 0
        for k in keys:
            if k in self.data:
                del self.data[k]
                n += 1
        return n

    async def exists(self, key: str) -> int:
        return 1 if key in self.data else 0

    async def expire(self, key: str, ttl: int) -> int:
        if key not in self.data:
            return 0
        self.ttls[key] = ttl
        return 1

    async def incrby(self, key: str, delta: int) -> int:
        current = self.data.get(key, 0)
        self.data[key] = current + delta
        return current + delta

    async def mget(self, keys: list[str]) -> list[Any | None]:
        return [self.data.get(k) for k in keys]

    async def ping(self) -> bytes:
        return b"PONG"

    async def aclose(self) -> None:
        return None

    def _match(self, pattern: str) -> list[str]:
        import fnmatch

        return [k for k in self.data if fnmatch.fnmatchcase(k, pattern)]

    async def scan(self, cursor: int = 0, match: str = "*", count: int = 100) -> tuple[int, list[str]]:
        self.scan_calls += 1
        return 0, self._match(match)


@pytest.fixture
async def backend() -> RedisBackend:
    fake = FakeRedis()
    b = RedisBackend(client=fake)
    yield b
    await b.close()


async def test_missing_extra_raises_helpful_error() -> None:
    import builtins

    real_import = builtins.__import__

    def blocked(name: str, *a: Any, **k: Any) -> Any:
        if name.startswith("redis"):
            raise ImportError("no redis")
        return real_import(name, *a, **k)

    builtins.__import__ = blocked
    try:
        with pytest.raises(ImportError, match=r"cachery\[redis\]"):
            RedisBackend(url="redis://localhost:6379/0")
    finally:
        builtins.__import__ = real_import


async def test_roundtrip(backend: RedisBackend) -> None:
    assert await backend.set("k", b"v", ttl=60) is True
    assert await backend.get("k") == b"v"


async def test_exist_flags_forwarded(backend: RedisBackend) -> None:
    assert await backend.set("k", b"1", exist=False) is True
    assert await backend.set("k", b"2", exist=False) is False
    assert await backend.set("k", b"3", exist=True) is True


async def test_get_many(backend: RedisBackend) -> None:
    await backend.set("a", b"1", ttl=60)
    await backend.set("b", b"2", ttl=60)
    assert await backend.get_many("a", "b", "zz") == (b"1", b"2", None)


async def test_scan_and_delete_match(backend: RedisBackend) -> None:
    for i in range(3):
        await backend.set(f"u:{i}", b"v", ttl=60)
    keys = sorted([k async for k in backend.scan("u:*")])
    assert keys == ["u:0", "u:1", "u:2"]
    assert await backend.delete_match("u:*") == 3
    assert await backend.get("u:0") is None


async def test_clear_uses_scan_not_flushall(backend: RedisBackend) -> None:
    await backend.set("a", b"1", ttl=60)
    assert await backend.clear() == 1
    fake: FakeRedis = backend._redis
    assert fake.scan_calls >= 1  # deleted via SCAN+DEL, never FLUSHALL


async def test_registry_redis_url() -> None:
    backend = create_backend("redis://localhost:6399/3?client_side=1")
    assert isinstance(backend, RedisBackend)
    await backend.close()


class FullFakeRedis(FakeRedis):
    """Adds the pipeline/pttl/pexpire/px surface so batch + TTL paths run."""

    async def set(
        self,
        key: str,
        value: Any,
        ex: float | None = None,
        px: float | None = None,
        xx: bool = False,
        nx: bool = False,
    ) -> bool:
        exists = key in self.data
        if (xx and not exists) or (nx and exists):
            return False
        self.data[key] = value
        if px is not None:
            self.ttls[key] = px / 1000.0
        else:
            self.ttls[key] = ex if ex is not None else float("inf")
        return True

    async def pexpire(self, key: str, ttl_ms: int) -> int:
        if key not in self.data:
            return 0
        self.ttls[key] = ttl_ms / 1000.0
        return 1

    async def pttl(self, key: str) -> int | None:
        if key not in self.data:
            return -2
        ttl = self.ttls.get(key)
        if ttl is None or ttl == float("inf"):
            return -1
        return int(ttl * 1000)

    def pipeline(self, transaction: bool = True) -> FakePipeline:
        return FakePipeline(self)


class FakePipeline:
    """Collects commands and replays them against the fake on execute()."""

    def __init__(self, fake: FullFakeRedis) -> None:
        self._fake = fake
        self._queued: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    def set(self, key: str, value: Any, **kwargs: Any) -> FakePipeline:
        self._queued.append(("set", (key, value), kwargs))
        return self

    async def execute(self) -> list[Any]:
        out = []
        for name, args, kwargs in self._queued:
            if name == "set":
                out.append(await self._fake.set(*args, **kwargs))
        return out

    async def __aenter__(self) -> FakePipeline:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None


@pytest.fixture
async def full_backend() -> RedisBackend:
    fake = FullFakeRedis()
    b = RedisBackend(client=fake)
    yield b
    await b.close()


async def test_set_sub_second_ttl_uses_px(full_backend: RedisBackend) -> None:
    assert await full_backend.set("k", b"v", ttl=0.25) is True
    fake: FullFakeRedis = full_backend._redis
    assert fake.ttls["k"] == 0.25  # px=250ms, not truncated ex


async def test_exists_delete_expire_incr(full_backend: RedisBackend) -> None:
    assert await full_backend.exists("k") is False
    await full_backend.set("k", b"v", ttl=60)
    assert await full_backend.exists("k") is True
    assert await full_backend.delete("k") is True
    assert await full_backend.delete("k") is False
    await full_backend.set("k", b"v", ttl=60)
    assert await full_backend.expire("k", 5.0) is True  # pexpire path
    assert await full_backend.incr("n") == 1
    assert await full_backend.incr("n", 4) == 5


async def test_ttl_of_mappings(full_backend: RedisBackend) -> None:
    assert await full_backend.ttl_of("absent") is None  # pttl -2
    await full_backend.set("k", b"v")  # no expiry → pttl -1
    assert await full_backend.ttl_of("k") is None
    await full_backend.set("t", b"v", ttl=2.0)
    assert 1.0 <= (await full_backend.ttl_of("t") or 0) <= 2.0


async def test_set_many_pipeline_and_empty(full_backend: RedisBackend) -> None:
    await full_backend.set_many({}, ttl=60)  # no-op
    await full_backend.set_many({"a": b"1", "b": b"2"}, ttl=60)
    assert await full_backend.get_many("a", "b") == (b"1", b"2")
    await full_backend.set_many({"c": b"3"}, ttl=None)
    assert await full_backend.get("c") == b"3"


async def test_delete_many_empty_and_nonempty(full_backend: RedisBackend) -> None:
    assert await full_backend.delete_many() == 0
    await full_backend.set("a", b"1", ttl=60)
    await full_backend.set("b", b"2", ttl=60)
    assert await full_backend.delete_many("a", "b", "zz") == 2


async def test_ping_returns_pong(full_backend: RedisBackend) -> None:
    assert await full_backend.ping() == b"PONG"


async def test_client_side_init_kwargs() -> None:
    from unittest.mock import patch

    with patch("redis.asyncio.Redis.from_url") as from_url:
        RedisBackend(url="redis://localhost:6379/0", client_side=True)
    kwargs = from_url.call_args.kwargs
    assert kwargs["protocol"] == 3
    assert kwargs["client_name"] == "cachery"
    assert kwargs["decode_responses"] is False


class BatchingScanRedis(FullFakeRedis):
    """scan() that returns keys in bounded batches → exercises DEL batching."""

    def __init__(self, batch: int) -> None:
        super().__init__()
        self._batch = batch

    async def scan(self, cursor: int = 0, match: str = "*", count: int = 100) -> tuple[int, list[str]]:
        keys = self._match(match)
        page = keys[cursor * self._batch : (cursor + 1) * self._batch]
        next_cursor = cursor + 1 if (cursor + 1) * self._batch < len(keys) else 0
        return next_cursor, page


async def test_delete_match_batches_deletes() -> None:
    fake = BatchingScanRedis(batch=2)
    backend = RedisBackend(client=fake)
    for i in range(5):
        await backend.set(f"u:{i}", b"v", ttl=60)
    assert await backend.delete_match("u:*") == 5
    assert fake.data == {}


async def test_clear_batches_deletes() -> None:
    fake = BatchingScanRedis(batch=2)
    backend = RedisBackend(client=fake)
    for i in range(5):
        await backend.set(f"u:{i}", b"v", ttl=60)
    assert await backend.clear() == 5
    assert fake.data == {}
