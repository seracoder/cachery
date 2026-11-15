"""Memory backend tests (plan §13.1.1 seed pattern)."""

from __future__ import annotations

import asyncio
import time

import pytest

from cachery.backends.memory import MemoryBackend


@pytest.fixture
async def backend() -> MemoryBackend:
    b = MemoryBackend(size=100)
    yield b
    await b.close()


async def test_set_then_get_roundtrip(backend: MemoryBackend) -> None:
    assert await backend.set("k", {"a": 1}, ttl=60) is True
    assert await backend.get("k") == {"a": 1}


async def test_get_missing_returns_none(backend: MemoryBackend) -> None:
    assert await backend.get("missing") is None


async def test_expired_key_returns_none(backend: MemoryBackend) -> None:
    await backend.set("k", "v", ttl=0.05)
    await asyncio.sleep(0.1)
    assert await backend.get("k") is None


async def test_exists_reflects_expiry(backend: MemoryBackend) -> None:
    await backend.set("k", "v", ttl=0.05)
    assert await backend.exists("k") is True
    await asyncio.sleep(0.1)
    assert await backend.exists("k") is False


async def test_no_ttl_persists(backend: MemoryBackend) -> None:
    await backend.set("k", "v")
    assert await backend.get("k") == "v"


async def test_lru_eviction_under_size_pressure() -> None:
    b = MemoryBackend(size=2)
    await b.set("a", 1, ttl=60)
    await b.set("b", 2, ttl=60)
    await b.get("a")  # touch a → b is now LRU
    await b.set("c", 3, ttl=60)  # evicts b
    assert await b.get("a") == 1
    assert await b.get("b") is None
    assert await b.get("c") == 3
    await b.close()


async def test_zero_size_rejected() -> None:
    with pytest.raises(ValueError):
        MemoryBackend(size=0)


async def test_exist_semantics(backend: MemoryBackend) -> None:
    assert await backend.set("k", 1, exist=False) is True  # NX on absent → ok
    assert await backend.set("k", 2, exist=False) is False  # NX on present → no
    assert await backend.get("k") == 1
    assert await backend.set("k", 3, exist=True) is True  # XX on present → ok
    assert await backend.set("nope", 1, exist=True) is False
    assert await backend.get("nope") is None


async def test_delete(backend: MemoryBackend) -> None:
    await backend.set("k", 1)
    assert await backend.delete("k") is True
    assert await backend.delete("k") is False
    assert await backend.get("k") is None


async def test_delete_many_counts(backend: MemoryBackend) -> None:
    await backend.set("a", 1)
    await backend.set("b", 2)
    assert await backend.delete_many("a", "b", "c") == 2


async def test_delete_match_glob(backend: MemoryBackend) -> None:
    await backend.set("user:1", 1)
    await backend.set("user:2", 2)
    await backend.set("order:1", 3)
    assert await backend.delete_match("user:*") == 2
    assert await backend.get("user:1") is None
    assert await backend.get("order:1") == 3


async def test_scan_yields_matching_keys(backend: MemoryBackend) -> None:
    for i in range(5):
        await backend.set(f"user:{i}", i)
    keys = [k async for k in backend.scan("user:*")]
    assert sorted(keys) == [f"user:{i}" for i in range(5)]


async def test_incr(backend: MemoryBackend) -> None:
    assert await backend.incr("n") == 1
    assert await backend.incr("n") == 2
    assert await backend.incr("n", 10) == 12
    assert await backend.incr("n", -2) == 10


async def test_incr_on_non_integer_raises(backend: MemoryBackend) -> None:
    await backend.set("k", "str")
    with pytest.raises(ValueError):
        await backend.incr("k")


async def test_expire_extends_or_shortens(backend: MemoryBackend) -> None:
    await backend.set("k", "v", ttl=60)
    assert await backend.expire("k", 0.05) is True
    await asyncio.sleep(0.1)
    assert await backend.get("k") is None
    assert await backend.expire("missing", 60) is False


async def test_get_many_matches_sequential_get(backend: MemoryBackend) -> None:
    await backend.set("k1", 1, ttl=60)
    await backend.set("k2", 2, ttl=60)
    assert await backend.get_many("k1", "k2", "missing") == (1, 2, None)


async def test_set_many(backend: MemoryBackend) -> None:
    await backend.set_many({"a": 1, "b": 2}, ttl=60)
    assert await backend.get("a") == 1
    assert await backend.get("b") == 2


async def test_clear_returns_count(backend: MemoryBackend) -> None:
    await backend.set("a", 1)
    await backend.set("b", 2)
    assert await backend.clear() == 2
    assert await backend.get("a") is None


async def test_ping(backend: MemoryBackend) -> None:
    assert await backend.ping() == b"PONG"


async def test_expiry_reaper_amortized_under_many_keys() -> None:
    b = MemoryBackend(size=1000)
    t0 = time.perf_counter()
    for i in range(200):
        await b.set(f"k{i}", i, ttl=0.01)
    await asyncio.sleep(0.05)
    for i in range(200):  # each get triggers a bounded reap
        await b.get(f"k{i}")
    elapsed = time.perf_counter() - t0
    assert elapsed < 2.0  # no O(n²) blowup
    await b.close()
