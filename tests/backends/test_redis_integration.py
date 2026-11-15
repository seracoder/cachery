"""Integration tests against a real Redis (marked ``integration``).

Runs in CI via the Redis service container. Locally, either run a Redis on
localhost:6379 or set ``CACHEERY_TEST_REDIS_URL``. Without a server, these
tests skip cleanly — the unit suite (fake client) still covers the logic.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator

import pytest

from cachery.backends.redis import RedisBackend
from cachery.backends.registry import create_backend

pytestmark = pytest.mark.integration

_REDIS_URL = os.environ.get("CACHEERY_TEST_REDIS_URL") or os.environ.get(
    "CACHERY_TEST_REDIS_URL", "redis://localhost:6379/15"
)


async def _redis_available() -> bool:
    try:
        backend = RedisBackend(url=_REDIS_URL)
    except ImportError:
        return False
    try:
        await asyncio.wait_for(backend.ping(), timeout=1.5)
    except Exception:
        return False
    finally:
        await backend.close()
    return True


@pytest.fixture
async def backend() -> AsyncIterator[RedisBackend]:
    if not await _redis_available():
        pytest.skip(f"No Redis server at {_REDIS_URL}")
    b = RedisBackend(url=_REDIS_URL)
    await b.clear()
    yield b
    await b.clear()
    await b.close()


async def test_roundtrip(backend: RedisBackend) -> None:
    assert await backend.set("k", b"v", ttl=60) is True
    assert await backend.get("k") == b"v"


async def test_ttl_expiry(backend: RedisBackend) -> None:
    await backend.set("k", b"v", ttl=1)
    assert await backend.get("k") == b"v"
    await backend.expire("k", 1)
    await asyncio.sleep(1.1)
    assert await backend.get("k") is None


async def test_exist_flags(backend: RedisBackend) -> None:
    assert await backend.set("nx", b"1", exist=False) is True
    assert await backend.set("nx", b"2", exist=False) is False
    assert await backend.set("nx", b"3", exist=True) is True


async def test_batch_ops(backend: RedisBackend) -> None:
    await backend.set_many({"a": b"1", "b": b"2"}, ttl=60)
    assert await backend.get_many("a", "b") == (b"1", b"2")
    assert await backend.delete_many("a", "b") == 2


async def test_scan_delete_match_and_clear(backend: RedisBackend) -> None:
    for i in range(5):
        await backend.set(f"user:{i}", b"v", ttl=60)
    keys = sorted([k async for k in backend.scan("user:*")])
    assert keys == [f"user:{i}" for i in range(5)]
    assert await backend.delete_match("user:*") == 5
    await backend.set("x", b"1", ttl=60)
    assert await backend.clear() >= 1


async def test_incr(backend: RedisBackend) -> None:
    await backend.delete("n")
    assert await backend.incr("n") == 1
    assert await backend.incr("n", 9) == 10


async def test_tiered_url_creates_composite(backend: RedisBackend) -> None:
    l2_part = _REDIS_URL.split("://", 1)[1]
    tiered = create_backend(f"tiered://mem+redis://{l2_part}")
    await tiered.set("k", b"v", ttl=60)
    assert await tiered.get("k") == b"v"
    assert await tiered.delete("k") is True
    await tiered.close()
