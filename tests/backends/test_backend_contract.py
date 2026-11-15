"""Shared contract suite every backend must satisfy (plan §13.1).

Parametrized over Memory/Disk (and Tiered in its own module). Redis runs this
same suite in the integration-marked tests. This is how "identical behavior
across backends" is *enforced*, not claimed.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import pytest

from cachery.backends.disk import DiskBackend
from cachery.backends.memory import MemoryBackend


async def _make_backend(kind: str, tmp: Any) -> Any:
    if kind == "memory":
        return MemoryBackend(size=100)
    if kind == "disk":
        return DiskBackend(path=str(tmp / f"{kind}.sqlite3"))
    raise ValueError(kind)


@pytest.fixture(params=["memory", "disk"])
async def any_backend(request: Any, tmp_path: Any) -> AsyncIterator[Any]:
    backend = await _make_backend(request.param, tmp_path)
    yield backend
    await backend.close()


async def test_get_set_roundtrip(any_backend: Any) -> None:
    await any_backend.set("k", b"\x01payload", ttl=60)
    assert await any_backend.get("k") == b"\x01payload"


async def test_missing_get_returns_none(any_backend: Any) -> None:
    assert await any_backend.get("nope") is None


async def test_ttl_expiry(any_backend: Any) -> None:
    await any_backend.set("k", b"v", ttl=0.05)
    await asyncio.sleep(0.15)
    assert await any_backend.get("k") is None
    assert await any_backend.exists("k") is False


async def test_no_ttl_never_expires(any_backend: Any) -> None:
    await any_backend.set("k", b"v")
    assert await any_backend.get("k") == b"v"


async def test_overwrite_updates_value(any_backend: Any) -> None:
    await any_backend.set("k", b"1")
    await any_backend.set("k", b"2", ttl=60)
    assert await any_backend.get("k") == b"2"


async def test_exist_nx(any_backend: Any) -> None:
    assert await any_backend.set("k", b"1", exist=False) is True
    assert await any_backend.set("k", b"2", exist=False) is False
    assert await any_backend.get("k") == b"1"


async def test_exist_xx(any_backend: Any) -> None:
    assert await any_backend.set("k", b"1", exist=True) is False
    await any_backend.set("k", b"1")
    assert await any_backend.set("k", b"2", exist=True) is True


async def test_delete(any_backend: Any) -> None:
    await any_backend.set("k", b"v")
    assert await any_backend.delete("k") is True
    assert await any_backend.delete("k") is False


async def test_exists(any_backend: Any) -> None:
    await any_backend.set("k", b"v", ttl=60)
    assert await any_backend.exists("k") is True
    await any_backend.delete("k")
    assert await any_backend.exists("k") is False


async def test_expire_shortens_life(any_backend: Any) -> None:
    await any_backend.set("k", b"v", ttl=60)
    assert await any_backend.expire("k", 0.05) is True
    await asyncio.sleep(0.15)
    assert await any_backend.get("k") is None
    assert await any_backend.expire("missing", 60) is False


async def test_incr_from_zero(any_backend: Any) -> None:
    assert await any_backend.incr("n") == 1
    assert await any_backend.incr("n", 5) == 6


async def test_get_many(any_backend: Any) -> None:
    await any_backend.set("a", b"1", ttl=60)
    await any_backend.set("b", b"2", ttl=60)
    assert await any_backend.get_many("a", "b", "zzz") == (b"1", b"2", None)


async def test_set_many(any_backend: Any) -> None:
    await any_backend.set_many({"a": b"1", "b": b"2"}, ttl=60)
    assert await any_backend.get("a") == b"1"
    assert await any_backend.get("b") == b"2"


async def test_delete_many(any_backend: Any) -> None:
    await any_backend.set("a", b"1")
    await any_backend.set("b", b"2")
    assert await any_backend.delete_many("a", "b", "c") == 2


async def test_scan_and_delete_match(any_backend: Any) -> None:
    for i in range(3):
        await any_backend.set(f"user:{i}", b"v", ttl=60)
    await any_backend.set("order:1", b"v", ttl=60)
    keys = sorted([k async for k in any_backend.scan("user:*")])
    assert keys == ["user:0", "user:1", "user:2"]
    assert await any_backend.delete_match("user:*") == 3
    assert await any_backend.get("user:0") is None
    assert await any_backend.get("order:1") == b"v"


async def test_clear_returns_count(any_backend: Any) -> None:
    await any_backend.set("a", b"1")
    await any_backend.set("b", b"2")
    n = await any_backend.clear()
    assert n >= 2
    assert await any_backend.get("a") is None


async def test_ping(any_backend: Any) -> None:
    assert await any_backend.ping() == b"PONG"
