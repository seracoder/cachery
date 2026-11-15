"""TieredBackend tests: L1 hit avoids L2 round-trip (plan §14 Phase 3 exit criterion)."""

from __future__ import annotations

from typing import Any

import pytest

from cachery.backends.disk import DiskBackend
from cachery.backends.memory import MemoryBackend
from cachery.backends.tiered import TieredBackend


class CountingMemory(MemoryBackend):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.get_calls = 0
        self.set_calls = 0

    async def get(self, key: str) -> Any | None:
        self.get_calls += 1
        return await super().get(key)

    async def set(self, key: str, value: Any, ttl: float | None = None, exist: bool | None = None) -> bool:
        self.set_calls += 1
        return await super().set(key, value, ttl, exist)


@pytest.fixture
async def tiered() -> TieredBackend:
    l1 = CountingMemory(size=100)
    l2 = CountingMemory(size=100)
    backend = TieredBackend(l1=l1, l2=l2, l1_ttl_cap=60)
    yield backend
    await backend.close()


async def test_l1_hit_avoids_l2_roundtrip(tiered: TieredBackend) -> None:
    await tiered.set("k", b"v", ttl=3600)
    l2_gets_before = tiered.l2.get_calls

    assert await tiered.get("k") == b"v"
    assert await tiered.get("k") == b"v"
    assert tiered.l2.get_calls == l2_gets_before  # served from L1 alone


async def test_l2_hit_promotes_to_l1(tiered: TieredBackend) -> None:
    await tiered.l2.set("k", b"from-l2", ttl=3600)
    assert tiered.l1.get_calls == 0

    assert await tiered.get("k") == b"from-l2"  # miss L1, hit L2
    assert await tiered.get("k") == b"from-l2"  # now served by L1
    assert tiered.l2.get_calls == 1
    assert await tiered.l1.get("k") == b"from-l2"  # promoted


async def test_read_through_populates_both_levels(tiered: TieredBackend) -> None:
    await tiered.set("k", b"v", ttl=3600)
    assert await tiered.l1.get("k") == b"v"
    assert await tiered.l2.get("k") == b"v"


async def test_delete_invalidates_both_levels(tiered: TieredBackend) -> None:
    await tiered.set("k", b"v", ttl=60)
    assert await tiered.delete("k") is True
    assert await tiered.l1.get("k") is None
    assert await tiered.l2.get("k") is None


async def test_l1_ttl_cap_bounds_promotion(tiered: TieredBackend) -> None:
    await tiered.set("k", b"v", ttl=3600)  # cap is 60
    # peek L1 store directly: promotion TTL was min(3600, 60)
    await tiered.get("k")
    with tiered.l1._lock:
        entry = tiered.l1._data["k"]
    (expire_at,) = (entry[1],)
    assert expire_at is not None and expire_at - tiered.l1._now() <= 60.0


async def test_scan_unions_levels(tiered: TieredBackend) -> None:
    await tiered.l1.set("only-l1", b"1", ttl=60)
    await tiered.l2.set("only-l2", b"2", ttl=60)
    keys = sorted([k async for k in tiered.scan("*")])
    assert keys == ["only-l1", "only-l2"]


async def test_tiered_with_disk_l2(tmp_path: Any) -> None:
    l1 = MemoryBackend(size=10)
    l2 = DiskBackend(path=str(tmp_path / "t.sqlite3"))
    backend = TieredBackend(l1=l1, l2=l2)
    await backend.set("k", b"v", ttl=60)
    await l1.clear()  # simulate L1 eviction
    assert await backend.get("k") == b"v"  # read-through from disk
    await backend.close()
