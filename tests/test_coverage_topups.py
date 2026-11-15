"""Coverage top-ups: conditions, direct API, registry, bridge, tiered edges."""

from __future__ import annotations

import asyncio
import datetime as dt
from typing import Any

import pytest

from cachery import NOT_NONE, Cache, time_condition
from cachery.backends.disk import DiskBackend
from cachery.backends.memory import MemoryBackend
from cachery.backends.registry import create_backend, register_scheme
from cachery.backends.tiered import TieredBackend
from cachery.core.engine import Engine
from cachery.testing.fakes import FakeBackend

# -- conditions -------------------------------------------------------------------


def test_not_none() -> None:
    assert NOT_NONE(object()) is True
    assert NOT_NONE(None) is False


def test_time_condition_inside_window() -> None:
    now = dt.datetime.now().time()
    start = (dt.datetime.combine(dt.date.today(), now) - dt.timedelta(seconds=30)).time()
    end = (dt.datetime.combine(dt.date.today(), now) + dt.timedelta(seconds=30)).time()
    cond = time_condition(start, end)
    assert cond("anything") is True


def test_time_condition_outside_window() -> None:
    now = dt.datetime.now().time()
    start = (dt.datetime.combine(dt.date.today(), now) - dt.timedelta(seconds=600)).time()
    end = (dt.datetime.combine(dt.date.today(), now) - dt.timedelta(seconds=300)).time()
    assert time_condition(start, end)("x") is False


def test_time_condition_midnight_crossing() -> None:
    late = dt.time(23, 0)
    early = dt.time(1, 0)
    cond = time_condition(late, early)  # window crosses midnight
    now = dt.datetime.now().time()
    expected = now >= late or now < early
    assert cond("x") is expected


# -- direct / low-level API (plan §5.7) ----------------------------------------------


async def test_direct_api_surface() -> None:
    c = Cache()
    c.setup("mem://")

    assert await c.set("k", 1, ttl="1h") is True
    assert await c.get("k") == 1
    assert await c.get("missing", default="d") == "d"

    # get_or_set with callable and coroutine defaults
    assert await c.get_or_set("gos1", lambda: 42, ttl="1h") == 42
    assert await c.get_or_set("gos2", 7, ttl="1h") == 7

    async def async_default() -> int:
        return 99

    assert await c.get_or_set("gos3", async_default(), ttl="1h") == 99

    await c.set_many({"m1": 1, "m2": 2}, ttl="1h")
    assert await c.get_many("m1", "m2") == (1, 2)

    assert await c.exists("k") is True
    assert await c.incr("ctr", 5) == 5
    assert await c.expire("k", "1h") is True
    assert await c.delete("k") is True
    assert await c.delete_many("m1", "m2") == 2

    await c.set("p:1", "v1", ttl="1h")
    assert await c.delete_match("p:*") == 1

    pairs = [(k, v) async for k, v in c.get_match("*")]
    assert isinstance(pairs, list)

    assert await c.ping() == b"PONG"
    assert await c.clear() >= 0

    lock = c.lock("lk", ttl=5)
    async with lock:
        pass
    await c.close()


async def test_get_or_set_second_call_hits_cache() -> None:
    c = Cache()
    c.setup("mem://")
    calls = 0

    def factory() -> int:
        nonlocal calls
        calls += 1
        return 1

    assert await c.get_or_set("k", factory, ttl="1h") == 1
    assert await c.get_or_set("k", factory, ttl="1h") == 1
    assert calls == 1
    await c.close()


# -- registry ------------------------------------------------------------------------


def test_register_custom_scheme() -> None:
    created: list[str] = []

    def factory(url: str) -> MemoryBackend:
        created.append(url)
        return MemoryBackend(size=3)

    register_scheme("custom", factory)
    backend = create_backend("custom://whatever")
    assert isinstance(backend, MemoryBackend)
    assert created == ["custom://whatever"]


def test_unknown_scheme_raises() -> None:
    with pytest.raises(ValueError, match="scheme"):
        create_backend("ftp://nope")


def test_memory_url_with_params() -> None:
    backend = create_backend("mem://?size=7")
    assert isinstance(backend, MemoryBackend)
    assert backend.size == 7


# -- sync bridge -----------------------------------------------------------------------


def test_bridge_run_blocks_until_done() -> None:
    from cachery.core.sync_bridge import SyncBridge

    bridge = SyncBridge()

    async def coro() -> str:
        await asyncio.sleep(0.01)
        return "done"

    assert bridge.run(coro()) == "done"
    bridge.close()


def test_bridge_rejects_reentrant_deadlock() -> None:
    from cachery.core.sync_bridge import SyncBridge

    bridge = SyncBridge()

    async def inner() -> None:
        # Re-enter run() from inside the bridge loop thread → deadlock guard.
        import threading

        assert threading.current_thread().name == "cachery-sync-bridge"
        bridge.run(asyncio.sleep(0))  # type: ignore[arg-type]

    with pytest.raises(RuntimeError, match="deadlock"):
        bridge.run(inner())
    bridge.close()


# -- tiered edges ------------------------------------------------------------------------


async def test_tiered_get_many_promotes_in_bulk() -> None:
    l1 = MemoryBackend(size=10)
    l2 = MemoryBackend(size=10)
    backend = TieredBackend(l1=l1, l2=l2)
    await backend.l2.set("a", b"1", ttl=60)
    await backend.l2.set("b", b"2", ttl=60)
    assert await backend.get_many("a", "b", "zz") == (b"1", b"2", None)
    assert await backend.l1.get("a") == b"1"  # promoted
    assert await backend.exists("a") is True
    await backend.close()


async def test_tiered_incr_and_expire_delegate_to_l2() -> None:
    l1 = MemoryBackend(size=10)
    l2 = MemoryBackend(size=10)
    backend = TieredBackend(l1=l1, l2=l2)
    assert await backend.incr("n", 3) == 3
    await backend.set("k", b"v", ttl=60)
    assert await backend.expire("k", 60) is True
    assert await backend.exists("k") is True
    await backend.close()


async def test_tiered_ping_and_clear() -> None:
    l1 = MemoryBackend(size=10)
    l2 = MemoryBackend(size=10)
    backend = TieredBackend(l1=l1, l2=l2)
    await backend.set("x", b"1", ttl=60)
    assert await backend.ping() == b"PONG"
    assert await backend.clear() == 1
    await backend.close()


# -- engine misc ---------------------------------------------------------------------------


async def test_engine_scan_and_delete_match_with_prefix() -> None:
    fake = FakeBackend()
    engine = Engine(fake, prefix="app")
    await engine.set("user:1", "v", ttl=60)
    raw_keys = [k async for k in engine.scan("user:*")]
    assert raw_keys == ["app:user:1"]
    assert await engine.delete_match("user:*") == 1
    await engine.close()


async def test_engine_suppressed_bad_payload_returns_default() -> None:
    fake = FakeBackend()
    engine = Engine(fake)
    await fake.set("bad", b"\x99corrupt", ttl=60)
    assert await engine.get("bad", default="fallback") == "fallback"
    await engine.close()


async def test_engine_set_many_and_get_many_roundtrip() -> None:
    fake = FakeBackend()
    engine = Engine(fake)
    await engine.set_many({"a": 1, "b": 2}, ttl=60)
    assert await engine.get_many("a", "b") == (1, 2)
    await engine.close()


# -- fakes: exercise the remaining contract surface -------------------------------------------


async def test_fake_backend_full_surface(fake_backend: FakeBackend) -> None:
    b = fake_backend
    assert await b.set("a", b"1", ttl=60) is True
    assert await b.set("a", b"2", exist=False) is False
    assert await b.set_many({"x": b"1", "y": b"2"}, ttl=60) is None
    assert await b.get_many("a", "x", "nope") == (b"1", b"1", None)
    assert await b.delete_many("a", "x") == 2
    assert await b.expire("y", 30) is True
    assert await b.ping() == b"PONG"
    keys = [k async for k in b.scan("*")]
    assert keys == ["y"]
    assert await b.clear() == 1
    assert await b.close() is None


async def test_fake_backend_incr_on_stored_value(fake_backend: FakeBackend) -> None:
    await fake_backend.set("n", 5, ttl=60)
    assert await fake_backend.incr("n", 2) == 7


# -- disk URL edge ------------------------------------------------------------------------------


async def test_disk_url_windows_drive(tmp_path: Any) -> None:
    backend = create_backend(f"disk:///{tmp_path}/win.sqlite3")
    assert isinstance(backend, DiskBackend)
    await backend.set("k", b"v", ttl=60)
    await backend.close()
    assert (tmp_path / "win.sqlite3").exists()
