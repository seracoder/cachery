"""The sync fast path (engine.sync_load/sync_store + SimpleStrategy.sync_run).

Sync callers of ``@cache``-decorated functions skip the cross-thread
event-loop bridge when both the backend (``_sync_get``/``_sync_set``) and the
strategy (``supports_sync_run``) can run inline. These tests pin the fast path
itself and every fallback/error branch around it.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from cachery import Cache
from cachery.backends.memory import MemoryBackend
from cachery.core.engine import Engine
from cachery.core.exceptions import BackendConnectionError, SerializationError
from cachery.serialize.types import clear_custom_types, register_type


class NoSyncBackend(MemoryBackend):
    """A backend that deliberately does not advertise inline sync primitives."""

    _sync_get = None  # type: ignore[assignment]
    _sync_set = None  # type: ignore[assignment]


class FlakySyncBackend(MemoryBackend):
    """Backend whose inline primitives fail like a down cache would."""

    def __init__(self, *, fail_get: bool = False, fail_set: bool = False) -> None:
        super().__init__()
        self.fail_get = fail_get
        self.fail_set = fail_set

    def _sync_get(self, key: str) -> Any | None:
        if self.fail_get:
            raise ConnectionError("boom")
        return super()._sync_get(key)

    def _sync_set(self, key: str, value: Any, ttl: float | None = None, exist: bool | None = None) -> bool:
        if self.fail_set:
            raise ConnectionError("boom")
        return super()._sync_set(key, value, ttl, exist)


# -- strategy-level fast path -----------------------------------------------------


def test_sync_simple_uses_inline_fast_path_and_hits() -> None:
    c = Cache()
    c.setup("mem://")
    calls = 0

    @c(ttl="1h")
    def fn(x: int) -> int:
        nonlocal calls
        calls += 1
        return x * 2

    assert fn(2) == 4  # miss + inline store
    assert fn(2) == 4  # inline hit, no recompute
    assert calls == 1
    assert fn.cache_info()["hits"] == 1
    assert fn.cache_info()["misses"] == 1
    assert asyncio.iscoroutinefunction(c.engine.backend.get)  # sanity: real backend


def test_sync_condition_false_skips_store() -> None:
    c = Cache()
    c.setup("mem://")
    calls = 0

    @c(ttl="1h", condition=lambda r: r is not None)
    def fn() -> int | None:
        nonlocal calls
        calls += 1
        return None

    assert fn() is None
    assert fn() is None
    assert calls == 2  # never cached


def test_sync_detect_records_meta() -> None:
    c = Cache()
    c.setup("mem://")

    @c(ttl="1h")
    def fn() -> int:
        return 1

    fn()
    with c.detect as d:
        fn()
    assert d.calls_total == 1
    assert d.calls[0]["name"] == "simple"
    assert d.calls[0]["ttl"] == 3600


def test_sync_disabled_runs_through() -> None:
    c = Cache()
    c.setup("mem://")
    calls = 0

    @c(ttl="1h")
    def fn() -> int:
        nonlocal calls
        calls += 1
        return calls

    with c.disable():
        fn()
        fn()
    assert calls == 2


def test_sync_callable_ttl_falls_back_to_bridge() -> None:
    c = Cache()
    c.setup("mem://")
    calls = 0

    def ttl_fn() -> float:
        return 60.0

    @c(ttl=ttl_fn)
    def fn() -> int:
        nonlocal calls
        calls += 1
        return 5

    assert fn() == 5  # bridge path (callable TTL needs the async engine)
    assert fn() == 5  # and still caches
    assert calls == 1


def test_sync_non_sync_backend_falls_back_to_bridge() -> None:
    c = Cache()
    c.setup(backend=NoSyncBackend())
    calls = 0

    @c(ttl="1h")
    def fn() -> int:
        nonlocal calls
        calls += 1
        return 7

    assert fn() == 7
    assert fn() == 7  # cached via the bridge path
    assert calls == 1


def test_sync_setup_after_decoration_switches_to_fast_path() -> None:
    c = Cache()
    calls = 0

    @c(ttl="1h")
    def fn() -> int:
        nonlocal calls
        calls += 1
        return 3

    # No setup yet → engine exists (memory default), fast path still applies.
    assert fn() == 3
    assert calls == 1


def test_sync_fail_soft_backend_error_serves_function() -> None:
    c = Cache()
    c.setup(backend=FlakySyncBackend(fail_get=True, fail_set=True), suppress=True)
    calls = 0

    @c(ttl="1h")
    def fn() -> int:
        nonlocal calls
        calls += 1
        return 9

    assert fn() == 9
    assert fn() == 9
    assert calls == 2  # cache never worked; function always ran


def test_sync_strict_backend_error_raises() -> None:
    c = Cache()
    c.setup(backend=FlakySyncBackend(fail_get=True), suppress=False)

    @c(ttl="1h")
    def fn() -> int:
        return 1

    with pytest.raises(BackendConnectionError, match="get"):
        fn()


def test_sync_store_error_suppressed() -> None:
    c = Cache()
    c.setup(backend=FlakySyncBackend(fail_set=True), suppress=True)
    calls = 0

    @c(ttl="1h")
    def fn() -> int:
        nonlocal calls
        calls += 1
        return 1

    assert fn() == 1  # store failed silently; function result returned
    assert calls == 1


# -- engine primitives ------------------------------------------------------------


async def test_sync_load_stored_none_is_a_hit() -> None:
    engine = Engine(MemoryBackend())
    await engine.set("k", None)
    hit, value = engine.sync_load("k")
    assert hit is True
    assert value is None


async def test_sync_load_corrupt_payload_suppressed() -> None:
    backend = MemoryBackend()
    engine = Engine(backend)
    await backend.set("k", b"\xff\xfebroken")
    hit, value = engine.sync_load("k")
    assert hit is False
    assert value is None


async def test_sync_load_corrupt_payload_strict_raises() -> None:
    backend = MemoryBackend()
    engine = Engine(backend, suppress=False)
    await backend.set("k", b"\xff\xfebroken")
    with pytest.raises(Exception):  # noqa: B017 - pickle/msgspec corruption error
        engine.sync_load("k")


class Unserializable:
    """Type whose registered custom encoder always fails."""


async def test_sync_store_unserializable_suppressed_returns_false() -> None:
    engine = Engine(MemoryBackend())

    def bad(_v: Any) -> Any:
        raise RuntimeError("cannot encode")

    register_type(Unserializable, bad, bad)
    try:
        assert engine.sync_store("k", Unserializable(), 60.0) is False
    finally:
        clear_custom_types()


async def test_sync_store_unserializable_strict_raises() -> None:
    engine = Engine(MemoryBackend(), suppress=False)

    def bad(_v: Any) -> Any:
        raise RuntimeError("cannot encode")

    register_type(Unserializable, bad, bad)
    try:
        with pytest.raises(SerializationError):
            engine.sync_store("k", Unserializable(), 60.0)
    finally:
        clear_custom_types()


async def test_sync_primitives_absent_for_plain_backend() -> None:
    engine = Engine(object())  # type: ignore[arg-type]
    assert engine.supports_sync() is False
