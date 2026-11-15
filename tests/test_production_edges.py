"""Production-readiness edge tests: engine guards, strategy edges, codec wire
format errors, backend defaults, key-building failures, CLI, and observability
plumbing. Complements the per-area suites with the failure/defensive branches
a long-running service actually hits.
"""

from __future__ import annotations

import asyncio
import dataclasses
import functools
import sys
import threading
from typing import Any

import pytest

from cachery import Cache
from cachery.backends.memory import MemoryBackend
from cachery.backends.protocol import BaseBackend
from cachery.backends.registry import create_backend
from cachery.backends.tiered import TieredBackend
from cachery.core.engine import Engine
from cachery.core.exceptions import (
    BackendConnectionError,
    CacheryError,
    InvalidKeyTemplateError,
    LockNotAcquiredError,
)
from cachery.core.keys import KeyBuilder
from cachery.core.middleware import emit
from cachery.serialize import codec, compress
from cachery.serialize.exceptions import SerializationError
from cachery.serialize.hashing import digest, hmac_digest
from cachery.serialize.types import clear_custom_types, register_type
from cachery.strategies._base import jittered, schedule_background
from cachery.testing.fakes import FakeBackend

# -- engine: fail-soft and strict guards -------------------------------------------


async def test_engine_set_unserializable_suppressed() -> None:
    class Boom:
        pass

    def bad(_v: Any) -> Any:
        raise RuntimeError("nope")

    register_type(Boom, bad, bad)
    try:
        engine = Engine(MemoryBackend())
        assert await engine.set("k", Boom(), ttl=60) is False
    finally:
        clear_custom_types()


async def test_engine_set_unserializable_strict() -> None:
    class Boom:
        pass

    def bad(_v: Any) -> Any:
        raise RuntimeError("nope")

    register_type(Boom, bad, bad)
    try:
        engine = Engine(MemoryBackend(), suppress=False)
        with pytest.raises(SerializationError):
            await engine.set("k", Boom(), ttl=60)
    finally:
        clear_custom_types()


async def test_engine_get_corrupt_payload_strict_raises() -> None:
    backend = MemoryBackend()
    await backend.set("k", b"\x10\x00not-pickle")
    engine = Engine(backend, suppress=False)
    with pytest.raises(Exception, match=r"Deserialization|pickle"):
        await engine.get("k")


async def test_engine_get_entry_corrupt_payload_suppressed() -> None:
    backend = MemoryBackend()
    await backend.set("k", b"\x10\x00not-pickle")
    engine = Engine(backend)
    hit, value = await engine.get_entry("k")
    assert hit is False and value is None


async def test_engine_get_entry_corrupt_payload_strict() -> None:
    backend = MemoryBackend()
    await backend.set("k", b"\x10\x00not-pickle")
    engine = Engine(backend, suppress=False)
    with pytest.raises(Exception, match=r"Deserialization|pickle"):
        await engine.get_entry("k")


async def test_engine_load_value_corrupt_payload_suppressed() -> None:
    backend = MemoryBackend()
    await backend.set("k", b"\x10\x00not-pickle")
    engine = Engine(backend)
    hit, value = await engine.load_value("k")
    assert hit is False and value is None


async def test_engine_load_value_corrupt_payload_strict() -> None:
    backend = MemoryBackend()
    await backend.set("k", b"\x10\x00not-pickle")
    engine = Engine(backend, suppress=False)
    with pytest.raises(Exception, match=r"Deserialization|pickle"):
        await engine.load_value("k")


async def test_engine_incr_on_down_backend_returns_delta() -> None:
    class Down(MemoryBackend):
        async def incr(self, key: str, delta: int = 1) -> int:
            raise ConnectionError("down")

    engine = Engine(Down())
    assert await engine.incr("n") == 1
    assert await engine.incr("n", 5) == 5


async def test_engine_set_many_unserializable_suppressed() -> None:
    class Boom:
        pass

    def bad(_v: Any) -> Any:
        raise RuntimeError("nope")

    register_type(Boom, bad, bad)
    try:
        engine = Engine(MemoryBackend())
        await engine.set_many({"ok": 1, "bad": Boom()}, ttl=60)  # silently dropped
    finally:
        clear_custom_types()


async def test_engine_set_many_unserializable_strict() -> None:
    class Boom:
        pass

    def bad(_v: Any) -> Any:
        raise RuntimeError("nope")

    register_type(Boom, bad, bad)
    try:
        engine = Engine(MemoryBackend(), suppress=False)
        with pytest.raises(SerializationError):
            await engine.set_many({"bad": Boom()}, ttl=60)
    finally:
        clear_custom_types()


class CacheryRaisingBackend(MemoryBackend):
    """A backend erroring with a CacheryError — re-raised as-is, never wrapped."""

    async def get(self, key: str) -> Any | None:
        raise CacheryError("structured failure")


async def test_engine_guard_reraises_cachery_errors_untouched() -> None:
    engine = Engine(CacheryRaisingBackend(), suppress=True)
    with pytest.raises(CacheryError):
        await engine.get("k")


async def test_engine_scan_backend_error_suppressed_stops() -> None:
    class ExplodingScan(MemoryBackend):
        async def scan(self, pattern: str, batch_size: int = 100) -> Any:
            yield "one"
            raise ConnectionError("scan died")

    engine = Engine(ExplodingScan())
    keys = [k async for k in engine.scan("*")]
    assert keys == ["one"]  # error swallowed mid-iteration


async def test_engine_scan_backend_error_strict_raises() -> None:
    class ExplodingScan(MemoryBackend):
        async def scan(self, pattern: str, batch_size: int = 100) -> Any:
            yield "one"
            raise ConnectionError("scan died")

    engine = Engine(ExplodingScan(), suppress=False)
    with pytest.raises(BackendConnectionError, match="scan"):
        [k async for k in engine.scan("*")]


async def test_engine_scan_error_emits_middleware_event() -> None:
    events: list[Any] = []

    async def mw(event: Any) -> None:
        events.append(event)

    class ExplodingScan(MemoryBackend):
        async def scan(self, pattern: str, batch_size: int = 100) -> Any:
            raise ConnectionError("scan died")
            yield "never"  # pragma: no cover - make it a generator

    engine = Engine(ExplodingScan(), middlewares=[mw])
    keys = [k async for k in engine.scan("*")]
    assert keys == []
    assert len(events) == 1
    assert events[0].name == "error"


async def test_engine_scan_cachery_error_not_wrapped() -> None:
    class CacheryScan(MemoryBackend):
        async def scan(self, pattern: str, batch_size: int = 100) -> Any:
            raise CacheryError("structured")
            yield "never"  # pragma: no cover

    engine = Engine(CacheryScan(), suppress=True)
    with pytest.raises(CacheryError):
        [k async for k in engine.scan("*")]


# -- engine: lock ------------------------------------------------------------------


async def test_lock_timeout_raises_lock_not_acquired() -> None:
    engine = Engine(MemoryBackend())
    # Another "process" holds the lock:
    await engine.set("__lock__:k", b"other", ttl=60)
    with pytest.raises(LockNotAcquiredError):
        async with engine.lock("k", ttl=-100):  # deadline already in the past
            pass  # pragma: no cover


async def test_full_key_hashes_tag_suffix_outside_hmac() -> None:
    engine = Engine(MemoryBackend(), secret=b"s3cret")
    plain = engine.full_key("app:fn:5:tags:abc123def456")
    assert plain.endswith(":tags:abc123def456")  # suffix stays readable
    assert "app:fn:5" not in plain  # body is HMAC'd


async def test_engine_guard_iter_stop_async_iteration_is_normal() -> None:
    engine = Engine(MemoryBackend())
    await engine.set("a", 1)
    keys = [k async for k in engine.scan("*")]
    assert keys == ["a"]


# -- engine: middlewares on set -----------------------------------------------------


async def test_engine_set_emits_middleware_event() -> None:
    events: list[Any] = []

    async def mw(event: Any) -> None:
        events.append(event)

    engine = Engine(MemoryBackend(), middlewares=[mw])
    await engine.set("k", 1, ttl=60)
    assert len(events) == 1
    assert events[0].name == "set"


async def test_middleware_errors_swallowed() -> None:
    def bad(_event: Any) -> None:
        raise RuntimeError("middleware bug")

    events: list[Any] = []

    async def good(event: Any) -> None:
        events.append(event)

    await emit([bad, good], object())  # type: ignore[arg-type]
    assert len(events) == 1  # good still ran despite bad raising


# -- decorator surface ---------------------------------------------------------------


async def test_disable_async_context_manager() -> None:
    c = Cache()
    c.setup("mem://")
    calls = 0

    @c(ttl="1h")
    async def fn() -> int:
        nonlocal calls
        calls += 1
        return 1

    async with c.disable():
        await fn()
        await fn()
    await fn()
    assert calls == 3
    await c.close()


def test_cache_info_reset_and_clear() -> None:
    c = Cache()
    c.setup("mem://")

    @c(ttl="1h")
    def fn() -> int:
        return 1

    fn()
    fn()
    info = fn.cache_info()
    assert info["hits"] == 1 and info["misses"] == 1 and info["total"] == 2
    assert info["hit_rate"] == 0.5
    fn.cache_info.reset()
    assert fn.cache_info()["total"] == 0
    fn()
    fn.cache_info.clear()
    assert fn.cache_info()["total"] == 0


def test_invalidate_and_cache_clear_from_sync_context() -> None:
    c = Cache()
    c.setup("mem://")
    calls = 0

    @c(ttl="1h")
    def fn(x: int) -> int:
        nonlocal calls
        calls += 1
        return x

    fn(1)
    fn(1)
    assert calls == 1
    fn.invalidate(1)  # sync context → runs via bridge, returns None-ish
    fn(1)
    assert calls == 2
    fn.cache_clear()
    fn(1)
    assert calls == 3


async def test_invalidate_and_cache_clear_from_async_context() -> None:
    c = Cache()
    c.setup("mem://")
    calls = 0

    @c(ttl="1h")
    async def fn(x: int) -> int:
        nonlocal calls
        calls += 1
        return x

    await fn(1)
    await fn.invalidate(1)  # inside a loop → awaitable
    await fn(1)
    assert calls == 2
    await fn.cache_clear()
    await fn(1)
    assert calls == 3
    await c.close()


def test_cache_clear_on_partial_returns_zero() -> None:
    c = Cache()
    c.setup("mem://")

    def base(x: int) -> int:
        return x

    wrapped = c(ttl="1h")(functools.partial(base, 1))  # partial has no __qualname__
    assert wrapped() == 1
    assert wrapped.cache_clear() == 0  # no owner pattern derivable


def test_cache_decorates_function_directly_without_parens() -> None:
    c = Cache()
    c.setup("mem://")
    calls = 0

    def fn(x: int) -> int:
        nonlocal calls
        calls += 1
        return x

    wrapped = c(fn)  # no parentheses — direct pass
    assert wrapped(5) == 5
    assert wrapped(5) == 5
    assert calls == 1


async def test_strategy_decorator_applied_directly() -> None:
    c = Cache()
    c.setup("mem://")
    calls = 0

    def fn() -> int:
        nonlocal calls
        calls += 1
        return 1

    wrapped = c.early(fn, ttl="1h", early_ttl="30m")  # strategy applied without parens
    assert wrapped() == 1
    assert wrapped() == 1
    assert calls == 1


async def test_stats_aggregates_all_decorated_functions() -> None:
    c = Cache()
    c.setup("mem://")

    @c(ttl="1h")
    async def a() -> int:
        return 1

    @c(ttl="1h")
    def b() -> int:
        return 2

    await a()
    await a()
    b()
    stats = c.stats()
    assert stats["functions"] == 2
    assert stats["hits"] == 1 and stats["misses"] == 2
    assert stats["hit_rate"] == pytest.approx(1 / 3)


async def test_get_match_skips_undecodable_entries() -> None:
    c = Cache()
    c.setup("mem://", prefix="p")
    await c.set("good", "value", ttl="1h")
    await c.engine.backend.set("p:bad", b"\x10\x00corrupt")
    seen = {k: v async for k, v in c.get_match("*")}
    assert seen == {"good": "value"}


async def test_agen_wrapper_yields_single_non_list_value() -> None:
    c = Cache()
    c.setup("mem://")

    @c.iterator(ttl="1h")
    async def agen(n: int):
        yield n

    # A plain (non-list) cached value under the function's key is yielded once.
    from cachery.core.keys import KeyBuilder

    key = KeyBuilder(agen.__wrapped__, None).build((7,), {})
    await c.set(key, 42, ttl="1h")
    got = [x async for x in agen(7)]
    assert got == [42]
    await c.close()


async def test_multi_cached_sync_and_async() -> None:
    c = Cache()
    c.setup("mem://")
    calls: list[list[str]] = []

    @c.multi_cached(ttl="1h", keys_from_attr="keys")
    async def fetch(keys: list[str]) -> dict[str, int]:
        calls.append(list(keys))
        return {k: len(k) for k in keys}

    assert await fetch(keys=["a", "b"]) == {"a": 1, "b": 1}
    assert await fetch(keys=["a", "c"]) == {"a": 1, "c": 1}
    assert calls == [["a", "b"], ["c"]]  # 'a' served from cache

    @c.multi_cached(ttl="1h", keys_from_attr="keys", key_builder=lambda k: f"kb:{k}")
    def fetch_sync(keys: list[str]) -> dict[str, int]:
        calls.append(list(keys))
        return {k: len(k) for k in keys}

    assert fetch_sync(keys=["x"]) == {"x": 1}
    assert fetch_sync(keys=["x"]) == {"x": 1}
    assert calls[-1] == ["x"]
    await c.close()


# -- keys: format specs, conversions, failures ---------------------------------------


def test_key_spec_hash_xxhash_and_unknown_algo() -> None:
    c = Cache()
    c.setup("mem://")

    @c(ttl="1h", key="h:{secret:hash(xxhash)}")
    def fn(secret: str) -> str:
        return secret

    assert fn("abc") == "abc"  # built + ran

    @c(ttl="1h", key="h:{secret:hash(notalgo)}")
    def fn2(secret: str) -> str:
        return secret

    with pytest.raises(CacheryError, match="Unknown hash algorithm"):
        fn2("abc")


def test_key_spec_lower_upper() -> None:
    c = Cache()
    c.setup("mem://")

    @c(ttl="1h", key="k:{x:upper}:{y:lower}")
    def fn(x: str, y: str) -> int:
        return 1

    assert fn("ab", "CD") == 1


def test_key_conversion_r_s_and_unsupported() -> None:
    c = Cache()
    c.setup("mem://")

    @c(ttl="1h", key="r:{x!r}")
    def fn_r(x: str) -> int:
        return 1

    assert fn_r("a") == 1

    @c(ttl="1h", key="s:{x!s}")
    def fn_s(x: str) -> int:
        return 1

    assert fn_s("a") == 1

    with pytest.raises(InvalidKeyTemplateError, match="conversion"):

        @c(ttl="1h", key="z:{x!z}")
        def fn_z(x: str) -> int: ...


def test_key_unsupported_format_spec_raises_at_call() -> None:
    c = Cache()
    c.setup("mem://")

    @c(ttl="1h", key="k:{x:bogus}")
    def fn(x: str) -> int:
        return 1

    with pytest.raises(CacheryError, match="Unsupported key format spec"):
        fn("a")


def test_key_xxhash_without_extra_raises_helpfully(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "xxhash", None)
    kb = KeyBuilder(lambda x: x, "{x:hash(xxhash)}")
    with pytest.raises(CacheryError, match=r"cachery\[speed\]"):
        kb.build(("a",), {})


def test_key_long_repr_is_hashed_and_addresses_scrubbed() -> None:
    class Weird:
        def __repr__(self) -> str:
            return f"<Weird 0x7f0000000000 {'x' * 100}>"

    kb = KeyBuilder(lambda w: w, "{w}")
    key = kb.build((Weird(),), {})
    assert "0x7f" not in key  # scrubbed
    assert "Weird" not in key  # repr replaced by a digest


def test_key_dotted_dict_missing_list_index_attr_missing() -> None:
    c = Cache()
    c.setup("mem://")

    @c(ttl="1h", key="k:{d.b}")
    def fn_dict(d: dict[str, int]) -> int:
        return 1

    with pytest.raises(CacheryError, match="missing in mapping"):
        fn_dict({"a": 1})

    @c(ttl="1h", key="k:{d.0}")
    def fn_list(d: list[int]) -> int:
        return 1

    assert fn_list([10, 20]) == 1

    @c(ttl="1h", key="k:{d.attr}")
    def fn_obj(d: object) -> int:
        return 1

    with pytest.raises(CacheryError, match="Attribute"):
        fn_obj(object())


def test_key_var_positional_and_var_keyword_roots() -> None:
    c = Cache()
    c.setup("mem://")

    @c(ttl="1h", key="k:{args}")
    def fn_args(*args: int) -> int:
        return sum(args)

    assert fn_args(1, 2) == 3

    @c(ttl="1h", key="k:{kwargs}")
    def fn_kw(**kwargs: int) -> int:
        return sum(kwargs.values())

    assert fn_kw(a=1, b=2) == 3


def test_key_builder_falls_back_for_unintrospectable() -> None:
    kb = KeyBuilder(len, None)  # builtins have no Python signature
    key = kb.build((5,), {})
    assert "len" in key


# -- memory backend: policies, eviction, cull, sync primitives -----------------------


async def test_memory_invalid_policy_raises() -> None:
    with pytest.raises(ValueError, match="policy"):
        MemoryBackend(policy="arc")


async def test_memory_invalid_size_raises() -> None:
    with pytest.raises(ValueError, match="size"):
        MemoryBackend(size=0)


async def test_memory_lfu_eviction_and_touch() -> None:
    b = MemoryBackend(size=2, policy="lfu")
    await b.set("hot", 1, ttl=60)
    await b.set("cold", 2, ttl=60)
    await b.get("hot")  # hot gains frequency
    await b.set("c", 3, ttl=60)  # evicts "cold" (lowest freq)
    assert await b.get("cold") is None
    assert await b.get("hot") == 1
    await b.close()


async def test_memory_mru_and_rr_eviction() -> None:
    b = MemoryBackend(size=2, policy="mru")
    await b.set("a", 1, ttl=60)
    await b.set("b", 2, ttl=60)
    await b.set("c", 3, ttl=60)  # c is the most recently used → evicted
    assert await b.get("c") is None
    assert await b.get("a") == 1
    assert await b.get("b") == 2
    await b.close()

    b2 = MemoryBackend(size=2, policy="rr")
    await b2.set("a", 1, ttl=60)
    await b2.set("b", 2, ttl=60)
    await b2.set("c", 3, ttl=60)  # random victim — data stays consistent
    alive = 0
    for k in ("a", "b", "c"):
        if await b2.get(k) is not None:
            alive += 1
    assert alive == 2
    await b2.close()


async def test_memory_fifo_policy() -> None:
    b = MemoryBackend(size=2, policy="fifo")
    await b.set("a", 1, ttl=60)
    await b.set("b", 2, ttl=60)
    await b.get("a")  # no recency effect for fifo
    await b.set("c", 3, ttl=60)  # evicts a (oldest inserted)
    assert await b.get("a") is None
    assert await b.get("b") == 2
    await b.close()


async def test_memory_incr_non_integer_raises() -> None:
    b = MemoryBackend()
    await b.set("k", "not-int", ttl=60)
    with pytest.raises(ValueError, match="non-integer"):
        await b.incr("k")
    assert await b.incr("fresh") == 1  # absent key starts at 0
    await b.close()


async def test_memory_ttl_of_absent_and_no_expiry() -> None:
    b = MemoryBackend()
    assert await b.ttl_of("absent") is None
    await b.set("k", 1)
    assert await b.ttl_of("k") is None
    await b.close()


async def test_memory_cull_drops_expired() -> None:
    b = MemoryBackend()
    await b.set("gone", 1, ttl=0.05)
    await b.set("kept", 2, ttl=60)
    await b.set("forever", 3)
    await asyncio.sleep(0.1)
    assert await b.cull() == 1
    assert await b.get("gone") is None
    assert await b.get("kept") == 2
    assert await b.get("forever") == 3
    assert b.currsize() == 2
    await b.close()


async def test_memory_live_entry_expiry_bypasses_reap_heap() -> None:
    b = MemoryBackend()
    await b.set("k", 1, ttl=0.05)
    await asyncio.sleep(0.1)
    b._heap.clear()  # simulate reaper budget exhaustion
    assert await b.get("k") is None
    await b.close()


async def test_memory_sync_primitives_contract() -> None:
    b = MemoryBackend()
    assert b._sync_get("missing") is None
    assert b._sync_set("k", b"v", 60.0) is True
    assert b._sync_get("k") == b"v"
    assert b._sync_set("k", b"2", 60.0, exist=True) is True
    assert b._sync_set("absent", b"2", 60.0, exist=True) is False
    assert b._sync_set("k2", b"2", 60.0, exist=False) is True
    assert b._sync_set("k2", b"3", 60.0, exist=False) is False
    assert b._sync_delete("k") is True
    assert b._sync_delete("k") is False


async def test_memory_unbounded_size_skips_eviction() -> None:
    b = MemoryBackend(size=None)
    for i in range(50):
        await b.set(f"k{i}", i, ttl=60)
    assert b.currsize() == 50
    assert await b.get("k0") == 0
    await b.close()


# -- locked strategy: waiter exception propagation + cross-process poll ---------------


async def test_locked_waiters_receive_the_winner_exception() -> None:
    c = Cache()
    c.setup("mem://")
    calls = 0

    @c.locked(ttl="1h")
    async def fn() -> int:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.05)
        raise ValueError("boom")

    results = await asyncio.gather(fn(), fn(), return_exceptions=True)
    assert calls == 1  # single-flight
    assert all(isinstance(r, ValueError) for r in results)
    await c.close()


async def test_locked_cross_process_waiter_polls_value() -> None:
    c = Cache()
    c.setup("mem://")

    @c.locked(ttl="1h")
    async def fn() -> str:
        raise AssertionError("must not recompute")  # pragma: no cover

    from cachery.core.keys import KeyBuilder

    key = KeyBuilder(fn.__wrapped__, None).build((), {})
    # Another process holds the lock and already stored the value:
    await c.engine.set(f"__slock__:{key}", b"1", ttl=60)
    await c.engine.set(key, "from-other-process", ttl=3600)
    assert await fn() == "from-other-process"  # waiter polled and returned it
    await c.close()


# -- early strategy edges -------------------------------------------------------------


async def test_early_serves_plain_value_written_by_something_else() -> None:
    c = Cache()
    c.setup("mem://")
    calls = 0

    @c.early(ttl="1h", early_ttl="30m")
    async def fn() -> str:
        nonlocal calls
        calls += 1
        return "recomputed"  # pragma: no cover

    from cachery.core.keys import KeyBuilder

    key = KeyBuilder(fn.__wrapped__, None).build((), {})
    await c.set(key, "plain-value", ttl="1h")  # not an envelope
    assert await fn() == "plain-value"
    assert calls == 0
    await c.close()


async def test_early_envelope_without_expiry_is_always_fresh() -> None:
    c = Cache()
    c.setup("mem://")
    calls = 0

    @c.early()  # ttl=None → stored envelope ttl=0 → always fresh
    async def fn() -> int:
        nonlocal calls
        calls += 1
        return calls

    assert await fn() == 1
    assert await fn() == 1
    assert calls == 1
    await c.close()


async def test_early_refreshes_once_past_threshold() -> None:
    c = Cache()
    c.setup("mem://")
    calls = 0

    @c.early(ttl="1s", early_ttl="0.2s")
    async def fn() -> int:
        nonlocal calls
        calls += 1
        return calls

    assert await fn() == 1
    assert await fn() == 1  # fresh, no refresh yet
    await asyncio.sleep(0.85)  # inside the early window, before hard expiry
    assert await fn() == 1  # serves fresh + schedules background refresh
    await asyncio.sleep(0.15)  # let the background task run
    assert calls == 2
    await c.close()


async def test_soft_refreshes_in_stale_window() -> None:
    c = Cache()
    c.setup("mem://")
    calls = 0

    @c.soft(ttl="0.25s", soft_ttl="0.6s")
    async def fn() -> int:
        nonlocal calls
        calls += 1
        return calls

    assert await fn() == 1
    await asyncio.sleep(0.4)  # stale window, before hard expiry
    assert await fn() == 1  # stale value served, refresh scheduled
    await asyncio.sleep(0.1)
    assert calls == 2
    await c.close()


# -- failover / hit / tags edges -------------------------------------------------------


async def test_failover_default_stale_ttl_floor() -> None:
    c = Cache()
    c.setup("mem://")
    calls = 0

    @c.failover(ttl="0.1s", exceptions=(RuntimeError,))
    async def fn() -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            return "v1"
        raise RuntimeError("down")

    assert await fn() == "v1"
    await asyncio.sleep(0.2)  # original ttl expired; stale copy survives
    assert await fn() == "v1"  # served from the failover copy
    await c.close()


async def test_hit_without_ttl_never_expires() -> None:
    c = Cache()
    c.setup("mem://")
    calls = 0

    @c.hit()  # ttl=None
    async def fn() -> int:
        nonlocal calls
        calls += 1
        return calls

    assert await fn() == 1
    assert await fn() == 1
    assert await fn() == 1
    assert calls == 1
    await c.close()


async def test_hit_reset_after_cache_hits() -> None:
    c = Cache()
    c.setup("mem://")
    recomputes = 0

    @c.hit(ttl="1h", cache_hits=2, update_after=2)
    async def fn() -> int:
        nonlocal recomputes
        recomputes += 1
        return recomputes

    await fn()  # miss
    for _ in range(4):
        await fn()  # hits
    await asyncio.sleep(0.1)
    assert recomputes <= 2  # counter reset bounds the refresh count
    await c.close()


async def test_invalidate_tags_missing_tag_is_noop() -> None:
    c = Cache()
    c.setup("mem://")

    @c(ttl="1h", tags=["tenant:acme"])
    async def fn(uid: int) -> int:
        return uid

    await fn(1)
    assert await c.invalidate("tenant:missing") == 0
    assert await c.invalidate("tenant:acme") >= 1
    await c.close()


# -- tiered backend: capability fallbacks and batch ops ---------------------------------


class NoTtlOfBackend(BaseBackend):
    """L2 without ``ttl_of`` — tiered must use conservative promotion TTLs."""

    def __init__(self) -> None:
        self.data: dict[str, Any] = {}

    async def get(self, key: str) -> Any | None:
        return self.data.get(key)

    async def set(self, key: str, value: Any, ttl: float | None = None, exist: bool | None = None) -> bool:
        self.data[key] = value
        return True

    async def delete(self, key: str) -> bool:
        return self.data.pop(key, None) is not None

    async def exists(self, key: str) -> bool:
        return key in self.data

    async def expire(self, key: str, ttl: float) -> bool:
        return key in self.data

    async def incr(self, key: str, delta: int = 1) -> int:
        self.data[key] = self.data.get(key, 0) + delta
        return self.data[key]

    async def scan(self, pattern: str, batch_size: int = 100) -> Any:
        import fnmatch

        for key in list(self.data):
            if fnmatch.fnmatchcase(key, pattern):
                yield key

    async def clear(self) -> int:
        n = len(self.data)
        self.data.clear()
        return n

    async def ping(self) -> bytes:
        return b"PONG"

    async def close(self) -> None:
        return None


async def test_tiered_promotion_without_ttl_of_uses_default_cap() -> None:
    l1 = MemoryBackend(size=10)
    t = TieredBackend(l1=l1, l2=NoTtlOfBackend())
    await t.l2.set("k", b"v", ttl=3600)
    assert await t.get("k") == b"v"  # promoted with the default promotion TTL
    assert await l1.get("k") == b"v"
    ttl = await t.ttl_of("k")
    assert ttl is not None and 0 < ttl <= 61  # finite, not forever
    await t.close()


async def test_tiered_l1_ttl_cap_bounds_promotion() -> None:
    l1 = MemoryBackend(size=10)
    t = TieredBackend(l1=l1, l2=MemoryBackend(size=10), l1_ttl_cap=5.0)
    await t.l2.set("k", b"v", ttl=3600)
    await t.get("k")  # promote
    cap_ttl = await t.ttl_of("k")
    assert cap_ttl is not None and cap_ttl <= 5.0
    await t.close()


class WeirdTtlOfBackend(NoTtlOfBackend):
    async def ttl_of(self, key: str) -> Any:
        return "not-a-number"


async def test_tiered_survives_non_numeric_ttl_of() -> None:
    t = TieredBackend(l1=MemoryBackend(size=10), l2=WeirdTtlOfBackend())
    await t.l2.set("k", b"v", ttl=3600)
    assert await t.get("k") == b"v"  # non-numeric ttl_of → default promotion
    await t.close()


async def test_tiered_batch_ops_and_scan_dedup() -> None:
    t = TieredBackend(l1=MemoryBackend(size=10), l2=MemoryBackend(size=10))
    await t.set_many({"a": b"1", "b": b"2"}, ttl=60)
    assert await t.get_many("a", "b", "zz") == (b"1", b"2", None)
    assert await t.delete_many("a", "zz") == 1
    assert await t.delete_match("b*") == 1
    await t.set("keep", b"k", ttl=60)
    keys = sorted([k async for k in t.scan("*")])
    assert keys == ["keep"]
    assert await t.ping() == b"PONG"
    assert await t.clear() == 1
    assert t.now() > 0
    await t.close()


# -- codec wire-format error paths ------------------------------------------------------


def _typed_payload(path: str, payload: bytes) -> bytes:
    return bytes([0x12]) + len(path).to_bytes(2, "big") + path.encode() + payload


def test_codec_typed_msgspec_struct_roundtrip() -> None:
    raw = codec.dumps(_StructPoint(x=1, y=2))
    back = codec.loads(raw)
    assert isinstance(back, _StructPoint) and back.x == 1 and back.y == 2


def test_codec_unresolvable_type_raises_helpfully() -> None:
    payload = _typed_payload("no.such.module:Gone", b"\x00")
    with pytest.raises(SerializationError, match="moved or renamed"):
        codec.loads(payload)


def test_codec_typed_decode_falls_back_to_untyped() -> None:
    import msgspec

    payload = msgspec.msgpack.encode([1, 2, 3])
    raw = _typed_payload("cachery.core.engine:Engine", payload)  # wrong type → fallback
    assert codec.loads(raw) == [1, 2, 3]


def test_codec_pickle_failure_raises() -> None:
    with pytest.raises(SerializationError, match="Cannot serialize"):
        codec.dumps(threading.Lock())


def test_codec_deeply_nested_falls_back_to_pickle() -> None:
    deep: Any = 1
    for _ in range(40):
        deep = [deep]
    assert codec.loads(codec.dumps(deep)) == deep


def test_codec_unknown_compression_method_raises() -> None:
    engine = Engine(MemoryBackend(), compress="bogus")
    assert engine.suppress is True
    result = asyncio.run(engine.set("k", 1, ttl=60))
    assert result is False  # wrapped + suppressed


def test_codec_corrupt_pickle_and_unknown_tag() -> None:
    with pytest.raises(SerializationError, match="Deserialization failed"):
        codec.loads(b"\x10\x00\x01garbage")
    with pytest.raises(SerializationError, match="Unknown serialization tag"):
        codec.loads(b"\x99\xff")


def test_codec_custom_encoder_failure_is_wrapped() -> None:
    class Boom:
        pass

    def bad(_v: Any) -> Any:
        raise RuntimeError("encoder bug")

    register_type(Boom, bad, bad)
    try:
        with pytest.raises(SerializationError, match="Serialization failed"):
            codec.dumps(Boom())
    finally:
        clear_custom_types()


def test_codec_compression_roundtrips() -> None:
    blob = b"x" * 1000
    for method in ("zlib", "zstd"):
        raw = codec.dumps(blob, compress=method)
        assert codec.loads(raw) == blob


# -- hashing / compress optional-extra errors -------------------------------------------


def test_digest_xxhash_and_algorithms() -> None:
    assert digest(b"data", "blake2b") == hmac_digest(b"", b"data", "blake2b")
    assert len(digest(b"data", "xxhash")) > 0
    assert len(hmac_digest(b"k", b"d", "sha256")) == 64


def test_hashing_and_compress_without_extras_raise_helpfully(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import importlib

    from cachery.serialize import hashing as hashing_mod

    monkeypatch.setitem(sys.modules, "xxhash", None)
    importlib.reload(hashing_mod)
    try:
        with pytest.raises(ImportError, match=r"cachery\[speed\]"):
            digest(b"data", "xxhash")
    finally:
        importlib.reload(hashing_mod)

    monkeypatch.setitem(sys.modules, "zstandard", None)
    importlib.reload(compress)
    try:
        with pytest.raises(ImportError, match=r"cachery\[compress\]"):
            compress.compress(b"x", "zstd")
        with pytest.raises(ImportError, match=r"cachery\[compress\]"):
            compress.decompress(b"x", "zstd")
    finally:
        importlib.reload(compress)


def test_compress_unknown_method_raises() -> None:
    with pytest.raises(ValueError, match="Unknown compression"):
        compress.compress(b"x", "brotli")
    with pytest.raises(ValueError, match="Unknown compression"):
        compress.decompress(b"x", "brotli")


def test_compress_zlib_roundtrip() -> None:
    blob = b"x" * 500
    assert compress.decompress(compress.compress(blob), "zlib") == blob


# -- BaseBackend derived batch defaults --------------------------------------------------


async def test_base_backend_derived_batch_ops() -> None:
    b = NoTtlOfBackend()  # implements only single-key primitives
    await b.set("a", b"1")
    await b.set("b", b"2")
    assert await b.get_many("a", "b") == (b"1", b"2")
    await b.set_many({"c": b"3"})
    assert await b.delete_many("a", "c") == 2
    await b.set("m:1", b"x")
    assert await b.delete_match("m:*") == 1
    assert await b.exists("b") is True
    assert await b.close() is None


# -- fakes + registry edges ---------------------------------------------------------------


async def test_fake_backend_expiry_and_ops() -> None:
    fake = FakeBackend()
    await fake.set("k", "v", ttl=5.0)
    assert fake.ops[0] == ("set", "k")
    assert await fake.get("k") == "v"
    fake.now += 10  # pretend time passed
    assert await fake.get("k") is None
    assert await fake.exists("k") is False
    assert await fake.expire("k", 5.0) is False  # already expired
    await fake.set("n", 1)
    assert await fake.incr("n") == 2
    assert [k async for k in fake.scan("*")] == ["n"]
    assert await fake.clear() == 2  # expired entry still occupies a slot until cleared
    assert await fake.ping() == b"PONG"
    assert await fake.close() is None


async def test_fake_backend_fail_on_injection() -> None:
    fake = FakeBackend()
    fake.fail_on.add("k")
    with pytest.raises(ConnectionError, match="injected"):
        await fake.get("k")


def test_registry_disk_netloc_normalisation_and_bool_params(tmp_path: Any) -> None:
    from cachery.backends.disk import DiskBackend

    db = tmp_path / "x.sqlite3"
    url = f"disk://{db}".replace(chr(92), "/")  # disk://C:/... → netloc "C:" normalised
    backend = create_backend(f"{url}?purge_on_start=false")
    assert isinstance(backend, DiskBackend)
    assert backend.path.endswith("x.sqlite3")
    assert backend.size_limit is None
    import asyncio

    asyncio.run(backend.close())


def test_jittered_spread_and_zero() -> None:
    assert jittered(10.0, 0.0) == 10.0
    for _ in range(20):
        v = jittered(10.0, 2.0)
        assert 8.0 <= v <= 10.0


async def test_schedule_background_dedupes_in_flight() -> None:
    started = asyncio.Event()

    async def work() -> None:
        started.set()
        await asyncio.sleep(0.05)

    schedule_background("k", work)
    schedule_background("k", work)  # same key, in flight → ignored
    await asyncio.wait_for(started.wait(), timeout=2)
    from cachery.strategies import _base

    assert len(_base._inflight) == 1
    await asyncio.sleep(0.1)
    assert "k" not in _base._inflight  # cleaned up on completion


# -- CLI ---------------------------------------------------------------------------------


def test_cli_ping_ok_and_fail(capsys: pytest.CaptureFixture[str]) -> None:
    from cachery._cli import __main__ as cli

    assert asyncio.run(cli._ping("mem://")) == 0
    out = capsys.readouterr().out
    assert "OK mem://" in out

    assert asyncio.run(cli._ping("ftp://nope")) == 1
    assert "FAIL" in capsys.readouterr().out


def test_cli_keys_and_stats(capsys: pytest.CaptureFixture[str], tmp_path: Any) -> None:
    from cachery._cli import __main__ as cli

    db = tmp_path / "cli.sqlite3"
    url = f"disk://{db}".replace(chr(92), "/")  # shared store the CLI can re-open

    async def seed() -> None:
        b = create_backend(url)
        await b.set("user:1", "alice")
        await b.set("user:2", "bob")
        await b.close()

    asyncio.run(seed())

    assert asyncio.run(cli._keys(url, "user:*", limit=10)) == 0
    out = capsys.readouterr().out
    assert "user:1" in out and "user:2" in out

    assert asyncio.run(cli._stats(url, "user:*")) == 0
    out = capsys.readouterr().out
    assert "keys:" in out and "2" in out

    assert asyncio.run(cli._keys("ftp://nope", "*", limit=5)) == 1
    assert "FAIL" in capsys.readouterr().err

    assert asyncio.run(cli._stats("ftp://nope", "*")) == 1
    assert "FAIL" in capsys.readouterr().err


def test_cli_main_argparse() -> None:
    from cachery._cli import __main__ as cli

    assert cli.main(["--url", "mem://", "ping"]) == 0
    assert cli.main(["--url", "mem://", "keys", "zz*", "--limit", "1"]) == 0
    assert cli.main(["--url", "mem://", "stats"]) == 0
    with pytest.raises(SystemExit):
        cli.main(["--url", "mem://", "bogus-command"])


# -- misc: envelope + detect --------------------------------------------------------------


def test_envelope_is_not_mistaken_for_plain_lists() -> None:
    from cachery.core.envelope import Envelope
    from cachery.strategies._base import unpack_envelope

    assert unpack_envelope([1.0, 2.0, 3.0]) is None  # plain list
    env = Envelope(value=1, created_at=0.0, ttl=0.0)
    assert unpack_envelope(env) is env


async def test_detect_records_miss_and_hit_with_meta() -> None:
    c = Cache()
    c.setup("mem://")

    @c(ttl="1h")
    async def fn(x: int) -> int:
        return x

    await fn(1)
    with c.detect as d:
        await fn(1)  # hit
        await fn(2)  # miss
    assert d.calls_total == 2
    assert d.hits == 1
    assert d.calls[0]["hit"] is True and d.calls[0]["name"] == "simple"
    assert d.calls[1]["hit"] is False and "name" not in d.calls[1]
    assert all("duration_ms" in rec for rec in d.calls)
    await c.close()


@dataclasses.dataclass
class _Point:
    x: int
    y: int


try:
    import msgspec as _msgspec

    class _StructPoint(_msgspec.Struct):
        x: int
        y: int

except ImportError:  # pragma: no cover - speed extra optional
    _StructPoint = None  # type: ignore[assignment,misc]


def test_dataclass_repr_used_in_auto_keys() -> None:
    c = Cache()
    c.setup("mem://")
    calls = 0

    @c(ttl="1h")
    def fn(p: _Point) -> int:
        nonlocal calls
        calls += 1
        return p.x

    fn(_Point(1, 2))
    fn(_Point(1, 2))  # same deterministic repr → cached
    fn(_Point(3, 4))  # different → recompute
    assert calls == 2
