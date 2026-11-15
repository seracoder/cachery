"""Decorator end-to-end tests on mem:// (sync + async)."""

from __future__ import annotations

import asyncio

import pytest

from cachery.core.decorator import Cache


async def test_async_cache_hit_and_miss() -> None:
    c = Cache()
    c.setup("mem://")
    calls = 0

    @c(ttl="1h")
    async def fetch(user_id: int) -> dict:
        nonlocal calls
        calls += 1
        return {"id": user_id}

    assert await fetch(1) == {"id": 1}
    assert await fetch(1) == {"id": 1}
    assert calls == 1
    assert await fetch(2) == {"id": 2}  # different key
    assert calls == 2
    await c.close()


async def test_sync_cache_hit_and_miss() -> None:
    c = Cache()
    c.setup("mem://")
    calls = 0

    @c(ttl="1h")
    def compute(customer_id: int, year: int) -> str:
        nonlocal calls
        calls += 1
        return f"report-{customer_id}-{year}"

    assert compute(7, 2026) == "report-7-2026"
    assert compute(7, 2026) == "report-7-2026"
    assert calls == 1
    await c.close()


async def test_expiry_recomputes() -> None:
    c = Cache()
    c.setup("mem://")
    calls = 0

    @c(ttl="1s")
    async def now() -> int:
        nonlocal calls
        calls += 1
        return calls

    assert await now() == 1
    await asyncio.sleep(1.2)
    assert await now() == 2
    await c.close()


async def test_condition_skips_cache_write() -> None:
    from cachery import NOT_NONE

    c = Cache()
    c.setup("mem://")
    calls = 0

    @c(ttl="1h", condition=NOT_NONE)
    async def maybe(flag: bool) -> int | None:
        nonlocal calls
        calls += 1
        return 42 if flag else None

    assert await maybe(False) is None  # not cached
    assert await maybe(False) is None
    assert calls == 2
    assert await maybe(True) == 42
    assert await maybe(True) == 42  # cached
    assert calls == 3
    await c.close()


async def test_disable_context_bypasses_cache() -> None:
    c = Cache()
    c.setup("mem://")
    calls = 0

    @c(ttl="1h")
    async def fn() -> int:
        nonlocal calls
        calls += 1
        return calls

    await fn()  # calls=1, cached
    with c.disable():
        assert await fn() == 2
        assert await fn() == 3
    assert await fn() == 1  # cache hit: value stored before the disable block
    assert calls == 3
    await c.close()


async def test_detect_records_hits_and_misses() -> None:
    c = Cache()
    c.setup("mem://")

    @c(ttl="1h")
    async def fn(x: int) -> int:
        return x

    with c.detect as detector:
        await fn(1)
        await fn(1)
    assert detector.calls_total == 2
    assert detector.hits == 1
    assert detector.calls[0]["hit"] is False
    assert detector.calls[1]["hit"] is True
    await c.close()


async def test_middleware_sees_events() -> None:
    c = Cache()
    c.setup("mem://")
    events = []

    async def mw(event) -> None:
        events.append(event.name)

    c.add_middleware(mw)

    @c(ttl="1h")
    async def fn(x: int) -> int:
        return x

    await fn(1)  # miss + set
    await fn(1)  # hit
    assert "miss" in events
    assert "set" in events
    assert "hit" in events
    await c.close()


async def test_bad_ttl_raises_at_decoration() -> None:
    c = Cache()
    c.setup("mem://")
    from cachery.core.exceptions import InvalidTTLError

    with pytest.raises(InvalidTTLError):

        @c(ttl="1h30x")
        async def fn() -> int:
            return 1

    await c.close()


async def test_prefix_namespaces_keys() -> None:
    c1 = Cache()
    c1.setup("mem://", prefix="a")
    c2 = Cache()
    c2.setup("mem://", prefix="b")

    await c1.set("k", "from-a")
    await c2.set("k", "from-b")
    assert await c1.get("k") == "from-a"
    assert await c2.get("k") == "from-b"
    await c1.close()
    await c2.close()


async def test_explicit_key_template_with_args() -> None:
    c = Cache()
    c.setup("mem://")
    calls = 0

    @c(ttl="1h", key="user:{user_id}")
    async def get_user(user_id: int) -> str:
        nonlocal calls
        calls += 1
        return f"u{user_id}"

    assert await get_user(5) == "u5"
    assert await get_user(5) == "u5"
    assert calls == 1
    await c.close()


async def test_callable_ttl_receives_arguments() -> None:
    c = Cache()
    c.setup("mem://")

    def ttl_for(user_id: int) -> str:
        return "1h" if user_id == 1 else "50ms"

    @c(ttl=ttl_for)
    async def fn(user_id: int) -> int:
        return user_id

    assert await fn(1) == 1
    assert await fn(2) == 2
    await asyncio.sleep(0.1)
    assert await fn(1) == 1  # still cached
    await c.close()
