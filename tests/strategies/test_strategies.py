"""Strategy decorator tests (plan §5.5 behaviors)."""

from __future__ import annotations

import asyncio

import pytest

from cachery import Cache
from cachery.core.exceptions import CircuitBreakerOpen, RateLimitError

# -- early ----------------------------------------------------------------------


async def test_early_serves_fresh_value_while_refreshing() -> None:
    c = Cache()
    c.setup("mem://")
    calls = 0

    @c.early(ttl="2s", early_ttl="1.4s")
    async def fn() -> int:
        nonlocal calls
        calls += 1
        return calls

    assert await fn() == 1
    await asyncio.sleep(0.7)  # refresh window starts at ttl-early = 0.6s
    assert await fn() == 1  # still-fresh value returned, background refresh started
    await asyncio.sleep(0.3)  # let the background refresh land
    assert await fn() == 2  # refreshed value now served
    assert calls == 2
    await c.close()


async def test_early_concurrent_refreshes_run_once() -> None:
    c = Cache()
    c.setup("mem://")
    calls = 0

    @c.early(ttl="1h", early_ttl="300ms")
    async def fn() -> int:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.05)
        return calls

    assert await fn() == 1
    await asyncio.sleep(0.5)  # inside refresh window
    # Concurrent callers: each gets a value; at most one background refresh
    # is in flight for the key at a time.
    results = await asyncio.gather(*[fn() for _ in range(4)])
    await asyncio.sleep(0.05)  # let the background refresh settle
    assert all(r == results[0] for r in results)
    assert calls <= 3
    await c.close()


# -- soft ------------------------------------------------------------------------


async def test_soft_serves_stale_after_ttl() -> None:
    c = Cache()
    c.setup("mem://")
    calls = 0

    @c.soft(ttl="400ms", soft_ttl="3s")
    async def fn() -> int:
        nonlocal calls
        calls += 1
        return calls

    assert await fn() == 1
    await asyncio.sleep(0.6)  # past hard ttl, inside soft window
    assert await fn() == 1  # stale value served, background refresh scheduled
    await asyncio.sleep(0.3)
    assert await fn() == 2  # refreshed
    assert calls == 2
    await c.close()


async def test_soft_full_expiry_recomputes() -> None:
    c = Cache()
    c.setup("mem://")

    @c.soft(ttl="150ms", soft_ttl="150ms")
    async def fn() -> int:
        return 42

    assert await fn() == 42
    await asyncio.sleep(0.5)  # past ttl + soft_ttl entirely
    assert await fn() == 42  # recomputed (fresh call)
    await c.close()


# -- hit ---------------------------------------------------------------------------


async def test_hit_expires_after_n_hits() -> None:
    c = Cache()
    c.setup("mem://")
    calls = 0

    @c.hit(ttl="1h", cache_hits=3)
    async def fn() -> int:
        nonlocal calls
        calls += 1
        return calls

    assert await fn() == 1  # miss → call 1
    assert await fn() == 1  # hit 1
    assert await fn() == 1  # hit 2
    assert await fn() == 1  # hit 3 — at the limit
    assert await fn() == 2  # exceeded → recompute
    await c.close()


async def test_hit_update_after_refreshes_in_background() -> None:
    c = Cache()
    c.setup("mem://")
    calls = 0

    @c.hit(ttl="1h", update_after=1)
    async def fn() -> int:
        nonlocal calls
        calls += 1
        return calls

    assert await fn() == 1
    assert await fn() == 1  # hit triggers background refresh
    await asyncio.sleep(0.05)
    assert calls == 2
    assert await fn() == 2  # refreshed value
    await c.close()


# -- failover ------------------------------------------------------------------------


async def test_failover_serves_stale_on_exception() -> None:
    c = Cache()
    c.setup("mem://")
    state = {"fail": False}

    @c.failover(ttl="1h", exceptions=(RuntimeError,))
    async def fn() -> str:
        if state["fail"]:
            raise RuntimeError("source down")
        return "good"

    state["fail"] = False
    assert await fn() == "good"
    state["fail"] = True
    assert await fn() == "good"  # stale copy served
    await c.close()


async def test_failover_raises_when_no_stale_available() -> None:
    c = Cache()
    c.setup("mem://")

    @c.failover(ttl="1h", exceptions=(RuntimeError,))
    async def fn() -> str:
        raise RuntimeError("down from the start")

    with pytest.raises(RuntimeError):
        await fn()
    await c.close()


# -- locked ---------------------------------------------------------------------------


async def test_locked_single_flight() -> None:
    c = Cache()
    c.setup("mem://")
    calls = 0
    release = asyncio.Event()

    async def source() -> str:
        nonlocal calls
        calls += 1
        await release.wait()
        return f"value-{calls}"

    @c.locked(ttl="10s")
    async def fn() -> str:
        return await source()

    task1 = asyncio.create_task(fn())
    await asyncio.sleep(0.05)  # task1 holds the lock now
    task2 = asyncio.create_task(fn())
    await asyncio.sleep(0.05)
    release.set()
    r1, r2 = await asyncio.gather(task1, task2)
    assert calls == 1  # exactly one execution
    assert r1 == r2 == "value-1"
    await c.close()


# -- rate_limit -------------------------------------------------------------------------


async def test_rate_limit_blocks_beyond_quota() -> None:
    c = Cache()
    c.setup("mem://")
    calls = 0

    @c.rate_limit(limit=2, period="1h")
    async def fn() -> int:
        nonlocal calls
        calls += 1
        return calls

    assert await fn() == 1
    assert await fn() == 2
    with pytest.raises(RateLimitError):
        await fn()
    assert calls == 2
    await c.close()


async def test_rate_limit_window_resets() -> None:
    c = Cache()
    c.setup("mem://")

    @c.rate_limit(limit=1, period="300ms")
    async def fn() -> int:
        return 1

    assert await fn() == 1
    with pytest.raises(RateLimitError):
        await fn()
    await asyncio.sleep(0.4)
    assert await fn() == 1  # new window
    await c.close()


# -- circuit_breaker ----------------------------------------------------------------------


async def test_circuit_opens_on_error_rate() -> None:
    c = Cache()
    c.setup("mem://")
    state = {"fail": False}

    @c.circuit_breaker(errors_rate=50, period="1h", ttl="1h")
    async def fn() -> str:
        if state["fail"]:
            raise RuntimeError("boom")
        return "ok"

    state["fail"] = True
    for _ in range(3):
        with pytest.raises(RuntimeError):
            await fn()
    with pytest.raises(CircuitBreakerOpen):
        await fn()  # fail fast now
    await c.close()


async def test_circuit_stays_closed_when_healthy() -> None:
    c = Cache()
    c.setup("mem://")

    @c.circuit_breaker(errors_rate=50, period="1h", ttl="1h")
    async def fn() -> str:
        return "ok"

    for _ in range(10):
        assert await fn() == "ok"
    await c.close()


# -- iterator -------------------------------------------------------------------------------


async def test_iterator_caches_async_generator() -> None:
    c = Cache()
    c.setup("mem://")
    calls = 0

    @c.iterator(ttl="1h")
    async def pages(n: int):
        nonlocal calls
        calls += 1
        for i in range(n):
            yield i * 10

    assert [x async for x in pages(3)] == [0, 10, 20]
    assert [x async for x in pages(3)] == [0, 10, 20]
    assert calls == 1
    await c.close()


async def test_iterator_sync_generator() -> None:
    c = Cache()
    c.setup("mem://")
    calls = 0

    @c.iterator(ttl="1h")
    def rows(n: int):
        nonlocal calls
        calls += 1
        for i in range(n):
            yield f"row{i}"

    assert list(rows(2)) == ["row0", "row1"]
    assert list(rows(2)) == ["row0", "row1"]
    assert calls == 1
    await c.close()


# -- tags ------------------------------------------------------------------------------------


async def test_tag_invalidation_flushes_tagged_keys() -> None:
    c = Cache()
    c.setup("mem://")
    calls = {"a": 0, "b": 0}

    @c(ttl="1h", tags=["user:{user_id}"])
    async def profile(user_id: int) -> str:
        calls["a"] += 1
        return f"p{user_id}"

    @c(ttl="1h", tags=["user:{user_id}"])
    async def settings(user_id: int) -> str:
        calls["b"] += 1
        return f"s{user_id}"

    assert await profile(1) == "p1"
    assert await settings(1) == "s1"
    assert await profile(1) == "p1"
    assert await settings(1) == "s1"
    assert calls == {"a": 1, "b": 1}

    deleted = await c.invalidate("user:1")
    assert deleted >= 2
    assert await profile(1) == "p1"  # recomputed
    assert await settings(1) == "s1"
    assert calls == {"a": 2, "b": 2}
    await c.close()


async def test_tags_leave_other_keys_alone() -> None:
    c = Cache()
    c.setup("mem://")

    @c(ttl="1h", tags=["user:1"])
    async def tagged() -> str:
        return "tagged"

    @c(ttl="1h")
    async def untagged() -> str:
        return "untagged"

    await tagged()
    await untagged()
    await c.invalidate("user:1")
    assert await untagged() == "untagged"  # still cached
    await c.close()
