"""Dogfooding pass: exercise every public path and verify suspected bugs.

Run: uv run --no-sync python scripts/dogfood.py
Each check prints PASS (behaviour correct) or FAIL (bug confirmed).
"""

from __future__ import annotations

import asyncio
import dataclasses
import subprocess
import sys
import tempfile
import time
from typing import Any

from cachery import Cache, CacheryError
from cachery.backends.disk import DiskBackend
from cachery.backends.memory import MemoryBackend
from cachery.backends.tiered import TieredBackend

RESULTS: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    RESULTS.append((name, ok, detail))
    print(f"{'PASS' if ok else 'FAIL'}: {name}" + (f" — {detail}" if detail else ""))


# ---------------------------------------------------------------- smoke usage


async def smoke_everything() -> None:
    c = Cache()
    c.setup("mem://", prefix="app")

    @c(ttl="1h", key="user:{uid}")
    async def user(uid: int) -> dict:
        return {"id": uid}

    assert await user(1) == {"id": 1}
    assert await user(1) == {"id": 1}

    @c(ttl="1h", condition=lambda r: r is not None)
    async def maybe(x: int) -> int | None:
        return None if x < 0 else x

    await maybe(-1)
    await maybe(5)

    @c.early(ttl="500ms", early_ttl="400ms")
    async def early_fn() -> int:
        return 1

    await early_fn()

    @c.soft(ttl="200ms", soft_ttl="1s")
    async def soft_fn() -> int:
        return 1

    await soft_fn()

    @c.hit(ttl="1h", cache_hits=3, update_after=2)
    async def hit_fn() -> int:
        return 1

    for _ in range(4):
        await hit_fn()

    @c.locked(ttl="1h")
    async def locked_fn() -> int:
        return 1

    await locked_fn()

    @c.failover(ttl="100ms", exceptions=(RuntimeError,))
    async def failover_fn() -> int:
        return 7

    await failover_fn()

    @c.iterator(ttl="1h")
    async def iter_fn():
        yield 1
        yield 2

    assert [x async for x in iter_fn()] == [1, 2]

    @c.rate_limit(limit=100, period="1m")
    async def rl_fn() -> int:
        return 1

    await rl_fn()

    @c.circuit_breaker(ttl="1m", errors_rate=50)
    async def cb_fn() -> int:
        return 1

    await cb_fn()

    # direct API
    await c.set("k", {"a": 1}, ttl="1m")
    assert await c.get("k") == {"a": 1}
    assert await c.get_or_set("k2", lambda: 42, ttl="1m") == 42
    assert await c.get_many("k", "k2") == ({"a": 1}, 42)
    await c.set_many({"m1": 1, "m2": 2}, ttl="30s")
    assert await c.delete("k") is True
    assert await c.delete_many("m1", "m2") == 2
    await c.set("p1", 1, ttl="1h")
    await c.set("p2", 2, ttl="1h")
    assert await c.delete_match("p*") == 2
    assert await c.exists("nope") is False
    await c.set("e", 1, ttl="1h")
    assert await c.expire("e", "1h") is True
    assert await c.incr("counter") == 1
    async for _k, _v in c.get_match("*"):
        pass
    assert await c.ping() == b"PONG"

    async with c.lock("lk", ttl=1.0):
        pass

    with c.detect as d:
        await user(9)
    assert d.calls_total >= 1

    async with c.disable():
        pass

    @c(ttl="1h", tags=["tenant:acme"])
    async def tagged(uid: int) -> int:
        return uid

    await tagged(3)
    assert await c.invalidate("tenant:acme") >= 1
    await c.close()
    check("smoke: all decorators + direct API run", True)


# ---------------------------------------------------------------- bug hunts


async def bug_tiered_l1_never_expires() -> None:
    """B1: promotion into L1 with ttl=None when ttl_of is unavailable → L1
    serves the value forever, even after L2 expired."""
    l1 = MemoryBackend()
    l2 = MemoryBackend()
    t = TieredBackend(l1=l1, l2=l2)
    await t.set("quote:AAPL", b"100.0", ttl=0.2)
    await asyncio.sleep(0.35)  # L2 entry now expired
    value = await t.get("quote:AAPL")
    check("B1 tiered: L1 promotion honours L2 expiry", value is None, f"got {value!r} after expiry")


async def bug_tiered_stale_after_l2_delete() -> None:
    """B1b: promotion must be TTL-bounded so an out-of-band L2 delete is
    picked up within a bounded window (documented tiered-cache trade-off)."""
    l1 = MemoryBackend()
    l2 = MemoryBackend()
    t = TieredBackend(l1=l1, l2=l2)
    await t.set("k", b"v1", ttl=0.3)
    await t.get("k")  # promoted into L1
    with l1._lock:  # promotion carries a finite expiry
        entry = l1._data.get("k")
    bounded = entry is not None and entry[1] is not None
    await l2.delete("k")  # invalidated directly in L2 (another process)
    await asyncio.sleep(0.4)  # beyond the promotion bound
    value = await t.get("k")
    check(
        "B1b tiered: L1 staleness bounded after L2 delete",
        bounded and value is None,
        f"bounded={bounded}, got {value!r}",
    )


async def bug_early_no_ttl_never_hits() -> None:
    """B2: @cache.early() with ttl=None discards hits — every call recomputes."""
    c = Cache()
    c.setup("mem://")
    calls = 0

    @c.early()
    async def fn() -> int:
        nonlocal calls
        calls += 1
        return calls

    await fn()
    await fn()
    await fn()
    check("B2 early(ttl=None): serves from cache", calls == 1, f"{calls} calls")
    await c.close()


async def bug_inflight_task_leak() -> None:
    """B3: _inflight never removes finished tasks → unbounded growth."""
    from cachery.strategies import _base

    c = Cache()
    c.setup("mem://")

    @c.early(ttl="1h", early_ttl="59m")
    async def fn(i: int) -> int:
        return i

    await fn(0)
    await asyncio.sleep(0.05)
    for i in range(1, 200):
        await fn(i)
        await asyncio.sleep(0.001)
    await asyncio.sleep(0.1)
    size = len(_base._inflight)
    check("B3 schedule_background: finished tasks cleaned up", size < 10, f"{size} entries retained")
    await c.close()


async def bug_envelope_collision() -> None:
    """B4: a plain 3-element numeric list stored under the exact key an
    early-decorated function uses must be served as-is (not misread as an
    envelope), while real envelopes still cache."""
    from cachery.core.keys import KeyBuilder

    c = Cache()
    c.setup("mem://")
    calls = 0

    @c.early(ttl="1h", early_ttl="30m")
    async def fn(uid: int) -> Any:
        nonlocal calls
        calls += 1
        return "recomputed"

    strategy_key = KeyBuilder(fn.__wrapped__, None).build((5,), {})
    await c.set(strategy_key, [37.77, -122.42, 10.0], ttl="1h")
    got = await fn(5)
    check(
        "B4 envelope: plain [lat, lon, alt] not treated as envelope",
        got == [37.77, -122.42, 10.0],
        f"got {got!r}",
    )
    await c.delete(strategy_key)

    calls = 0

    @c.early(ttl="1h", early_ttl="59m")
    async def fn4() -> Any:
        nonlocal calls
        calls += 1
        return {"n": calls}

    await fn4()
    second = await fn4()
    check("B4 envelope: real envelope still caches", calls == 1 and second == {"n": 1})
    await c.close()


def bug_monotonic_across_hosts() -> None:
    """B5: envelopes store time.monotonic(); different hosts (or even the same
    host after reboot) have unrelated bases → age math is garbage in Redis."""
    outs = subprocess.run(
        [sys.executable, "-c", "import time; print(time.monotonic())"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    check(
        "B5 monotonic base differs across hosts (info)",
        True,
        f"this process: {time.monotonic():.1f}, fresh interpreter: {outs} — "
        "same machine/boot agree; two hosts sharing Redis will not",
    )


async def bug_locked_none_waiters_recompute() -> None:
    """B6: LockedStrategy waiters poll engine.get(key) and treat None as
    'not ready' — a legitimately cached None is never returned."""
    c = Cache()
    c.setup("mem://")
    calls = 0

    @c.locked(ttl="300ms")
    async def fn() -> None:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.08)

    first = asyncio.create_task(fn())
    await asyncio.sleep(0.02)
    second = asyncio.create_task(fn())
    await asyncio.gather(first, second)
    check("B6 locked: cached None served to waiters", calls == 1, f"{calls} calls")
    await c.close()


async def bug_disk_size_limit_eviction() -> None:
    """B7: eviction decrements total by 1 per deleted key (not bytes) and does
    a full-table SUM on every set."""
    with tempfile.TemporaryDirectory() as td:
        db = DiskBackend(f"{td}/c.db", size_limit="1kb")
        for i in range(20):
            await db.set(f"k{i}", b"x" * 200, ttl=3600)  # 20 x 200B = 4kb > 1kb
        remaining = 0
        for i in range(20):
            if await db.get(f"k{i}") is not None:
                remaining += 1
        check(
            "B7 disk size_limit: keeps ~1kb not ~everything",
            remaining <= 7,
            f"{remaining}/20 keys remain (~{remaining * 200} bytes vs 1024 limit)",
        )
        await db.close()


async def bug_delete_many_unsuppressed() -> None:
    """B8: Cache.delete_many/get_match bypass the fail-soft guard."""
    from cachery.testing.fakes import FakeBackend

    class Exploding(FakeBackend):
        async def delete_many(self, *keys: str) -> int:
            raise ConnectionError("boom")

        async def get(self, key: str) -> Any | None:
            raise ConnectionError("boom")

        async def scan(self, pattern: str, batch_size: int = 100):
            yield "x"

    c = Cache()
    c.setup(backend=Exploding())  # suppress=True default
    try:
        await c.delete_many("a", "b")
        delete_ok = True
    except ConnectionError:
        delete_ok = False
    try:
        async for _ in c.get_match("*"):
            pass
        match_ok = True
    except ConnectionError:
        match_ok = False
    check("B8 delete_many fail-soft", delete_ok, "raw ConnectionError escaped")
    check("B8 get_match fail-soft", match_ok, "raw ConnectionError escaped")


async def bug_tags_with_secret() -> None:
    """B9: with a secret, keys are HMAC'd so tag hashes are not matchable —
    invalidate() silently deletes nothing."""
    c = Cache()
    c.setup("mem://", secret=b"s3cret")

    @c(ttl="1h", tags=["tenant:acme"])
    async def tagged(uid: int) -> int:
        return uid

    await tagged(1)
    deleted = await c.invalidate("tenant:acme")
    check("B9 tags+secret: invalidation works or raises loudly", deleted >= 1, f"deleted {deleted}")


async def bug_hit_counter_never_resets() -> None:
    """B10: after update_after triggers a background refresh, the counter must
    reset → ~one refresh per update_after hits (not one per hit)."""
    c = Cache()
    c.setup("mem://")
    recomputes = 0

    @c.hit(ttl="1h", update_after=3)
    async def fn() -> int:
        nonlocal recomputes
        recomputes += 1
        return recomputes

    await fn()  # miss + store
    await asyncio.sleep(0.05)
    for _ in range(8):  # hits; one refresh per 3 hits expected → ≤ 3 total
        await fn()
        await asyncio.sleep(0.05)
    check("B10 hit(update_after): refresh amortised", recomputes <= 3, f"{recomputes} recomputes for 9 calls")
    await c.close()


async def bug_failover_none_stale() -> None:
    """B33: failover must serve a stale None (same args) on downstream failure."""
    c = Cache()
    c.setup("mem://")
    calls = 0

    @c.failover(ttl="1h", exceptions=(RuntimeError,))
    async def fn() -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            return  # legitimate None result, stored as stale copy
        raise RuntimeError("downstream down")

    await fn()  # stores stale copy (None)
    try:
        value = await fn()
        check("B33 failover: stale None served on failure", value is None, f"got {value!r}")
    except RuntimeError:
        check("B33 failover: stale None served on failure", False, "raised despite stale None")
    await c.close()


async def bug_get_match_prefix_and_counters() -> None:
    """B24: get_match yields engine-prefixed keys; internal counter keys leak
    into results as raw ints."""
    c = Cache()
    c.setup("mem://", prefix="app")
    await c.set("user:1", "alice", ttl="1h")
    await c.incr("user:1:__hits__")  # internal-looking counter
    seen: dict[str, Any] = {}
    async for k, v in c.get_match("user:*"):
        seen[k] = v
    check(
        "B24 get_match: unprefixed keys, no internal counters",
        list(seen) == ["user:1"],
        f"seen={list(seen)}",
    )
    await c.close()


@dataclasses.dataclass
class Quote:
    sym: str
    px: float


async def smoke_sync_paths() -> None:
    c = Cache()
    c.setup("mem://")

    @c(ttl="1h")
    def sync_fn(x: int) -> int:
        return x * 2

    assert sync_fn(2) == 4
    assert sync_fn(2) == 4

    @c.iterator(ttl="1h")
    def sync_iter(n: int):
        return [n, n + 1]

    got_iter = list(sync_iter(1))
    check("sync iterator-list under @cache.iterator", got_iter == [1, 2], f"got {got_iter!r}")

    @c(ttl="1h")
    def sync_gen(n: int):
        yield from range(n)

    got_gen = list(sync_gen(3))
    check("sync generator under plain @cache yields items", got_gen == [0, 1, 2], f"got {got_gen!r}")

    @c(ttl="1h")
    async def async_gen(n: int):
        for i in range(n):
            yield i

    got = [x async for x in async_gen(3)]
    check("async generator under plain @cache yields items", got == [0, 1, 2], f"got {got!r}")

    check("smoke: sync functions", True)
    await c.close()


def guard(fn: Any) -> Any:
    """Run a check coroutine, recording a crash as a failed check."""

    async def wrapped() -> None:
        try:
            await fn()
        except Exception as e:
            check(f"{fn.__name__} (no crash)", False, f"{type(e).__name__}: {e}")

    return wrapped()


async def main() -> None:
    for step in (
        smoke_everything,
        smoke_sync_paths,
        bug_tiered_l1_never_expires,
        bug_tiered_stale_after_l2_delete,
        bug_early_no_ttl_never_hits,
        bug_inflight_task_leak,
        bug_envelope_collision,
        bug_monotonic_across_hosts,
        bug_locked_none_waiters_recompute,
        bug_disk_size_limit_eviction,
        bug_delete_many_unsuppressed,
        bug_tags_with_secret,
        bug_hit_counter_never_resets,
        bug_failover_none_stale,
        bug_get_match_prefix_and_counters,
    ):
        if asyncio.iscoroutinefunction(step):
            await guard(step)
        else:
            step()

    failures = [name for name, ok, _ in RESULTS if not ok]
    print(f"\n{len(RESULTS) - len(failures)}/{len(RESULTS)} passed")
    if failures:
        print("CONFIRMED BUGS:")
        for f in failures:
            print(f"  - {f}")
    # silence CacheryError import warning for linters
    _ = CacheryError


if __name__ == "__main__":
    asyncio.run(main())
