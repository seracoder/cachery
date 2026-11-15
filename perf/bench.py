"""Head-to-head benchmarks: cachery vs aiocache.

Run with:  uv run pytest -m benchmark --benchmark-only -q perf/
(or `uv run python perf/bench.py` for a plain timing report).

Benchmarks measure what the library controls: cold key-build+serialize
latency and hot in-memory throughput. Redis round-trips are dominated by the
network and excluded here.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

pytestmark = [pytest.mark.benchmark]

PAYLOAD_SMALL = {"id": 1, "name": "alice", "active": True}
PAYLOAD_MEDIUM = {f"key{i}": {"n": i, "s": f"value-{i}"} for i in range(50)}


# -- cachery ----------------------------------------------------------------------


def bench_cachery_cold(benchmark: Any) -> None:
    from cachery.core.keys import KeyBuilder
    from cachery.serialize import codec

    async def fn(user_id: int, tenant: str) -> dict:
        return PAYLOAD_SMALL

    kb = KeyBuilder(fn, "user:{user_id}:{tenant}")

    def run() -> None:
        key = kb.build((1, "acme"), {})
        blob = codec.dumps(PAYLOAD_SMALL)
        codec.loads(blob)
        return key, blob

    benchmark(run)


def bench_cachery_hot(benchmark: Any) -> None:
    from cachery import Cache

    c = Cache()
    c.setup("mem://")

    @c(ttl="1h")
    async def get_user(user_id: int) -> dict:
        return dict(PAYLOAD_SMALL, id=user_id)

    async def scenario() -> None:
        for i in range(100):
            await get_user(i % 10)

    benchmark(lambda: asyncio.new_event_loop().run_until_complete(scenario()))
    asyncio.new_event_loop().run_until_complete(c.close())


# -- aiocache -------------------------------------------------------------------------


def bench_aiocache_hot(benchmark: Any) -> None:
    pytest.importorskip("aiocache")
    from aiocache import Cache as AioCache

    cache = AioCache(AioCache.MEMORY)

    async def scenario() -> None:
        for i in range(100):
            key = f"user:{i % 10}"
            if await cache.get(key) is None:
                await cache.set(key, dict(PAYLOAD_SMALL, id=i % 10), ttl=3600)

    benchmark(lambda: asyncio.new_event_loop().run_until_complete(scenario()))


# -- payload scaling --------------------------------------------------------------------


def bench_cachery_serialize_medium(benchmark: Any) -> None:
    from cachery.serialize import codec

    def run() -> None:
        return codec.loads(codec.dumps(PAYLOAD_MEDIUM))

    benchmark(run)


def bench_pickle_serialize_medium(benchmark: Any) -> None:
    import pickle

    def run() -> None:
        return pickle.loads(pickle.dumps(PAYLOAD_MEDIUM))

    benchmark(run)
