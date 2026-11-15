"""Plain-timing benchmark runner (no pytest-benchmark required).

Usage: uv run python perf/bench.py
"""

from __future__ import annotations

import asyncio
import statistics
import time


def timeit(fn: object, *, repeat: int = 5, inner: int = 200) -> float:
    """Median microseconds per call over ``repeat`` batches of ``inner`` calls."""
    timings = []
    for _ in range(repeat):
        t0 = time.perf_counter()
        for _ in range(inner):
            if callable(fn):
                fn()
        timings.append((time.perf_counter() - t0) / inner * 1e6)
    return statistics.median(timings)


def main() -> None:
    print("cachery micro-benchmarks (median µs/op, lower is better)\n")

    from cachery.core.keys import KeyBuilder
    from cachery.serialize import codec

    payload_small = {"id": 1, "name": "alice", "active": True}
    payload_medium = {f"key{i}": {"n": i, "s": f"value-{i}"} for i in range(50)}

    async def fn(user_id: int, tenant: str) -> dict:
        return payload_small

    kb = KeyBuilder(fn, "user:{user_id}:{tenant}")
    print(f"  key build + small serialize : {timeit(lambda: codec.dumps(kb.build((1, 'acme'), {}))):8.1f}")

    def roundtrip_medium() -> None:
        codec.loads(codec.dumps(payload_medium))

    print(f"  medium payload roundtrip    : {timeit(roundtrip_medium):8.1f}")

    from cachery import Cache

    c = Cache()
    c.setup("mem://")

    @c(ttl="1h")
    async def get_user(user_id: int) -> dict:
        return dict(payload_small, id=user_id)

    async def hot_100() -> None:
        for i in range(100):
            await get_user(i % 10)

    loop = asyncio.new_event_loop()

    def hot() -> None:
        loop.run_until_complete(hot_100())

    print(f"  hot path (100 ops)          : {timeit(hot):8.1f}")
    loop.run_until_complete(c.close())
    loop.close()

    print("\nComparatives (when installed):")
    try:
        import pickle

        def pickle_medium() -> None:
            pickle.loads(pickle.dumps(payload_medium))

        print(f"  pickle medium roundtrip     : {timeit(pickle_medium):8.1f}")
    except ImportError:
        pass


if __name__ == "__main__":
    main()
