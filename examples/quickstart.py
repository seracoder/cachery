"""Quick-start tour of cachery — runnable end to end.

Usage: uv run python examples/quickstart.py
"""

from __future__ import annotations

import asyncio

from cachery import cache


async def main() -> None:
    # Zero-dependency in-memory setup — a complete, supported configuration.
    cache.setup("mem://")

    calls = 0

    # The 90% path: decorator + TTL string + explicit key template.
    @cache(ttl="1h", key="user:{user_id}")
    async def get_user(user_id: int) -> dict:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.01)  # pretend to hit a database
        return {"id": user_id, "name": f"user{user_id}"}

    print(await get_user(1))
    print(await get_user(1))  # served from cache
    print(f"source executions: {calls}")  # 1

    # Sync functions work identically.
    @cache(ttl="1h")
    def compute_report(customer_id: int, year: int) -> str:
        return f"report-{customer_id}-{year}"

    print(compute_report(7, 2026))

    # Static validation: this would raise InvalidKeyTemplateError at import —
    # typos fail in CI, never silently at runtime.
    from cachery.core.exceptions import InvalidKeyTemplateError

    try:

        @cache(ttl="1h", key="user:{used_id}")  # typo!
        async def broken(user_id: int) -> dict: ...

    except InvalidKeyTemplateError as e:
        print(f"caught at decoration time: {str(e).splitlines()[0]}")

    # Direct low-level API.
    await cache.set("hits", 90, ttl="2h")
    await cache.incr("hits")
    print("hits:", await cache.get("hits"))

    # detect(): what did each call actually do? (e.g. for X-From-Cache headers)
    with cache.detect as detector:
        await get_user(1)
        await get_user(2)
    print(f"detect: {detector.calls_total} calls, {detector.hits} hits")

    # Invalidation by tag.
    await cache.invalidate("user:1") if False else None  # see tagged example

    await cache.close()


if __name__ == "__main__":
    asyncio.run(main())
