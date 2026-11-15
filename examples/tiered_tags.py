"""Tiered caching and tag invalidation.

Usage: uv run python examples/tiered_tags.py
(With Redis available: set CACHEERY_DEMO_REDIS=redis://localhost:6379/0)
"""

from __future__ import annotations

import asyncio
import os

from cachery import cache


async def main() -> None:
    redis_url = os.environ.get("CACHEERY_DEMO_REDIS")
    if redis_url:
        cache.setup(f"tiered://mem+redis://{redis_url.split('://', 1)[1]}")
        print(f"tiered: L1 memory + L2 redis ({redis_url})")
    else:
        # Same code path, disk as L2 — tiering works with any L2 backend.
        import tempfile
        from pathlib import Path

        demo_db = Path(tempfile.gettempdir()) / "cachery-demo.sqlite3"
        cache.setup(f"disk:///{demo_db}?size_limit=100mb")
        print(f"tiered: L1 memory + L2 disk ({demo_db})")

    calls = {"profile": 0, "settings": 0}

    @cache(ttl="1h", tags=["user:{user_id}"])
    async def profile(user_id: int) -> str:
        calls["profile"] += 1
        await asyncio.sleep(0.01)
        return f"profile-of-{user_id}"

    @cache(ttl="1h", tags=["user:{user_id}"])
    async def settings(user_id: int) -> str:
        calls["settings"] += 1
        return f"settings-of-{user_id}"

    print(await profile(1), "|", await settings(1))
    print(await profile(1), "|", await settings(1))  # both cached
    print(f"executions: {calls}")  # each ran once

    # Invalidate everything about user 1 with one call.
    deleted = await cache.invalidate("user:1")
    print(f"invalidated {deleted} tagged entries")
    print(await profile(1))  # recomputed
    print(f"executions: {calls}")

    await cache.close()


if __name__ == "__main__":
    asyncio.run(main())
