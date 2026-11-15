"""Stampede protection showcase: early / soft / locked strategies.

Usage: uv run python examples/strategies.py
"""

from __future__ import annotations

import asyncio

from cachery import cache


async def main() -> None:
    cache.setup("mem://")

    # early: background refresh before expiry — callers never wait post-warmup.
    @cache.early(ttl="200ms", early_ttl="100ms")
    async def early_price(symbol: str) -> float:
        await asyncio.sleep(0.02)
        return hash(symbol) % 1000 / 10

    # soft: keep serving stale values while a refresh runs.
    @cache.soft(ttl="100ms", soft_ttl="5s")
    async def soft_feed(page: int) -> list[int]:
        await asyncio.sleep(0.02)
        return list(range(page, page + 10))

    # locked: single-flight — one caller recomputes, the rest wait for it.
    @cache.locked(ttl="1h")
    async def locked_report(report_id: int) -> str:
        await asyncio.sleep(0.05)
        return f"report-{report_id}"

    # rate limiting: quota calls per window, then RateLimitError.
    from cachery.core.exceptions import RateLimitError

    @cache.rate_limit(limit=3, period="1m")
    async def limited() -> int:
        return 1

    print("early:", await early_price("AAPL"), await early_price("AAPL"))
    print("soft :", await soft_feed(1), await soft_feed(1))
    t1, t2 = await asyncio.gather(locked_report(9), locked_report(9))
    print("locked:", t1, t2)
    try:
        for _ in range(5):
            await limited()
    except RateLimitError as e:
        print("rate limited:", str(e).split(":")[0])

    # failover: source exception → stale value served instead of an error.
    state = {"down": False}

    @cache.failover(ttl="1h", exceptions=(RuntimeError,))
    async def flaky() -> str:
        if state["down"]:
            raise RuntimeError("source down")
        return "fresh-value"

    print("failover fresh:", await flaky())
    state["down"] = True
    print("failover stale:", await flaky())  # served from the stale copy

    await cache.close()


if __name__ == "__main__":
    asyncio.run(main())
