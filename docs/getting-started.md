# Getting started

This page takes you from zero to a working cache in five minutes, then walks through everything you'll touch in daily use. Every code block is runnable as-is.

## Install

=== "uv (recommended)"

    ```bash
    uv add cachery
    ```

=== "pip"

    ```bash
    pip install cachery
    ```

There are no dependencies — the core package is pure stdlib. Optional features are extras:

```bash
pip install "cachery[redis]"     # Redis backend
pip install "cachery[speed]"     # msgspec + xxhash fast paths
pip install "cachery[compress]"  # zstandard compression
pip install "cachery[prometheus]" # metrics middleware
pip install "cachery[otel]"      # OpenTelemetry middleware
```

## Your first cache

```python
from cachery import cache

cache.setup("mem://")  # (1)


@cache(ttl="1h")  # (2)
async def get_profile(user_id: int) -> dict:
    print(f"  ...querying database for user {user_id}")  # (3)
    return {"id": user_id, "name": f"user-{user_id}"}


async def main():
    print(await get_profile(42))  # miss → function runs
    print(await get_profile(42))  # hit  → cached, function NOT called
    print(await get_profile(43))  # miss → different arguments, different key


# Expected output:
#   ...querying database for user 42
#   {'id': 42, 'name': 'user-42'}
#   {'id': 42, 'name': 'user-42'}
#   ...querying database for user 43
#   {'id': 43, 'name': 'user-43'}
```

1. `mem://` is the in-memory backend. It's not a toy — it's a fully supported, first-class backend with LRU eviction and TTL expiry. Perfect for single-process apps, scripts, and tests.
2. The TTL accepts `"1h"`, `"500ms"`, `"1h30m"`, `timedelta(hours=1)`, plain seconds (`3600`), or a callable that returns any of those.
3. This print is your proof: on the second call it doesn't appear — the cached value was returned without touching your function.

**That's the 90% use case.** The rest of this page covers what you need next; the other pages go deep on each area.

## Using it without a decorator

Sometimes you want a plain key-value store, not memoization:

```python
from cachery import cache

cache.setup("mem://")

# All of these are coroutines
await cache.set("hits", 100, ttl="1h")
await cache.incr("hits")  # → 101
await cache.get("hits")  # → 101
await cache.get("missing", default=0)  # → 0 (None by default)

await cache.set_many({"a": 1, "b": 2}, ttl="10m")
await cache.get_many("a", "b", "zz")  # → (1, 2, None)

await cache.delete("a")
await cache.delete_match("user:*")  # glob delete, returns count
await cache.exists("b")  # → True
await cache.expire("b", "5m")  # change remaining TTL


# Atomic get-or-compute:
async def load_user():
    return expensive_db_call()


value = await cache.get_or_set("user:1", load_user, ttl="1h")

# Iterate everything matching a pattern:
async for key, value in cache.get_match("user:*"):
    print(key, value)
```

!!! tip "Sync or async?"
    Every `cache.*` method shown here is async (`await` it). The **decorators** work on both sync and async functions — that's the difference. Under the hood, sync decorated functions run on a shared background event loop, so they're efficient, not a per-call `asyncio.run()`.

## The three backends you'll actually use

```python
# 1. In-memory — zero deps, single process, fastest
cache.setup("mem://")
cache.setup("mem://?size=10000")  # cap at 10k entries (LRU eviction)

# 2. Disk — zero deps, survives restarts, good for local dev & single-machine apps
cache.setup("disk:///var/cache/app.sqlite3")
cache.setup("disk:///var/cache/app.sqlite3?size_limit=2gb")  # evict when file grows

# 3. Redis — multi-process, multi-machine (requires cachery[redis])
cache.setup("redis://localhost:6379/0")
cache.setup("redis://:password@redis.example.com:6379/1")

# Bonus: memory in front of Redis — one URL
cache.setup("tiered://mem+redis://localhost:6379/0")
```

All backends pass the same [contract test suite](https://github.com/seracoder/cachery/blob/main/tests/backends/test_backend_contract.py), so behavior is identical across them. Switching from `mem://` in dev to `redis://` in production is a one-line change — read [Backends](backends.md) for the details of each.

## Namespacing and isolation

Multiple caches in one process, each with its own backend and prefix:

```python
from cachery import Cache

user_cache = Cache()
user_cache.setup("redis://localhost:6379/0", prefix="users")

session_cache = Cache()
session_cache.setup("mem://", prefix="sessions")


@user_cache(ttl="1h")
async def get_user(user_id: int) -> dict: ...


@session_cache(ttl="30m")
async def get_session(token: str) -> dict: ...
```

The `prefix` is prepended to every key — `users:...` vs `sessions:...` — so two caches can even share one Redis database without collisions. With a `secret` set, keys are additionally HMAC'd so cached data can't be enumerated by key names:

```python
cache.setup("redis://localhost:6379/0", prefix="app", secret="server-secret")
```

## What happens when the backend breaks?

By default, **nothing bad**. If Redis goes down mid-request, cachery logs nothing, raises nothing — your decorated function just runs as if there were no cache. This is the "fail soft" design (plan §2): a cache should never be the reason your app is down.

```python
# Opt into strictness when you WANT to know (e.g. a cache-warming script):
cache.setup("redis://localhost:6379/0", suppress=False)
# Now backend failures raise BackendConnectionError instead of degrading.
```

## Temporarily bypassing the cache

```python
# For a block of code:
with cache.disable():
    await get_profile(42)  # always calls the real function

# As a decorator-ish async context:
async with cache.disable():
    ...

# In tests: prove your function still works without the cache
```

## What did that call actually do? `detect()`

`detect()` records every cache decision inside its block — the foundation for `X-From-Cache` response headers and test assertions:

```python
with cache.detect as detector:
    await get_profile(42)  # miss
    await get_profile(42)  # hit

print(detector.calls)
# [
#   {"key": "app.get_profile:user_id:42", "backend": "MemoryBackend",
#    "hit": False, "duration_ms": 0.01},
#   {"key": "app.get_profile:user_id:42", "backend": "MemoryBackend",
#    "hit": True, "duration_ms": 0.005, "name": "simple", "ttl": 3600.0},
# ]

print(detector.hits)  # 1
print(detector.calls_total)  # 2
```

A practical FastAPI example:

```python
from fastapi import FastAPI, Response

app = FastAPI()


@app.get("/users/{user_id}")
async def read_user(user_id: int, response: Response):
    with cache.detect as detector:
        profile = await get_profile(user_id)
    response.headers["X-From-Cache"] = "true" if detector.hits else "false"
    return profile
```

## Where to go next

- **[Key building](keys.md)** — the decoration-time template validation (cachery's headline feature), `Annotated` key control, dotted paths, and auto-keys
- **[Strategies](strategies.md)** — early/soft/locked stampede protection, rate limiting, circuit breaking, failover
- **[Backends](backends.md)** — each backend in depth, `TieredBackend`, and writing your own via the Protocol
- **[Serialization](serialization.md)** — how values become bytes, the fast path, msgspec, custom types
- **[Observability](observability.md)** — middlewares, Prometheus, OpenTelemetry, the CLI
- **[Recipes](recipes.md)** — FastAPI/FastAPI-adjacent patterns, testing your cached app, production checklists
