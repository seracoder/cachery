# Recipes

Solutions to the situations that come up after the tutorial ends. Each recipe is self-contained; copy and adapt.

## Web apps

### FastAPI with cache-aware headers

```python
from fastapi import FastAPI, Response
from cachery import cache

app = FastAPI()
cache.setup("tiered://mem+redis://localhost:6379/0")


@app.get("/users/{user_id}", response_model=dict)
async def read_user(user_id: int, response: Response):
    with cache.detect as detector:
        user = await get_user(user_id)
    response.headers["X-From-Cache"] = "true" if detector.hits else "false"
    return user


@cache(ttl="1h", tags=["user:{user_id}"])
async def get_user(user_id: int) -> dict:
    return await db.fetch_user(user_id)  # your data layer
```

### Cache per request tenant

Multi-tenant app where each tenant's data must be isolated — use a context variable in the key:

```python
from contextvars import ContextVar
from cachery import cache, register_context_var

tenant_var: ContextVar[str] = register_context_var("tenant")


@cache(ttl="10m", key="{@tenant}:config")
async def tenant_config() -> dict:
    return await db.load_config(tenant_var.get())
```

Set `tenant_var` in your middleware; asyncio context isolation keeps concurrent requests separate. (With a `secret` configured, prefer this pattern over glob deletes — see [Keys](keys.md#prefixes-secrets-and-what-lands-in-your-backend).)

### Invalidate on write

The write path flushes what the read path cached — tags make it two lines:

```python
@app.put("/users/{user_id}")
async def update_user(user_id: int, body: UserUpdate):
    user = await db.update_user(user_id, body)
    await cache.invalidate(f"user:{user_id}")  # flushes every cache entry tagged user:{id}
    return user
```

### Don't cache "not found" as data

```python
from cachery import NOT_NONE


@cache(ttl="5m", condition=NOT_NONE)
async def find_user(email: str) -> dict | None:
    return await db.find_by_email(email)  # None results are NOT cached
```

Or cache negative results briefly with a custom condition:

```python
@cache(ttl="1h", condition=lambda r: r is not None)
async def find_user(email): ...


@cache(ttl="10s", condition=lambda r: r is None)  # short negative-TTL — separate decorator
async def find_user_negative(email): ...
```

## Data pipelines

### Expensive report generation

One caller computes, everyone waits — then cache for a day:

```python
@cache.locked(ttl="24h")
async def build_quarterly_report(year: int, quarter: int) -> bytes:
    return await heavy_aggregation(year, quarter)
```

Add failover so yesterday's report still serves if the pipeline source is down:

```python
@cache.failover(ttl="24h", exceptions=(TimeoutError, ConnectionError))
async def build_quarterly_report(year: int, quarter: int) -> bytes: ...
```

### Paginated API responses, cached as one list

```python
@cache.iterator(ttl="15m")
async def walk_all_records(source: str):
    async for batch in api.batches(source):
        for record in batch:
            yield record


records = [r async for r in walk_all_records("orders")]
```

The first caller pays for the walk; everyone else replays the list.

### Protecting a flaky third-party API

Rate limit *and* circuit-break the same call — they answer different questions:

```python
from cachery import cache
from cachery.core.exceptions import CircuitBreakerOpen, RateLimitError


@cache.rate_limit(limit=100, period="1m")
async def call_api(endpoint: str) -> dict:
    return await _call_api_inner(endpoint)


@cache.circuit_breaker(errors_rate=50, period="1m", ttl="30s", min_calls=5)
async def _call_api_inner(endpoint: str) -> dict:
    return await http.get(f"https://api.example.com/{endpoint}").json()


# At the call site: degrade gracefully
async def get_data(endpoint: str) -> dict:
    try:
        return await call_api(endpoint)
    except RateLimitError:
        return local_fallback(endpoint)
    except CircuitBreakerOpen:
        return local_fallback(endpoint)
```

## Production concerns

### Configuring per environment

```python
import os
from cachery import cache

CACHE_URL = os.environ.get("CACHEERY_URL", "mem://")
cache.setup(
    CACHE_URL,
    prefix=os.environ.get("CACHEERY_PREFIX", "app"),
    secret=os.environ.get("CACHEERY_SECRET"),  # key-hiding in prod
    suppress=os.environ.get("CACHEERY_STRICT") != "1",  # fail-soft unless opted out
)
```

Local dev needs no Redis: the default `mem://` keeps everything working; CI can run the Redis integration tests with a real server.

### Surviving a backend outage (the full picture)

With default `suppress=True`:

- Reads → miss (function runs), writes → no-op.
- Decorated functions work normally; only cache benefits pause.
- Every swallowed failure emits an `error` event — alert on it (see [Observability](observability.md#6-failure-visibility-the-error-event)).

To prove fail-soft behavior in tests, inject failures with the fake backend:

```python
from cachery import Cache
from cachery.testing.fakes import FakeBackend


async def test_cache_degrades():
    fake = FakeBackend()
    fake.fail_on.add("everything")  # every op raises ConnectionError
    c = Cache()
    c.setup(backend=fake)

    @c(ttl="1h")
    async def fn() -> str:
        return "real-value"

    assert await fn() == "real-value"  # backend broken, app fine
```

### Warming the cache at startup

```python
async def warm_cache():
    for uid in await db.active_user_ids():
        await get_user(uid)  # populate; hits in steady state
```

With `@cache.locked` on the warmed functions, multiple workers can warm concurrently without duplicate work.

### Cache stampede checklist

Before an event (launch, batch job, news spike), audit expensive cached functions:

1. TTL under 5 minutes? → `@cache.early(ttl=..., early_ttl=...)` so refreshes happen before expiry.
2. Minutes of staleness OK? → `@cache.soft(ttl=..., soft_ttl=...)` — the soft window also absorbs source slowness.
3. Function takes > seconds? → `@cache.locked(ttl=...)` so concurrent misses don't dogpile.

### Monitoring what matters

```promql
# Hit ratio — the number that justifies the cache
sum(rate(cachery_hits_total[5m])) / (sum(rate(cachery_hits_total[5m])) + sum(rate(cachery_misses_total[5m])))

# Backend health — should be ~0
rate(cachery_errors_total[5m])

# Latency — compare against your uncached function latency
histogram_quantile(0.99, rate(cachery_backend_latency_seconds_bucket[5m]))
```

## Testing

### The pytest plugin (fixtures included)

`pip install cachery` ships a pytest plugin with ready fixtures:

```python
# tests are given a `cache` fixture (fresh per test) over a deterministic FakeBackend
async def test_my_feature(cache):
    calls = 0

    @cache(ttl="1h")
    async def fn(x: int) -> int:
        nonlocal calls
        calls += 1
        return x * 2

    assert await fn(2) == 4
    assert await fn(2) == 4
    assert calls == 1
```

The `FakeBackend` records every operation (`backend.ops`) and lets you time-travel (`backend.now += 60`) or inject failures (`backend.fail_on.add(key)`) — TTL tests without sleeping:

```python
async def test_expiry_without_sleeping(fake_backend, cache):
    await cache.set("k", "v", ttl=10)
    fake_backend.now += 11  # 11 seconds later, instantly
    assert await cache.get("k") is None
```

### Deterministic TTLs in integration tests

When testing through a real backend (no fake), keep TTLs tiny and use `cache.expire()` to force the boundary instead of long sleeps.

### Asserting the key shape

```python
from cachery.core.keys import KeyBuilder


def test_key_layout():
    kb = KeyBuilder(get_user.__wrapped__, "user:{user_id}")
    assert kb.build((1,), {}) == "app.get_user:user:1"
```

`__wrapped__` is the original function (the decorator preserves it via `functools.wraps`) — building keys directly keeps key-layout tests fast and backend-free.

## Patterns to avoid

| Anti-pattern | Why it hurts | Instead |
|---|---|---|
| `@cache(ttl=None)` forever | Stale until restart, unbounded growth | Always set a TTL, even a generous one |
| Caching unbounded result sets | Memory bloat (mem://), giant values (redis) | Paginate; cache per-page |
| Per-request objects in keys | `Request`/session objects have unstable reprs | `Annotated[Request, KeyIgnore]` or explicit templates |
| Glob deletes on a huge keyspace | `SCAN`-based, O(keyspace) | Tags; or per-key deletes |
| Very long TTLs on user-scoped data | User sees other users' changes… or worse | Short TTL + `invalidate()` on write |
| Shared `mem://` across "services" in one process | One prefix per service, or one `Cache()` each | `Cache()` instances with `prefix=` |
