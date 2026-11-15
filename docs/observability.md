# Observability

A cache you can't see is a liability. cachery gives you four layers of visibility, from casual debugging to production dashboards — all optional, none required to use the library.

## 1. `detect()` — what did this call do?

The finest-grained tool: a context manager that records every cache decision inside it.

```python
with cache.detect as detector:
    await get_profile(42)  # miss
    await get_profile(42)  # hit
    await get_settings(42)  # miss

detector.calls_total  # 3
detector.hits  # 1
detector.calls[1]  # the hit record:
# {"key": "app.get_profile:user_id:42", "backend": "MemoryBackend",
#  "hit": True, "duration_ms": 0.004, "name": "simple", "ttl": 3600.0}
```

Records carry the key, backend name, hit/miss, duration, and (on hits) the strategy name and TTL. It's context-local, so concurrent requests in one process don't pollute each other's records.

**The classic use — cache-aware response headers:**

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

**In tests — prove your caching works:**

```python
async def test_profile_is_cached(cache):
    with cache.detect as detector:
        await get_profile(1)
        await get_profile(1)
    assert detector.calls_total == 2
    assert detector.hits == 1
```

## 2. Middlewares — an event stream for your cache

Every cache operation emits a typed `CacheEvent` to registered middlewares. A middleware is any callable (sync or async) taking one event:

```python
from cachery import cache


def log_slow(event):
    if event.duration_ms > 50:
        print(f"SLOW {event.name} {event.key} {event.duration_ms:.0f}ms")


cache.add_middleware(log_slow)
```

The event fields:

```python
@dataclass(frozen=True)
class CacheEvent:
    name: str  # "hit" | "miss" | "set" | "error"
    key: str | None
    backend: str  # e.g. "RedisBackend"
    duration_ms: float
    strategy: str | None
    error: str | None  # repr of the failure, for name == "error"
    extra: dict
```

Middleware errors are swallowed by design — observability must never break the cache path. Register before or after `setup()`; middlewares attach either way.

## 3. Prometheus metrics (extra)

```bash
pip install cachery[prometheus]
```

```python
from cachery import cache
from cachery.contrib.prometheus import PrometheusMiddleware

cache.add_middleware(PrometheusMiddleware())
```

Exports, under the `cachery_` prefix:

| Metric | Type | Labels |
|---|---|---|
| `cachery_hits_total` | Counter | `backend` |
| `cachery_misses_total` | Counter | `backend` |
| `cachery_sets_total` | Counter | `backend` |
| `cachery_errors_total` | Counter | `backend` |
| `cachery_backend_latency_seconds` | Histogram | `backend`, `op` |

Wire into your existing exposition:

```python
from prometheus_client import make_asgi_app

app.mount("/metrics", make_asgi_app())
```

The dashboard everyone wants — hit ratio over time:

```promql
sum(rate(cachery_hits_total[5m]))
/
(sum(rate(cachery_hits_total[5m])) + sum(rate(cachery_misses_total[5m])))
```

## 4. OpenTelemetry tracing (extra)

```bash
pip install cachery[otel]
```

```python
from opentelemetry import trace
from cachery import cache
from cachery.contrib.opentelemetry import OpenTelemetryMiddleware

provider = ...  # your OTel setup
cache.add_middleware(OpenTelemetryMiddleware())
```

Each operation becomes a span (`cachery.hit`, `cachery.set`, …) with `cachery.backend`, `cachery.key`, and `cachery.duration_ms` attributes, auto-parented under the calling span (e.g. your HTTP request span). Errors set span status. No tracer wiring needed if a global provider is configured.

## 5. The CLI — inspect any backend from your terminal

```bash
# Is the backend reachable?
python -m cachery --url redis://prod-redis:6379/0 ping
# OK redis://prod-redis:6379/0 -> PONG

# What's in there?
python -m cachery --url redis://prod-redis:6379/0 keys "user:*" --limit 20

# How much is cached?
python -m cachery --url redis://prod-redis:6379/0 stats
# url:     redis://prod-redis:6379/0
# pattern: *
# keys:    15,231
# bytes:   ~84 MB (first 1000 keys)
```

Works with every URL scheme — `mem://`, `disk://`, `redis://`, `tiered://` — stdlib only, zero extra installs. Put it in your runbooks: `ping` is the fastest "is the cache the problem?" check that exists.

## 6. Failure visibility — the `error` event

When a backend operation fails and `suppress=True` swallows it (the default — see [Getting started](getting-started.md#what-happens-when-the-backend-breaks)), the failure still surfaces as an `error` event:

```python
def alert_on_backend_error(event):
    if event.name == "error":
        send_to_pagerduty(f"cache backend error: {event.error}")


cache.add_middleware(alert_on_backend_error)
```

This resolves the apparent tension of fail-soft design: your app keeps working, but you still find out. Prometheus' `cachery_errors_total` is the same signal as a counter.
