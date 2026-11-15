# API reference

The complete public surface, organized by where you'll import it from. Everything here passes `mypy --strict` and ships `py.typed`.

## `cachery` (top level)

### `cache`

The module-level `Cache` instance — the 90% entry point.

```python
from cachery import cache
```

### `Cache`

The facade class. Create additional, isolated caches with it:

```python
from cachery import Cache

user_cache = Cache()
user_cache.setup("redis://localhost:6379/0", prefix="users")
```

**Methods:** `setup`, `add_middleware`, `detect`, `disable`, `lock`, `invalidate`, `close` — plus the direct API below and all decorator variants.

### Decorators

| Decorator | Docs |
|---|---|
| `@cache(ttl, key, condition, tags)` | [Strategies](strategies.md#the-plain-cache) |
| `@cache.early(ttl, early_ttl, ...)` | [Strategies](strategies.md#cacheearly-refresh-before-the-cliff) |
| `@cache.soft(ttl, soft_ttl, ...)` | [Strategies](strategies.md#cachesoft-stale-while-revalidate) |
| `@cache.hit(ttl, cache_hits, update_after, ...)` | [Strategies](strategies.md#cachehit-expire-by-popularity) |
| `@cache.failover(ttl, exceptions, stale_ttl, ...)` | [Strategies](strategies.md#cachefailover-serve-stale-when-the-source-dies) |
| `@cache.locked(ttl, ...)` | [Strategies](strategies.md#cachelocked-single-flight-full-stop) |
| `@cache.rate_limit(limit, period, ...)` | [Strategies](strategies.md#cacherate_limit-quota-the-function-itself) |
| `@cache.circuit_breaker(errors_rate, period, ttl, min_calls, ...)` | [Strategies](strategies.md#cachecircuit_breaker-stop-hammering-a-dead-dependency) |
| `@cache.iterator(ttl, ...)` | [Strategies](strategies.md#cacheiterator-cache-generators) |

Common options on all: `ttl` (seconds | `timedelta` | `"1h30m"` | callable), `key` (template), `condition` (callable), `tags` (template list).

### Direct (low-level) API

```python
await cache.get(key, default=None)
await cache.set(key, value, ttl, exist=None)  # exist: None|False(NX)|True(XX)
await cache.get_or_set(key, default, ttl)  # default may be callable/awaitable
await cache.get_many(*keys)  # → tuple
await cache.set_many(mapping, ttl)
await cache.delete(key) / await cache.delete_many(*keys)
await cache.delete_match("pattern:*")
async for key, value in cache.get_match("pattern:*"):
    ...
await cache.exists(key)
await cache.expire(key, ttl)
await cache.incr(key, delta=1)
await cache.clear()
await cache.ping()
await cache.close()
await cache.invalidate(*tags)
cache.lock(key, ttl)  # async context manager
```

### Key control

| Name | Purpose |
|---|---|
| `KeyIgnore` | `Annotated[Request, KeyIgnore]` — exclude from auto keys |
| `KeyPart(name=..., fmt=...)` | `Annotated[str, KeyPart(...)]` — rename/format in auto keys |
| `register_context_var(name)` | Register a `ContextVar` usable as `{@name}` in templates |

### Exceptions

All inherit `CacheryError`:

| Exception | Raised when |
|---|---|
| `InvalidKeyTemplateError` | Bad key template — **at decoration time** |
| `InvalidTTLError` | Unparseable TTL expression |
| `SerializationError` | Value can't be (de)serialized |
| `BackendConnectionError` | Backend failed and `suppress=False` |
| `RateLimitError` | `rate_limit` quota exceeded |
| `CircuitBreakerOpen` | `circuit_breaker` circuit is open |
| `LockNotAcquiredError` | `cache.lock(...)` couldn't acquire in budget |

### Conditions

| Name | Purpose |
|---|---|
| `NOT_NONE` | Only cache non-`None` results |
| `time_condition(start, end, tz=None)` | Only cache within a daily window (handles midnight crossing) |

## `cachery.core`

Internal machinery, public for power users:

- `cachery.core.keys.KeyBuilder(func, template)` — [build keys directly in tests](recipes.md#asserting-the-key-shape)
- `cachery.core.ttl.parse_ttl("1h30m")` — parse without resolving
- `cachery.core.sync_bridge.get_bridge()` — the shared background loop (sync⇄async)
- `cachery.core.middleware.CacheEvent` — the middleware event type

## `cachery.backends`

| Name | Purpose |
|---|---|
| `protocol.Backend` | The duck-typed contract — [custom backends](backends.md#custom-backends-via-protocol) |
| `protocol.BaseBackend` | Optional ABC deriving batch ops |
| `memory.MemoryBackend(size=1024)` | LRU in-memory |
| `disk.DiskBackend(path, size_limit=None)` | sqlite3 WAL |
| `redis.RedisBackend(url, client_side=False)` | redis-py (extra: `redis`) |
| `tiered.TieredBackend(l1, l2, l1_ttl_cap=None)` | L1+L2 composite |
| `registry.create_backend(url)` | URL → backend factory |
| `registry.register_scheme(scheme, factory)` | Teach the registry your scheme |

## `cachery.serialize`

| Name | Purpose |
|---|---|
| `codec.dumps(value, compress=None)` / `codec.loads(data)` | The tag-dispatch codec |
| `types.register_type(cls, encoder, decoder)` | [Custom wire formats](serialization.md#register_type-teach-cachery-your-classes) |
| `hashing.digest(data, alg)` / `hashing.hmac_digest(key, data, alg)` | Key digests |
| `compress.compress(data, method)` / `decompress` | zlib / zstd |

## `cachery.contrib`

| Name | Purpose |
|---|---|
| `prometheus.PrometheusMiddleware()` | Metrics (extra: `prometheus`) |
| `opentelemetry.OpenTelemetryMiddleware()` | Tracing spans (extra: `otel`) |

## `cachery.testing`

| Name | Purpose |
|---|---|
| `fakes.FakeBackend` | Deterministic recording backend: `.ops`, `.fail_on`, `.now` |
| `pytest_plugin` | Auto-loaded fixtures: `cache`, `fake_backend` |

## CLI

```bash
python -m cachery --url <backend-url> ping
python -m cachery --url <backend-url> keys "pattern:*" --limit 100
python -m cachery --url <backend-url> stats
```
