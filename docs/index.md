# cachery

A fast, type-safe, sync **and** async caching library for Python.

- Zero mandatory dependencies (core is stdlib-only)
- Static key-template validation at decoration time
- Sync and async callables through the same decorator
- Memory / disk (sqlite3) / Redis / tiered backends, plus duck-typed `Protocol` backends
- Stampede protection (early / soft / locked), rate limiting, circuit breaking, tags

## Quick start

```python
from cachery import cache

cache.setup("mem://")


@cache(ttl="1h", key="user:{user_id}")
async def get_user(user_id: int) -> dict: ...
```

See the [README on GitHub](https://github.com/seracoder/cachery) for full docs.
