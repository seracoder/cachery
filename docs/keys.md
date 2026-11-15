# Key building

A cache key maps *arguments* to *cached values*. Get the key wrong and you serve the wrong data — which is why cachery validates keys **at import time** and makes the common cases hard to get wrong.

This is the feature that most distinguishes cachery from other caching libraries, so this page is detailed. Read it once; your future self will thank you at 3 a.m.

## The problem cachery solves

In most cache libraries, key templates are unvalidated strings. This is the classic bug:

```python
@cache(ttl="1h", key="user:{used_id}")  # ← typo: used_id vs user_id
async def get_user(user_id: int) -> dict: ...


await get_user(1)  # KeyError at runtime? No — worse: it "works".
```

What actually happens in libraries without validation: the placeholder fails to resolve, and depending on the library you either get a runtime exception on first call (in production), or the error is silently swallowed and *every call misses the cache* — your expensive function runs every time and the cache does nothing. Both are bad; both surface after deploy.

cachery raises at **decoration time** — which is import time — which is CI time:

```python
@cache(ttl="1h", key="user:{used_id}")
async def get_user(user_id: int) -> dict: ...
```

```
cachery.core.exceptions.InvalidKeyTemplateError:
  Unknown placeholder 'used_id' in key template 'user:{used_id}' for
  'get_user' at app/users.py:42 — did you mean 'user_id'?
  Available parameters: user_id
```

The "did you mean" suggestion comes from `difflib.get_close_matches` — the same engine behind Python's own `AttributeError` hints.

## Templates in 60 seconds

A template is a format string where placeholders reference **function parameters**:

```python
@cache(ttl="1h", key="user:{user_id}")
async def get_user(user_id: int) -> dict: ...


@cache(ttl="1h", key="report:{customer_id}:{year}")
async def report(customer_id: int, year: int) -> dict: ...
```

**Dotted paths** traverse attributes and dict keys, and index lists:

```python
@cache(ttl="1h", key="u:{request.user.uid}")
async def handler(request: Request) -> dict: ...


# request.user.uid → attribute attribute


@cache(ttl="1h", key="cfg:{settings['env']}")  # dict access via attribute syntax
async def load(settings: dict) -> dict: ...


@cache(ttl="1h", key="row:{items.0}")
async def first_cell(items: list) -> str: ...
```

**Format specs** transform values inline:

| Spec | Example | Result |
|---|---|---|
| `lower` / `upper` | `{locale:lower}` | `"EN"` → `"en"` |
| `hash(sha256)` | `{token:hash(sha256)}` | long/secret value → fixed-length digest |
| `hash(blake2b)` (default for long values) | `{payload}` | values > 96 chars are digested automatically |
| `!r` / `!s` conversions | `{obj!r}` | `repr()` / `str()` of the value |

```python
@cache(ttl="1h", key="q:{query:hash(sha256)}")
async def search(query: str) -> list: ...


# keys stay short and don't leak the raw query into backend key listings
```

## Auto-generated keys (the default)

When you don't pass `key=`, cachery builds one from **all bound arguments in declaration order**. This is deliberately boring and predictable:

```python
@cache(ttl="1h")
async def get_orders(customer_id: int, include_archived: bool = False) -> list: ...


await get_orders(7)
# key: "app.get_orders:customer_id:7:include_archived:False"

await get_orders(7, include_archived=True)
# key: "app.get_orders:customer_id:7:include_archived:True"
```

Rules worth knowing:

- **Defaults are part of the key.** `get_orders(7)` and `get_orders(7, include_archived=False)` produce the *same* key — defaults are applied before keying, so identical calls never split the cache.
- **`*args` and `**kwargs` are included** (`args.0:...`, `kwargs.name:...`), sorted for stability.
- **Long values are digested.** Anything over 96 characters is hashed (blake2b) instead of embedded — keys stay bounded.
- **Dataclasses/attrs/pydantic use their `repr`**, which is deterministic — two equal values key identically. Plain objects whose `repr` contains a memory address get the address scrubbed, so keys don't change run-to-run.
- **Methods key on instance identity.** `self` is rendered from its value when the instance has a deterministic repr (a frozen dataclass service is ideal), or `id(self)` for plain classes — so two instances never share one entry. If your method's cache should be shared across instances, give an explicit `key="svc:{user_id}"` template instead.

```python
@dataclasses.dataclass(frozen=True)
class PricingService:
    tenant: str

    @cache(ttl="10m")  # auto key: "...PricingService.get:self:PricingService(tenant='acme'):user_id:1"
    async def get(self, user_id: int) -> dict: ...

    # Or explicit, cleaner:
    @cache(ttl="10m", key="price:{self.tenant}:{user_id}")
    async def get2(self, user_id: int) -> dict: ...
```

## `Annotated` key control — no strings at all

The alternative to templates: control keys through **type annotations**, which your IDE and type checker already understand. Two markers, both importable from the top level:

```python
from typing import Annotated
from cachery import cache, KeyIgnore, KeyPart


@cache(ttl="1h")
async def get_profile(
    user_id: int,
    request: Annotated[Request, KeyIgnore],  # ← excluded from the key
    locale: Annotated[str, KeyPart(name="lang", fmt="lower")] = "en",
) -> Profile: ...


await get_profile(1, request=req)  # key: "...get_profile:user_id:1:lang:en"
await get_profile(1, request=req, locale="EN")  # key: "...get_profile:user_id:1:lang:en" (same!)
```

Why this beats strings:

| | String templates | `Annotated` markers |
|---|---|---|
| Refactoring safety | Rename the parameter → template silently breaks (well, loudly — but you must fix it) | Rename the parameter → key updates automatically |
| IDE support | Placeholders are opaque strings | `KeyIgnore`/`KeyPart` are understood as metadata; autocomplete and rename "just work" |
| Per-argument granularity | All-or-nothing per template | Each argument independently ignored/renamed/formatted |

Both styles compose: markers refine the auto-key; an explicit `key=` replaces it entirely.

## Context variables in keys

Request-scoped data (current tenant, auth user) that isn't a function parameter can still key the cache. Register a `ContextVar` once, then reference it in templates as `{@name}`:

```python
from contextvars import ContextVar
from cachery import cache, register_context_var

tenant_var: ContextVar[str] = register_context_var("tenant")


@cache(ttl="10m", key="config:{@tenant}")
async def get_config() -> dict: ...


# elsewhere, per request:
token = tenant_var.set("acme")  # in your middleware
try:
    config = await get_config()  # key: "config:acme"
finally:
    tenant_var.reset(token)
```

Context variables flow with `asyncio` tasks, so per-request isolation is automatic. Using an unregistered name raises at key-build time with a "call `register_context_var(...)` first" message.

## Prefixes, secrets, and what lands in your backend

Full key layout:

```
<your prefix>:<template or auto-key>            ← what you control
```

```python
cache.setup("redis://localhost/0", prefix="myapp:prod", secret="s3cret")
```

- `prefix` namespaces every key — the cheapest way to share one Redis between environments.
- `secret` HMACs the *final* key (blake2b by default; `digestmod="sha256"` for an HMAC-style salt): key names in Redis become opaque digests, so a read-only Redis leak doesn't reveal what's cached. `cache.delete_match("user:*")` still works — pattern operations are matched before hashing… no wait, they can't be: **pattern deletes are prefix-aware but cannot match hashed bodies**, so with `secret` set, prefer tag invalidation (below) over glob deletes.

!!! warning "Tags + secret"
    With a `secret` configured, use `@cache(tags=[...])` + `await cache.invalidate(tag)` for bulk invalidation. Glob patterns can't match hashed key material.

## Tags: invalidation for the real world

Keys answer "what is this cached value?" — tags answer "what does it belong to?". Templates render per-call, so tags follow arguments:

```python
@cache(ttl="1h", tags=["user:{user_id}", "tenant:{tenant_id}"])
async def get_profile(user_id: int, tenant_id: str) -> dict: ...


@cache(ttl="1h", tags=["user:{user_id}"])
async def get_settings(user_id: int) -> dict: ...


# User 1 logs out / changes data → flush everything about them:
deleted = await cache.invalidate("user:1")  # flushes BOTH functions' entries
```

Tag hashes are appended to each key, so invalidation is a single pattern delete — no secondary index structures, which means it works on every backend, including your custom one. The trade-off: tagged keys are longer, and invalidation scans (`delete_match` under the hood), so it's O(matching keys) rather than O(1).

## How validation actually works

For the curious — `KeyBuilder.__init__` runs the following at decoration time, and any failure raises before your app serves a single request:

1. `inspect.signature(func)` → the parameter names.
2. `string.Formatter().parse(template)` → every placeholder, spec, and conversion.
3. Each placeholder's **root** (before the first dot) must be a parameter name, `args`, `kwargs`, or `@context_var`.
4. Unknown root → `difflib.get_close_matches` against the real parameters → `InvalidKeyTemplateError` with the suggestion.
5. Unsupported format specs/conversions are also rejected here, not at call time.

The one thing validation *can't* check statically: whether a dotted path like `{request.user.uid}` exists at runtime. A missing attribute raises a clear `CacheryError` on the first call — with the attribute name and the key being built in the message.

## Testing keys directly

`KeyBuilder` is public API — you can unit-test key generation without touching a backend:

```python
from cachery.core.keys import KeyBuilder


async def fn(user_id: int, year: int) -> dict: ...


kb = KeyBuilder(fn, "report:{customer_id}:{year}")  # raises immediately — good customer_id isn't a param
kb = KeyBuilder(fn, "report:{user_id}:{year}")
assert kb.build((7, 2026), {}) == "app.fn:report:7:2026"
```

See [`tests/core/test_key_validation.py`](https://github.com/seracoder/cachery/blob/main/tests/core/test_key_validation.py) for more patterns.
