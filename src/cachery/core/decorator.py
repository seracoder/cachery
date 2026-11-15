"""The public ``@cache`` decorator and ``Cache`` facade.

``cache`` is a module-level :class:`Cache` instance — call ``cache.setup(...)``
once, then decorate::

    from cachery import cache

    cache.setup("mem://")

    @cache(ttl="3h", key="user:{user_id}")
    async def get_user(user_id: int) -> dict: ...

Decorated functions keep their exact original signature (``ParamSpec`` overloads);
sync and async callables are both supported.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncIterator, Awaitable, Callable
from functools import wraps
from typing import Any, ParamSpec, TypeVar, cast, overload

from cachery.backends.memory import MemoryBackend
from cachery.backends.protocol import Backend
from cachery.backends.registry import create_backend
from cachery.core.engine import Detector, Engine, Lock
from cachery.core.keys import KeyBuilder
from cachery.core.middleware import Middleware
from cachery.core.sync_bridge import get_bridge
from cachery.core.ttl import TTLValue
from cachery.strategies._base import CacheStrategy, make_call
from cachery.strategies.circuit_breaker import CircuitBreakerStrategy
from cachery.strategies.early import EarlyStrategy
from cachery.strategies.failover import FailoverStrategy
from cachery.strategies.hit import HitStrategy
from cachery.strategies.iterator import IteratorStrategy
from cachery.strategies.locked import LockedStrategy
from cachery.strategies.rate_limit import RateLimitStrategy
from cachery.strategies.simple import SimpleStrategy
from cachery.strategies.soft import SoftStrategy
from cachery.strategies.tags import append_tag_hashes, invalidate_tags, render_tag

__all__ = ["Cache", "cache"]

P = ParamSpec("P")
R = TypeVar("R")

_Condition = Callable[[Any], bool]

_MISS = object()


class _TaggedKeyBuilder:
    """Composes the base key builder with rendered tag-template hashes."""

    __slots__ = ("_base", "_tag_builders")

    def __init__(self, base: KeyBuilder, tag_builders: list[KeyBuilder]) -> None:
        self._base = base
        self._tag_builders = tag_builders

    def build(self, args: tuple[Any, ...], kwargs: dict[str, Any], prefix: str = "") -> str:
        key = self._base.build(args, kwargs, prefix)
        hashes = [render_tag(tb, args, kwargs) for tb in self._tag_builders]
        return append_tag_hashes(key, hashes)


class _Disabled:
    """``cache.disable()`` — usable as a sync or async context manager."""

    def __init__(self, enabled: bool = True) -> None:
        self._enabled = enabled
        self._token: object | None = None

    def _enter(self) -> None:
        from cachery.core.engine import set_disabled  # noqa: PLC0415

        if self._enabled:
            self._token = set_disabled(True)

    def _exit(self) -> None:
        from cachery.core.engine import reset_disabled  # noqa: PLC0415

        if self._token is not None:
            reset_disabled(self._token)
            self._token = None

    def __enter__(self) -> _Disabled:
        self._enter()
        return self

    def __exit__(self, *exc: object) -> None:
        self._exit()

    async def __aenter__(self) -> _Disabled:
        self._enter()
        return self

    async def __aexit__(self, *exc: object) -> None:
        self._exit()


class CacheInfo:
    """``fn.cache_info()`` — per-decorated-function counters (memoize style).

    Counters are process-local and best-effort: with a shared backend other
    processes also serve hits that aren't counted here.
    """

    __slots__ = ("_strategy",)

    def __init__(self, strategy: CacheStrategy) -> None:
        self._strategy = strategy

    def __call__(self) -> dict[str, Any]:
        s = self._strategy
        total = s.hits + s.misses
        return {
            "hits": s.hits,
            "misses": s.misses,
            "total": total,
            "hit_rate": (s.hits / total) if total else 0.0,
        }

    def reset(self) -> None:
        self._strategy.hits = 0
        self._strategy.misses = 0

    def clear(self) -> None:
        self.reset()


def _defer_or_run(coro: Any) -> Any:
    """Return ``coro`` to an async caller; run it to completion for a sync one.

    ``fn.invalidate(...)`` / ``fn.cache_clear()`` must work from both worlds:
    inside an event loop the caller awaits the result, outside one we run it on
    the sync bridge so a plain ``fn.cache_clear()`` statement actually works.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return get_bridge().run(coro)
    return coro


class _FunctionInvalidator:
    """``fn.invalidate(*args, **kwargs)`` — drop the cache entry a call would use."""

    __slots__ = ("_strategy",)

    def __init__(self, strategy: CacheStrategy) -> None:
        self._strategy = strategy

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        key = self._strategy.key_builder.build(args, kwargs)
        return _defer_or_run(self._strategy.engine.delete(key))


def _invalidate_for(strategy: CacheStrategy) -> _FunctionInvalidator:
    return _FunctionInvalidator(strategy)


class _FunctionClearer:
    """``fn.cache_clear()`` — drop every entry this decorated function owns."""

    __slots__ = ("_strategy",)

    def __init__(self, strategy: CacheStrategy) -> None:
        self._strategy = strategy

    def __call__(self) -> Any:
        self._strategy.hits = 0
        self._strategy.misses = 0
        pattern = _owner_pattern(self._strategy)
        if pattern is None:
            return 0
        return _defer_or_run(self._strategy.engine.delete_match(pattern))


def _clear_for(strategy: CacheStrategy) -> _FunctionClearer:
    return _FunctionClearer(strategy)


def _owner_pattern(strategy: CacheStrategy) -> str | None:
    func = getattr(strategy.key_builder, "_func", None)
    module = getattr(func, "__module__", None)
    qualname = getattr(func, "__qualname__", None)
    if not module or not qualname:
        return None
    return f"{module}.{qualname}*"


async def _invoke(func: Any, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
    """Call ``func`` and await it if async (used by multi_cached)."""
    result = func(*args, **kwargs)
    if hasattr(result, "__await__"):
        result = await result
    return result


def _format_key_part(value: Any) -> str:
    from cachery.core.keys import _format_value  # noqa: PLC0415

    return _format_value(value, None)


class Cache:
    """Facade: setup, decorators, and direct low-level operations."""

    def __init__(self) -> None:
        self._engine: Engine | None = None
        self._middlewares: list[Middleware] = []
        self._strategies: list[CacheStrategy] = []

    # -- setup -----------------------------------------------------------------

    def setup(
        self,
        url: str | None = None,
        *,
        backend: Backend | None = None,
        prefix: str = "",
        secret: bytes | str | None = None,
        digestmod: str = "blake2b",
        compress: str | None = None,
        suppress: bool = True,
    ) -> Cache:
        """Configure the cache backend.

        ``cache.setup("mem://")``, ``cache.setup("disk:///cache.sqlite3")``,
        ``cache.setup("redis://localhost/0")``, or pass a duck-typed
        ``backend=`` implementing the :class:`~cachery.backends.protocol.Backend`
        protocol directly. ``prefix`` namespaces all keys.
        """
        if backend is None:
            backend = create_backend(url or "mem://")
        self._engine = Engine(
            backend,
            prefix=prefix,
            secret=secret,
            digestmod=digestmod,
            compress=compress,
            suppress=suppress,
            middlewares=self._middlewares,
        )
        return self

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            self._engine = Engine(MemoryBackend(), middlewares=self._middlewares)
        return self._engine

    # -- middleware / observability ----------------------------------------------

    def add_middleware(self, mw: Middleware) -> Cache:
        self._middlewares.append(mw)
        if self._engine is not None:
            self._engine.middlewares.append(mw)
        return self

    @property
    def detect(self) -> Detector:
        """``with cache.detect as detector: ...`` — inspect what calls would do."""
        return Detector()

    def disable(self, enabled: bool = True) -> _Disabled:
        return _Disabled(enabled)

    # -- decorator internals -------------------------------------------------------

    @staticmethod
    def _validate_static_options(options: dict[str, Any]) -> None:
        """Fail fast at decoration time on bad TTL *expressions*.

        Callables and numeric/timedelta TTLs are always valid; only literal
        strings can be checked statically (they may still resolve differently
        at call time when callable-produced).
        """
        from cachery.core.ttl import parse_ttl  # noqa: PLC0415

        for name in ("ttl", "early_ttl", "soft_ttl", "stale_ttl", "period"):
            value = options.get(name)
            if isinstance(value, str):
                parse_ttl(value)  # raises InvalidTTLError on a bad grammar

    def _prepare(
        self,
        func: Any,
        key: str | None,
        tags: tuple[str, ...] | list[str],
        strategy_cls: type[CacheStrategy],
        options: dict[str, Any],
    ) -> CacheStrategy:
        self._validate_static_options(options)
        kb = KeyBuilder(func, key)
        kb_any: Any = kb
        if tags:
            tag_builders = [KeyBuilder(func, t) for t in tags]
            kb_any = _TaggedKeyBuilder(kb, tag_builders)
        strategy = strategy_cls(self.engine, kb_any, **options)
        self._strategies.append(strategy)
        return strategy

    def _decorate(
        self,
        func: Callable[P, R],
        strategy: CacheStrategy,
        is_async: bool,
        is_async_gen: bool,
        is_sync_gen: bool,
    ) -> Callable[P, R]:
        # A generator function (sync or async) always caches its materialized
        # items, whatever the strategy — the raw generator object itself is
        # neither cacheable nor serializable.
        wants_iterator = strategy.wraps_as_iterator or is_async_gen or is_sync_gen
        if is_async_gen or (is_async and wants_iterator):

            @wraps(func)
            async def agen_wrapper(*args: Any, **kwargs: Any) -> Any:
                call = make_call(func, args, kwargs, True)
                items = await strategy.run(args, kwargs, call)
                if isinstance(items, list):
                    for item in items:
                        yield item
                else:
                    yield items

            wrapper: Any = agen_wrapper
        elif is_async:

            @wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                call = make_call(func, args, kwargs, True)
                return await strategy.run(args, kwargs, call)

            wrapper = async_wrapper
        elif wants_iterator:

            @wraps(func)
            def gen_wrapper(*args: Any, **kwargs: Any) -> Any:
                call = make_call(func, args, kwargs, False)
                items = get_bridge().run(strategy.run(args, kwargs, call))
                yield from items if isinstance(items, list) else [items]

            wrapper = gen_wrapper
        else:

            @wraps(func)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                # Inline fast path: no bridge hop when the strategy and backend
                # can run synchronously (memory/disk backends, simple strategy).
                # Checked per call: setup() may run after decoration.
                if strategy.engine.supports_sync() and strategy.supports_sync_run():
                    return strategy.sync_run(args, kwargs, lambda: func(*args, **kwargs))
                call = make_call(func, args, kwargs, False)
                return get_bridge().run(strategy.run(args, kwargs, call))

            wrapper = sync_wrapper

        # Introspection/management helpers (memoize-style ergonomics).
        wrapper.cache_info = CacheInfo(strategy)
        wrapper.invalidate = _invalidate_for(strategy)
        wrapper.cache_clear = _clear_for(strategy)
        return cast("Callable[P, R]", wrapper)

    def _wrap(
        self,
        func: Callable[P, R],
        strategy_cls: type[CacheStrategy],
        options: dict[str, Any],
        key: str | None,
        tags: tuple[str, ...] | list[str],
    ) -> Callable[P, R]:
        strategy = self._prepare(func, key, tags, strategy_cls, options)
        return self._decorate(
            func,
            strategy,
            is_async=asyncio.iscoroutinefunction(func),
            is_async_gen=inspect.isasyncgenfunction(func),
            is_sync_gen=inspect.isgeneratorfunction(func),
        )

    # -- the decorators -----------------------------------------------------------

    @overload
    def __call__(self, func: Callable[P, Awaitable[R]], /) -> Callable[P, Awaitable[R]]: ...

    @overload
    def __call__(self, func: Callable[P, R], /) -> Callable[P, R]: ...

    @overload
    def __call__(
        self,
        *,
        ttl: TTLValue = None,
        key: str | None = None,
        condition: _Condition | None = None,
        tags: tuple[str, ...] | list[str] = (),
    ) -> Callable[[Callable[P, R]], Callable[P, R]]: ...

    def __call__(self, func: Any = None, **options: Any) -> Any:
        key = options.pop("key", None)
        tags = tuple(options.pop("tags", ()) or ())
        if func is not None:
            return self._wrap(func, SimpleStrategy, options, key, tags)

        def decorate(f: Callable[P, R]) -> Callable[P, R]:
            return self._wrap(f, SimpleStrategy, options, key, tags)

        return decorate

    def early(
        self,
        func: Any = None,
        *,
        ttl: TTLValue = None,
        early_ttl: TTLValue = None,
        key: str | None = None,
        condition: _Condition | None = None,
        tags: tuple[str, ...] | list[str] = (),
    ) -> Any:
        return self._maybe_decorate(
            func, EarlyStrategy, ttl=ttl, early_ttl=early_ttl, key=key, condition=condition, tags=tags
        )

    def soft(
        self,
        func: Any = None,
        *,
        ttl: TTLValue = None,
        soft_ttl: TTLValue = None,
        key: str | None = None,
        condition: _Condition | None = None,
        tags: tuple[str, ...] | list[str] = (),
    ) -> Any:
        return self._maybe_decorate(
            func, SoftStrategy, ttl=ttl, soft_ttl=soft_ttl, key=key, condition=condition, tags=tags
        )

    def hit(
        self,
        func: Any = None,
        *,
        ttl: TTLValue = None,
        cache_hits: int | None = None,
        update_after: int | None = None,
        key: str | None = None,
        condition: _Condition | None = None,
        tags: tuple[str, ...] | list[str] = (),
    ) -> Any:
        return self._maybe_decorate(
            func,
            HitStrategy,
            ttl=ttl,
            cache_hits=cache_hits,
            update_after=update_after,
            key=key,
            condition=condition,
            tags=tags,
        )

    def failover(
        self,
        func: Any = None,
        *,
        ttl: TTLValue = None,
        exceptions: tuple[type[BaseException], ...] = (Exception,),
        stale_ttl: TTLValue = None,
        key: str | None = None,
        condition: _Condition | None = None,
        tags: tuple[str, ...] | list[str] = (),
    ) -> Any:
        return self._maybe_decorate(
            func,
            FailoverStrategy,
            ttl=ttl,
            exceptions=exceptions,
            stale_ttl=stale_ttl,
            key=key,
            condition=condition,
            tags=tags,
        )

    def locked(
        self,
        func: Any = None,
        *,
        ttl: TTLValue = None,
        key: str | None = None,
        condition: _Condition | None = None,
        tags: tuple[str, ...] | list[str] = (),
    ) -> Any:
        return self._maybe_decorate(func, LockedStrategy, ttl=ttl, key=key, condition=condition, tags=tags)

    def rate_limit(
        self,
        func: Any = None,
        *,
        ttl: TTLValue = None,
        limit: int = 10,
        period: TTLValue = "1m",
        key: str | None = None,
        condition: _Condition | None = None,
        tags: tuple[str, ...] | list[str] = (),
    ) -> Any:
        return self._maybe_decorate(
            func,
            RateLimitStrategy,
            ttl=ttl,
            limit=limit,
            period=period,
            key=key,
            condition=condition,
            tags=tags,
        )

    def circuit_breaker(
        self,
        func: Any = None,
        *,
        ttl: TTLValue = None,
        errors_rate: int = 50,
        period: TTLValue = "1m",
        key: str | None = None,
        condition: _Condition | None = None,
        tags: tuple[str, ...] | list[str] = (),
    ) -> Any:
        return self._maybe_decorate(
            func,
            CircuitBreakerStrategy,
            ttl=ttl,
            errors_rate=errors_rate,
            period=period,
            key=key,
            condition=condition,
            tags=tags,
        )

    def iterator(
        self,
        func: Any = None,
        *,
        ttl: TTLValue = None,
        key: str | None = None,
        condition: _Condition | None = None,
        tags: tuple[str, ...] | list[str] = (),
    ) -> Any:
        return self._maybe_decorate(func, IteratorStrategy, ttl=ttl, key=key, condition=condition, tags=tags)

    def multi_cached(
        self,
        func: Any = None,
        *,
        ttl: TTLValue = None,
        keys_from_attr: str,
        key_builder: Callable[..., str] | None = None,
        condition: _Condition | None = None,
    ) -> Any:
        """Batch-cache a dict-returning function per key (aiocache parity).

        The argument named by ``keys_from_attr`` is the list of keys the call
        wants. Cached keys are served from the backend and only the remaining
        keys are passed to the wrapped callable; returned values are stored
        individually. Works for sync and async callables.
        """
        if func is not None:
            return self._wrap_multi(func, ttl, keys_from_attr, key_builder, condition)

        def decorate(f: Callable[P, R]) -> Callable[P, R]:
            return cast("Callable[P, R]", self._wrap_multi(f, ttl, keys_from_attr, key_builder, condition))

        return decorate

    def _wrap_multi(
        self,
        func: Any,
        ttl: TTLValue,
        keys_from_attr: str,
        key_builder: Callable[..., str] | None,
        condition: _Condition | None,
    ) -> Any:
        from cachery.core.ttl import resolve_ttl  # noqa: PLC0415

        def cache_key(item: Any) -> str:
            if key_builder is not None:
                return key_builder(item)
            return f"{func.__module__}.{func.__qualname__}:{_format_key_part(item)}"

        async def run(args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
            bound = inspect.signature(func).bind(*args, **kwargs)
            bound.apply_defaults()
            if keys_from_attr not in bound.arguments:
                return await _invoke(func, args, kwargs)
            requested = list(bound.arguments[keys_from_attr])
            if not requested:
                return await _invoke(func, args, kwargs)
            seconds = await resolve_ttl(ttl)
            cached = await self.engine.get_many_entries(*(cache_key(k) for k in requested))
            result: dict[Any, Any] = {}
            missing: list[Any] = []
            for key, (hit, value) in zip(requested, cached, strict=False):
                if hit:
                    result[key] = value
                else:
                    missing.append(key)
            if missing:
                sub_args = dict(bound.arguments)
                sub_args[keys_from_attr] = missing
                fetched = await _invoke(func, (), sub_args)
                if fetched is None:
                    fetched = {}
                for key, value in fetched.items():
                    result[key] = value
                    if condition is None or condition(value):
                        await self.engine.set(cache_key(key), value, seconds)
            return {k: result[k] for k in requested if k in result}

        if asyncio.iscoroutinefunction(func):

            @wraps(func)
            async def async_multi(*args: Any, **kwargs: Any) -> Any:
                return await run(args, kwargs)

            return async_multi

        @wraps(func)
        def sync_multi(*args: Any, **kwargs: Any) -> Any:
            return get_bridge().run(run(args, kwargs))

        return sync_multi

    def _maybe_decorate(self, func: Any, strategy_cls: type[CacheStrategy], **kwargs: Any) -> Any:
        key = kwargs.pop("key", None)
        tags = tuple(kwargs.pop("tags", ()) or ())
        if func is not None:
            return self._wrap(func, strategy_cls, kwargs, key, tags)

        def decorate(f: Callable[P, R]) -> Callable[P, R]:
            return self._wrap(f, strategy_cls, kwargs, key, tags)

        return decorate

    # -- direct / low-level API ---------------------------------------

    async def get(self, key: str, default: Any = None) -> Any:
        return await self.engine.get(key, default)

    async def set(
        self,
        key: str,
        value: Any,
        ttl: TTLValue = None,
        exist: bool | None = None,
    ) -> bool:
        from cachery.core.ttl import resolve_ttl  # noqa: PLC0415

        return await self.engine.set(key, value, await resolve_ttl(ttl), exist)

    async def get_or_set(
        self,
        key: str,
        default: Any,
        ttl: TTLValue = None,
    ) -> Any:
        hit, value = await self.engine.get_entry(key)
        if hit:
            return value
        resolved: Any = default() if callable(default) else default
        if isinstance(resolved, Awaitable):
            resolved = await resolved
        from cachery.core.ttl import resolve_ttl  # noqa: PLC0415

        await self.engine.set(key, resolved, await resolve_ttl(ttl))
        return resolved

    async def get_many(self, *keys: str) -> tuple[Any, ...]:
        return await self.engine.get_many(*keys)

    async def set_many(self, pairs: dict[str, Any], ttl: TTLValue = None) -> None:
        from cachery.core.ttl import resolve_ttl  # noqa: PLC0415

        await self.engine.set_many(pairs, await resolve_ttl(ttl))

    async def delete(self, key: str) -> bool:
        return await self.engine.delete(key)

    async def delete_many(self, *keys: str) -> int:
        return await self.engine.delete_many(*keys)

    async def delete_match(self, pattern: str) -> int:
        return await self.engine.delete_match(pattern)

    async def get_match(self, pattern: str) -> AsyncIterator[tuple[str, Any]]:
        from cachery.serialize import codec  # noqa: PLC0415

        prefix = f"{self.engine.prefix}:" if self.engine.prefix else ""
        async for full_key in self.engine.scan(pattern):
            raw = await self.engine.get_raw(full_key)
            # Raw int payloads are internal counters (incr), not serialized
            # values — skip them rather than crash on decode.
            if isinstance(raw, (bytes, bytearray)):
                key = full_key[len(prefix) :] if prefix and full_key.startswith(prefix) else full_key
                try:
                    yield key, codec.loads(bytes(raw))
                except Exception:
                    continue

    async def incr(self, key: str, delta: int = 1) -> int:
        return await self.engine.incr(key, delta)

    async def exists(self, key: str) -> bool:
        return await self.engine.exists(key)

    async def expire(self, key: str, ttl: TTLValue) -> bool:
        from cachery.core.ttl import resolve_ttl  # noqa: PLC0415

        return await self.engine.expire(key, await resolve_ttl(ttl) or 0.0)

    async def clear(self) -> int:
        return await self.engine.clear()

    async def ping(self) -> bytes:
        return await self.engine.ping()

    async def close(self) -> None:
        await self.engine.close()

    def lock(self, key: str, ttl: float = 10.0) -> Lock:
        return self.engine.lock(key, ttl)

    def stats(self) -> dict[str, Any]:
        """Aggregate hit/miss counters across every function decorated so far.

        Process-local and best-effort (a shared backend also serves hits this
        process never sees). Per-function figures: ``fn.cache_info()``.
        """
        hits = sum(s.hits for s in self._strategies)
        misses = sum(s.misses for s in self._strategies)
        total = hits + misses
        return {
            "functions": len(self._strategies),
            "hits": hits,
            "misses": misses,
            "total": total,
            "hit_rate": (hits / total) if total else 0.0,
        }

    async def invalidate(self, *tags: str) -> int:
        """Flush every key tagged with any of ``tags`` (see ``@cache(tags=[...])``)."""
        return await invalidate_tags(self.engine, *tags)


cache = Cache()
