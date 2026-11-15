"""Core engine: orchestrates key building, serialization, backends and events.

Design principle: **fail fast at decoration time, fail soft at call
time.** A down or erroring backend degrades to "no cache" (the wrapped function
runs) unless ``suppress=False``; serialization problems are treated the same
way. Configuration errors (bad templates/TTLs) already raised at decoration.
"""

from __future__ import annotations

import contextvars
import re
import time
import uuid
from collections.abc import AsyncIterator
from types import TracebackType
from typing import Any, TypeAlias, cast

from cachery.backends.protocol import Backend
from cachery.core.exceptions import BackendConnectionError, CacheryError, LockNotAcquiredError
from cachery.core.middleware import CacheEvent, emit
from cachery.serialize import codec
from cachery.serialize.hashing import hmac_digest

__all__ = ["DetectRecord", "Detector", "Engine", "Lock", "current_detect"]

DetectRecord: TypeAlias = dict[str, Any]

_TAGS_MARKER = ":tags:"
_TAG_SECTION_RE = re.compile(r"[0-9a-f]{12}(;[0-9a-f]{12})*")

_detect_ctx: contextvars.ContextVar[list[DetectRecord] | None] = contextvars.ContextVar(
    "cachery_detect", default=None
)
_disabled_ctx: contextvars.ContextVar[bool] = contextvars.ContextVar("cachery_disabled", default=False)

_MISS = object()


def current_detect() -> list[DetectRecord] | None:
    """The active ``detect()`` collector for this context, if any."""
    return _detect_ctx.get()


def is_disabled() -> bool:
    return _disabled_ctx.get()


def set_disabled(disabled: bool) -> object:
    """Set the disable flag; returns an opaque token for :func:`reset_disabled`."""
    return _disabled_ctx.set(disabled)


def reset_disabled(token: object) -> None:
    """Reset the disable flag using the token from :func:`set_disabled`."""
    ctx_token = cast("contextvars.Token[bool]", token)
    _disabled_ctx.reset(ctx_token)


class Detector:
    """Collects what each decorated call did.

    ``detector.calls`` is a list of records shaped
    ``{"key": ..., "backend": ..., "hit": ..., "duration_ms": ..., "name": ..., "ttl": ...}``
    (``name``/``ttl`` present on hits), useful for e.g. ``X-From-Cache`` headers.
    """

    def __init__(self) -> None:
        self.calls: list[DetectRecord] = []
        self._token: contextvars.Token[list[DetectRecord] | None] | None = None

    def __enter__(self) -> Detector:
        self._token = _detect_ctx.set(self.calls)
        return self

    def __exit__(self, *exc: object) -> None:
        if self._token is not None:
            _detect_ctx.reset(self._token)
            self._token = None

    @property
    def calls_total(self) -> int:
        return len(self.calls)

    @property
    def hits(self) -> int:
        return sum(1 for c in self.calls if c["hit"])


class Lock:
    """Distributed lock built on the atomic ``set(..., exist=False)`` primitive,
    so it works on every backend (Memory/Disk/Redis/custom)."""

    def __init__(self, engine: Engine, key: str, ttl: float = 10.0) -> None:
        self._engine = engine
        self._key = f"__lock__:{key}"
        self._ttl = ttl
        self._token = uuid.uuid4().hex.encode()

    async def acquire(self) -> bool:
        return await self._engine.set(self._key, self._token, ttl=self._ttl, exist=False)

    async def release(self) -> None:
        current = await self._engine.get(self._key, default=None)
        if current == self._token or current is None:
            await self._engine.delete(self._key)

    async def __aenter__(self) -> Lock:
        import asyncio  # noqa: PLC0415 - stdlib

        deadline = time.monotonic() + max(self._ttl * 2, self._ttl + 10)
        while not await self.acquire():
            if time.monotonic() > deadline:
                raise LockNotAcquiredError(f"Could not acquire lock {self._key!r}")
            await asyncio.sleep(0.05)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.release()


class Engine:
    """One backend + serialization + policy configuration."""

    def __init__(
        self,
        backend: Backend,
        *,
        prefix: str = "",
        secret: bytes | str | None = None,
        digestmod: str = "blake2b",
        compress: str | None = None,
        suppress: bool = True,
        middlewares: Any = (),
    ) -> None:
        self.backend = backend
        self.prefix = prefix
        self.secret = secret.encode() if isinstance(secret, str) else secret
        self.digestmod = digestmod
        self.compress = compress
        self.suppress = suppress
        self.middlewares = list(middlewares)

    # -- keys ----------------------------------------------------------------

    def full_key(self, key: str) -> str:
        if self.prefix:
            key = f"{self.prefix}:{key}"
        if self.secret:
            # Tag hashes stay readable *outside* the HMAC so tag invalidation
            # keeps working: hash the body, append the ":tags:..." suffix.
            body, marker, suffix = key.rpartition(_TAGS_MARKER)
            if marker and _TAG_SECTION_RE.fullmatch(suffix):
                return hmac_digest(self.secret, body.encode(), self.digestmod) + marker + suffix
            return hmac_digest(self.secret, key.encode(), self.digestmod)
        return key

    def pattern_key(self, pattern: str) -> str:
        """Prefix a glob pattern. Secret hashing is deliberately NOT applied:
        hashing the pattern literal would make it match nothing."""
        return f"{self.prefix}:{pattern}" if self.prefix else pattern

    # -- suppressed backend access ---------------------------------------------

    async def _guard(self, coro: Any, key: str | None, op: str) -> Any:
        start = time.perf_counter()
        try:
            return await coro
        except CacheryError:
            raise
        except Exception as e:
            if self.middlewares:
                await emit(
                    self.middlewares,
                    CacheEvent(
                        name="error",
                        key=key,
                        backend=self.backend_name(),
                        duration_ms=(time.perf_counter() - start) * 1000,
                        error=repr(e),
                    ),
                )
            if not self.suppress:
                raise BackendConnectionError(f"Backend operation {op!r} failed: {e}") from e
            return _MISS

    def backend_name(self) -> str:
        return type(self.backend).__name__

    # -- primitives -----------------------------------------------------------

    async def get(self, key: str, default: Any = None) -> Any:
        raw = await self._guard(self.backend.get(self.full_key(key)), key, "get")
        if raw is _MISS or raw is None:
            return default
        try:
            return codec.loads(raw)
        except CacheryError:
            if not self.suppress:
                raise
            return default

    async def get_entry(self, key: str) -> tuple[bool, Any]:
        """``(hit, value)`` with a stored ``None`` counted as a hit.

        The correct primitive for strategies that must treat ``None`` as a
        legitimate cached value; ``get()`` keeps its ``default`` semantics.
        """
        raw = await self._guard(self.backend.get(self.full_key(key)), key, "get")
        if raw is _MISS or raw is None:
            return False, None
        try:
            return True, codec.loads(raw)
        except CacheryError:
            if not self.suppress:
                raise
            return False, None

    async def get_raw(self, key: str) -> Any:
        """Guarded backend ``get`` returning the raw stored payload (or ``_MISS``
        on a suppressed error / ``None`` when absent). For internal scans."""
        return await self._guard(self.backend.get(key), key, "get")

    # -- synchronous fast path ----------------------------------------------------

    def _sync_primitives(self) -> tuple[Any, Any] | None:
        """The backend's inline ``(get, set)`` pair when it advertises one."""
        sync_get = getattr(self.backend, "_sync_get", None)
        sync_set = getattr(self.backend, "_sync_set", None)
        if callable(sync_get) and callable(sync_set):
            return sync_get, sync_set
        return None

    def supports_sync(self) -> bool:
        """True when the backend exposes inline sync primitives, letting sync
        callers skip the cross-thread event-loop bridge entirely."""
        return self._sync_primitives() is not None

    def sync_load(self, key: str) -> tuple[bool, Any]:
        """Inline ``load_value`` for sync callers (no loop hop)."""
        prims = self._sync_primitives()
        if prims is None:  # pragma: no cover - callers gate on supports_sync()
            return False, None
        sync_get = prims[0]
        start = time.perf_counter()
        raw = self._sync_guard(lambda: sync_get(self.full_key(key)), key, "get")
        if raw is _MISS or raw is None:
            self._record_sync_detect(key, hit=False, start=start)
            return False, None
        try:
            value = codec.loads(raw)
        except CacheryError:
            if not self.suppress:
                raise
            self._record_sync_detect(key, hit=False, start=start)
            return False, None
        self._record_sync_detect(key, hit=True, start=start)
        return True, value

    def _record_sync_detect(self, key: str, *, hit: bool, start: float) -> None:
        """Detect-record for the sync fast path (no middleware hop — sync callers
        can't await; the detector contextvar write is cheap and synchronous)."""
        detector = current_detect()
        if detector is not None:
            detector.append(
                {
                    "key": key,
                    "backend": self.backend_name(),
                    "hit": hit,
                    "duration_ms": (time.perf_counter() - start) * 1000,
                }
            )

    def sync_store(self, key: str, value: Any, ttl: float | None) -> bool:
        """Inline ``set`` for sync callers (no loop hop)."""
        prims = self._sync_primitives()
        if prims is None:  # pragma: no cover - callers gate on supports_sync()
            return False
        try:
            raw = codec.dumps(value, compress=self.compress)
        except CacheryError:
            if not self.suppress:
                raise
            return False
        sync_set = prims[1]
        full = self.full_key(key)
        result = self._sync_guard(lambda: sync_set(full, raw, ttl, None), key, "set")
        return False if result is _MISS else bool(result)

    def _sync_guard(self, fn: Any, key: str | None, op: str) -> Any:
        try:
            return fn()
        except CacheryError:
            raise
        except Exception as e:
            if not self.suppress:
                raise BackendConnectionError(f"Backend operation {op!r} failed: {e}") from e
            return _MISS

    async def set(self, key: str, value: Any, ttl: float | None = None, exist: bool | None = None) -> bool:
        try:
            raw = codec.dumps(value, compress=self.compress)
        except CacheryError:
            if not self.suppress:
                raise
            return False
        start = time.perf_counter()
        result = await self._guard(self.backend.set(self.full_key(key), raw, ttl, exist), key, "set")
        if result is _MISS:
            return False
        if self.middlewares:
            await emit(
                self.middlewares,
                CacheEvent(
                    name="set",
                    key=key,
                    backend=self.backend_name(),
                    duration_ms=(time.perf_counter() - start) * 1000,
                ),
            )
        return bool(result)

    async def delete(self, key: str) -> bool:
        result = await self._guard(self.backend.delete(self.full_key(key)), key, "delete")
        return False if result is _MISS else bool(result)

    async def exists(self, key: str) -> bool:
        result = await self._guard(self.backend.exists(self.full_key(key)), key, "exists")
        return False if result is _MISS else bool(result)

    async def expire(self, key: str, ttl: float) -> bool:
        result = await self._guard(self.backend.expire(self.full_key(key), ttl), key, "expire")
        return False if result is _MISS else bool(result)

    async def incr(self, key: str, delta: int = 1) -> int:
        result = await self._guard(self.backend.incr(self.full_key(key), delta), key, "incr")
        if result is _MISS:
            return delta
        return int(result)

    async def clear(self) -> int:
        result = await self._guard(self.backend.clear(), None, "clear")
        return 0 if result is _MISS else int(result)

    async def ping(self) -> bytes:
        result = await self._guard(self.backend.ping(), None, "ping")
        return b"" if result is _MISS else bytes(result)

    async def close(self) -> None:
        await self.backend.close()

    async def get_many_entries(self, *keys: str) -> tuple[tuple[bool, Any], ...]:
        """Per-key ``(hit, value)`` pairs — a stored ``None`` counts as a hit.

        The batch counterpart of :meth:`get_entry`, used by
        ``@cache.multi_cached`` so a legitimately cached ``None`` isn't
        mistaken for "not cached" and re-fetched.
        """
        raws = await self._guard(self.backend.get_many(*(self.full_key(k) for k in keys)), None, "get_many")
        if raws is _MISS:
            return tuple((False, None) for _ in keys)
        out: list[tuple[bool, Any]] = []
        for raw in raws:
            if raw is None:
                out.append((False, None))
                continue
            try:
                out.append((True, codec.loads(raw)))
            except CacheryError:
                if not self.suppress:
                    raise
                out.append((False, None))
        return tuple(out)

    async def get_many(self, *keys: str) -> tuple[Any, ...]:
        raws = await self._guard(self.backend.get_many(*(self.full_key(k) for k in keys)), None, "get_many")
        if raws is _MISS:
            return tuple(None for _ in keys)
        out: list[Any] = []
        for raw in raws:
            if raw is None:
                out.append(None)
                continue
            try:
                out.append(codec.loads(raw))
            except CacheryError:
                if not self.suppress:
                    raise
                out.append(None)
        return tuple(out)

    async def set_many(self, pairs: dict[str, Any], ttl: float | None = None) -> None:
        try:
            raw_pairs = {self.full_key(k): codec.dumps(v, compress=self.compress) for k, v in pairs.items()}
        except CacheryError:
            if not self.suppress:
                raise
            return
        await self._guard(self.backend.set_many(raw_pairs, ttl), None, "set_many")

    async def delete_many(self, *keys: str) -> int:
        result = await self._guard(
            self.backend.delete_many(*(self.full_key(k) for k in keys)), None, "delete_many"
        )
        return 0 if result is _MISS else int(result)

    async def delete_match(self, pattern: str) -> int:
        result = await self._guard(self.backend.delete_match(self.pattern_key(pattern)), None, "delete_match")
        return 0 if result is _MISS else int(result)

    async def scan(self, pattern: str, batch_size: int = 100) -> AsyncIterator[str]:
        prefixed = self.pattern_key(pattern)
        async for key in self._guard_iter(self.backend.scan(prefixed, batch_size), pattern, "scan"):
            yield key

    async def _guard_iter(self, agen: Any, key: str | None, op: str) -> AsyncIterator[str]:
        """Fail-soft wrapper for async-generator backend ops (scan)."""
        ait = agen.__aiter__()
        while True:
            try:
                item = await ait.__anext__()
            except StopAsyncIteration:
                return
            except CacheryError:
                raise
            except Exception as e:
                await self._emit_error(key, op, e)
                if not self.suppress:
                    raise BackendConnectionError(f"Backend operation {op!r} failed: {e}") from e
                return
            yield item

    async def _emit_error(self, key: str | None, op: str, error: Exception) -> None:
        await emit(
            self.middlewares,
            CacheEvent(
                name="error",
                key=key,
                backend=self.backend_name(),
                duration_ms=0.0,
                error=repr(error),
            ),
        )

    def lock(self, key: str, ttl: float = 10.0) -> Lock:
        return Lock(self, key, ttl)

    # -- decorated-call helpers -------------------------------------------------

    async def load_value(self, key: str) -> tuple[bool, Any]:
        """Returns ``(hit, value)``; records a detect entry when active.

        A stored ``None`` counts as a hit: strategies must distinguish
        "cached None" from "absent" (a classic caching footgun).
        """
        start = time.perf_counter()
        raw = await self._guard(self.backend.get(self.full_key(key)), key, "get")
        hit = raw is not _MISS and raw is not None
        value: Any = _MISS
        if hit:
            try:
                value = codec.loads(raw)  # pyright: ignore[reportArgumentType]
            except CacheryError:
                if not self.suppress:
                    raise
                hit, value = False, _MISS
        elapsed_ms = (time.perf_counter() - start) * 1000
        detector = current_detect()
        if detector is not None:
            detector.append(
                {
                    "key": key,
                    "backend": self.backend_name(),
                    "hit": hit,
                    "duration_ms": elapsed_ms,
                }
            )
        if self.middlewares:
            await emit(
                self.middlewares,
                CacheEvent(
                    name="hit" if hit else "miss",
                    key=key,
                    backend=self.backend_name(),
                    duration_ms=elapsed_ms,
                ),
            )
        if not hit:
            return False, None
        return True, value

    def record_detect_meta(self, key: str, *, name: str, ttl: float | None) -> None:
        detector = current_detect()
        if detector is not None and detector:
            detector[-1]["name"] = name
            detector[-1]["ttl"] = ttl


_DISABLE_TOKEN: Any = None
