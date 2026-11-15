"""Shared machinery for the strategy decorators."""

from __future__ import annotations

import asyncio
import contextlib
import random
import time
from collections.abc import Awaitable, Callable
from typing import Any

from cachery.core.engine import Engine, is_disabled
from cachery.core.envelope import Envelope
from cachery.core.keys import KeyBuilder
from cachery.core.sync_bridge import get_bridge
from cachery.core.ttl import TTLValue, resolve_ttl

__all__ = ["CacheStrategy", "make_call", "pack_envelope", "unpack_envelope"]

_Call = Callable[[], Awaitable[Any]]

#: Marker for TTL expressions that must be resolved per call (callables).
_DYNAMIC = object()


def _static_ttl(ttl: TTLValue) -> Any:
    """Pre-resolve a TTL expression at decoration time when possible.

    Returns seconds (``float``), ``None`` (no expiry) or ``_DYNAMIC`` for
    callables, which need the decorated call's bound arguments.
    """
    if callable(ttl) and not isinstance(ttl, str):
        return _DYNAMIC
    return _as_seconds_safe(ttl)


def _as_seconds_safe(ttl: Any) -> Any:
    from cachery.core.ttl import _as_seconds  # noqa: PLC0415

    return _as_seconds(ttl)


def make_call(func: Any, args: tuple[Any, ...], kwargs: dict[str, Any], is_async: bool) -> _Call:
    """Return a zero-arg awaitable factory invoking the wrapped function.

    Sync functions are bridged off the event loop; async ones are awaited
    directly. The factory may be invoked multiple times (recompute paths).
    """
    if is_async:
        return lambda: func(*args, **kwargs)
    return lambda: get_bridge().run_sync(func, *args, **kwargs)


def pack_envelope(value: Any, created_at: float, ttl: float) -> Envelope:
    return Envelope(value, created_at, ttl)


def unpack_envelope(payload: Any) -> Envelope | None:
    if type(payload) is Envelope:
        return payload
    return None


class CacheStrategy:
    """Base decorated-call flow: get → call on miss → condition → set.

    ``load`` returns ``(hit, value)``; subclasses override it (and/or
    :meth:`store` / :meth:`on_miss`) to implement early refresh, soft TTL,
    single-flight, failover, hit counters, ...
    """

    name = "simple"
    #: Iterator strategies must be wrapped as generators regardless of the
    #: wrapped function's own shape (plain functions, not just generators).
    wraps_as_iterator = False

    def __init__(
        self,
        engine: Engine,
        key_builder: KeyBuilder,
        *,
        ttl: TTLValue = None,
        condition: Callable[[Any], bool] | None = None,
    ) -> None:
        self.engine = engine
        self.key_builder = key_builder
        self.ttl = ttl
        self.condition = condition
        self._ttl_static = _static_ttl(ttl)
        #: Per-strategy counters, exposed through the wrapper's ``cache_info()``.
        self.hits = 0
        self.misses = 0

    # -- lifecycle hooks -------------------------------------------------------

    async def load(self, key: str, ttl: float | None, call: _Call) -> tuple[bool, Any]:
        del call
        return await self.engine.load_value(key)

    async def store(self, key: str, value: Any, ttl: float | None) -> None:
        await self.engine.set(key, value, ttl)

    async def on_miss(self, key: str, ttl: float | None, call: _Call) -> Any:
        del key, ttl
        return await _materialize(await _call_value(call))

    async def _recompute(self, key: str, ttl: float | None, call: _Call) -> Any:
        """Recompute and re-store without touching the caller's return path."""
        result = await _materialize(await _call_value(call))
        if self.condition is None or self.condition(result):
            await self.store(key, result, ttl)
        return result

    def supports_sync_run(self) -> bool:
        """Whether :meth:`sync_run` can serve a sync caller inline.

        Only the simple strategy qualifies (no background tasks, locks or
        envelopes), and only when the backend has inline sync primitives.
        """
        return False

    def sync_run(self, args: tuple[Any, ...], kwargs: dict[str, Any], call: Any) -> Any:
        """Run the decorator flow synchronously without the event-loop bridge.

        ``call`` is a plain zero-arg callable returning the (sync) function's
        result. Only invoked when :meth:`supports_sync_run` is true.
        """
        raise NotImplementedError

    # -- main flow ---------------------------------------------------------------

    async def run(self, args: tuple[Any, ...], kwargs: dict[str, Any], call: _Call) -> Any:
        if is_disabled():
            return await _materialize(await _call_value(call))
        key = self.key_builder.build(args, kwargs)
        ttl = self._ttl_static
        if ttl is _DYNAMIC:
            ttl = await resolve_ttl(self.ttl, *args, **kwargs)
        hit, value = await self.load(key, ttl, call)
        if hit:
            self.hits += 1
            self.engine.record_detect_meta(key, name=self.name, ttl=ttl)
            return value
        self.misses += 1
        result = await self.on_miss(key, ttl, call)
        if self.condition is None or self.condition(result):
            await self.store(key, result, ttl)
        return result


# -- helpers shared by several strategies -------------------------------------


async def _call_value(call: _Call) -> Any:
    """Invoke a call factory; await coroutines, materialize generators."""
    result = call()
    if hasattr(result, "__await__"):
        result = await result
    return result


async def _materialize(result: Any) -> Any:
    """Consume generator results into lists so they can be cached.

    Generator functions decorated with any strategy cache their materialized
    items; plain values (and str/bytes/dict/list/tuple/set) pass through.
    Must run inside a coroutine (async generators need the loop).
    """
    if hasattr(result, "__aiter__"):
        return [item async for item in result]
    if hasattr(result, "__iter__") and not isinstance(
        result, (str, bytes, bytearray, dict, list, tuple, set, frozenset)
    ):
        return list(result)
    return result


def fresh(value: Any) -> tuple[bool, Any]:
    return True, value


def missed() -> tuple[bool, Any]:
    return False, None


_inflight: dict[str, asyncio.Task[Any]] = {}
_DEFAULT_SOFT_DIVISOR = 3


def schedule_background(key: str, coro_factory: Callable[[], Awaitable[Any]]) -> None:
    """Start a background recompute unless one is already in flight for ``key``.

    Finished tasks remove themselves from ``_inflight`` so long-lived
    processes don't leak one dict entry per refreshed key.
    """
    task = _inflight.get(key)
    if task is not None and not task.done():
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:  # pragma: no cover - no loop in a sync-only process
        return

    async def _run() -> None:
        with contextlib.suppress(Exception):  # background refresh must not crash callers
            await coro_factory()

    task = loop.create_task(_run())
    _inflight[key] = task

    def _cleanup(t: asyncio.Task[Any], k: str = key) -> None:
        if _inflight.get(k) is t:
            _inflight.pop(k, None)

    task.add_done_callback(_cleanup)


def jittered(threshold: float, jitter: float) -> float:
    """Spread refresh thresholds over ``[threshold - jitter, threshold]`` so
    keys refreshed at the same instant don't stampede in lockstep."""
    if jitter <= 0:
        return threshold
    return threshold - random.uniform(0, jitter)


def wall_clock() -> float:
    """Wall-clock timestamp used inside envelopes (safe across hosts)."""
    return time.time()
