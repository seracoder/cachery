"""Sync ⇄ async dispatch.

Sync callers of decorated functions run the whole cache flow on a lazily
created **shared background event loop** (one per process, reused across
calls) — never ``asyncio.run()`` per call, which pays full loop+thread setup
on every invocation. ``asyncio.run_coroutine_threadsafe`` keeps the calling
thread blocked but the loop warm.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Coroutine
from typing import Any, TypeVar

__all__ = ["SyncBridge"]

T = TypeVar("T")


class SyncBridge:
    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

    def _ensure_loop(self) -> asyncio.AbstractEventLoop:
        with self._lock:
            if self._loop is not None and self._loop.is_running():
                return self._loop
            ready = threading.Event()
            loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()

            def _run() -> None:
                asyncio.set_event_loop(loop)
                ready.set()
                loop.run_forever()

            thread = threading.Thread(target=_run, name="cachery-sync-bridge", daemon=True)
            thread.start()
            ready.wait()
            self._loop = loop
            self._thread = thread
            return loop

    def run(self, coro: Coroutine[Any, Any, T]) -> T:
        """Run ``coro`` to completion on the shared background loop, blocking.

        Calling from a thread that already runs *another* event loop is allowed
        (the call is simply blocking, as any sync call is). Only re-entry from
        the bridge's own loop thread would deadlock and is rejected.
        """
        if self._thread is not None and threading.current_thread() is self._thread:
            coro.close()  # never awaited → close it so callers don't get GC warnings
            raise RuntimeError(
                "Sync re-entry into the cachery bridge loop would deadlock: a "
                "sync-decorated function called another one from inside its own "
                "execution. This is not supported."
            )
        loop = self._ensure_loop()
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        return future.result()

    async def run_sync(self, fn: Any, /, *args: Any, **kwargs: Any) -> Any:
        """Run a plain sync callable off the loop (executor), so sync functions
        decorated with ``@cache`` do not block the bridge loop."""
        loop = asyncio.get_running_loop()
        if kwargs:
            import functools  # noqa: PLC0415

            return await loop.run_in_executor(None, functools.partial(fn, *args, **kwargs))
        return await loop.run_in_executor(None, fn, *args)

    def close(self) -> None:
        with self._lock:
            loop = self._loop
            if loop is not None and loop.is_running():
                loop.call_soon_threadsafe(loop.stop)
            if self._thread is not None:
                self._thread.join(timeout=5)
            self._loop = None
            self._thread = None


_BRIDGE = SyncBridge()


def get_bridge() -> SyncBridge:
    return _BRIDGE
