"""Typed, event-based middleware.

A middleware is any callable receiving a :class:`CacheEvent`; it may be sync or
async. Register with ``cache.add_middleware(fn)``. The ``contrib`` package
ships Prometheus and OpenTelemetry middlewares built on this contract.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, TypeAlias

__all__ = ["CacheEvent", "Middleware"]


@dataclass(frozen=True)
class CacheEvent:
    """One observable cache operation."""

    name: str  # "hit" | "miss" | "set" | "delete" | "error"
    key: str | None
    backend: str
    duration_ms: float
    strategy: str | None = None
    error: str | None = None
    extra: dict[str, Any] = field(default_factory=dict[str, Any])


Middleware: TypeAlias = Callable[[CacheEvent], Awaitable[None] | None]


async def emit(middlewares: Any, event: CacheEvent) -> None:
    """Deliver ``event`` to every middleware; middleware errors never propagate."""
    for mw in middlewares:
        try:
            result = mw(event)
            if isinstance(result, Awaitable):
                await result
        except Exception:
            pass
