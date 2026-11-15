"""Typed exception hierarchy for cachery.

All library-raised errors inherit from :class:`CacheryError`, so
``except CacheryError`` is a meaningful catch-all for callers.
"""

from __future__ import annotations

__all__ = [
    "BackendConnectionError",
    "CacheryError",
    "CircuitBreakerOpen",
    "InvalidKeyTemplateError",
    "InvalidTTLError",
    "LockNotAcquiredError",
    "RateLimitError",
    "SerializationError",
]


class CacheryError(Exception):
    """Base class for all errors raised by cachery."""


class BackendConnectionError(CacheryError):
    """The cache backend is unreachable or failed at runtime.

    With the default ``suppress=True`` this is caught internally and the
    wrapped function is executed instead; with ``suppress=False`` it propagates.
    """


class SerializationError(CacheryError):
    """A value could not be serialized or deserialized."""


class InvalidKeyTemplateError(CacheryError):
    """A key template references a parameter that does not exist.

    Raised at decoration time (import time), not on first call, so CI catches
    typos before they ship.
    """


class InvalidTTLError(CacheryError):
    """A TTL expression could not be parsed."""


class RateLimitError(CacheryError):
    """Raised by ``@cache.rate_limit`` when the call exceeds its quota."""


class CircuitBreakerOpen(CacheryError):
    """Raised by ``@cache.circuit_breaker`` while the circuit is open."""


class LockNotAcquiredError(CacheryError):
    """A single-flight lock could not be acquired within its budget."""
