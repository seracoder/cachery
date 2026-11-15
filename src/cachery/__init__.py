"""cachery — a fast, type-safe, sync+async caching library for Python.

Quick start::

    from cachery import cache

    cache.setup("mem://")

    @cache(ttl="1h", key="user:{user_id}")
    async def get_user(user_id: int) -> dict: ...
"""

from cachery.core.condition import NOT_NONE, Condition, time_condition
from cachery.core.decorator import Cache, cache
from cachery.core.engine import Detector, Engine, Lock
from cachery.core.exceptions import (
    BackendConnectionError,
    CacheryError,
    CircuitBreakerOpen,
    InvalidKeyTemplateError,
    InvalidTTLError,
    LockNotAcquiredError,
    RateLimitError,
    SerializationError,
)
from cachery.core.keys import KeyBuilder, KeyIgnore, KeyPart, register_context_var
from cachery.core.sync_bridge import get_bridge
from cachery.core.ttl import parse_ttl, resolve_ttl

__all__ = [
    "NOT_NONE",
    "BackendConnectionError",
    "Cache",
    "CacheryError",
    "CircuitBreakerOpen",
    "Condition",
    "Detector",
    "Engine",
    "InvalidKeyTemplateError",
    "InvalidTTLError",
    "KeyBuilder",
    "KeyIgnore",
    "KeyPart",
    "Lock",
    "LockNotAcquiredError",
    "RateLimitError",
    "SerializationError",
    "cache",
    "get_bridge",
    "parse_ttl",
    "register_context_var",
    "resolve_ttl",
    "time_condition",
]

__version__ = "0.1.0"
