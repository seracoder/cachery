"""``@cache(ttl=...)`` — the plain cache decorator."""

from __future__ import annotations

from typing import Any

from cachery.core.engine import is_disabled
from cachery.strategies._base import _DYNAMIC, CacheStrategy

__all__ = ["SimpleStrategy"]


class SimpleStrategy(CacheStrategy):
    name = "simple"

    def supports_sync_run(self) -> bool:
        """The simple flow has no background work, so it can run inline on the
        calling thread when the TTL is statically known — avoiding the
        sync-to-async bridge hop (about 10x faster for memory-backed sync callers).

        Callable TTLs need the bound arguments resolved through the async path,
        so those fall back to the bridge.
        """
        return self._ttl_static is not _DYNAMIC

    def sync_run(self, args: tuple[Any, ...], kwargs: dict[str, Any], call: Any) -> Any:
        if is_disabled():
            return call()
        key = self.key_builder.build(args, kwargs)
        hit, value = self.engine.sync_load(key)
        if hit:
            self.hits += 1
            self.engine.record_detect_meta(key, name=self.name, ttl=self._ttl_static)
            return value
        self.misses += 1
        result = call()
        if self.condition is None or self.condition(result):
            self.engine.sync_store(key, result, self._ttl_static)
        return result
