"""Tag-based invalidation.

Tags are rendered like key templates (they may reference function parameters,
e.g. ``tags=["user:{user_id}"]``), hashed, and appended to the cache key.
Flushing a tag is a single pattern delete — no set/union structures needed in
the backend, so tags work on *every* backend including custom duck-typed ones.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cachery.serialize.hashing import digest

if TYPE_CHECKING:
    from cachery.core.engine import Engine
    from cachery.core.keys import KeyBuilder

__all__ = ["append_tag_hashes", "invalidate_tags", "render_tag"]


def render_tag(tag_builder: KeyBuilder, args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
    rendered = tag_builder.render(args, kwargs)
    return digest(rendered.encode())[:12]


def append_tag_hashes(key: str, tag_hashes: list[str]) -> str:
    if not tag_hashes:
        return key
    return f"{key}:tags:{';'.join(tag_hashes)}"


async def invalidate_tags(engine: Engine, *tags: str) -> int:
    """Flush all keys tagged with any of ``tags`` (raw tag *values*, not hashes).

    Works with a configured ``secret`` too: the engine keeps tag hashes
    readable outside the HMAC'd key body (see ``Engine.full_key``).
    """
    deleted = 0
    for tag in tags:
        tag_hash = digest(tag.encode())[:12]
        deleted += await engine.delete_match(f"*{tag_hash}*")
    return deleted
