"""Key-digest helpers.

``hashlib.blake2b`` is the stdlib default (fast, modern, keyed mode built in);
``xxhash`` is an opt-in speed extra imported lazily.
"""

from __future__ import annotations

import hashlib
from typing import Any

__all__ = ["digest", "hmac_digest"]

_xxhash: Any | None
try:  # pragma: no cover - exercised only with the extra installed
    import xxhash as _xxhash
except ImportError:
    _xxhash = None


def digest(data: bytes, alg: str = "blake2b") -> str:
    """Hex digest of ``data``. ``alg`` may be any hashlib algorithm or ``xxhash``."""
    if alg == "xxhash":
        if _xxhash is None:
            raise ImportError("digest(alg='xxhash') requires the 'speed' extra: pip install cachery[speed]")
        return str(_xxhash.xxh3_64_hexdigest(data))
    return hashlib.new(alg, data, usedforsecurity=False).hexdigest()


def hmac_digest(key: bytes, data: bytes, alg: str = "blake2b") -> str:
    """Keyed digest used to hide key material when a ``secret`` is configured.

    Uses blake2b's native keyed mode (much faster than HMAC) with ``alg``
    restricted to blake2b/sha family names; unknown algorithms fall back to
    ``hashlib``'s HMAC.
    """
    import hmac as _hmac  # noqa: PLC0415 - cold path

    if alg == "blake2b":
        return hashlib.blake2b(data, key=key[:64], usedforsecurity=False).hexdigest()
    if alg == "blake2s":
        return hashlib.blake2s(data, key=key[:32], usedforsecurity=False).hexdigest()
    return _hmac.new(key, data, alg).hexdigest()
