"""Optional compression for serialized payloads.

``zlib`` (stdlib) is the default; ``zstandard`` is an opt-in extra
(``pip install cachery[compress]``) imported lazily.
"""

from __future__ import annotations

import zlib
from typing import Any

__all__ = ["compress", "decompress"]

_ZSTD = "zstd"

_zstandard: Any | None
try:  # pragma: no cover - exercised only with the extra installed
    import zstandard as _zstandard
except ImportError:
    _zstandard = None


def compress(data: bytes, method: str = "zlib") -> bytes:
    if method == "zlib":
        return zlib.compress(data)
    if method == _ZSTD:
        if _zstandard is None:
            raise ImportError(
                "Compression method 'zstd' requires the 'compress' extra: pip install cachery[compress]"
            )
        result: bytes = _zstandard.ZstdCompressor().compress(data)
        return result
    raise ValueError(f"Unknown compression method {method!r}; expected 'zlib' or 'zstd'")


def decompress(data: bytes, method: str = "zlib") -> bytes:
    if method == "zlib":
        return zlib.decompress(data)
    if method == _ZSTD:
        if _zstandard is None:
            raise ImportError(
                "Compression method 'zstd' requires the 'compress' extra: pip install cachery[compress]"
            )
        result: bytes = _zstandard.ZstdDecompressor().decompress(data)
        return result
    raise ValueError(f"Unknown compression method {method!r}; expected 'zlib' or 'zstd'")
