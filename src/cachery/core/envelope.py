"""Internal envelope wrapper used by the early/soft strategies.

A dedicated class (not a plain ``[value, created, ttl]`` list) makes envelope
detection an exact-type check: a user value that happens to be a 3-element
numeric list — ``[lat, lon, altitude]`` — can never be mistaken for strategy
bookkeeping.

``created_at`` is a **wall-clock** ``time.time()`` timestamp: envelopes may be
shared through Redis across hosts, whose ``time.monotonic()`` bases are
unrelated to each other.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

__all__ = ["Envelope"]


@dataclass(frozen=True, slots=True)
class Envelope:
    value: Any
    created_at: float
    #: TTL the *strategy* considers fresh; ``0.0`` means "no expiry, always fresh".
    ttl: float
