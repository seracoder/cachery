"""Serialization-specific exceptions (kept here so the codec stays import-lean)."""

from __future__ import annotations

from cachery.core.exceptions import SerializationError

__all__ = ["SerializationError"]
