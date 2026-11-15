"""Custom (de)serialization registry: per-type encode/decode hooks."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeAlias

__all__ = ["clear_custom_types", "get_custom_encoder", "register_type"]

Encoder: TypeAlias = Callable[[Any], Any]
Decoder: TypeAlias = Callable[[Any], Any]

_CUSTOM: dict[str, tuple[Encoder, Decoder]] = {}


def register_type(
    cls: type[Any],
    encoder: Encoder,
    decoder: Decoder,
) -> None:
    """Register custom encode/decode hooks for ``cls`` (and subclasses).

    ``encoder(obj) -> Any`` must return a value the codec can already handle
    (primitives, dicts, lists, ...); ``decoder(value) -> obj`` reverses it.
    """

    def _encode(obj: Any) -> Any:
        return encoder(obj)

    _CUSTOM[f"{cls.__module__}.{cls.__qualname__}"] = (_encode, decoder)


def get_custom_encoder(obj: Any) -> tuple[str, Encoder, Decoder] | None:
    """Find a registered hook for ``obj``'s type (most-derived match)."""
    for cls in type(obj).__mro__:
        entry = _CUSTOM.get(f"{cls.__module__}.{cls.__qualname__}")
        if entry is not None:
            encoder, decoder = entry
            return f"{cls.__module__}.{cls.__qualname__}", encoder, decoder
    return None


def get_custom_decoder(type_id: str) -> Decoder | None:
    entry = _CUSTOM.get(type_id)
    return entry[1] if entry else None


def clear_custom_types() -> None:
    _CUSTOM.clear()
