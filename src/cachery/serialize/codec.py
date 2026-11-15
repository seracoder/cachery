"""Serialization codec.

Dispatch order:

1. **Fast path** — ``str``/``int``/``float``/``bool``/``bytes``/``None`` skip the
   codec entirely: a 1-byte type tag plus raw bytes. This is the biggest win
   over pickle-everything for typical API-response caching.
2. **Typed msgspec path** *(extra: ``speed``)* — dataclasses, enums,
   ``msgspec.Struct`` and Pydantic models are msgpack-encoded *with their type
   recorded*, so decode reconstructs the original type exactly.
3. **Safe msgpack path** *(extra: ``speed``)* — plain dicts/lists of primitives
   (string keys only) go through untyped msgpack. Tuples, sets and non-string
   keys would lose fidelity, so they deliberately take the pickle path.
4. **Pickle fallback** — everything else, so nothing is a hard blocker.
5. **Custom hooks** — :func:`cachery.serialize.types.register_type` wins over
   all of the above.

Wire format: 1 tag byte, then type-specific payload. Compression envelopes the
whole encoded payload (``0xF0`` zlib / ``0xF1`` zstd).
"""

from __future__ import annotations

import dataclasses
import enum
import pickle
from typing import Any

from cachery.core.envelope import Envelope as _ENVELOPE_TYPE
from cachery.serialize import compress as _compress_mod
from cachery.serialize.exceptions import SerializationError
from cachery.serialize.types import get_custom_decoder, get_custom_encoder

__all__ = ["dumps", "loads"]

# Fast-path type tags
_T_STR = 0x01
_T_INT = 0x02
_T_FLOAT = 0x03
_T_BOOL = 0x04
_T_BYTES = 0x05
_T_NONE = 0x06
# General tags
_T_PICKLE = 0x10
_T_MSGPACK = 0x11
_T_TYPED_MSGPACK = 0x12
_T_CUSTOM = 0x20
_T_ENVELOPE = 0x30
# Compression envelopes
_T_ZLIB = 0xF0
_T_ZSTD = 0xF1

_msgspec_module: Any | None
try:  # pragma: no cover - exercised only with the extra installed
    import msgspec as _msgspec_module

    _ENCODER: Any = _msgspec_module.msgpack.Encoder()  # type: ignore[union-attr]
    _DECODER: Any = _msgspec_module.msgpack.Decoder()  # type: ignore[union-attr]
except ImportError:
    _msgspec_module = None


def _msgpack_encode(value: Any) -> bytes:
    if _ENCODER is None:  # pragma: no cover - unreachable with the extra
        raise SerializationError("msgspec not installed")
    payload: bytes = _ENCODER.encode(value)
    return payload


def _msgpack_decode(data: bytes) -> Any:
    if _DECODER is None:  # pragma: no cover - unreachable with the extra
        raise SerializationError("msgspec not installed")
    return _DECODER.decode(data)


_ENUM_BASE = enum.Enum
_MAX_MSGPACK_DEPTH = 32


def _length_prefixed(payload: bytes) -> bytes:
    return _pack_u16(len(payload)) + payload


def _is_typed_msgpack_value(value: Any) -> bool:
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        cls = type(value)
        return "<locals>" not in cls.__qualname__  # function-scoped classes can't be re-imported
    if isinstance(value, _ENUM_BASE):
        return "<locals>" not in type(value).__qualname__
    cls = type(value)
    if hasattr(cls, "__struct_fields__") or hasattr(cls, "model_fields"):
        return "<locals>" not in cls.__qualname__
    return False


def _resolve_type(type_path: str) -> type[Any]:
    module_name, _, qualname = type_path.partition(":")
    import importlib  # noqa: PLC0415 - cold path, decode-time only

    try:
        obj: Any = importlib.import_module(module_name)
        for part in qualname.split("."):
            obj = getattr(obj, part)
    except (ImportError, AttributeError) as e:
        raise SerializationError(
            f"Cannot resolve cached type {type_path!r} (moved or renamed since write?)"
        ) from e
    resolved: type[Any] = obj
    return resolved


def _encode_typed_path(value: Any) -> str:
    cls = type(value)
    if not hasattr(cls, "__module__") or not hasattr(cls, "__qualname__"):
        raise SerializationError(f"Type {cls!r} has no importable path")
    return f"{cls.__module__}:{cls.__qualname__}"


def _pack_u16(n: int) -> bytes:
    return n.to_bytes(2, "big")


def _read_u16(data: bytes, pos: int) -> tuple[int, int]:
    return int.from_bytes(data[pos : pos + 2], "big"), pos + 2


def _encode_core(value: Any) -> bytes:  # noqa: PLR0911, PLR0912 - type dispatch, one return per tag
    # 0. custom hooks win over everything
    custom = get_custom_encoder(value)
    if custom is not None:
        type_id, encoder, _ = custom
        inner = _encode_core(encoder(value))
        tid = type_id.encode()
        return bytes([_T_CUSTOM]) + _pack_u16(len(tid)) + tid + inner

    # 1. fast path: primitives skip the codec
    if value is None:
        return bytes([_T_NONE])
    if isinstance(value, bool):  # before int (bool is an int subclass)
        return bytes([_T_BOOL]) + (b"1" if value else b"0")
    if isinstance(value, int):
        return bytes([_T_INT]) + str(value).encode()
    if isinstance(value, float):
        return bytes([_T_FLOAT]) + repr(value).encode()
    if isinstance(value, str):
        return bytes([_T_STR]) + value.encode()
    if isinstance(value, bytes):
        return bytes([_T_BYTES]) + value

    # 2. strategy envelopes: dedicated wire tag (never collides with user data)
    if isinstance(value, _ENVELOPE_TYPE):
        return (
            bytes([_T_ENVELOPE])
            + _length_prefixed(_encode_core(value.value))
            + _length_prefixed(_encode_core(value.created_at))
            + _length_prefixed(_encode_core(value.ttl))
        )

    # 3. typed msgspec path: exact type reconstruction
    if _msgspec_module is not None and _is_typed_msgpack_value(value):
        try:
            payload = _msgpack_encode(value)
        except Exception:
            payload = None
        if payload is not None:
            path = _encode_typed_path(value).encode()
            return bytes([_T_TYPED_MSGPACK]) + _pack_u16(len(path)) + path + payload

    # 4. safe untyped msgpack — only for structures verified lossless (exact
    #    types: dict/list of primitives with str keys). Tuples decode as lists
    #    and sets encode as garbage, so those take the pickle path below.
    if _msgspec_module is not None and _is_msgpack_safe(value):
        return bytes([_T_MSGPACK]) + _msgpack_encode(value)

    # 5. pickle fallback
    try:
        return bytes([_T_PICKLE]) + pickle.dumps(value)
    except Exception as e:
        raise SerializationError(f"Cannot serialize value of type {type(value)!r}: {e}") from e


_ATOMIC_TYPES = frozenset({type(None), bool, int, float, str})
_ATOMIC_TUPLE = (type(None), bool, int, float, str)
_CONTAINER_TYPES = (dict, list)


def _contains_unsafe(value: Any, depth: int = 0) -> bool:
    """True when msgpack would lose fidelity anywhere inside ``value``.

    Fast path: exact-type membership (C-speed frozenset hit). Only containers
    recurse. Non-exact subtypes (e.g. a dict subclass) are treated unsafe —
    conservative and correct; pickle preserves them.
    """
    if type(value) in _ATOMIC_TYPES:
        return False
    if depth > _MAX_MSGPACK_DEPTH:
        return True
    value_type = type(value)
    if value_type is dict:
        return any(type(k) is not str or _contains_unsafe(v, depth + 1) for k, v in value.items())
    if value_type is list:
        return any(_contains_unsafe(v, depth + 1) for v in value)
    return True


def _is_msgpack_safe(value: Any) -> bool:
    return not _contains_unsafe(value)


def dumps(value: Any, *, compress: str | None = None) -> bytes:
    """Serialize ``value`` to bytes; optionally compress with ``'zlib'``/``'zstd'``."""
    try:
        payload = _encode_core(value)
        if compress is None:
            return payload
        if compress == "zlib":
            return bytes([_T_ZLIB]) + _compress_mod.compress(payload, "zlib")
        if compress == "zstd":
            return bytes([_T_ZSTD]) + _compress_mod.compress(payload, "zstd")
        raise ValueError(f"Unknown compression method {compress!r}; expected 'zlib' or 'zstd'")
    except SerializationError:
        raise
    except Exception as e:
        raise SerializationError(f"Serialization failed: {e}") from e


def loads(data: bytes) -> Any:  # noqa: PLR0911, PLR0912 - tag dispatch, one branch per tag
    """Deserialize bytes previously written by :func:`dumps`."""
    if not data:
        return None
    tag = data[0]
    body = data[1:]
    try:
        if tag == _T_ZLIB:
            return loads(_compress_mod.decompress(body, "zlib"))
        if tag == _T_ZSTD:
            return loads(_compress_mod.decompress(body, "zstd"))
        if tag == _T_CUSTOM:
            n, pos = _read_u16(body, 0)
            decoder = get_custom_decoder(body[pos : pos + n].decode())
            if decoder is None:
                raise SerializationError(
                    f"Custom type {body[pos : pos + n].decode()!r} is not registered on read"
                )
            return decoder(loads(body[pos + n :]))
        if tag == _T_STR:
            return body.decode()
        if tag == _T_INT:
            return int(body)
        if tag == _T_FLOAT:
            return float(body)
        if tag == _T_BOOL:
            return body == b"1"
        if tag == _T_BYTES:
            return body
        if tag == _T_NONE:
            return None
        if tag == _T_ENVELOPE:
            return _loads_envelope(body)
        if tag == _T_PICKLE:
            return pickle.loads(body)
        if tag in (_T_MSGPACK, _T_TYPED_MSGPACK):
            return _loads_msgspec(tag, body)
    except SerializationError:
        raise
    except Exception as e:
        raise SerializationError(f"Deserialization failed: {e}") from e
    raise SerializationError(f"Unknown serialization tag 0x{tag:02x} (corrupt or newer wire format?)")


def _loads_envelope(body: bytes) -> Any:
    """Decode ``0x30`` payloads: three length-prefixed encoded values."""
    parts: list[Any] = []
    pos = 0
    for _ in range(3):
        n, pos = _read_u16(body, pos)
        parts.append(loads(body[pos : pos + n]))
        pos += n
    value, created_at, ttl = parts
    return _ENVELOPE_TYPE(value=value, created_at=float(created_at), ttl=float(ttl))


def _loads_msgspec(tag: int, body: bytes) -> Any:
    if _msgspec_module is None:
        raise SerializationError(
            "Payload was written with the msgspec codec; install it with: pip install cachery[speed]"
        )
    if tag == _T_MSGPACK:
        return _msgpack_decode(body)
    n, pos = _read_u16(body, 0)
    type_path = body[pos : pos + n].decode()
    payload = body[pos + n :]
    cls = _resolve_type(type_path)
    try:
        return _msgspec_module.msgpack.decode(payload, type=cls)  # typed decode needs a fresh Decoder
    except Exception:
        return _msgpack_decode(payload)
