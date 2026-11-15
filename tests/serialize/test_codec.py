"""Codec tests: fast-path, msgspec, pickle fallback, compression, custom types."""

from __future__ import annotations

import dataclasses
import enum
from typing import Any

import pytest

from cachery.core.exceptions import SerializationError
from cachery.serialize import codec
from cachery.serialize.types import clear_custom_types, register_type


@dataclasses.dataclass(frozen=True)
class User:
    uid: int
    name: str


class Color(enum.Enum):
    RED = "red"


class Opaque:
    """No codec path handles this except pickle."""


# -- fast path ----------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        None,
        True,
        False,
        0,
        -17,
        3_000_000_000_000,
        1.5,
        -0.25,
        "",
        "hello",
        "héllo wörld",
        b"",
        b"\x00\x01\x02",
    ],
)
def test_fast_path_roundtrip(value: Any) -> None:
    assert codec.loads(codec.dumps(value)) == value
    assert isinstance(codec.loads(codec.dumps(value)), type(value))


def test_fast_path_tag_bytes_are_tiny() -> None:
    assert codec.dumps(None) == b"\x06"


def test_bool_not_confused_with_int() -> None:
    assert codec.loads(codec.dumps(True)) is True
    assert codec.loads(codec.dumps(1)) == 1


# -- msgpack paths -------------------------------------------------------------


def test_dict_roundtrip() -> None:
    value = {"a": 1, "b": [1, 2, 3], "c": {"nested": True}}
    assert codec.loads(codec.dumps(value)) == value


def test_dataclass_roundtrip() -> None:
    user = User(uid=1, name="alice")
    assert codec.loads(codec.dumps(user)) == user


def test_enum_roundtrip() -> None:
    assert codec.loads(codec.dumps(Color.RED)) is Color.RED


@dataclasses.dataclass(frozen=True)
class _NestedInner:
    x: int


@dataclasses.dataclass(frozen=True)
class _NestedOuter:
    inner: _NestedInner
    items: list[int]


def test_nested_dataclasses() -> None:
    value = _NestedOuter(inner=_NestedInner(1), items=[1, 2])
    assert codec.loads(codec.dumps(value)) == value


def test_list_of_mixed_primitives() -> None:
    value = [1, "two", 3.0, None, True]
    assert codec.loads(codec.dumps(value)) == value


# -- pickle fallback -------------------------------------------------------------


def test_opaque_object_via_pickle() -> None:
    obj = Opaque()
    restored = codec.loads(codec.dumps(obj))
    assert isinstance(restored, Opaque)


def test_tuple_fidelity_via_pickle() -> None:
    value = (1, "two", 3.0)
    restored = codec.loads(codec.dumps(value))
    assert restored == value
    assert isinstance(restored, tuple)


def test_set_fidelity_via_pickle() -> None:
    value = {1, 2, 3}
    assert codec.loads(codec.dumps(value)) == value


# -- custom registered types -------------------------------------------------------


class Money:
    def __init__(self, cents: int) -> None:
        self.cents = cents

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Money) and other.cents == self.cents

    def __hash__(self) -> int:
        return hash(self.cents)


def test_register_type_roundtrip() -> None:
    register_type(Money, lambda m: m.cents, Money)
    try:
        assert codec.loads(codec.dumps(Money(250))) == Money(250)
    finally:
        clear_custom_types()


def test_unregistered_custom_type_on_read_raises() -> None:
    register_type(Money, lambda m: m.cents, Money)
    blob = codec.dumps(Money(99))
    clear_custom_types()
    with pytest.raises(SerializationError, match="not registered"):
        codec.loads(blob)


# -- compression ---------------------------------------------------------------------


def test_zlib_compression_roundtrip() -> None:
    value = "compress me " * 100
    blob = codec.dumps(value, compress="zlib")
    assert blob[0] == 0xF0
    assert len(blob) < len(value)  # actually compressed
    assert codec.loads(blob) == value


def test_zstd_compression_roundtrip() -> None:
    zstandard = pytest.importorskip("zstandard")
    del zstandard
    value = "compress me " * 100
    blob = codec.dumps(value, compress="zstd")
    assert blob[0] == 0xF1
    assert codec.loads(blob) == value


def test_unknown_compression_rejected() -> None:
    with pytest.raises(SerializationError, match="compression"):
        codec.dumps("x", compress="brotli")


# -- corruption handling ----------------------------------------------------------


def test_garbage_raises_serialization_error() -> None:
    with pytest.raises(SerializationError):
        codec.loads(b"\xffgarbage")


def test_unknown_tag_raises() -> None:
    with pytest.raises(SerializationError, match="tag"):
        codec.loads(b"\x99whatever")


def test_empty_payload_is_none() -> None:
    assert codec.loads(b"") is None
