"""Property-based tests for the TTL parser and codec round-trips (plan §13.1)."""

from __future__ import annotations

import dataclasses
from typing import Any

from hypothesis import given, settings
from hypothesis import strategies as st

from cachery.core.ttl import parse_ttl
from cachery.serialize import codec

# -- TTL grammar -----------------------------------------------------------------

valid_units = st.sampled_from(["ms", "s", "m", "h", "d", "w"])


@st.composite
def ttl_expressions(draw: st.DrawFn) -> tuple[str, float]:
    """Generate (expression, expected_seconds) pairs from the grammar."""
    n_parts = draw(st.integers(min_value=1, max_value=4))
    expr = ""
    total = 0.0
    for _ in range(n_parts):
        number = draw(st.integers(min_value=0, max_value=999))
        unit = draw(valid_units)
        expr += f"{number}{unit}"
        total += number * {"ms": 0.001, "s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}[unit]
    return expr, total


@given(ttl_expressions())
@settings(max_examples=300, deadline=None)
def test_parse_composition_roundtrip(case: tuple[str, float]) -> None:
    expr, expected = case
    assert parse_ttl(expr) == expected


@given(st.integers(min_value=1, max_value=10**6))
def test_units_are_exact(count: int) -> None:
    assert parse_ttl(f"{count}s") == float(count)
    assert parse_ttl(f"{count}m") == count * 60.0


# -- codec round-trips --------------------------------------------------------------


primitives = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-(10**12), max_value=10**12),
    st.floats(allow_nan=False, allow_infinity=False),
    st.text(max_size=50),
    st.binary(max_size=50),
)


@given(primitives)
@settings(max_examples=500, deadline=None)
def test_primitive_roundtrip(value: Any) -> None:
    assert codec.loads(codec.dumps(value)) == value


json_safe = st.recursive(
    primitives,
    lambda children: st.one_of(
        st.lists(children, max_size=4),
        st.dictionaries(st.text(max_size=8), children, max_size=4),
    ),
    max_leaves=8,
)


@given(json_safe)
@settings(max_examples=300, deadline=None)
def test_container_roundtrip(value: Any) -> None:
    assert codec.loads(codec.dumps(value)) == value


@dataclasses.dataclass(frozen=True)
class Small:
    a: int
    b: str


@given(st.integers(), st.text(max_size=20))
@settings(max_examples=200, deadline=None)
def test_dataclass_roundtrip(a: int, b: str) -> None:
    value = Small(a, b)
    assert codec.loads(codec.dumps(value)) == value


@given(st.binary(max_size=500))
@settings(max_examples=100, deadline=None)
def test_compression_roundtrip(data: bytes) -> None:
    blob = codec.dumps(data, compress="zlib")
    assert codec.loads(blob) == data
