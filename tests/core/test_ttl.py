"""TTL parser tests (plan §13.1 property-based targets)."""

from __future__ import annotations

from datetime import timedelta

import pytest

from cachery.core.exceptions import InvalidTTLError
from cachery.core.ttl import parse_ttl, resolve_ttl


@pytest.mark.parametrize(
    ("expr", "expected"),
    [
        ("10s", 10.0),
        ("1m", 60.0),
        ("1h", 3600.0),
        ("1h30m", 5400.0),
        ("2d", 172800.0),
        ("1w", 604800.0),
        ("1m30s", 90.0),
        ("0s", 0.0),
        ("1.5h", 5400.0),
        ("1w2d3h4m5s", 788645.0),
    ],
)
def test_parse_valid(expr: str, expected: float) -> None:
    assert parse_ttl(expr) == expected


@pytest.mark.parametrize(
    "expr",
    ["", "10", "s", "10x", "h10", "1h 30m", "1hh", "10 s", "-5m", "1.2.3h"],
)
def test_parse_invalid(expr: str) -> None:
    with pytest.raises(InvalidTTLError):
        parse_ttl(expr)


async def test_resolve_int() -> None:
    assert await resolve_ttl(90) == 90.0


async def test_resolve_timedelta() -> None:
    assert await resolve_ttl(timedelta(minutes=1, seconds=30)) == 90.0


async def test_resolve_string() -> None:
    assert await resolve_ttl("1m30s") == 90.0


async def test_resolve_none() -> None:
    assert await resolve_ttl(None) is None


async def test_resolve_callable_sync() -> None:
    def ttl(user_id: int) -> int:
        return user_id

    assert await resolve_ttl(ttl, 42) == 42.0


async def test_resolve_callable_async() -> None:
    async def ttl(*, tenant: str) -> str:
        return f"{tenant}m"

    assert await resolve_ttl(ttl, tenant="5") == 300.0


async def test_resolve_callable_returning_none() -> None:
    assert await resolve_ttl(lambda: None) is None


async def test_resolve_bool_rejected() -> None:
    with pytest.raises(InvalidTTLError):
        await resolve_ttl(True)


async def test_resolve_bad_type_rejected() -> None:
    with pytest.raises(InvalidTTLError):
        await resolve_ttl(object())  # type: ignore[arg-type]
