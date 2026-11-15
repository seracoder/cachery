"""TTL resolution: ``int``, ``float``, ``datetime.timedelta``, ``"1h30m"`` strings,
or callables returning any of those (evaluated per call, with the bound arguments).

The string grammar is ``<number><unit>`` repeated, units ``ms``/``s``/``m``/``h``/``d``/``w``,
e.g. ``"500ms"``, ``"30s"``, ``"10m"``, ``"1h30m"``, ``"2d"``, ``"1w"``.
"""

from __future__ import annotations

import re
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any, TypeAlias

from cachery.core.exceptions import InvalidTTLError

__all__ = ["TTLValue", "parse_ttl", "resolve_ttl"]

_Resolved: TypeAlias = "int | float | timedelta | str | None"
TTLValue: TypeAlias = (
    int | float | timedelta | str | Callable[..., _Resolved] | Callable[..., Awaitable[_Resolved]] | None
)

_UNIT_SECONDS: dict[str, float] = {
    "ms": 0.001,
    "s": 1.0,
    "m": 60.0,
    "h": 3600.0,
    "d": 86400.0,
    "w": 604800.0,
}

_TTL_PART_RE = re.compile(r"(\d+(?:\.\d+)?)(ms|[smhdw])")


def parse_ttl(ttl: str) -> float:
    """Parse a TTL expression like ``"1h30m"`` into seconds.

    Raises:
        InvalidTTLError: if the expression is empty or malformed.
    """
    total = 0.0
    pos = 0
    for match in _TTL_PART_RE.finditer(ttl):
        if match.start() != pos:
            break
        total += float(match.group(1)) * _UNIT_SECONDS[match.group(2)]
        pos = match.end()
    if pos != len(ttl) or pos == 0:
        raise InvalidTTLError(
            f"Invalid TTL expression: {ttl!r}. "
            "Expected a combination of <number><unit> with units s/m/h/d/w, e.g. '1h30m'."
        )
    return total


def _as_seconds(ttl: Any) -> float | None:
    if ttl is None:
        return None
    if isinstance(ttl, bool):  # bool is an int subclass; reject explicitly
        raise InvalidTTLError(f"Invalid TTL value: {ttl!r}")
    if isinstance(ttl, (int, float)):
        return float(ttl)
    if isinstance(ttl, timedelta):
        return ttl.total_seconds()
    if isinstance(ttl, str):
        return parse_ttl(ttl)
    raise InvalidTTLError(
        f"Invalid TTL value: {ttl!r}. Expected int/float seconds, timedelta, or '1h30m' string."
    )


async def resolve_ttl(
    ttl: TTLValue,
    *args: Any,
    **kwargs: Any,
) -> float | None:
    """Resolve any supported TTL expression to seconds (or ``None`` for no expiry).

    Callables are invoked with the decorated function's bound arguments; if the
    callable returns an awaitable, it is awaited.
    """
    if callable(ttl) and not isinstance(ttl, (str, timedelta)):
        result = ttl(*args, **kwargs)
        if isinstance(result, Awaitable):
            result = await result
        return _as_seconds(result)
    return _as_seconds(ttl)
