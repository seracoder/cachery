"""URL scheme → backend factory.

Supported schemes: ``mem://``, ``memory://``, ``disk://``, ``redis://``,
``rediss://`` (TLS) and ``tiered://mem+redis://host/0`` for an L1+L2 composite.
Query parameters are passed to the backend constructor where they match its
signature (e.g. ``mem://?size=100``, ``disk:///path?size_limit=2gb``).
"""

from __future__ import annotations

from typing import Any
from urllib.parse import parse_qs, urlsplit

from cachery.backends.protocol import Backend

__all__ = ["create_backend", "register_scheme"]

_FACTORIES: dict[str, Any] = {}


def register_scheme(scheme: str, factory: Any) -> None:
    """Register (or override) the factory for a URL scheme."""
    _FACTORIES[scheme] = factory


def _backend_class(name: str) -> Any:
    """Lazily import a built-in backend class (keeps core import cheap)."""
    if name == "MemoryBackend":
        from cachery.backends.memory import MemoryBackend  # noqa: PLC0415

        return MemoryBackend
    if name == "DiskBackend":
        from cachery.backends.disk import DiskBackend  # noqa: PLC0415

        return DiskBackend
    if name == "RedisBackend":
        from cachery.backends.redis import RedisBackend  # noqa: PLC0415

        return RedisBackend
    if name == "TieredBackend":
        from cachery.backends.tiered import TieredBackend  # noqa: PLC0415

        return TieredBackend
    raise ValueError(f"Unknown backend class {name!r}")


def _filter_kwargs(kwargs: dict[str, Any], cls: Any) -> dict[str, Any]:
    """Pass through query params the backend constructor actually accepts."""
    import inspect  # noqa: PLC0415

    params = inspect.signature(cls.__init__).parameters
    return {k: v for k, v in kwargs.items() if k in params}


def create_backend(url: str) -> Backend:
    parts = urlsplit(url)
    scheme = (parts.scheme or "mem").lower()

    if scheme == "tiered":
        # tiered://mem+redis://localhost/0 → l1=mem://, l2=redis://localhost/0
        rest = url.split("://", 1)[1]
        l1_spec, sep, l2_rest = rest.partition("://")
        if not sep:
            raise ValueError(f"Malformed tiered URL {url!r}; expected tiered://mem+redis://host/db")
        tiers = l1_spec.split("+")
        l1 = create_backend(f"{tiers[0]}://")
        l2_scheme = tiers[1] if len(tiers) > 1 else "redis"
        l2 = create_backend(f"{l2_scheme}://{l2_rest}")
        backend: Backend = _backend_class("TieredBackend")(l1=l1, l2=l2)
        return backend

    if scheme in ("mem", "memory"):
        cls = _backend_class("MemoryBackend")
        kwargs = _filter_kwargs(_query_params(parts.query), cls)
        mem_backend: Backend = cls(**kwargs)
        return mem_backend

    if scheme == "disk":
        cls = _backend_class("DiskBackend")
        path = (parts.netloc + parts.path).replace("\\", "/")
        if path.startswith("//"):  # disk://C:/x → netloc "C:" — normalise
            path = path[1:]
        kwargs = _query_params(parts.query)
        disk_backend: Backend = cls(path=path or "cachery.sqlite3", **_filter_kwargs(kwargs, cls))
        return disk_backend

    if scheme in ("redis", "rediss"):
        cls = _backend_class("RedisBackend")
        redis_backend: Backend = cls(url=url)
        return redis_backend

    factory = _FACTORIES.get(scheme)
    if factory is not None:
        custom_backend: Backend = factory(url)
        return custom_backend

    raise ValueError(
        f"Unknown cache URL scheme {scheme!r} in {url!r}. "
        "Supported: mem://, disk://, redis://, rediss://, tiered://"
    )


def _query_params(query: str) -> dict[str, Any]:
    parsed = parse_qs(query)
    out: dict[str, Any] = {}
    for key, values in parsed.items():
        raw = values[-1]
        if raw.lower() in ("true", "false"):
            out[key] = raw.lower() == "true"
        else:
            try:
                out[key] = int(raw)
            except ValueError:
                try:
                    out[key] = float(raw)
                except ValueError:
                    out[key] = raw
    return out
