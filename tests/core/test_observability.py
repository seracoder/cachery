"""CLI, testing fakes, engine suppress-semantics, and contrib middleware tests."""

from __future__ import annotations

from typing import Any

import pytest

from cachery._cli.__main__ import main as cli_main
from cachery.core.decorator import Cache
from cachery.core.exceptions import CacheryError
from cachery.testing.fakes import FakeBackend

# -- CLI -------------------------------------------------------------------------


def test_cli_ping_disk(tmp_path: Any) -> None:
    assert cli_main(["--url", f"disk:///{tmp_path}/cli.sqlite3", "ping"]) == 0


def test_cli_keys_and_stats(tmp_path: Any) -> None:
    import asyncio

    from cachery.backends.disk import DiskBackend

    url_path = str(tmp_path / "cli.sqlite3")
    b = DiskBackend(path=url_path)
    asyncio.run(b.set("user:1", b"v", ttl=60))
    asyncio.run(b.set("user:2", b"v", ttl=60))
    asyncio.run(b.close())

    assert cli_main(["--url", f"disk:///{url_path}", "keys", "user:*"]) == 0
    assert cli_main(["--url", f"disk:///{url_path}", "stats"]) == 0


def test_cli_ping_failure_reports() -> None:
    # Port 1 refuses connections quickly; the CLI must return 1, not crash.
    assert cli_main(["--url", "redis://127.0.0.1:1/0", "ping"]) == 1


# -- fake backend ------------------------------------------------------------------


async def test_fake_backend_records_ops(fake_backend: FakeBackend) -> None:
    await fake_backend.set("k", b"v", ttl=60)
    await fake_backend.get("k")
    assert ("set", "k") in fake_backend.ops
    assert ("get", "k") in fake_backend.ops


async def test_fake_backend_injected_failure(fake_backend: FakeBackend) -> None:
    fake_backend.fail_on.add("boom")
    c = Cache()
    c.setup(backend=fake_backend)

    @c(ttl="1h")
    async def fn() -> str:
        return "computed"

    # Fail-soft: the wrapped function runs, the caller never sees the error.
    assert await c.engine.set("boom", b"v") is False
    assert await fn() == "computed"
    await c.close()


async def test_fake_backend_deterministic_clock(fake_backend: FakeBackend) -> None:
    await fake_backend.set("k", b"v", ttl=10)
    fake_backend.now += 11  # time travel
    assert await fake_backend.get("k") is None


# -- engine suppress semantics --------------------------------------------------------


async def test_suppress_true_swallows_backend_errors() -> None:
    fake = FakeBackend()
    fake.fail_on.add("k")
    c = Cache()
    c.setup(backend=fake, suppress=True)
    assert await c.get("k") is None  # degrades to "no cache"
    await c.close()


async def test_suppress_false_raises_backend_connection_error() -> None:
    fake = FakeBackend()
    fake.fail_on.add("k")
    c = Cache()
    c.setup(backend=fake, suppress=False)
    from cachery.core.exceptions import BackendConnectionError

    with pytest.raises(BackendConnectionError):
        await c.get("k")
    await c.close()


async def test_secret_hashes_keys() -> None:
    fake = FakeBackend()
    c = Cache()
    c.setup(backend=fake, secret="s3cret")
    await c.set("user:1", "alice", ttl=60)
    stored_keys = list(fake.data)
    assert len(stored_keys) == 1
    assert "user:1" not in stored_keys[0]  # key material hidden
    assert await c.get("user:1") == "alice"
    await c.close()


# -- contrib ---------------------------------------------------------------------------


def test_prometheus_middleware_counts() -> None:
    pytest.importorskip("prometheus_client")
    from cachery.contrib.prometheus import PrometheusMiddleware
    from cachery.core.middleware import CacheEvent

    mw = PrometheusMiddleware()
    mw(CacheEvent(name="hit", key="k", backend="MemoryBackend", duration_ms=1.0))
    mw(CacheEvent(name="miss", key="k", backend="MemoryBackend", duration_ms=1.0))
    mw(CacheEvent(name="error", key="k", backend="MemoryBackend", duration_ms=1.0, error="x"))
    # Counter families exist and were incremented.
    assert mw._hits.labels("MemoryBackend")._value.get() == 1.0
    assert mw._errors.labels("MemoryBackend")._value.get() == 1.0


def test_prometheus_middleware_requires_extra() -> None:
    import builtins

    import cachery.contrib.prometheus as prom_mod

    real_import = builtins.__import__

    def blocked(name: str, *a: object, **k: object) -> object:
        if name.startswith("prometheus"):
            raise ImportError("no prom")
        return real_import(name, *a, **k)

    builtins.__import__ = blocked
    try:
        with pytest.raises(ImportError, match=r"cachery\[prometheus\]"):
            prom_mod.PrometheusMiddleware()
    finally:
        builtins.__import__ = real_import


def test_otel_middleware_requires_extra() -> None:
    import builtins

    import cachery.contrib.opentelemetry as otel_mod

    real_import = builtins.__import__

    def blocked(name: str, *a: object, **k: object) -> object:
        if name.startswith("opentelemetry"):
            raise ImportError("no otel")
        return real_import(name, *a, **k)

    builtins.__import__ = blocked
    try:
        with pytest.raises(ImportError, match=r"cachery\[otel\]"):
            otel_mod.OpenTelemetryMiddleware()
    finally:
        builtins.__import__ = real_import


def test_otel_middleware_emits_spans() -> None:
    pytest.importorskip("opentelemetry")
    from cachery.contrib.opentelemetry import OpenTelemetryMiddleware
    from cachery.core.middleware import CacheEvent

    mw = OpenTelemetryMiddleware()
    mw(CacheEvent(name="hit", key="k", backend="MemoryBackend", duration_ms=2.0))


# -- exception hierarchy ------------------------------------------------------------------


def test_exception_hierarchy() -> None:
    from cachery.core.exceptions import (
        BackendConnectionError,
        InvalidKeyTemplateError,
        InvalidTTLError,
        RateLimitError,
        SerializationError,
    )

    for exc in (
        BackendConnectionError,
        InvalidKeyTemplateError,
        InvalidTTLError,
        RateLimitError,
        SerializationError,
    ):
        assert issubclass(exc, CacheryError)
