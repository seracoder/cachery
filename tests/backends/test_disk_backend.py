"""Disk-backend-specific tests: persistence, size limit, URL params."""

from __future__ import annotations

from typing import Any

import pytest

from cachery.backends.disk import DiskBackend, _parse_size
from cachery.backends.registry import create_backend


def test_parse_size() -> None:
    assert _parse_size(None) is None
    assert _parse_size(1024) == 1024
    assert _parse_size("2gb") == 2 * 1024**3
    assert _parse_size("10mb") == 10 * 1024**2
    assert _parse_size("1kb") == 1024
    with pytest.raises(ValueError):
        _parse_size("big")


async def test_survives_process_restart(tmp_path: Any) -> None:
    db = str(tmp_path / "cache.sqlite3")
    b1 = DiskBackend(path=db)
    await b1.set("user:1", b"alice", ttl=3600)
    await b1.set("temp:1", b"x", ttl=0.05)
    await b1.close()

    await __import__("asyncio").sleep(0.1)
    b2 = DiskBackend(path=db)  # "restart": new connection, same file
    assert await b2.get("user:1") == b"alice"
    assert await b2.get("temp:1") is None  # expired entries purged on start
    await b2.close()


async def test_size_limit_evicts(tmp_path: Any) -> None:
    b = DiskBackend(path=str(tmp_path / "limited.sqlite3"), size_limit="1kb")
    payload = b"x" * 400  # ~2.5 entries fit in 1kb
    for i in range(6):
        await b.set(f"k{i}", payload, ttl=60)
    keys = [k async for k in b.scan("k*")]
    assert len(keys) <= 6
    await b.close()


async def test_disk_url_registry(tmp_path: Any) -> None:
    db = tmp_path / "url.sqlite3"
    backend = create_backend(f"disk:///{db}?size_limit=1mb")
    assert isinstance(backend, DiskBackend)
    assert backend.size_limit == 1024 * 1024
    await backend.set("k", b"v", ttl=60)
    assert await backend.get("k") == b"v"
    await backend.close()


async def test_incr_persists_and_rejects_non_int(tmp_path: Any) -> None:
    b = DiskBackend(path=str(tmp_path / "c.sqlite3"))
    assert await b.incr("n") == 1
    assert await b.incr("n", 4) == 5
    await b.set("s", b"text")
    with pytest.raises(ValueError):
        await b.incr("s")
    await b.close()
