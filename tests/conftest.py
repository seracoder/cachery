"""Shared test fixtures."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from cachery import Cache
from cachery.backends.memory import MemoryBackend


@pytest.fixture
async def cache() -> AsyncIterator[Cache]:
    """A fresh Cache on a fresh MemoryBackend per test."""
    c = Cache()
    c.setup("mem://")
    yield c
    await c.close()


@pytest.fixture
async def backend() -> AsyncIterator[MemoryBackend]:
    b = MemoryBackend(size=100)
    yield b
    await b.close()
