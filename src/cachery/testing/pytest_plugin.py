"""pytest fixtures: ``cache`` and ``fake_backend``."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from cachery.core.decorator import Cache
from cachery.testing.fakes import FakeBackend


@pytest.fixture
async def fake_backend() -> AsyncIterator[FakeBackend]:
    yield FakeBackend()


@pytest.fixture
async def cache(fake_backend: FakeBackend) -> AsyncIterator[Cache]:
    """A Cache over the deterministic FakeBackend (records every op)."""
    c = Cache()
    c.setup(backend=fake_backend)
    yield c


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "integration: requires a real backend")
