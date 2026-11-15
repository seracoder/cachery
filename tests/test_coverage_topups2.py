"""Second round of coverage top-ups: registry parsing, iterator, soft, engine, bridge."""

from __future__ import annotations

import pytest

from cachery import Cache
from cachery.backends.memory import MemoryBackend
from cachery.backends.registry import create_backend
from cachery.core.engine import Engine
from cachery.core.exceptions import SerializationError
from cachery.core.keys import KeyBuilder
from cachery.core.sync_bridge import get_bridge
from cachery.serialize import codec
from cachery.testing.fakes import FakeBackend

# -- registry: tiered URL parsing ------------------------------------------------


def test_tiered_url_parses_l1_and_l2() -> None:
    backend = create_backend("tiered://mem+mem://")  # two memory tiers for parsing test
    assert type(backend).__name__ == "TieredBackend"
    assert isinstance(backend.l1, MemoryBackend)
    assert isinstance(backend.l2, MemoryBackend)


def test_tiered_url_malformed_raises() -> None:
    with pytest.raises(ValueError, match="tiered"):
        create_backend("tiered://just-one-part")


# -- iterator: sync source through async machinery ---------------------------------


async def test_iterator_with_plain_return_value() -> None:
    c = Cache()
    c.setup("mem://")

    @c.iterator(ttl="1h")
    async def one_item() -> int:
        return 5

    assert [x async for x in one_item()] == [5]
    await c.close()


async def test_iterator_non_generator_sync_list() -> None:
    c = Cache()
    c.setup("mem://")

    @c.iterator(ttl="1h")
    def rows() -> list[int]:
        return [1, 2, 3]

    assert list(rows()) == [1, 2, 3]
    assert list(rows()) == [1, 2, 3]  # cached
    await c.close()


# -- soft: callable soft_ttl and None ttl paths ---------------------------------------


async def test_soft_with_none_ttl() -> None:
    c = Cache()
    c.setup("mem://")

    @c.soft(ttl=None, soft_ttl=None)
    async def fn() -> int:
        return 1

    assert await fn() == 1
    assert await fn() == 1
    await c.close()


async def test_soft_with_callable_ttls() -> None:
    c = Cache()
    c.setup("mem://")
    calls = 0

    @c.soft(ttl=lambda: "1h", soft_ttl=lambda: "1h")
    async def fn() -> int:
        nonlocal calls
        calls += 1
        return calls

    assert await fn() == 1
    assert await fn() == 1
    assert calls == 1
    await c.close()


# -- engine: strict-mode serialization errors propagate ---------------------------------


async def test_engine_strict_mode_raises_on_corrupt() -> None:
    fake = FakeBackend()
    engine = Engine(fake, suppress=False)
    await fake.set("bad", b"\x99corrupt", ttl=60)
    with pytest.raises(SerializationError):
        await engine.get("bad")
    await engine.close()


async def test_engine_error_event_emitted_on_backend_failure() -> None:
    fake = FakeBackend()
    fake.fail_on.add("k")
    events = []
    engine = Engine(fake, middlewares=[lambda e: events.append(e.name)])
    await engine.get("k")
    assert "error" in events
    await engine.close()


# -- sync bridge: run_sync with kwargs ------------------------------------------------------


async def test_bridge_run_sync_kwargs() -> None:
    bridge = get_bridge()
    result = await bridge.run_sync(lambda a, b=0: a + b, 1, b=2)
    assert result == 3


# -- codec: compress disabled path and float precision ------------------------------------


def test_codec_float_precision() -> None:
    assert codec.loads(codec.dumps(3.141592653589793)) == 3.141592653589793


# -- keys: unbound root raises cleanly ------------------------------------------------------


async def test_key_builder_missing_attr_raises() -> None:
    async def fn(user_id: int) -> dict: ...

    kb = KeyBuilder(fn, "u:{user_id.profile.name}")
    with pytest.raises(Exception, match="Attribute"):
        kb.build((object(),), {})


# -- decorator: middleware added after setup ------------------------------------------------


async def test_middleware_registered_before_setup() -> None:
    c = Cache()
    events = []
    c.add_middleware(lambda e: events.append(e.name))
    c.setup("mem://")  # setup after middleware registration
    await c.set("k", "v", ttl="1h")
    assert "set" in events
    await c.close()
