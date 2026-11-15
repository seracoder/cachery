"""Static key-template validation tests — the headline DX feature (plan §5.3, §8)."""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Annotated

import pytest

from cachery import KeyIgnore, KeyPart, cache
from cachery.core.exceptions import CacheryError, InvalidKeyTemplateError
from cachery.core.keys import KeyBuilder, register_context_var


def test_typo_raises_at_decoration_time() -> None:
    with pytest.raises(InvalidKeyTemplateError, match="did you mean 'user_id'"):

        @cache(ttl="1h", key="user:{used_id}")
        async def get_user(user_id: int) -> dict: ...


def test_error_lists_available_parameters() -> None:
    with pytest.raises(InvalidKeyTemplateError, match="Available parameters: user_id, year"):

        @cache(ttl="1h", key="r:{yer}")
        async def report(user_id: int, year: int) -> dict: ...


def test_valid_template_accepted() -> None:
    @cache(ttl="1h", key="user:{user_id}")
    async def get_user(user_id: int) -> dict: ...

    assert callable(get_user)


def test_dotted_path_template_accepted_and_built() -> None:
    class Request:
        user = type("User", (), {"uid": 42})

    @cache(ttl="1h", key="user:{request.user.uid}")
    async def handler(request: object) -> dict: ...

    kb = KeyBuilder(handler.__wrapped__, "user:{request.user.uid}")
    key = kb.build((Request(),), {})
    assert key.endswith("user:42")


def test_kwargs_placeholder_rejected_without_var_keyword() -> None:
    with pytest.raises(InvalidKeyTemplateError):

        @cache(ttl="1h", key="k:{kwargs.x}")
        async def f(a: int) -> dict: ...


def test_kwargs_placeholder_allowed_with_var_keyword() -> None:
    @cache(ttl="1h", key="k:{kwargs.x}")
    async def f(**kwargs: object) -> dict: ...

    assert callable(f)


def test_empty_placeholder_rejected() -> None:
    with pytest.raises(InvalidKeyTemplateError):

        @cache(ttl="1h", key="k:{}")
        async def f(a: int) -> dict: ...


def test_context_var_placeholder() -> None:
    register_context_var("tenant")
    from contextvars import copy_context

    @cache(ttl="1h", key="t:{@tenant}:u:{user_id}")
    async def get(user_id: int) -> dict: ...

    kb = KeyBuilder(get.__wrapped__, "t:{@tenant}:u:{user_id}")
    var = register_context_var("tenant")
    ctx = copy_context()
    ctx.run(var.set, "acme")
    key = ctx.run(kb.build, (), {"user_id": 1})
    assert "acme" in key


def test_unregistered_context_var_raises_at_build() -> None:
    @cache(ttl="1h", key="k:{@nope}")
    async def f(a: int) -> dict: ...

    kb = KeyBuilder(f.__wrapped__, "k:{@nope}")
    with pytest.raises(CacheryError, match="register_context_var"):
        kb.build((), {"a": 1})


def test_auto_key_uses_declaration_order_and_defaults() -> None:
    async def fn(a: int, b: str = "x") -> dict: ...

    kb = KeyBuilder(fn, None)
    key1 = kb.build((1,), {})
    assert ":a:1:b:x" in key1
    # same args, different path to binding → same key
    assert kb.build((), {"a": 1, "b": "x"}) == key1


def test_auto_key_hashes_long_values_for_stability() -> None:
    async def fn(payload: str) -> dict: ...

    kb = KeyBuilder(fn, None)
    key = kb.build(("x" * 500,), {})
    assert "x" * 100 not in key  # long value digested, not embedded verbatim


def test_auto_key_dataclass_repr_is_deterministic() -> None:
    @dataclass(frozen=True)
    class User:
        uid: int
        name: str

    async def fn(user: User) -> dict: ...

    kb = KeyBuilder(fn, None)
    assert kb.build((User(1, "a"),), {}) == kb.build((User(1, "a"),), {})


def test_annotated_key_ignore_excludes_param() -> None:
    async def fn(user_id: int, request: Annotated[object, KeyIgnore]) -> dict: ...

    kb = KeyBuilder(fn, None)
    key = kb.build((1, object()), {})
    assert "user_id:1" in key
    assert "request" not in key


def test_annotated_key_part_renames_and_formats() -> None:
    async def fn(locale: Annotated[str, KeyPart(name="lang", fmt="lower")] = "en") -> dict: ...

    kb = KeyBuilder(fn, None)
    key = kb.build((), {})
    assert ":lang:en" in key


def test_var_args_and_kwargs_in_auto_key_sorted() -> None:
    async def fn(a: int, *args: object, **kwargs: object) -> dict: ...

    kb = KeyBuilder(fn, None)
    k1 = kb.build((1, 2, 3), {"z": 1, "y": 2})
    k2 = kb.build((1, 2, 3), {"y": 2, "z": 1})
    assert k1 == k2
    assert "args.0:2" in k1 and "kwargs.y:2" in k1


def test_decorated_function_preserves_signature() -> None:
    async def original(user_id: int, *, locale: str = "en") -> dict: ...

    decorated = cache(ttl="1h")(original)
    assert inspect.signature(decorated) == inspect.signature(original)


def test_format_specs() -> None:
    async def fn(user_id: int) -> dict: ...

    kb = KeyBuilder(fn, "u:{user_id:hash(sha1)}")
    key = kb.build((1,), {})
    assert key.endswith(f"u:{__import__('hashlib').sha1(b'1').hexdigest()}")


def test_bad_format_spec_raises_at_build() -> None:
    async def fn(user_id: int) -> dict: ...

    kb = KeyBuilder(fn, "u:{user_id:nope}")
    with pytest.raises(CacheryError, match="format spec"):
        kb.build((1,), {})
