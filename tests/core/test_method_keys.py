"""Method auto-key tests: instance isolation for decorated methods."""

from __future__ import annotations

import dataclasses

from cachery.core.keys import KeyBuilder


@dataclasses.dataclass(frozen=True)
class TenantService:
    tenant: str

    async def get(self, user_id: int) -> dict:
        return {"tenant": self.tenant, "user": user_id}


class PlainService:
    def __init__(self, tenant: str) -> None:
        self.tenant = tenant

    async def get(self, user_id: int) -> dict:
        return {"tenant": self.tenant, "user": user_id}


def test_dataclass_instances_get_distinct_stable_keys() -> None:
    kb = KeyBuilder(TenantService.get, None)
    k_a = kb.build((TenantService("acme"), 1), {})
    k_b = kb.build((TenantService("globex"), 1), {})
    assert k_a != k_b
    assert "acme" in k_a and "globex" in k_b
    # value-based → stable across "restarts" (same value, new builder)
    kb2 = KeyBuilder(TenantService.get, None)
    assert kb2.build((TenantService("acme"), 1), {}) == k_a


def test_plain_instances_get_distinct_keys() -> None:
    svc_a = PlainService("a")
    svc_b = PlainService("b")  # both alive simultaneously (realistic case)
    kb = KeyBuilder(PlainService.get, None)
    assert kb.build((svc_a, 1), {}) != kb.build((svc_b, 1), {})


def test_explicit_self_template_still_works() -> None:
    kb = KeyBuilder(PlainService.get, "svc:{self.tenant}:u:{user_id}")
    assert kb.build((PlainService("acme"), 7), {}).endswith("svc:acme:u:7")
