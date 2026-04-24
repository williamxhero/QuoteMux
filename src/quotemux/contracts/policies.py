from __future__ import annotations

from dataclasses import dataclass

from quotemux.capabilities import get_capability_definition, list_capability_definitions, normalize_capability_id
from quotemux.config_runtime.models import ContractPolicyOverride
from quotemux.contracts.strategies import normalize_merge_strategy


FALLBACK_MODE_AUTO = "auto"
FALLBACK_MODE_DEGRADED = "degraded"
FALLBACK_MODE_A2_ONLY = "a2_only"


@dataclass(frozen=True)
class ContractPolicy:
    name: str
    mode: str
    source_order: tuple[str, ...]
    stage_namespace: tuple[str, ...]
    merge_strategy: str


def _build_policy(capability_id: str) -> ContractPolicy:
    definition = get_capability_definition(capability_id)
    return ContractPolicy(
        name=definition.capability_id,
        mode=definition.policy_mode,
        source_order=definition.default_source_order,
        stage_namespace=tuple(definition.capability_id.split(".")),
        merge_strategy=definition.default_merge_strategy,
    )


CONTRACT_POLICIES = {
    definition.capability_id: _build_policy(definition.capability_id)
    for definition in list_capability_definitions()
}

AUTO_FALLBACK_CONTRACTS = {name for name, policy in CONTRACT_POLICIES.items() if policy.mode == FALLBACK_MODE_AUTO}
DEGRADED_FALLBACK_CONTRACTS = {name for name, policy in CONTRACT_POLICIES.items() if policy.mode == FALLBACK_MODE_DEGRADED}
A2_ONLY_CONTRACTS = {name for name, policy in CONTRACT_POLICIES.items() if policy.mode == FALLBACK_MODE_A2_ONLY}


def get_contract_policy(contract_name: str) -> ContractPolicy:
    normalized = normalize_capability_id(contract_name)
    policy = CONTRACT_POLICIES.get(normalized)
    if policy is None:
        raise KeyError(f"未知 capability: {contract_name}")
    return policy


def list_contract_policies() -> tuple[ContractPolicy, ...]:
    return tuple(CONTRACT_POLICIES.values())


def list_default_contract_policies() -> tuple[ContractPolicyOverride, ...]:
    return tuple(
        ContractPolicyOverride(
            contract_name=policy.name,
            mode=policy.mode,
            source_order=policy.source_order,
            merge_strategy=normalize_merge_strategy(policy.merge_strategy),
        )
        for policy in CONTRACT_POLICIES.values()
    )


def is_auto_fallback_contract(contract_name: str) -> bool:
    return get_contract_policy(contract_name).mode == FALLBACK_MODE_AUTO


def is_degraded_fallback_contract(contract_name: str) -> bool:
    return get_contract_policy(contract_name).mode == FALLBACK_MODE_DEGRADED


def is_a2_only_contract(contract_name: str) -> bool:
    return get_contract_policy(contract_name).mode == FALLBACK_MODE_A2_ONLY
