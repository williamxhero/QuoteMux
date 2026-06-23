from __future__ import annotations

from dataclasses import dataclass

from quotemux.capabilities import is_known_capability_id
from quotemux.provider_timeout.adaptive import resolve_capability_timeout, resolve_provider_timeout
from quotemux.provider_timeout.policy import CapabilityTimeoutPolicy, ProviderTimeoutPolicy
from quotemux.store.timeout_policy import get_timeout_store, sync_default_timeout_policies


@dataclass(frozen=True)
class CapabilityTimeoutPolicyUpdate:
    capability_id: str
    default_timeout_seconds: float
    min_timeout_seconds: float
    max_timeout_seconds: float
    sample_window_size: int
    min_sample_count: int


@dataclass(frozen=True)
class ProviderTimeoutPolicyUpdate:
    capability_id: str
    provider: str
    default_timeout_seconds: float
    min_timeout_seconds: float
    max_timeout_seconds: float
    sample_window_size: int
    min_sample_count: int


class QuoteMuxTimeoutAdmin:
    def sync_defaults(self) -> bool:
        return sync_default_timeout_policies()

    def list_capability_policies(self) -> tuple[dict[str, object], ...]:
        return tuple(_capability_policy_to_dict(policy) for policy in get_timeout_store().list_capability_policies())

    def list_provider_policies(self) -> tuple[dict[str, object], ...]:
        return tuple(_provider_policy_to_dict(policy) for policy in get_timeout_store().list_provider_policies())

    def update_capability_policy(self, update: CapabilityTimeoutPolicyUpdate) -> dict[str, object]:
        if not is_known_capability_id(update.capability_id):
            raise KeyError(f"未知 capability: {update.capability_id}")
        policy = CapabilityTimeoutPolicy(
            capability_id=update.capability_id,
            default_timeout_seconds=update.default_timeout_seconds,
            min_timeout_seconds=update.min_timeout_seconds,
            max_timeout_seconds=update.max_timeout_seconds,
            sample_window_size=update.sample_window_size,
            min_sample_count=update.min_sample_count,
        )
        if not get_timeout_store().update_capability_policy(policy):
            raise RuntimeError(f"timeout 策略更新失败: {update.capability_id}")
        return _capability_policy_to_dict(policy)

    def update_provider_policy(self, update: ProviderTimeoutPolicyUpdate) -> dict[str, object]:
        if not is_known_capability_id(update.capability_id):
            raise KeyError(f"未知 capability: {update.capability_id}")
        policy = ProviderTimeoutPolicy(
            capability_id=update.capability_id,
            provider=update.provider,
            default_timeout_seconds=update.default_timeout_seconds,
            min_timeout_seconds=update.min_timeout_seconds,
            max_timeout_seconds=update.max_timeout_seconds,
            sample_window_size=update.sample_window_size,
            min_sample_count=update.min_sample_count,
        )
        if not get_timeout_store().update_provider_policy(policy):
            raise RuntimeError(f"provider timeout 策略更新失败: {update.capability_id} {update.provider}")
        return _provider_policy_to_dict(policy)

    def list_effective_provider_timeouts(self) -> tuple[dict[str, object], ...]:
        rows: list[dict[str, object]] = []
        for policy in get_timeout_store().list_provider_policies():
            resolved = resolve_provider_timeout(policy.capability_id, policy.provider, None)
            rows.append(
                {
                    **_provider_policy_to_dict(policy),
                    "effective_timeout_seconds": resolved.timeout_seconds,
                    "effective_source": resolved.source,
                    "sample_count": resolved.sample_count,
                }
            )
        return tuple(rows)

    def list_effective_capability_timeouts(self) -> tuple[dict[str, object], ...]:
        rows: list[dict[str, object]] = []
        for policy in get_timeout_store().list_capability_policies():
            resolved = resolve_capability_timeout(policy.capability_id)
            rows.append(
                {
                    **_capability_policy_to_dict(policy),
                    "effective_timeout_seconds": resolved.timeout_seconds,
                    "effective_source": resolved.source,
                    "sample_count": resolved.sample_count,
                }
            )
        return tuple(rows)

    def list_provider_metrics(self, capability_id: str = "", provider: str = "", limit: int = 100) -> tuple[dict[str, object], ...]:
        return get_timeout_store().list_provider_metrics(capability_id, provider, limit)

    def list_capability_metrics(self, capability_id: str = "", limit: int = 100) -> tuple[dict[str, object], ...]:
        return get_timeout_store().list_capability_metrics(capability_id, limit)


def _capability_policy_to_dict(policy: CapabilityTimeoutPolicy) -> dict[str, object]:
    return {
        "capability_id": policy.capability_id,
        "default_timeout_seconds": policy.default_timeout_seconds,
        "min_timeout_seconds": policy.min_timeout_seconds,
        "max_timeout_seconds": policy.max_timeout_seconds,
        "sample_window_size": policy.sample_window_size,
        "min_sample_count": policy.min_sample_count,
    }


def _provider_policy_to_dict(policy: ProviderTimeoutPolicy) -> dict[str, object]:
    return {
        "capability_id": policy.capability_id,
        "provider": policy.provider,
        "default_timeout_seconds": policy.default_timeout_seconds,
        "min_timeout_seconds": policy.min_timeout_seconds,
        "max_timeout_seconds": policy.max_timeout_seconds,
        "sample_window_size": policy.sample_window_size,
        "min_sample_count": policy.min_sample_count,
    }
