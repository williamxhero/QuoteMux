from __future__ import annotations

from quotemux.provider_timeout.policy import EffectiveTimeout


def resolve_provider_timeout(capability_id: str, provider: str, source_instance_timeout_seconds: int | None) -> EffectiveTimeout:
    if source_instance_timeout_seconds is not None and source_instance_timeout_seconds > 0:
        return EffectiveTimeout(float(source_instance_timeout_seconds), "source_instance", 0)
    store = _get_timeout_store()
    policy = store.get_provider_policy(capability_id, provider)
    samples = store.list_provider_success_elapsed_ms(capability_id, provider, policy.sample_window_size)
    if len(samples) < policy.min_sample_count:
        return EffectiveTimeout(policy.default_timeout_seconds, "default", len(samples))
    timeout_seconds = _clamp(
        _percentile(samples, 0.95) / 1000.0 * 1.5,
        policy.min_timeout_seconds,
        policy.max_timeout_seconds,
    )
    return EffectiveTimeout(timeout_seconds, "adaptive", len(samples))


def resolve_capability_timeout(capability_id: str) -> EffectiveTimeout:
    store = _get_timeout_store()
    policy = store.get_capability_policy(capability_id)
    samples = store.list_capability_success_elapsed_ms(capability_id, policy.sample_window_size)
    if len(samples) < policy.min_sample_count:
        return EffectiveTimeout(policy.default_timeout_seconds, "default", len(samples))
    timeout_seconds = _clamp(
        _percentile(samples, 0.95) / 1000.0 * 1.5,
        policy.min_timeout_seconds,
        policy.max_timeout_seconds,
    )
    return EffectiveTimeout(timeout_seconds, "adaptive", len(samples))


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return min(max(value, minimum), maximum)


def _percentile(values: tuple[float, ...], percentile: float) -> float:
    ordered = sorted(values)
    if ordered == []:
        return 0.0
    if len(ordered) == 1:
        return ordered[0]
    index = (len(ordered) - 1) * percentile
    lower_index = int(index)
    upper_index = min(lower_index + 1, len(ordered) - 1)
    weight = index - lower_index
    return ordered[lower_index] * (1.0 - weight) + ordered[upper_index] * weight


def _get_timeout_store():
    from quotemux.store.timeout_policy import get_timeout_store

    return get_timeout_store()
