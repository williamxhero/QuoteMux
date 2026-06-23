from __future__ import annotations

from quotemux.provider_timeout.policy import CapabilityTimeoutMetric, ProviderTimeoutMetric


def record_provider_timeout_metric(metric: ProviderTimeoutMetric) -> None:
    _get_timeout_store().write_provider_metric(metric)


def record_capability_timeout_metric(metric: CapabilityTimeoutMetric) -> None:
    _get_timeout_store().write_capability_metric(metric)


def _get_timeout_store():
    from quotemux.store.timeout_policy import get_timeout_store

    return get_timeout_store()
