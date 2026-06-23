from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


CAPABILITY_TIMEOUT_DEFAULT_SECONDS = 30.0
PROVIDER_TIMEOUT_DEFAULT_SECONDS = 10.0
TIMEOUT_MIN_SECONDS = 3.0
TIMEOUT_MAX_SECONDS = 60.0
TIMEOUT_SAMPLE_WINDOW_SIZE = 200
TIMEOUT_MIN_SAMPLE_COUNT = 20
TIMEOUT_P95_MULTIPLIER = 1.5

TIMEOUT_STATUS_SUCCESS = "success"
TIMEOUT_STATUS_EMPTY = "empty"
TIMEOUT_STATUS_ERROR = "error"
TIMEOUT_STATUS_TIMEOUT = "timeout"


@dataclass(frozen=True)
class CapabilityTimeoutPolicy:
    capability_id: str
    default_timeout_seconds: float
    min_timeout_seconds: float
    max_timeout_seconds: float
    sample_window_size: int
    min_sample_count: int


@dataclass(frozen=True)
class ProviderTimeoutPolicy:
    capability_id: str
    provider: str
    default_timeout_seconds: float
    min_timeout_seconds: float
    max_timeout_seconds: float
    sample_window_size: int
    min_sample_count: int


@dataclass(frozen=True)
class EffectiveTimeout:
    timeout_seconds: float
    source: str
    sample_count: int


@dataclass(frozen=True)
class ProviderTimeoutMetric:
    capability_id: str
    provider: str
    source_instance_id: str
    handler: str
    status: str
    elapsed_ms: float
    effective_timeout_seconds: float
    row_count: int
    error_text: str
    created_at: datetime


@dataclass(frozen=True)
class CapabilityTimeoutMetric:
    capability_id: str
    status: str
    elapsed_ms: float
    effective_timeout_seconds: float
    provider_request_count: int
    row_count: int
    error_count: int
    created_at: datetime


def default_capability_timeout_policy(capability_id: str) -> CapabilityTimeoutPolicy:
    return CapabilityTimeoutPolicy(
        capability_id=capability_id,
        default_timeout_seconds=CAPABILITY_TIMEOUT_DEFAULT_SECONDS,
        min_timeout_seconds=TIMEOUT_MIN_SECONDS,
        max_timeout_seconds=TIMEOUT_MAX_SECONDS,
        sample_window_size=TIMEOUT_SAMPLE_WINDOW_SIZE,
        min_sample_count=TIMEOUT_MIN_SAMPLE_COUNT,
    )


def default_provider_timeout_policy(capability_id: str, provider: str) -> ProviderTimeoutPolicy:
    return ProviderTimeoutPolicy(
        capability_id=capability_id,
        provider=provider,
        default_timeout_seconds=PROVIDER_TIMEOUT_DEFAULT_SECONDS,
        min_timeout_seconds=TIMEOUT_MIN_SECONDS,
        max_timeout_seconds=TIMEOUT_MAX_SECONDS,
        sample_window_size=TIMEOUT_SAMPLE_WINDOW_SIZE,
        min_sample_count=TIMEOUT_MIN_SAMPLE_COUNT,
    )
