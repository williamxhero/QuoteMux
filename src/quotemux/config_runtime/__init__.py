from __future__ import annotations

from quotemux.config_runtime.models import ContractPolicyOverride, RuntimeProfile, RuntimeSnapshot, SourceInstanceConfig

__all__ = [
    "ContractPolicyOverride",
    "RuntimeProfile",
    "RuntimeSnapshot",
    "SourceInstanceConfig",
    "QuoteMuxConfigRuntime",
    "get_config_runtime",
    "reset_config_runtime_cache",
]


def __getattr__(name: str):
    if name in {"QuoteMuxConfigRuntime", "get_config_runtime", "reset_config_runtime_cache"}:
        from quotemux.config_runtime.runtime import QuoteMuxConfigRuntime, get_config_runtime, reset_config_runtime_cache

        mapping = {
            "QuoteMuxConfigRuntime": QuoteMuxConfigRuntime,
            "get_config_runtime": get_config_runtime,
            "reset_config_runtime_cache": reset_config_runtime_cache,
        }
        return mapping[name]
    raise AttributeError(name)
