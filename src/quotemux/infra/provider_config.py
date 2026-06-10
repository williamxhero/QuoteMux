from __future__ import annotations

import json
import os

from quotemux.config_runtime.models import SourceInstanceConfig
from quotemux.source_packages.instance_context import current_config_value, current_secret_value, current_source_instance


def get_provider_config_value(name: str) -> str:
    value = current_config_value(name)
    if value != "":
        return value
    source_instance = _env_source_instance()
    if source_instance is None:
        return ""
    return source_instance.config_values.get(name, "")


def get_provider_secret_value(name: str) -> str:
    value = current_secret_value(name)
    if value != "":
        return value
    source_instance = _env_source_instance()
    if source_instance is None:
        return ""
    return source_instance.secret_values.get(name, "")


def get_provider_api_key() -> str:
    return get_provider_secret_value("api_key")


def get_current_source_instance_id() -> str:
    instance = current_source_instance()
    if instance is not None:
        return instance.instance_id
    source_instance = _env_source_instance()
    if source_instance is None:
        return ""
    return source_instance.instance_id


def _env_source_instance() -> SourceInstanceConfig | None:
    text = os.getenv("QUOTEMUX_SOURCE_INSTANCE", "")
    if text == "":
        return None
    try:
        payload = json.loads(text)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    return SourceInstanceConfig.from_dict(payload)
