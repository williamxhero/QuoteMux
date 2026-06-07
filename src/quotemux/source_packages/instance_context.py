from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar

from quotemux.config_runtime.models import SourceInstanceConfig


_CURRENT_SOURCE_INSTANCE: ContextVar[SourceInstanceConfig | None] = ContextVar("quotemux_current_source_instance", default=None)


@contextmanager
def use_source_instance(instance: SourceInstanceConfig):
    token = _CURRENT_SOURCE_INSTANCE.set(instance)
    try:
        yield
    finally:
        _CURRENT_SOURCE_INSTANCE.reset(token)


def current_source_instance() -> SourceInstanceConfig | None:
    return _CURRENT_SOURCE_INSTANCE.get()


def current_config_value(name: str) -> str:
    instance = current_source_instance()
    if instance is None:
        return ""
    return instance.config_values.get(name, "")


def current_secret_value(name: str) -> str:
    instance = current_source_instance()
    if instance is None:
        return ""
    return instance.secret_values.get(name, "")
