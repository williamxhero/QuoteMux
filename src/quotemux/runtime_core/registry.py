from __future__ import annotations

from functools import lru_cache

from quotemux.config_runtime.runtime import get_config_runtime
from quotemux.source_packages.registry import get_default_source_package_registry
from quotemux.source_packages.instance_context import current_source_instance, use_source_instance
from quotemux.sources.base import SourceDefinition


class SourceRegistry:
    def __init__(self, definitions: tuple[SourceDefinition, ...]) -> None:
        self._definitions = {definition.name: definition for definition in definitions}

    def get_source(self, source_name: str) -> SourceDefinition:
        definition = self._definitions.get(source_name)
        if definition is None:
            raise KeyError(f"未知 source: {source_name}")
        return definition

    def get_handler(self, source_name: str, handler_name: str):
        return self.get_source(source_name).get_handler(handler_name)

    def has_handler(self, source_name: str, handler_name: str) -> bool:
        definition = self._definitions.get(source_name)
        if definition is None:
            return False
        return definition.has_handler(handler_name)

    def list_sources(self) -> tuple[str, ...]:
        return tuple(self._definitions.keys())


class SourceProxy:
    def __init__(self, source_name: str) -> None:
        self._source_name = source_name

    def __getattr__(self, handler_name: str):
        try:
            handler = get_default_source_registry().get_handler(self._source_name, handler_name)
        except KeyError as exc:
            raise AttributeError(str(exc)) from exc

        def _call_with_default_instance(*args: object, **kwargs: object):
            if current_source_instance() is not None:
                return handler(*args, **kwargs)
            source_instance = _default_source_instance(self._source_name)
            if source_instance is None:
                return handler(*args, **kwargs)
            with use_source_instance(source_instance):
                return handler(*args, **kwargs)

        return _call_with_default_instance


@lru_cache(maxsize=1)
def get_default_source_registry() -> SourceRegistry:
    package_registry = get_default_source_package_registry()
    definitions: list[SourceDefinition] = []
    for package_id in package_registry.list_package_ids():
        handlers = {
            handler_name: package_registry.get_handler(package_id, handler_name)
            for handler_name in package_registry.list_handler_names(package_id)
        }
        definitions.append(SourceDefinition(name=package_id, handlers=handlers))
    return SourceRegistry(tuple(definitions))


def _default_source_instance(source_name: str):
    snapshot = get_config_runtime().get_active_snapshot()
    for instance in snapshot.list_enabled_source_instances():
        if instance.package_id == source_name:
            return instance
    return None
