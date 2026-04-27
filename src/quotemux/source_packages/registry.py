from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from importlib import import_module
import sys

from quotemux.config_runtime.store import read_import_roots
from quotemux.source_packages.loader import load_builtin_manifests, load_external_manifests
from quotemux.source_packages.manifest import SourcePackageManifest


@dataclass(frozen=True)
class SourcePackageHealth:
    package_id: str
    version: str
    origin: str
    status: str
    handler_count: int
    missing_handlers: tuple[str, ...]
    capability_ids: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "package_id": self.package_id,
            "version": self.version,
            "origin": self.origin,
            "status": self.status,
            "handler_count": self.handler_count,
            "missing_handlers": list(self.missing_handlers),
            "capability_ids": list(self.capability_ids),
        }


class SourcePackageRegistry:
    def __init__(self, manifests: tuple[SourcePackageManifest, ...]) -> None:
        self._manifests = {manifest.package_id: manifest for manifest in manifests}
        self._handlers: dict[str, dict[str, object]] = {}
        for manifest in manifests:
            handlers: dict[str, object] = {}
            for handler_name in manifest.list_handler_names():
                try:
                    handlers[handler_name] = _load_handler(manifest.get_handler_target(handler_name))
                except (AttributeError, ImportError):
                    continue
            self._handlers[manifest.package_id] = handlers

    def list_packages(self) -> tuple[SourcePackageManifest, ...]:
        return tuple(self._manifests.values())

    def list_package_ids(self) -> tuple[str, ...]:
        return tuple(self._manifests.keys())

    def get_manifest(self, package_id: str) -> SourcePackageManifest:
        manifest = self._manifests.get(package_id)
        if manifest is None:
            raise KeyError(f"未知 source package: {package_id}")
        return manifest

    def get_handler(self, package_id: str, handler_name: str):
        manifest_handlers = self._handlers.get(package_id)
        if manifest_handlers is None:
            raise KeyError(f"未知 source package: {package_id}")
        handler = manifest_handlers.get(handler_name)
        if handler is None:
            raise KeyError(f"source package {package_id} 未注册 handler: {handler_name}")
        return handler

    def has_handler(self, package_id: str, handler_name: str) -> bool:
        manifest_handlers = self._handlers.get(package_id)
        if manifest_handlers is None:
            return False
        return handler_name in manifest_handlers

    def list_handler_names(self, package_id: str) -> tuple[str, ...]:
        manifest_handlers = self._handlers.get(package_id)
        if manifest_handlers is None:
            return ()
        return tuple(manifest_handlers.keys())

    def check_package_health(self, package_id: str) -> SourcePackageHealth:
        manifest = self.get_manifest(package_id)
        loaded_handler_names = set(self.list_handler_names(package_id))
        missing_handlers = tuple(handler_name for handler_name in manifest.list_handler_names() if handler_name not in loaded_handler_names)
        status = "ok" if missing_handlers == () else "error"
        return SourcePackageHealth(
            package_id=manifest.package_id,
            version=manifest.version,
            origin=manifest.origin,
            status=status,
            handler_count=len(loaded_handler_names),
            missing_handlers=missing_handlers,
            capability_ids=manifest.contract_names,
        )

    def list_package_health(self) -> tuple[SourcePackageHealth, ...]:
        return tuple(self.check_package_health(package_id) for package_id in self.list_package_ids())


def _load_handler(target: str):
    module_name, _, attr_name = target.partition(":")
    if module_name == "" or attr_name == "":
        raise ValueError(f"非法 handler 目标: {target}")
    module = import_module(module_name)
    return getattr(module, attr_name)


def _activate_import_roots(import_roots: tuple[str, ...]) -> None:
    for root_text in import_roots:
        if root_text != "" and root_text not in sys.path:
            sys.path.insert(0, root_text)


def build_source_package_registry(import_roots: tuple[str, ...]) -> SourcePackageRegistry:
    _activate_import_roots(import_roots)
    manifests = [*load_builtin_manifests(), *load_external_manifests(import_roots)]
    from quotemux.config_runtime.validation import validate_manifests

    validate_manifests(tuple(manifests))
    return SourcePackageRegistry(tuple(manifests))


@lru_cache(maxsize=1)
def get_default_source_package_registry() -> SourcePackageRegistry:
    return build_source_package_registry(read_import_roots())


def refresh_default_source_package_registry() -> SourcePackageRegistry:
    get_default_source_package_registry.cache_clear()
    return get_default_source_package_registry()
