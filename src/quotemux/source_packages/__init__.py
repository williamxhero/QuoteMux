from __future__ import annotations

from quotemux.source_packages.manifest import ConfigFieldSchema, SourcePackageManifest

__all__ = [
    "ConfigFieldSchema",
    "SourcePackageManifest",
    "SourcePackageRegistry",
    "get_default_source_package_registry",
    "refresh_default_source_package_registry",
]


def __getattr__(name: str):
    if name in {"SourcePackageRegistry", "get_default_source_package_registry", "refresh_default_source_package_registry"}:
        from quotemux.source_packages.registry import SourcePackageRegistry, get_default_source_package_registry, refresh_default_source_package_registry

        mapping = {
            "SourcePackageRegistry": SourcePackageRegistry,
            "get_default_source_package_registry": get_default_source_package_registry,
            "refresh_default_source_package_registry": refresh_default_source_package_registry,
        }
        return mapping[name]
    raise AttributeError(name)
