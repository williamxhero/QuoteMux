from __future__ import annotations

import json
from importlib import resources
from importlib.util import find_spec
from pathlib import Path

from quotemux.source_packages.manifest import SourcePackageManifest


MANIFEST_FILE_NAME = "quotemux_package.json"
BUILTIN_PACKAGE_IDS = ("tushare", "efinance", "mootdx", "opentdx", "akshare", "derived_core")
BUILTIN_PACKAGE_MODULE = "quotemux_packages"


def _load_builtin_manifest(package_id: str) -> SourcePackageManifest:
    package_name = f"{BUILTIN_PACKAGE_MODULE}.{package_id}"
    package_files = resources.files(package_name)
    manifest_path = package_files.joinpath(MANIFEST_FILE_NAME)
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    package_root = _resource_package_root(package_files)
    return SourcePackageManifest.from_dict(payload, package_root=package_root)


def _resource_package_root(package_files) -> str:
    package_path = Path(str(package_files))
    if package_path.is_dir():
        return str(package_path)
    return ""


def load_builtin_manifests() -> tuple[SourcePackageManifest, ...]:
    if find_spec(BUILTIN_PACKAGE_MODULE) is None:
        return ()
    return tuple(_load_builtin_manifest(package_id) for package_id in BUILTIN_PACKAGE_IDS)


def _iter_manifest_candidates(import_root: Path) -> list[Path]:
    if not import_root.exists():
        return []
    candidates = []
    direct_file = import_root / MANIFEST_FILE_NAME
    if direct_file.is_file():
        candidates.append(direct_file)
    candidates.extend(path for path in import_root.glob(f"*/{MANIFEST_FILE_NAME}") if path.is_file())
    candidates.extend(path for path in import_root.glob(f"*/*/{MANIFEST_FILE_NAME}") if path.is_file())
    return sorted(set(candidates))


def load_external_manifests(import_roots: tuple[str, ...]) -> tuple[SourcePackageManifest, ...]:
    manifests: list[SourcePackageManifest] = []
    for root_text in import_roots:
        root_path = Path(root_text)
        for manifest_path in _iter_manifest_candidates(root_path):
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifests.append(SourcePackageManifest.from_dict(payload, package_root=str(manifest_path.parent)))
    return tuple(manifests)
