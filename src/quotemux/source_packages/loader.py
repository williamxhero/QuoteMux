from __future__ import annotations

import json
import os
from importlib import resources
from importlib.util import find_spec
from pathlib import Path
import sys

from quotemux.source_packages.manifest import SourcePackageManifest


MANIFEST_FILE_NAME = "quotemux_package.json"
BUILTIN_PACKAGE_IDS = ("tushare", "efinance", "mootdx", "opentdx", "akshare")
BUILTIN_PACKAGE_MODULE = "markethub_packages"


def _default_package_project_root() -> Path:
    root_text = os.getenv("MARKETHUB_PACKAGES_ROOT", "")
    if root_text != "":
        return Path(root_text)
    return Path(__file__).resolve().parents[4] / "MarketHub_Packages"


def _ensure_builtin_packages_importable() -> None:
    if find_spec(BUILTIN_PACKAGE_MODULE) is not None:
        return
    package_root = _default_package_project_root()
    if not package_root.is_dir():
        return
    root_text = str(package_root)
    if root_text not in sys.path:
        sys.path.insert(0, root_text)


def _load_builtin_manifest(package_id: str) -> SourcePackageManifest:
    _ensure_builtin_packages_importable()
    package_name = f"{BUILTIN_PACKAGE_MODULE}.{package_id}"
    manifest_path = resources.files(package_name).joinpath(MANIFEST_FILE_NAME)
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    return SourcePackageManifest.from_dict(payload)


def load_builtin_manifests() -> tuple[SourcePackageManifest, ...]:
    return tuple(_load_builtin_manifest(package_id) for package_id in BUILTIN_PACKAGE_IDS)


def _iter_manifest_candidates(import_root: Path) -> list[Path]:
    if not import_root.exists():
        return []
    direct_file = import_root / MANIFEST_FILE_NAME
    if direct_file.is_file():
        return [direct_file]
    return sorted(import_root.glob(f"*/{MANIFEST_FILE_NAME}"))


def load_external_manifests(import_roots: tuple[str, ...]) -> tuple[SourcePackageManifest, ...]:
    manifests: list[SourcePackageManifest] = []
    for root_text in import_roots:
        root_path = Path(root_text)
        for manifest_path in _iter_manifest_candidates(root_path):
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifests.append(SourcePackageManifest.from_dict(payload, package_root=str(manifest_path.parent)))
    return tuple(manifests)
