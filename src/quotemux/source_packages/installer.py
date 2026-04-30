from __future__ import annotations

from datetime import datetime
from pathlib import Path
from shutil import copy2, copytree, ignore_patterns


MANIFEST_FILE_NAME = "quotemux_package.json"


def install_source_package_directory(source_path: Path, install_root: Path) -> str:
    source_root = _resolve_source_root(source_path)
    installed_root = install_root / datetime.now().strftime("install-%Y%m%d%H%M%S%f")
    installed_root.mkdir(parents=True, exist_ok=False)
    if _has_direct_manifest(source_root):
        _copy_directory_contents(source_root, installed_root)
    elif _is_module_package_root(source_root):
        copytree(source_root, installed_root / _module_directory_name(source_root), ignore=_copy_ignore())
    else:
        _copy_directory_contents(source_root, installed_root)
    return str(installed_root)


def _resolve_source_root(source_path: Path) -> Path:
    if _has_manifest_candidates(source_path):
        return source_path
    nested_root = source_path / "packages"
    if _has_manifest_candidates(nested_root):
        return nested_root
    raise ValueError(f"未找到 source package manifest: {source_path}")


def _has_manifest_candidates(source_root: Path) -> bool:
    if not source_root.is_dir():
        return False
    if _has_direct_manifest(source_root):
        return True
    return any(path.is_file() for path in source_root.glob(f"*/{MANIFEST_FILE_NAME}"))


def _has_direct_manifest(source_root: Path) -> bool:
    return (source_root / MANIFEST_FILE_NAME).is_file()


def _is_module_package_root(source_root: Path) -> bool:
    return (source_root / "__init__.py").is_file()


def _module_directory_name(source_root: Path) -> str:
    if source_root.name == "packages":
        return "quotemux_packages"
    return source_root.name


def _copy_directory_contents(source_root: Path, target_root: Path) -> None:
    for source_item in source_root.iterdir():
        target_item = target_root / source_item.name
        if source_item.is_dir():
            copytree(source_item, target_item, ignore=_copy_ignore())
        else:
            copy2(source_item, target_item)


def _copy_ignore():
    return ignore_patterns("__pycache__", "*.pyc", ".git", "build", "dist", "*.egg-info")
