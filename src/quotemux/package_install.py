from __future__ import annotations

from dataclasses import dataclass
from importlib import metadata
from pathlib import Path
import hashlib
import shutil
import subprocess
import sys

from quotemux.source_packages.registry import clear_loaded_source_package_modules, refresh_default_source_package_registry
from quotemux.source_packages.environment import ensure_package_environment, package_uses_isolated_environment
from quotemux.source_packages.manifest import SourcePackageManifest


import os

PACKAGE_REPO_SPEC = os.getenv("QUOTEMUX_PACKAGE_REPO_SPEC", "git+https://github.com/williamxhero/QuoteMux_Packages.git@main")
PACKAGE_DISTRIBUTION_NAME = "quotemux-packages"
MANIFEST_FILE_NAME = "quotemux_package.json"


@dataclass(frozen=True)
class PackageInstallResult:
    installed_package_ids: tuple[str, ...]
    visible_package_ids: tuple[str, ...]
    package_count: int


def install_all_packages() -> PackageInstallResult:
    from quotemux.config_runtime.runtime import get_config_runtime

    python_executable = sys.executable
    _install_distribution(python_executable)
    clear_loaded_source_package_modules()
    refresh_default_source_package_registry()
    runtime = get_config_runtime()
    packages = runtime.refresh_source_packages()
    _ensure_isolated_package_environments(packages)
    package_ids = tuple(manifest.package_id for manifest in packages)
    return PackageInstallResult(
        installed_package_ids=package_ids,
        visible_package_ids=package_ids,
        package_count=len(package_ids),
    )


def _ensure_isolated_package_environments(packages: tuple[SourcePackageManifest, ...]) -> None:
    for manifest in packages:
        if package_uses_isolated_environment(manifest):
            ensure_package_environment(manifest)


def install_distribution_for_python(python_executable: str) -> None:
    _install_distribution(python_executable)


def installed_packages_fingerprint() -> str:
    try:
        distribution = metadata.distribution(PACKAGE_DISTRIBUTION_NAME)
    except metadata.PackageNotFoundError:
        return ""
    digest = hashlib.sha256()
    base_path = Path(str(distribution.locate_file(""))).resolve()
    files = distribution.files or ()
    for file_entry in sorted(files, key=lambda item: str(item)):
        file_path = Path(distribution.locate_file(file_entry)).resolve()
        if not file_path.is_file():
            continue
        if MANIFEST_FILE_NAME not in file_path.parts and file_path.suffix not in {".py", ".txt"}:
            continue
        relative_path = file_path.relative_to(base_path)
        digest.update(str(relative_path).encode("utf-8"))
        digest.update(file_path.read_bytes())
    return digest.hexdigest()


def _install_distribution(python_executable: str) -> None:
    target = _package_install_target()
    _clean_local_package_build_artifacts(target)
    subprocess.run([python_executable, "-m", "pip", "install", "--upgrade", "--force-reinstall", "--no-cache-dir", target], check=True)


def _package_install_target() -> str:
    target = os.getenv("QUOTEMUX_PACKAGE_REPO_SPEC", PACKAGE_REPO_SPEC)
    if Path(target).expanduser().is_dir() and os.getenv("QUOTEMUX_ALLOW_LOCAL_PACKAGE_REPO", "").lower() != "true":
        raise RuntimeError("QuoteMux_Packages 必须在线安装，不能使用本地目录")
    return target


def _clean_local_package_build_artifacts(target: str) -> None:
    project_root = Path(target)
    if not project_root.is_dir():
        return
    for child in project_root.iterdir():
        if child.name == "build" or child.name.endswith(".egg-info"):
            shutil.rmtree(child, ignore_errors=True)
