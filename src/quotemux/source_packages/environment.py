from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys

from quotemux.source_packages.manifest import SourcePackageManifest


REQUIREMENTS_FILE_NAME = "requirements.txt"


@dataclass(frozen=True)
class PackageEnvironment:
    package_id: str
    python_executable: str
    requirements_path: str


def package_requirements_path(manifest: SourcePackageManifest) -> Path | None:
    if manifest.package_root == "":
        return None
    path = Path(manifest.package_root) / REQUIREMENTS_FILE_NAME
    if not path.is_file():
        return None
    return path


def package_uses_isolated_environment(manifest: SourcePackageManifest) -> bool:
    return package_requirements_path(manifest) is not None


def ensure_package_environment(manifest: SourcePackageManifest) -> PackageEnvironment:
    requirements_path = package_requirements_path(manifest)
    if requirements_path is None:
        raise ValueError(f"package {manifest.package_id} 未声明 requirements.txt")
    venv_path = _venv_root() / _environment_directory_name(manifest, requirements_path)
    python_executable = _venv_python_executable(venv_path)
    marker_path = venv_path / ".quotemux-installed.json"
    requirements_hash = _requirements_hash(requirements_path)
    runtime_hash = _runtime_requirements_hash()
    if not _environment_is_ready(marker_path, requirements_hash, runtime_hash, python_executable):
        _create_venv(venv_path)
        _install_runtime_requirements(python_executable)
        _install_requirements(python_executable, requirements_path)
        _write_marker(marker_path, manifest, requirements_hash, runtime_hash)
    return PackageEnvironment(
        package_id=manifest.package_id,
        python_executable=str(python_executable),
        requirements_path=str(requirements_path),
    )


def _venv_root() -> Path:
    root_text = os.getenv("QUOTEMUX_PACKAGE_VENV_ROOT", "")
    if root_text != "":
        return Path(root_text)
    runtime_root = os.getenv("QUOTEMUX_RUNTIME_ROOT", "")
    if runtime_root != "":
        return Path(runtime_root) / "package_venvs"
    return Path.home() / ".quotemux" / "runtime" / "package_venvs"


def _environment_directory_name(manifest: SourcePackageManifest, requirements_path: Path) -> str:
    package_name = re.sub(r"[^A-Za-z0-9_.-]+", "-", manifest.package_id).strip("-")
    version_name = re.sub(r"[^A-Za-z0-9_.-]+", "-", manifest.version).strip("-")
    digest = _requirements_hash(requirements_path)[:12]
    return f"{package_name}-{version_name}-{digest}"


def _requirements_hash(requirements_path: Path) -> str:
    content = requirements_path.read_bytes()
    return hashlib.sha256(content).hexdigest()


def _runtime_project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _runtime_requirements_hash() -> str:
    pyproject_path = _runtime_project_root() / "pyproject.toml"
    if not pyproject_path.is_file():
        return ""
    content = str(_runtime_project_root()).encode("utf-8") + pyproject_path.read_bytes()
    packages_pyproject = _builtin_packages_project_root() / "pyproject.toml"
    if packages_pyproject.is_file():
        content += packages_pyproject.read_bytes()
    return hashlib.sha256(content).hexdigest()


def _builtin_packages_project_root() -> Path:
    return _runtime_project_root().parent / "QuoteMux_Packages"


def _venv_python_executable(venv_path: Path) -> Path:
    if os.name == "nt":
        return venv_path / "Scripts" / "python.exe"
    return venv_path / "bin" / "python"


def _environment_is_ready(marker_path: Path, requirements_hash: str, runtime_hash: str, python_executable: Path) -> bool:
    if not marker_path.is_file() or not python_executable.is_file():
        return False
    try:
        payload = json.loads(marker_path.read_text(encoding="utf-8"))
    except Exception:
        return False
    if not isinstance(payload, dict):
        return False
    return str(payload.get("requirements_hash", "")) == requirements_hash and str(payload.get("runtime_hash", "")) == runtime_hash


def _create_venv(venv_path: Path) -> None:
    venv_path.parent.mkdir(parents=True, exist_ok=True)
    if _venv_python_executable(venv_path).is_file():
        return
    subprocess.run(
        [sys.executable, "-m", "venv", "--system-site-packages", str(venv_path)],
        check=True,
    )


def _install_requirements(python_executable: Path, requirements_path: Path) -> None:
    subprocess.run(
        [str(python_executable), "-m", "pip", "install", "-r", str(requirements_path)],
        cwd=str(requirements_path.parent),
        check=True,
    )


def _install_runtime_requirements(python_executable: Path) -> None:
    subprocess.run(
        [str(python_executable), "-m", "pip", "install", "-e", str(_runtime_project_root())],
        check=True,
    )
    packages_root = _builtin_packages_project_root()
    if packages_root.is_dir():
        subprocess.run(
            [str(python_executable), "-m", "pip", "install", "-e", str(packages_root)],
            check=True,
        )


def _write_marker(marker_path: Path, manifest: SourcePackageManifest, requirements_hash: str, runtime_hash: str) -> None:
    marker_path.write_text(
        json.dumps(
            {
                "package_id": manifest.package_id,
                "version": manifest.version,
                "requirements_hash": requirements_hash,
                "runtime_hash": runtime_hash,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
