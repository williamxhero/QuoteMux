from __future__ import annotations

from importlib import metadata
from pathlib import Path
import sys


SOURCE_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import quotemux.package_install as package_install
from quotemux.package_install import PackageInstallResult, _ensure_isolated_package_environments, _package_install_target, installed_packages_fingerprint
from quotemux.source_packages.environment import (
    DEFAULT_PACKAGE_REPO_SPEC,
    _environment_is_ready,
    _install_local_project_copy,
    _install_runtime_requirements,
    _package_source_hash,
    _runtime_requirements_hash,
    package_install_target,
    package_repo_spec,
)


def test_package_install_result_uses_minimal_shape() -> None:
    result = PackageInstallResult(installed_package_ids=("tushare",), visible_package_ids=("tushare",), package_count=1)

    assert result.installed_package_ids == ("tushare",)
    assert result.visible_package_ids == ("tushare",)
    assert result.package_count == 1


def test_installed_packages_fingerprint_returns_empty_when_distribution_missing(monkeypatch) -> None:
    def raise_not_found(_: str):
        raise metadata.PackageNotFoundError

    monkeypatch.setattr(metadata, "distribution", raise_not_found)

    assert installed_packages_fingerprint() == ""


def test_installed_packages_fingerprint_reads_python_text_and_manifest_files(monkeypatch, tmp_path: Path) -> None:
    site_root = tmp_path / "site"
    package_root = site_root / "quotemux_packages" / "demo"
    package_root.mkdir(parents=True)
    (package_root / "source.py").write_text("def run():\n    return 1\n", encoding="utf-8")
    (package_root / "requirements.txt").write_text("demo==1\n", encoding="utf-8")
    (package_root / "quotemux_package.json").write_text('{"package_id":"demo"}', encoding="utf-8")

    class FakeDistribution:
        files = (
            Path("quotemux_packages/demo/source.py"),
            Path("quotemux_packages/demo/requirements.txt"),
            Path("quotemux_packages/demo/quotemux_package.json"),
        )

        def locate_file(self, value):
            return site_root / Path(str(value))

    monkeypatch.setattr(metadata, "distribution", lambda _: FakeDistribution())

    assert installed_packages_fingerprint() != ""


def test_package_repo_spec_prefers_environment_value(monkeypatch) -> None:
    monkeypatch.setenv("QUOTEMUX_PACKAGE_REPO_SPEC", "/tmp/local-packages")

    assert package_repo_spec() == "/tmp/local-packages"


def test_package_repo_spec_uses_default_when_environment_missing(monkeypatch) -> None:
    monkeypatch.delenv("QUOTEMUX_PACKAGE_REPO_SPEC", raising=False)

    assert package_repo_spec() == DEFAULT_PACKAGE_REPO_SPEC


def test_package_install_target_uses_configured_online_repository(monkeypatch) -> None:
    monkeypatch.setenv("QUOTEMUX_PACKAGE_REPO_SPEC", "git+https://example.invalid/repo.git")

    target = _package_install_target()

    assert target == "git+https://example.invalid/repo.git"


def test_source_package_install_target_uses_configured_online_repository(monkeypatch) -> None:
    monkeypatch.setenv("QUOTEMUX_PACKAGE_REPO_SPEC", "git+https://example.invalid/source-packages.git")

    target = package_install_target()

    assert target == "git+https://example.invalid/source-packages.git"


def test_install_all_packages_prepares_only_isolated_environments(monkeypatch) -> None:
    isolated = object()
    embedded = object()
    prepared: list[object] = []
    monkeypatch.setattr(package_install, "package_uses_isolated_environment", lambda manifest: manifest is isolated)
    monkeypatch.setattr(package_install, "ensure_package_environment", prepared.append)

    _ensure_isolated_package_environments((isolated, embedded))

    assert prepared == [isolated]


def test_package_source_hash_changes_when_source_changes(tmp_path: Path) -> None:
    package_root = tmp_path / "tushare"
    package_root.mkdir()
    source_path = package_root / "source.py"
    source_path.write_text("VALUE = 1\n", encoding="utf-8")

    first_hash = _package_source_hash(package_root)
    source_path.write_text("VALUE = 2\n", encoding="utf-8")

    assert _package_source_hash(package_root) != first_hash


def test_runtime_requirements_hash_is_stable_across_release_directories(monkeypatch, tmp_path: Path) -> None:
    first_release = tmp_path / "deploy_1"
    second_release = tmp_path / "deploy_2"
    for release in (first_release, second_release):
        release.mkdir()
        (release / "pyproject.toml").write_text("[project]\nname = 'quotemux'\n", encoding="utf-8")

    monkeypatch.setattr("quotemux.source_packages.environment._runtime_project_root", lambda: first_release)
    first_hash = _runtime_requirements_hash()
    monkeypatch.setattr("quotemux.source_packages.environment._runtime_project_root", lambda: second_release)

    assert _runtime_requirements_hash() == first_hash


def test_runtime_requirements_hash_changes_when_runtime_source_changes(monkeypatch, tmp_path: Path) -> None:
    runtime_root = tmp_path / "release"
    source_path = runtime_root / "src" / "quotemux" / "runtime.py"
    source_path.parent.mkdir(parents=True)
    (runtime_root / "pyproject.toml").write_text("[project]\nname = 'quotemux'\n", encoding="utf-8")
    source_path.write_text("VALUE = 1\n", encoding="utf-8")
    monkeypatch.setattr("quotemux.source_packages.environment._runtime_project_root", lambda: runtime_root)

    first_hash = _runtime_requirements_hash()
    source_path.write_text("VALUE = 2\n", encoding="utf-8")

    assert _runtime_requirements_hash() != first_hash


def test_environment_is_not_ready_for_legacy_marker_without_source_hash(tmp_path: Path) -> None:
    marker_path = tmp_path / ".quotemux-installed.json"
    marker_path.write_text(
        "{\"requirements_hash\":\"req\",\"runtime_hash\":\"runtime\",\"packages_hash\":\"packages\"}",
        encoding="utf-8",
    )
    python_executable = tmp_path / "python"
    python_executable.write_text("", encoding="utf-8")

    assert not _environment_is_ready(
        marker_path, "req", "source", "runtime", "packages", python_executable
    )


def test_environment_is_ready_when_marker_source_hash_matches(tmp_path: Path) -> None:
    marker_path = tmp_path / ".quotemux-installed.json"
    marker_path.write_text(
        "{\"requirements_hash\":\"req\",\"package_source_hash\":\"source\","
        "\"runtime_hash\":\"runtime\",\"packages_hash\":\"packages\"}",
        encoding="utf-8",
    )
    python_executable = tmp_path / "python"
    python_executable.write_text("", encoding="utf-8")

    assert _environment_is_ready(
        marker_path, "req", "source", "runtime", "packages", python_executable
    )


def test_local_project_install_builds_from_temporary_copy(monkeypatch, tmp_path: Path) -> None:
    source_root = tmp_path / "source-project"
    source_root.mkdir()
    (source_root / "pyproject.toml").write_text("[build-system]", encoding="utf-8")
    (source_root / "source_project.egg-info").mkdir()
    commands: list[list[str]] = []

    def capture(command: list[str], *, check: bool) -> None:
        assert check
        commands.append(command)
        build_root = Path(command[-1])
        assert build_root != source_root
        assert (build_root / "pyproject.toml").is_file()
        assert not (build_root / "source_project.egg-info").exists()

    monkeypatch.setattr("quotemux.source_packages.environment.subprocess.run", capture)

    _install_local_project_copy("python", source_root)

    assert commands[0][0:4] == ["python", "-m", "pip", "install"]
    assert "--no-deps" in commands[0]
    assert (source_root / "source_project.egg-info").is_dir()


def test_runtime_install_resolves_quotemux_declared_dependencies(monkeypatch, tmp_path: Path) -> None:
    runtime_root = tmp_path / "quotemux"
    runtime_root.mkdir()
    (runtime_root / "pyproject.toml").write_text(
        "[project]\nname='quotemux'\ndependencies=['pydantic>=2']\n",
        encoding="utf-8",
    )
    commands: list[list[str]] = []

    def capture(command: list[str], *, check: bool) -> None:
        assert check
        commands.append(command)

    monkeypatch.setattr("quotemux.source_packages.environment._runtime_project_root", lambda: runtime_root)
    monkeypatch.setattr("quotemux.source_packages.environment._install_distribution_for_python", lambda python: None)
    monkeypatch.setattr("quotemux.source_packages.environment.subprocess.run", capture)

    _install_runtime_requirements(Path("python"))

    assert commands
    assert "--no-deps" not in commands[0]
