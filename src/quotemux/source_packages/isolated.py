from __future__ import annotations

import base64
from dataclasses import dataclass
import json
import os
import pickle
from pathlib import Path
import subprocess
import sys

from quotemux.source_packages.environment import ensure_package_environment
from quotemux.source_packages.instance_context import current_source_instance
from quotemux.source_packages.manifest import SourcePackageManifest


@dataclass(frozen=True)
class IsolatedPackageHandler:
    manifest: SourcePackageManifest
    target: str
    import_roots: tuple[str, ...]

    def __call__(self, *args: object, **kwargs: object):
        environment = ensure_package_environment(self.manifest)
        payload = {
            "target": self.target,
            "args": args,
            "kwargs": kwargs,
            "import_roots": self.import_roots,
            "package_root": self.manifest.package_root,
            "source_instance": _source_instance_payload(),
            "sys_paths": _portable_sys_paths(),
        }
        completed = subprocess.run(
            [environment.python_executable, "-m", "quotemux.source_packages.worker"],
            input=pickle.dumps(payload),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=_worker_env(self.import_roots, self.manifest.package_root),
            check=False,
        )
        if completed.returncode != 0:
            error_text = completed.stderr.decode("utf-8", errors="replace")
            raise RuntimeError(f"source package {self.manifest.package_id} 执行失败: {error_text}")
        response = pickle.loads(base64.b64decode(completed.stdout))
        if response["status"] == "ok":
            return response["result"]
        raise RuntimeError(str(response["message"]))


def _portable_sys_paths() -> tuple[str, ...]:
    return tuple(path for path in sys.path if path != "")


def _worker_env(import_roots: tuple[str, ...], package_root: str) -> dict[str, str]:
    env = os.environ.copy()
    python_paths = list(_worker_bootstrap_paths())
    existing_path = env.get("PYTHONPATH", "")
    if existing_path != "":
        python_paths.append(existing_path)
    env["PYTHONPATH"] = os.pathsep.join(python_paths)
    source_instance = current_source_instance()
    if source_instance is not None:
        env["QUOTEMUX_SOURCE_INSTANCE"] = json.dumps(source_instance.to_dict(), ensure_ascii=False)
    return env


def _worker_bootstrap_paths() -> tuple[str, ...]:
    paths: list[str] = []
    for path in _portable_sys_paths():
        path_root = Path(path)
        if _is_runtime_source_path(path_root) and path not in paths:
            paths.append(path)
    return tuple(paths)


def _is_runtime_source_path(path: Path) -> bool:
    return (path / "quotemux" / "source_packages" / "worker.py").is_file() or (path / "platform_models" / "__init__.py").is_file()


def _source_instance_payload() -> dict[str, object]:
    source_instance = current_source_instance()
    if source_instance is None:
        return {}
    return source_instance.to_dict()
