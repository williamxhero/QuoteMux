from __future__ import annotations

import base64
from contextlib import redirect_stderr, redirect_stdout
from importlib import import_module
from importlib import invalidate_caches
from io import StringIO
import json
import os
from pathlib import Path
import pickle
import sys
import traceback

from quotemux.config_runtime.models import SourceInstanceConfig
from quotemux.source_packages.instance_context import use_source_instance


def main() -> None:
    request = pickle.loads(sys.stdin.buffer.read())
    response = _run_request(request)
    sys.stdout.buffer.write(base64.b64encode(pickle.dumps(response)))


def _run_request(request: dict[str, object]) -> dict[str, object]:
    output = StringIO()
    error_output = StringIO()
    try:
        with redirect_stdout(output), redirect_stderr(error_output):
            _activate_import_paths(
                tuple(str(item) for item in request["sys_paths"]),
                tuple(str(item) for item in request["import_roots"]),
                str(request["package_root"]),
            )
            target = str(request["target"])
            args = request["args"]
            kwargs = request["kwargs"]
            if not isinstance(args, tuple) or not isinstance(kwargs, dict):
                raise TypeError("handler 鍙傛暟涓嶆槸鍚堟硶璋冪敤鍙傛暟")
            source_instance = _load_source_instance(request)
            if source_instance is None:
                result = _call_handler(target, args, kwargs)
            else:
                with use_source_instance(source_instance):
                    result = _call_handler(target, args, kwargs)
    except Exception as exc:
        return {
            "status": "error",
            "message": f"{type(exc).__name__}: {exc}",
            "traceback": traceback.format_exc(),
        }
    return {"status": "ok", "result": result}


def _activate_import_paths(sys_paths: tuple[str, ...], import_roots: tuple[str, ...], package_root: str) -> None:
    for path in sys_paths:
        path_root = Path(path)
        if (path_root / "quotemux" / "__init__.py").is_file() or (path_root / "platform_models" / "__init__.py").is_file():
            _prepend_sys_path(path)
    for root_text in import_roots:
        if root_text == "":
            continue
        root_path = Path(root_text)
        import_path = root_path.parent if root_path.name == "quotemux_packages" else root_path
        _append_sys_path(str(import_path))
        if root_path.name == "quotemux_packages":
            try:
                namespace_package = import_module("quotemux_packages")
            except ImportError:
                continue
            namespace_path = str(root_path)
            if namespace_path not in namespace_package.__path__:
                namespace_package.__path__.insert(0, namespace_path)
    if package_root != "":
        package_path = Path(package_root)
        package_parent = package_path.parent
        source_package_root = package_parent.parent if package_parent.name == "packages" else package_parent
        _prepend_sys_path(str(source_package_root))
        if package_parent.name == "packages":
            namespace_package = import_module("quotemux_packages")
            namespace_path = str(package_parent)
            if namespace_path in namespace_package.__path__:
                namespace_package.__path__.remove(namespace_path)
            namespace_package.__path__.insert(0, namespace_path)
        _prepend_sys_path(str(package_path))
    invalidate_caches()


def _prepend_sys_path(path: str) -> None:
    if path == "" or path in sys.path:
        return
    sys.path.insert(0, path)


def _append_sys_path(path: str) -> None:
    if path == "" or path in sys.path:
        return
    sys.path.append(path)


def _call_handler(target: str, args: tuple[object, ...], kwargs: dict[str, object]) -> object:
    module_name, _, attr_name = target.partition(":")
    if module_name == "" or attr_name == "":
        raise ValueError(f"闈炴硶 handler 鐩爣: {target}")
    module = import_module(module_name)
    handler = getattr(module, attr_name)
    if not callable(handler):
        raise TypeError(f"{target} 涓嶆槸鍙皟鐢ㄥ璞?")
    return handler(*args, **kwargs)


def _load_source_instance(request: dict[str, object]) -> SourceInstanceConfig | None:
    source_instance = request.get("source_instance", {})
    if isinstance(source_instance, dict) and source_instance != {}:
        return SourceInstanceConfig.from_dict(source_instance)
    source_instance_text = os.getenv("QUOTEMUX_SOURCE_INSTANCE", "")
    if source_instance_text == "":
        return None
    try:
        payload = json.loads(source_instance_text)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    return SourceInstanceConfig.from_dict(payload)


if __name__ == "__main__":
    main()
