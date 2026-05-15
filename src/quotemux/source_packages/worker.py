from __future__ import annotations

import base64
from contextlib import redirect_stderr, redirect_stdout
from importlib import import_module
from importlib import invalidate_caches
from io import StringIO
from pathlib import Path
import pickle
import sys
import traceback


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
                raise TypeError("handler 参数不是合法调用参数")
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
        _append_sys_path(str(package_path.parent))
        _append_sys_path(str(package_path))
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
        raise ValueError(f"非法 handler 目标: {target}")
    module = import_module(module_name)
    handler = getattr(module, attr_name)
    if not callable(handler):
        raise TypeError(f"{target} 不是可调用对象")
    return handler(*args, **kwargs)


if __name__ == "__main__":
    main()
