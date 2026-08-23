from __future__ import annotations

from collections.abc import Callable, Generator
from contextlib import contextmanager
from contextvars import ContextVar, copy_context
from functools import wraps
from typing import ParamSpec, TypeVar


P = ParamSpec("P")
T = TypeVar("T")

_STRICT_PUBLIC_READ: ContextVar[bool] = ContextVar("quotemux_strict_public_read", default=False)


class StrictReadViolation(RuntimeError):
    def __init__(self, operation: str) -> None:
        self.operation = operation
        super().__init__(f"strict public read forbids {operation}")


def is_strict_public_read() -> bool:
    return _STRICT_PUBLIC_READ.get()


@contextmanager
def strict_public_read_boundary() -> Generator[None, None, None]:
    """Make provider calls, dependency installation, and writes fail closed."""
    token = _STRICT_PUBLIC_READ.set(True)
    try:
        yield
    finally:
        _STRICT_PUBLIC_READ.reset(token)


def bind_strict_public_read_context(function: Callable[P, T]) -> Callable[P, T]:
    """Capture the current context for an explicit worker-thread handoff."""
    captured = copy_context()

    @wraps(function)
    def invoke(*args: P.args, **kwargs: P.kwargs) -> T:
        return captured.copy().run(function, *args, **kwargs)

    return invoke


def reject_in_strict_public_read(operation: str) -> None:
    if is_strict_public_read():
        raise StrictReadViolation(operation)
