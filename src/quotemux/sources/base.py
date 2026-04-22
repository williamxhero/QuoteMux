from __future__ import annotations

from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True)
class SourceDefinition:
    name: str
    handlers: dict[str, Callable[..., object]]

    def get_handler(self, handler_name: str) -> Callable[..., object]:
        handler = self.handlers.get(handler_name)
        if handler is None:
            raise KeyError(f"source {self.name} 未注册 handler: {handler_name}")
        return handler

    def has_handler(self, handler_name: str) -> bool:
        return handler_name in self.handlers
