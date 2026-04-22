from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass
class CooldownState:
    until: datetime | None = None


class SourceCooldownRegistry:
    def __init__(self) -> None:
        self._states: dict[str, CooldownState] = {}

    def set_cooldown(self, source_name: str, seconds: int) -> None:
        self._states[source_name] = CooldownState(until=datetime.now() + timedelta(seconds=seconds))

    def clear_cooldown(self, source_name: str) -> None:
        self._states.pop(source_name, None)

    def is_available(self, source_name: str) -> bool:
        state = self._states.get(source_name)
        if state is None or state.until is None:
            return True
        if datetime.now() >= state.until:
            self._states.pop(source_name, None)
            return True
        return False
