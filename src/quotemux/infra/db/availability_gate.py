from __future__ import annotations

import os
import socket
import threading
import time


def _float_env(name: str, default: float) -> float:
    text = os.getenv(name, "")
    try:
        return float(text)
    except ValueError:
        return default


DB_PROBE_TIMEOUT_SECONDS = _float_env("MHK_DB_PROBE_TIMEOUT_SECONDS", 0.25)


class DbAvailabilityGate:
    def __init__(self, cooldown_seconds: float) -> None:
        self._cooldown_seconds = cooldown_seconds
        self._unavailable_until = 0.0
        self._lock = threading.Lock()

    def available_for_attempt(self) -> bool:
        with self._lock:
            return time.monotonic() >= self._unavailable_until

    def mark_unavailable(self) -> None:
        with self._lock:
            self._unavailable_until = time.monotonic() + self._cooldown_seconds

    def mark_available(self) -> None:
        with self._lock:
            self._unavailable_until = 0.0

    def probe_port(self, host: str, port: int) -> bool:
        if not self.available_for_attempt():
            return False
        try:
            with socket.create_connection((host, port), timeout=DB_PROBE_TIMEOUT_SECONDS):
                return True
        except OSError:
            self.mark_unavailable()
            return False
