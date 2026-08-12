from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from hashlib import sha256
import json
import threading
import time

from platform_models.p0_fundamentals import P0Request

from quotemux.p0_fundamentals.errors import P0CacheWriteError
from quotemux.p0_fundamentals.policy import (
    P0_CACHE_BYTES_BY_CAPABILITY,
    P0_CACHE_TOTAL_BYTES,
    P0_CACHE_TTL_SECONDS_BY_CAPABILITY,
)


@dataclass(frozen=True)
class _CacheEntry:
    capability_id: str
    expires_at: float
    payload: bytes


class BoundedP0Cache:
    def __init__(self) -> None:
        self._entries: OrderedDict[str, _CacheEntry] = OrderedDict()
        self._lock = threading.Lock()
        self._total_bytes = 0
        self._capability_bytes = {
            capability_id: 0 for capability_id in P0_CACHE_BYTES_BY_CAPABILITY
        }

    def get(self, request: P0Request) -> bytes | None:
        key = _cache_key(request)
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            if entry.expires_at <= time.time():
                self._remove(key, entry)
                return None
            self._entries.move_to_end(key)
            return entry.payload

    def put(self, request: P0Request, payload: bytes) -> None:
        capability_id = request.capability_id
        capability_limit = P0_CACHE_BYTES_BY_CAPABILITY[capability_id]
        if len(payload) > capability_limit:
            raise P0CacheWriteError("单个 P0Page 超过 capability cache 容量")
        key = _cache_key(request)
        expires_at = time.time() + P0_CACHE_TTL_SECONDS_BY_CAPABILITY[capability_id]
        entry = _CacheEntry(
            capability_id=capability_id, expires_at=expires_at, payload=payload
        )
        with self._lock:
            current = self._entries.pop(key, None)
            if current is not None:
                self._subtract(current)
            self._evict_capability(capability_id, len(payload), capability_limit)
            self._evict_total(len(payload))
            self._entries[key] = entry
            self._total_bytes += len(payload)
            self._capability_bytes[capability_id] += len(payload)

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()
            self._total_bytes = 0
            for capability_id in self._capability_bytes:
                self._capability_bytes[capability_id] = 0

    def _evict_capability(
        self, capability_id: str, incoming_bytes: int, limit: int
    ) -> None:
        while self._capability_bytes[capability_id] + incoming_bytes > limit:
            candidate = next(
                (
                    (key, entry)
                    for key, entry in self._entries.items()
                    if entry.capability_id == capability_id
                ),
                None,
            )
            if candidate is None:
                raise P0CacheWriteError("无法满足 capability cache 容量")
            self._remove(*candidate)

    def _evict_total(self, incoming_bytes: int) -> None:
        while self._total_bytes + incoming_bytes > P0_CACHE_TOTAL_BYTES:
            if not self._entries:
                raise P0CacheWriteError("无法满足 P0 cache 总容量")
            key, entry = next(iter(self._entries.items()))
            self._remove(key, entry)

    def _remove(self, key: str, entry: _CacheEntry) -> None:
        self._entries.pop(key, None)
        self._subtract(entry)

    def _subtract(self, entry: _CacheEntry) -> None:
        self._total_bytes -= len(entry.payload)
        self._capability_bytes[entry.capability_id] -= len(entry.payload)


def _cache_key(request: P0Request) -> str:
    payload = json.dumps(
        request.model_dump(mode="json"),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return sha256(payload).hexdigest()
