from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Sequence, TypeVar

from pydantic import BaseModel

from quotemux.config_runtime.runtime import get_config_runtime
from quotemux.reports import ContractReport
from quotemux.runtime_core.audit import record_provider_event
from quotemux.store.postgres import CACHE_HIT, CACHE_MISS, CACHE_PARTIAL_HIT, CACHE_SKIP, CACHE_STALE, CacheReadResult, CacheWriteResult, get_postgres_cache_store


T = TypeVar("T", bound=BaseModel)


@dataclass(frozen=True)
class CapabilityStoreReadResult:
    capability_id: str
    status: str
    items: tuple[dict[str, object], ...]
    watermark: str
    written_at: str
    provenance: dict[str, object]

    @property
    def hit(self) -> bool:
        return self.status == CACHE_HIT

    @property
    def partial_hit(self) -> bool:
        return self.status == CACHE_PARTIAL_HIT

    @property
    def miss(self) -> bool:
        return self.status in {CACHE_MISS, CACHE_STALE, CACHE_SKIP}


def _record_store_event(capability_id: str, status: str, detail: dict[str, object]) -> None:
    snapshot = get_config_runtime().get_active_snapshot()
    record_provider_event(
        capability_id,
        "quotemux_store",
        status,
        {
            "profile_id": snapshot.profile_id,
            "profile_version": snapshot.version,
            "package_id": "quotemux_store",
            "source_instance_id": "quotemux-store",
            "handler": "store",
            **detail,
        },
    )


def _watermark(items: Sequence[dict[str, object]]) -> str:
    marker = ""
    for item in items:
        for field_name in ("trade_time", "trade_date", "report_period", "announce_date", "effective_date", "event_time", "as_of_date"):
            value = item.get(field_name, "")
            if isinstance(value, str) and value > marker:
                marker = value
    return marker


def _runtime_status(status: str) -> str:
    return {
        CACHE_HIT: "store_hit",
        CACHE_PARTIAL_HIT: "store_partial_hit",
        CACHE_MISS: "store_miss",
        CACHE_STALE: "store_stale",
        CACHE_SKIP: "store_skip",
    }.get(status, "store_miss")


def _to_store_read_result(capability_id: str, result: CacheReadResult) -> CapabilityStoreReadResult:
    payload_items = result.items
    return CapabilityStoreReadResult(
        capability_id=capability_id,
        status=result.status,
        items=payload_items,
        watermark=_watermark(payload_items),
        written_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        provenance=result.detail,
    )


def load_store_result(capability_id: str, request_identity: dict[str, object], model_type: type[T]) -> tuple[list[T], CapabilityStoreReadResult]:
    result = get_postgres_cache_store().read(capability_id, request_identity)
    read_result = _to_store_read_result(capability_id, result)
    _record_store_event(
        capability_id,
        _runtime_status(result.status),
        {
            "request_identity": request_identity,
            "row_count": len(result.items),
            "scope_identity": result.scope_identity,
            "watermark": read_result.watermark,
        },
    )
    return [model_type(**item) for item in result.items], read_result


def store_result(capability_id: str, request_identity: dict[str, object], items: Sequence[object], report: ContractReport, quarantine_count: int = 0) -> CacheWriteResult:
    result = get_postgres_cache_store().write(capability_id, request_identity, items, report)
    _record_store_event(
        capability_id,
        "store_write" if result.status == "write" else "store_skip",
        {
            "request_identity": request_identity,
            "row_count": result.row_count,
            "coverage_count": result.coverage_count,
            "quarantine_count": quarantine_count,
        },
    )
    if quarantine_count > 0:
        _record_store_event(capability_id, "quarantine", {"request_identity": request_identity, "quarantine_count": quarantine_count})
    return result
