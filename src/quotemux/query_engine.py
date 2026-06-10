from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Generic, Sequence, TypeVar

from pydantic import BaseModel

from quotemux.common import merge_model_lists, sort_items
from quotemux.config_runtime.runtime import get_config_runtime
from quotemux.reports import ContractReport
from quotemux.runtime_core.executor import ProviderStep, run_fallback_chain_with_report
from quotemux.store import load_store_result, store_result


T = TypeVar("T", bound=BaseModel)


@dataclass(frozen=True)
class CapabilityQuerySpec(Generic[T]):
    capability_id: str
    store_identity: dict[str, object]
    model_type: type[T]
    key_fields: tuple[str, ...]
    sort_fields: tuple[str, ...]
    request_builder: Callable[[list[T]], list[tuple[object, ...]]]
    provider_steps: tuple[ProviderStep[T], ...] | Callable[[], tuple[ProviderStep[T], ...]]
    source_order: tuple[str, ...]
    base_items: Sequence[T] = ()
    base_source_name: str = ""
    store_enabled: bool = True
    write_empty_coverage: bool = False
    payload_builder: Callable[[list[T]], list[object]] | None = None
    fact_ref_writer: Callable[[list[T]], bool] | None = None


def _store_hit_report(capability_id: str) -> ContractReport:
    active_snapshot = get_config_runtime().get_active_snapshot()
    return ContractReport(
        contract_name=capability_id,
        profile_id=active_snapshot.profile_id,
        profile_version=active_snapshot.version,
    ).with_store_stats(hit=True)


def _base_report(capability_id: str, base_source_name: str, base_hit: bool) -> ContractReport:
    active_snapshot = get_config_runtime().get_active_snapshot()
    source_hit_counts = {base_source_name: int(base_hit)} if base_source_name else {}
    source_request_counts = {base_source_name: 1} if base_source_name else {}
    return ContractReport(
        contract_name=capability_id,
        profile_id=active_snapshot.profile_id,
        profile_version=active_snapshot.version,
        source_hit_counts=source_hit_counts,
        source_request_counts=source_request_counts,
    )


def _store_payload(spec: CapabilityQuerySpec[T], items: list[T]) -> list[object]:
    if spec.payload_builder is None:
        return items
    return spec.payload_builder(items)


def _provider_steps(spec: CapabilityQuerySpec[T]) -> tuple[ProviderStep[T], ...]:
    if callable(spec.provider_steps):
        return spec.provider_steps()
    return spec.provider_steps


def _merge_base_items(store_items: list[T], base_items: Sequence[T], key_fields: tuple[str, ...]) -> list[T]:
    if base_items == ():
        return store_items
    return merge_model_lists(store_items, list(base_items), key_fields)


def _has_items(items: Sequence[T]) -> bool:
    return len(items) > 0


def _item_key(item: T, key_fields: tuple[str, ...]) -> tuple[object, ...]:
    return tuple(getattr(item, field) for field in key_fields)


def _changed_items(before_items: Sequence[T], after_items: Sequence[T], key_fields: tuple[str, ...]) -> list[T]:
    before_by_key = {_item_key(item, key_fields): item.model_dump() for item in before_items}
    changed: list[T] = []
    for item in after_items:
        before_payload = before_by_key.get(_item_key(item, key_fields))
        if before_payload is None or before_payload != item.model_dump():
            changed.append(item)
    return changed


def execute_capability_query(spec: CapabilityQuerySpec[T]) -> tuple[list[T], ContractReport]:
    store_items: list[T] = []
    store_status = "skip"
    uses_fact_ref = spec.fact_ref_writer is not None
    cache_enabled = spec.store_enabled and not uses_fact_ref
    if cache_enabled:
        store_items, store_read = load_store_result(spec.capability_id, spec.store_identity, spec.model_type)
        store_status = store_read.status
        if store_read.hit:
            if spec.request_builder(store_items) == []:
                sorted_items = sort_items(store_items, spec.sort_fields) if spec.sort_fields else store_items
                return sorted_items, _store_hit_report(spec.capability_id)
            store_status = "partial_hit"

    base_items = _merge_base_items(store_items if store_status == "partial_hit" else [], spec.base_items, spec.key_fields)
    if spec.request_builder(base_items) == []:
        sorted_items = sort_items(base_items, spec.sort_fields) if spec.sort_fields else base_items
        report = _base_report(spec.capability_id, spec.base_source_name, _has_items(spec.base_items))
        if cache_enabled and (sorted_items != [] or spec.write_empty_coverage):
            store_write = store_result(spec.capability_id, spec.store_identity, _store_payload(spec, sorted_items), report, report.quarantine_count)
            report = report.with_store_stats(partial_hit=store_status == "partial_hit", miss=store_status in {"miss", "skip"}, stale=store_status == "stale", write=store_write.status == "write")
        else:
            report = report.with_store_stats(partial_hit=store_status == "partial_hit", miss=store_status in {"miss", "skip"}, stale=store_status == "stale")
        return sorted_items, report

    merged_items, fallback_report = run_fallback_chain_with_report(
        spec.capability_id,
        base_items,
        spec.key_fields,
        spec.request_builder,
        _provider_steps(spec),
        spec.source_order,
    )
    sorted_items = sort_items(merged_items, spec.sort_fields) if spec.sort_fields else merged_items
    report = ContractReport.from_fallback_report(spec.capability_id, fallback_report, spec.base_source_name, _has_items(spec.base_items))
    if spec.fact_ref_writer is not None:
        provider_items = _changed_items(base_items, sorted_items, spec.key_fields)
        if provider_items != []:
            spec.fact_ref_writer(provider_items)
        report = report.with_store_stats(partial_hit=store_status == "partial_hit", miss=store_status in {"miss", "skip"}, stale=store_status == "stale")
    elif cache_enabled and (sorted_items != [] or spec.write_empty_coverage):
        store_write = store_result(spec.capability_id, spec.store_identity, _store_payload(spec, sorted_items), report, report.quarantine_count)
        report = report.with_store_stats(partial_hit=store_status == "partial_hit", miss=store_status in {"miss", "skip"}, stale=store_status == "stale", write=store_write.status == "write")
    else:
        report = report.with_store_stats(partial_hit=store_status == "partial_hit", miss=store_status in {"miss", "skip"}, stale=store_status == "stale")
    return sorted_items, report
