from __future__ import annotations

from dataclasses import dataclass
import inspect
import time
from typing import Callable, Generic, Mapping, Sequence, TypeVar

from pydantic import BaseModel

from quotemux.config_runtime.models import SourceInstanceConfig
from quotemux.config_runtime.runtime import get_config_runtime
from quotemux.runtime_core.audit import record_provider_event
from quotemux.contracts.policies import get_contract_policy
from quotemux.settings import QuoteMuxSettings


T = TypeVar("T", bound=BaseModel)


@dataclass(frozen=True)
class ProviderStep(Generic[T]):
    name: str
    fetcher: Callable[..., list[T]]
    source_instance_id: str = ""
    handler: str = ""

    @property
    def step_id(self) -> str:
        if self.source_instance_id != "":
            return self.source_instance_id
        return self.name


@dataclass(frozen=True)
class ProviderMergeStats:
    name: str
    package_id: str
    source_instance_id: str
    handler: str
    request_count: int
    fetched_row_count: int
    added_count: int
    filled_field_count: int
    conflict_count: int
    skipped_count: int
    error_count: int
    elapsed_ms: float

    @property
    def provider_hit(self) -> bool:
        return (self.added_count + self.filled_field_count) > 0


@dataclass(frozen=True)
class FallbackReport:
    contract_name: str
    profile_id: str
    profile_version: str
    steps: tuple[ProviderMergeStats, ...]

    def provider_hit_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for step in self.steps:
            counts[step.package_id] = counts.get(step.package_id, 0) + int(step.provider_hit)
        return counts

    def provider_request_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for step in self.steps:
            counts[step.package_id] = counts.get(step.package_id, 0) + step.request_count
        return counts

    def total_conflict_count(self) -> int:
        return sum(step.conflict_count for step in self.steps)

    def total_error_count(self) -> int:
        return sum(step.error_count for step in self.steps)

    def total_skipped_count(self) -> int:
        return sum(step.skipped_count for step in self.steps)


class SourceInstanceExecutor:
    def __init__(self, settings: QuoteMuxSettings) -> None:
        self._settings = settings

    def build_steps(
        self,
        contract_name: str,
        handlers: Mapping[str, tuple[str, Callable[[SourceInstanceConfig], Callable[..., list[T]]]]],
        fallback_order: tuple[str, ...],
    ) -> tuple[ProviderStep[T], ...]:
        steps: list[ProviderStep[T]] = []
        for instance in self._settings.get_contract_source_instances(contract_name, fallback_order):
            handler_entry = handlers.get(instance.package_id)
            if handler_entry is None:
                continue
            handler_name, fetcher_builder = handler_entry
            steps.append(
                ProviderStep(
                    name=instance.package_id,
                    fetcher=fetcher_builder(instance),
                    source_instance_id=instance.instance_id,
                    handler=handler_name,
                )
            )
        return tuple(steps)


def _merge_model_lists(
    high_priority: Sequence[T],
    low_priority: Sequence[T],
    key_fields: tuple[str, ...],
) -> tuple[list[T], int, int, int]:
    merged: list[T] = []
    index_map: dict[tuple[object, ...], int] = {}
    added_count = 0
    filled_field_count = 0
    conflict_count = 0
    for item in high_priority:
        key = tuple(getattr(item, field) for field in key_fields)
        index_map[key] = len(merged)
        merged.append(item.model_copy(deep=True))
    for item in low_priority:
        key = tuple(getattr(item, field) for field in key_fields)
        if key not in index_map:
            index_map[key] = len(merged)
            merged.append(item.model_copy(deep=True))
            added_count += 1
            continue
        current = merged[index_map[key]]
        payload = current.model_dump()
        for field_name, value in item.model_dump().items():
            if field_name in key_fields:
                continue
            if payload[field_name] in {None, ""} and value not in {None, ""}:
                payload[field_name] = value
                filled_field_count += 1
                continue
            if payload[field_name] not in {None, ""} and value not in {None, ""} and payload[field_name] != value:
                conflict_count += 1
        merged[index_map[key]] = type(current)(**payload)
    return merged, added_count, filled_field_count, conflict_count


def _run_fallback_chain_internal(
    contract_name: str,
    base_items: Sequence[T],
    key_fields: tuple[str, ...],
    request_builder: Callable[[list[T]], list[tuple[object, ...]]],
    steps: Sequence[ProviderStep[T]],
    source_order: tuple[str, ...],
) -> tuple[list[T], FallbackReport]:
    policy = get_contract_policy(contract_name)
    ordered_names = source_order if source_order != () else policy.source_order
    snapshot = get_config_runtime().get_active_snapshot()
    ordered_steps = _order_steps(steps, ordered_names)
    merged_items = [item.model_copy(deep=True) for item in base_items]
    reports: list[ProviderMergeStats] = []
    for step in ordered_steps:
        requests = request_builder(merged_items)
        if requests == []:
            record_provider_event(
                contract_name,
                step.name,
                "skipped",
                {
                    "reason": "request_builder_empty",
                    "profile_id": snapshot.profile_id,
                    "profile_version": snapshot.version,
                    "package_id": step.name,
                    "source_instance_id": step.step_id,
                    "handler": step.handler,
                },
            )
            reports.append(
                ProviderMergeStats(
                    name=step.name,
                    package_id=step.name,
                    source_instance_id=step.step_id,
                    handler=step.handler,
                    request_count=0,
                    fetched_row_count=0,
                    added_count=0,
                    filled_field_count=0,
                    conflict_count=0,
                    skipped_count=1,
                    error_count=0,
                    elapsed_ms=0.0,
                )
            )
            break
        fetched_row_count = 0
        added_count = 0
        filled_field_count = 0
        conflict_count = 0
        error_count = 0
        elapsed_ms = 0.0
        for request in requests:
            started_at = time.perf_counter()
            try:
                fetched_items = step.fetcher(*request)
            except Exception as exc:
                elapsed_ms += (time.perf_counter() - started_at) * 1000
                error_count += 1
                record_provider_event(
                    contract_name,
                    step.name,
                    "error",
                    {
                        "request": list(request),
                        "error": str(exc),
                        "profile_id": snapshot.profile_id,
                        "profile_version": snapshot.version,
                        "package_id": step.name,
                        "source_instance_id": step.step_id,
                        "handler": step.handler,
                    },
                )
                continue
            elapsed_ms += (time.perf_counter() - started_at) * 1000
            fetched_row_count += len(fetched_items)
            merged_items, request_added_count, request_filled_count, request_conflict_count = _merge_model_lists(
                merged_items,
                fetched_items,
                key_fields,
            )
            added_count += request_added_count
            filled_field_count += request_filled_count
            conflict_count += request_conflict_count
        record_provider_event(
            contract_name,
            step.name,
            "success",
            {
                "request_count": len(requests),
                "fetched_row_count": fetched_row_count,
                "added_count": added_count,
                "filled_field_count": filled_field_count,
                "conflict_count": conflict_count,
                "provider_hit": (added_count + filled_field_count) > 0,
                "elapsed_ms": round(elapsed_ms, 3),
                "profile_id": snapshot.profile_id,
                "profile_version": snapshot.version,
                "package_id": step.name,
                "source_instance_id": step.step_id,
                "handler": step.handler,
            },
        )
        if conflict_count > 0:
            record_provider_event(
                contract_name,
                step.name,
                "conflict",
                {
                    "request_count": len(requests),
                    "conflict_count": conflict_count,
                    "profile_id": snapshot.profile_id,
                    "profile_version": snapshot.version,
                    "package_id": step.name,
                    "source_instance_id": step.step_id,
                    "handler": step.handler,
                },
            )
        reports.append(
            ProviderMergeStats(
                name=step.name,
                package_id=step.name,
                source_instance_id=step.step_id,
                handler=step.handler,
                request_count=len(requests),
                fetched_row_count=fetched_row_count,
                added_count=added_count,
                filled_field_count=filled_field_count,
                conflict_count=conflict_count,
                skipped_count=0,
                error_count=error_count,
                elapsed_ms=round(elapsed_ms, 3),
            )
        )
    return merged_items, FallbackReport(contract_name=contract_name, profile_id=snapshot.profile_id, profile_version=snapshot.version, steps=tuple(reports))


def _order_steps(steps: Sequence[ProviderStep[T]], ordered_names: tuple[str, ...]) -> tuple[ProviderStep[T], ...]:
    ordered: list[ProviderStep[T]] = []
    for source_id in ordered_names:
        for step in steps:
            if step.step_id == source_id and step not in ordered:
                ordered.append(step)
        for step in steps:
            if step.name == source_id and step not in ordered:
                ordered.append(step)
    for step in steps:
        if step not in ordered:
            ordered.append(step)
    return tuple(ordered)


def accepts_instance_context(fetcher: Callable[..., object]) -> bool:
    try:
        signature = inspect.signature(fetcher)
    except (TypeError, ValueError):
        return False
    return "source_instance" in signature.parameters


def run_fallback_chain(
    contract_name: str,
    base_items: Sequence[T],
    key_fields: tuple[str, ...],
    request_builder: Callable[[list[T]], list[tuple[object, ...]]],
    steps: Sequence[ProviderStep[T]],
    source_order: tuple[str, ...] = (),
) -> list[T]:
    merged_items, _ = _run_fallback_chain_internal(contract_name, base_items, key_fields, request_builder, steps, source_order)
    return merged_items


def run_fallback_chain_with_report(
    contract_name: str,
    base_items: Sequence[T],
    key_fields: tuple[str, ...],
    request_builder: Callable[[list[T]], list[tuple[object, ...]]],
    steps: Sequence[ProviderStep[T]],
    source_order: tuple[str, ...] = (),
) -> tuple[list[T], FallbackReport]:
    return _run_fallback_chain_internal(contract_name, base_items, key_fields, request_builder, steps, source_order)

