from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Generic, Sequence, TypeVar

from pydantic import BaseModel

from quotemux.runtime_core.audit import record_provider_event
from quotemux.contracts.policies import get_contract_policy


T = TypeVar("T", bound=BaseModel)


@dataclass(frozen=True)
class ProviderStep(Generic[T]):
    name: str
    fetcher: Callable[..., list[T]]


@dataclass(frozen=True)
class ProviderMergeStats:
    name: str
    request_count: int
    fetched_row_count: int
    added_count: int
    filled_field_count: int
    conflict_count: int
    skipped_count: int
    error_count: int

    @property
    def provider_hit(self) -> bool:
        return (self.added_count + self.filled_field_count) > 0


@dataclass(frozen=True)
class FallbackReport:
    contract_name: str
    steps: tuple[ProviderMergeStats, ...]

    def provider_hit_counts(self) -> dict[str, int]:
        return {step.name: int(step.provider_hit) for step in self.steps}

    def provider_request_counts(self) -> dict[str, int]:
        return {step.name: step.request_count for step in self.steps}

    def total_conflict_count(self) -> int:
        return sum(step.conflict_count for step in self.steps)

    def total_error_count(self) -> int:
        return sum(step.error_count for step in self.steps)

    def total_skipped_count(self) -> int:
        return sum(step.skipped_count for step in self.steps)


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
) -> tuple[list[T], FallbackReport]:
    policy = get_contract_policy(contract_name)
    merged_items = [item.model_copy(deep=True) for item in base_items]
    reports: list[ProviderMergeStats] = []
    for step in steps:
        if step.name not in policy.source_order:
            continue
        requests = request_builder(merged_items)
        if requests == []:
            record_provider_event(
                contract_name,
                step.name,
                "skipped",
                {"reason": "request_builder_empty"},
            )
            reports.append(
                ProviderMergeStats(
                    name=step.name,
                    request_count=0,
                    fetched_row_count=0,
                    added_count=0,
                    filled_field_count=0,
                    conflict_count=0,
                    skipped_count=1,
                    error_count=0,
                )
            )
            break
        fetched_row_count = 0
        added_count = 0
        filled_field_count = 0
        conflict_count = 0
        error_count = 0
        for request in requests:
            try:
                fetched_items = step.fetcher(*request)
            except Exception as exc:
                error_count += 1
                record_provider_event(
                    contract_name,
                    step.name,
                    "error",
                    {
                        "request": list(request),
                        "error": str(exc),
                    },
                )
                continue
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
                },
            )
        reports.append(
            ProviderMergeStats(
                name=step.name,
                request_count=len(requests),
                fetched_row_count=fetched_row_count,
                added_count=added_count,
                filled_field_count=filled_field_count,
                conflict_count=conflict_count,
                skipped_count=0,
                error_count=error_count,
            )
        )
    return merged_items, FallbackReport(contract_name=contract_name, steps=tuple(reports))


def run_fallback_chain(
    contract_name: str,
    base_items: Sequence[T],
    key_fields: tuple[str, ...],
    request_builder: Callable[[list[T]], list[tuple[object, ...]]],
    steps: Sequence[ProviderStep[T]],
) -> list[T]:
    merged_items, _ = _run_fallback_chain_internal(contract_name, base_items, key_fields, request_builder, steps)
    return merged_items


def run_fallback_chain_with_report(
    contract_name: str,
    base_items: Sequence[T],
    key_fields: tuple[str, ...],
    request_builder: Callable[[list[T]], list[tuple[object, ...]]],
    steps: Sequence[ProviderStep[T]],
) -> tuple[list[T], FallbackReport]:
    return _run_fallback_chain_internal(contract_name, base_items, key_fields, request_builder, steps)

