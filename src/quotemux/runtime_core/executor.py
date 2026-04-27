from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import inspect
import time
from typing import Callable, Generic, Mapping, Sequence, TypeVar

from pydantic import BaseModel

from quotemux.config_runtime.models import SourceInstanceConfig
from quotemux.config_runtime.runtime import get_config_runtime
from quotemux.runtime_core.audit import record_provider_event
from quotemux.contracts.policies import get_contract_policy
from quotemux.contracts.strategies import MERGE_STRATEGY_APPEND_DEDUPE, MERGE_STRATEGY_FIELD_CONSENSUS, MERGE_STRATEGY_FIRST_SUCCESS, MERGE_STRATEGY_FRESHEST_WINS, MERGE_STRATEGY_PRIORITY_FALLBACK, MERGE_STRATEGY_RAW_PASSTHROUGH, normalize_merge_strategy
from quotemux.settings import QuoteMuxSettings
from quotemux.source_packages.registry import get_default_source_package_registry


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


@dataclass(frozen=True)
class ProviderFetchResult(Generic[T]):
    step: ProviderStep[T]
    items: tuple[T, ...]
    request_count: int
    error_count: int
    elapsed_ms: float

    @property
    def fetched_row_count(self) -> int:
        return len(self.items)


class SourceInstanceExecutor:
    def __init__(self, settings: QuoteMuxSettings) -> None:
        self._settings = settings

    def build_steps(
        self,
        contract_name: str,
        handlers: Mapping[str, object],
        fallback_order: tuple[str, ...],
    ) -> tuple[ProviderStep[T], ...]:
        registry = get_default_source_package_registry()
        steps: list[ProviderStep[T]] = []
        for instance in self._settings.get_contract_source_instances(contract_name, fallback_order):
            try:
                manifest = registry.get_manifest(instance.package_id)
                handler_name = manifest.get_handler_name_for_capability(contract_name)
            except KeyError:
                continue
            fetcher_builder = handlers.get(handler_name)
            if fetcher_builder is None:
                legacy_handler = handlers.get(instance.package_id)
                if isinstance(legacy_handler, tuple) and len(legacy_handler) == 2 and legacy_handler[0] == handler_name:
                    fetcher_builder = legacy_handler[1]
            if fetcher_builder is None:
                continue
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


def _is_empty_value(value: object) -> bool:
    return value in {None, ""}


def _is_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _weighted_median(values: list[tuple[float, float]]) -> float:
    ordered = sorted(values, key=lambda item: item[0])
    total_weight = sum(weight for _, weight in ordered)
    if total_weight <= 0:
        return ordered[0][0]
    cursor = 0.0
    for value, weight in ordered:
        cursor += weight
        if cursor >= total_weight / 2.0:
            return value
    return ordered[-1][0]


def _result_score(result: ProviderFetchResult[T], order_index: int) -> float:
    if result.fetched_row_count == 0 or result.error_count >= result.request_count:
        return 0.0
    source_trust = {
        "tushare": 0.86,
        "opentdx": 0.82,
        "efinance": 0.72,
        "mootdx": 0.68,
        "akshare": 0.62,
    }.get(result.step.name, 0.5)
    latency_score = 1.0 / (1.0 + result.elapsed_ms / 1000.0)
    priority_score = 1.0 / (1.0 + order_index)
    error_score = 1.0 - (result.error_count / max(1, result.request_count))
    presence_score = min(1.0, result.fetched_row_count)
    return round(source_trust * 0.45 + priority_score * 0.2 + latency_score * 0.2 + error_score * 0.1 + presence_score * 0.05, 6)


def _select_best_result(results: Sequence[ProviderFetchResult[T]], strategy: str) -> ProviderFetchResult[T] | None:
    valid_results = [result for result in results if result.fetched_row_count > 0]
    if valid_results == []:
        return None
    if strategy == MERGE_STRATEGY_FIRST_SUCCESS:
        return valid_results[0]
    if strategy == MERGE_STRATEGY_FRESHEST_WINS:
        freshest_result = max(valid_results, key=_result_latest_marker)
        if _result_latest_marker(freshest_result) != "":
            return freshest_result
    scores = {result.step.step_id: _result_score(result, index) for index, result in enumerate(results)}
    return max(valid_results, key=lambda item: scores[item.step.step_id])


def _merge_consensus_items(results: Sequence[ProviderFetchResult[T]], key_fields: tuple[str, ...], strategy: str) -> tuple[list[T], int]:
    scored_results = [(result, _result_score(result, index)) for index, result in enumerate(results) if result.fetched_row_count > 0]
    if scored_results == []:
        return [], 0
    keyed_payloads: dict[tuple[object, ...], list[tuple[dict[str, object], float, type[T]]]] = {}
    for result, score in scored_results:
        for item in result.items:
            payload = item.model_dump()
            key = tuple(payload[field] for field in key_fields)
            keyed_payloads.setdefault(key, []).append((payload, score, type(item)))
    merged_items: list[T] = []
    conflict_count = 0
    for payloads in keyed_payloads.values():
        template_payload, _, model_type = max(payloads, key=lambda item: item[1])
        merged_payload = dict(template_payload)
        for field_name in template_payload:
            if field_name in key_fields:
                continue
            values = [(payload[field_name], score) for payload, score, _ in payloads if not _is_empty_value(payload[field_name])]
            unique_values = {repr(value) for value, _ in values}
            if len(unique_values) > 1:
                conflict_count += 1
            if values == []:
                continue
            if strategy == MERGE_STRATEGY_FIELD_CONSENSUS and all(_is_number(value) for value, _ in values):
                merged_payload[field_name] = _weighted_median([(float(value), score) for value, score in values])
                continue
            ranked_values: dict[str, tuple[object, float]] = {}
            for value, score in values:
                value_key = repr(value)
                current_value, current_score = ranked_values.get(value_key, (value, 0.0))
                ranked_values[value_key] = (current_value, current_score + score)
            merged_payload[field_name] = max(ranked_values.values(), key=lambda item: item[1])[0]
        merged_items.append(model_type(**merged_payload))
    return merged_items, conflict_count


def _fetch_step_requests(contract_name: str, step: ProviderStep[T], requests: list[tuple[object, ...]], snapshot) -> ProviderFetchResult[T]:
    fetched_items: list[T] = []
    error_count = 0
    elapsed_ms = 0.0
    for request in requests:
        started_at = time.perf_counter()
        try:
            fetched_items.extend(step.fetcher(*request))
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
    return ProviderFetchResult(step=step, items=tuple(fetched_items), request_count=len(requests), error_count=error_count, elapsed_ms=round(elapsed_ms, 3))


def _run_consensus_race_internal(
    contract_name: str,
    base_items: Sequence[T],
    key_fields: tuple[str, ...],
    request_builder: Callable[[list[T]], list[tuple[object, ...]]],
    steps: Sequence[ProviderStep[T]],
    source_order: tuple[str, ...],
    merge_strategy: str,
) -> tuple[list[T], FallbackReport]:
    snapshot = get_config_runtime().get_active_snapshot()
    ordered_steps = _order_steps(steps, source_order)
    requests = request_builder([item.model_copy(deep=True) for item in base_items])
    if requests == []:
        reports = tuple(
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
            for step in ordered_steps
        )
        return [item.model_copy(deep=True) for item in base_items], FallbackReport(contract_name=contract_name, profile_id=snapshot.profile_id, profile_version=snapshot.version, steps=reports)
    results: list[ProviderFetchResult[T]] = []
    if ordered_steps != ():
        with ThreadPoolExecutor(max_workers=len(ordered_steps)) as executor:
            futures = [executor.submit(_fetch_step_requests, contract_name, step, requests, snapshot) for step in ordered_steps]
            for future in as_completed(futures):
                results.append(future.result())
    result_order = {step.step_id: index for index, step in enumerate(ordered_steps)}
    results = sorted(results, key=lambda item: result_order[item.step.step_id])
    merge_strategy = normalize_merge_strategy(merge_strategy)
    if merge_strategy in {MERGE_STRATEGY_FIRST_SUCCESS, MERGE_STRATEGY_FRESHEST_WINS, MERGE_STRATEGY_RAW_PASSTHROUGH}:
        best_result = _select_best_result(results, merge_strategy)
        merged_items = [item.model_copy(deep=True) for item in base_items]
        conflict_count = 0
        if best_result is not None:
            merged_items, _, _, conflict_count = _merge_model_lists(merged_items, list(best_result.items), key_fields)
            adopted_step_ids = {best_result.step.step_id}
        else:
            adopted_step_ids = set()
    elif merge_strategy == MERGE_STRATEGY_FIELD_CONSENSUS:
        consensus_items, conflict_count = _merge_consensus_items(results, key_fields, merge_strategy)
        merged_items, _, _, base_conflict_count = _merge_model_lists([item.model_copy(deep=True) for item in base_items], consensus_items, key_fields)
        conflict_count += base_conflict_count
        adopted_step_ids = {result.step.step_id for result in results if result.fetched_row_count > 0}
    elif merge_strategy == MERGE_STRATEGY_APPEND_DEDUPE:
        merged_items = [item.model_copy(deep=True) for item in base_items]
        conflict_count = 0
        adopted_step_ids: set[str] = set()
        for result in results:
            merged_items, _, _, current_conflict_count = _merge_model_lists(merged_items, list(result.items), key_fields)
            conflict_count += current_conflict_count
            if result.fetched_row_count > 0:
                adopted_step_ids.add(result.step.step_id)
    else:
        return _run_fallback_chain_internal(contract_name, base_items, key_fields, request_builder, steps, source_order)
    reports: list[ProviderMergeStats] = []
    for result in results:
        adopted = result.step.step_id in adopted_step_ids
        record_provider_event(
            contract_name,
            result.step.name,
            "success" if result.error_count < result.request_count else "error",
            {
                "request_count": result.request_count,
                "fetched_row_count": result.fetched_row_count,
                "provider_hit": adopted and result.fetched_row_count > 0,
                "merge_strategy": merge_strategy,
                "elapsed_ms": result.elapsed_ms,
                "profile_id": snapshot.profile_id,
                "profile_version": snapshot.version,
                "package_id": result.step.name,
                "source_instance_id": result.step.step_id,
                "handler": result.step.handler,
            },
        )
        reports.append(
            ProviderMergeStats(
                name=result.step.name,
                package_id=result.step.name,
                source_instance_id=result.step.step_id,
                handler=result.step.handler,
                request_count=result.request_count,
                fetched_row_count=result.fetched_row_count,
                added_count=result.fetched_row_count if adopted else 0,
                filled_field_count=0,
                conflict_count=conflict_count if adopted else 0,
                skipped_count=0,
                error_count=result.error_count,
                elapsed_ms=result.elapsed_ms,
            )
        )
    return merged_items, FallbackReport(contract_name=contract_name, profile_id=snapshot.profile_id, profile_version=snapshot.version, steps=tuple(reports))


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
    merge_strategy = normalize_merge_strategy(snapshot.get_contract_merge_strategy(contract_name, policy.merge_strategy))
    if merge_strategy != MERGE_STRATEGY_PRIORITY_FALLBACK:
        return _run_consensus_race_internal(contract_name, base_items, key_fields, request_builder, steps, ordered_names, merge_strategy)
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


def _result_latest_marker(result: ProviderFetchResult[T]) -> str:
    marker = ""
    candidate_fields = ("trade_time", "trade_date", "report_period", "announce_date", "announcement_time", "crawl_time", "effective_date")
    for item in result.items:
        payload = item.model_dump()
        for field_name in candidate_fields:
            value = payload.get(field_name, "")
            if isinstance(value, str) and value > marker:
                marker = value
    return marker


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

