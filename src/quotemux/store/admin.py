from __future__ import annotations

from dataclasses import dataclass
from datetime import time

from quotemux.capabilities import is_known_capability_id
from quotemux.store.capture import CapturePolicyUpdate, QuoteMuxCaptureJob
from quotemux.store.postgres import CACHE_NEVER_EXPIRE_TTL_SECONDS, CachePolicy, get_postgres_cache_store


@dataclass(frozen=True)
class CachePolicyUpdate:
    capability_id: str
    enabled: bool
    ttl_seconds: int | None
    read_enabled: bool | None = None
    write_enabled: bool | None = None


@dataclass(frozen=True)
class CapturePolicyPayload:
    capability_id: str
    enabled: bool
    cadence: str
    run_time: time
    timezone: str
    weekday: int | None
    month: int | None
    month_day: int | None
    scope_profile: str
    window_count: int
    batch_size: int
    notes: str


class QuoteMuxCacheAdmin:
    def __init__(self, runtime: object | None = None) -> None:
        self._runtime = runtime

    def list_policies(self) -> tuple[dict[str, object], ...]:
        return tuple(self._policy_to_dict(policy) for policy in get_postgres_cache_store().list_policies())

    def get_policy(self, capability_id: str) -> dict[str, object]:
        policy = get_postgres_cache_store().get_policy(capability_id)
        if policy is None:
            raise KeyError(f"未知缓存策略: {capability_id}")
        return self._policy_to_dict(policy)

    def update_policy(self, update: CachePolicyUpdate) -> dict[str, object]:
        if not is_known_capability_id(update.capability_id):
            raise KeyError(f"未知 capability: {update.capability_id}")
        current = get_postgres_cache_store().get_policy(update.capability_id)
        if current is None:
            raise KeyError(f"未知缓存策略: {update.capability_id}")
        policy = CachePolicy(
            capability_id=current.capability_id,
            enabled=update.enabled,
            read_enabled=update.enabled if update.read_enabled is None else update.read_enabled,
            write_enabled=update.enabled if update.write_enabled is None else update.write_enabled,
            ttl_seconds=current.ttl_seconds if update.ttl_seconds is None else update.ttl_seconds,
            time_field=current.time_field,
            key_fields=current.key_fields,
            request_scope_fields=current.request_scope_fields,
            coverage_mode=current.coverage_mode,
        )
        if not get_postgres_cache_store().update_policy(policy):
            raise RuntimeError(f"缓存策略更新失败: {update.capability_id}")
        return self._policy_to_dict(policy)

    def list_status(self) -> tuple[dict[str, object], ...]:
        return get_postgres_cache_store().list_status()

    def list_audit(self, capability_id: str = "", event_type: str = "", limit: int = 100) -> tuple[dict[str, object], ...]:
        return get_postgres_cache_store().list_audit(capability_id, event_type, limit)

    def _policy_to_dict(self, policy: CachePolicy) -> dict[str, object]:
        return {
            "capability_id": policy.capability_id,
            "enabled": policy.enabled,
            "read_enabled": policy.read_enabled,
            "write_enabled": policy.write_enabled,
            "ttl_seconds": policy.ttl_seconds,
            "time_field": policy.time_field,
            "key_fields": list(policy.key_fields),
            "request_scope_fields": list(policy.request_scope_fields),
            "coverage_mode": policy.coverage_mode,
        }


def _cache_enabled_by_ttl(ttl_seconds: int) -> bool:
    return ttl_seconds == CACHE_NEVER_EXPIRE_TTL_SECONDS or ttl_seconds > 0


class QuoteMuxCaptureAdmin:
    def __init__(self, runtime: object | None = None, job: QuoteMuxCaptureJob | None = None) -> None:
        self._job = job or QuoteMuxCaptureJob(runtime)

    def list_policies(self) -> tuple[dict[str, object], ...]:
        return self._job.list_policies()

    def list_overview(self) -> tuple[dict[str, object], ...]:
        latest_runs = {}
        for run in self._job.list_runs(limit=1000):
            capability_id = str(run["capability_id"])
            if capability_id not in latest_runs:
                latest_runs[capability_id] = run
        return tuple({**policy, "latest_run": latest_runs.get(str(policy["capability_id"]), {})} for policy in self._job.list_policies())

    def get_policy(self, capability_id: str) -> dict[str, object]:
        return self._job.get_policy(capability_id)

    def update_policy(self, payload: CapturePolicyPayload) -> dict[str, object]:
        if not is_known_capability_id(payload.capability_id):
            raise KeyError(f"未知 capability: {payload.capability_id}")
        policy = self._job.update_policy(
            CapturePolicyUpdate(
                capability_id=payload.capability_id,
                enabled=payload.enabled,
                cadence=payload.cadence,
                run_time=payload.run_time,
                timezone=payload.timezone,
                weekday=payload.weekday,
                month=payload.month,
                month_day=payload.month_day,
                scope_profile=payload.scope_profile,
                window_count=payload.window_count,
                batch_size=payload.batch_size,
                notes=payload.notes,
            )
        )
        self._sync_cache_policy(payload.capability_id, payload.enabled)
        return policy

    def _sync_cache_policy(self, capability_id: str, capture_enabled: bool) -> None:
        current = get_postgres_cache_store().get_policy(capability_id)
        if current is None:
            raise KeyError(f"未知缓存策略: {capability_id}")
        ttl_keeps_cache_enabled = _cache_enabled_by_ttl(current.ttl_seconds)
        cache_enabled = capture_enabled or ttl_keeps_cache_enabled
        QuoteMuxCacheAdmin(self).update_policy(
            CachePolicyUpdate(
                capability_id=capability_id,
                enabled=cache_enabled,
                ttl_seconds=current.ttl_seconds,
                read_enabled=cache_enabled,
                write_enabled=cache_enabled,
            )
        )

    def list_runs(self, capability_id: str = "", status: str = "", limit: int = 100) -> tuple[dict[str, object], ...]:
        return self._job.list_runs(capability_id, status, limit)

    def run_capture(self, capability_id: str) -> dict[str, object]:
        return self._job.run_capture(capability_id)

    def run_due_captures(self) -> tuple[dict[str, object], ...]:
        return self._job.run_due_captures()
