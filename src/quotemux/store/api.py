from __future__ import annotations

from datetime import time

from quotemux.store.admin import CachePolicyUpdate, CapturePolicyPayload, QuoteMuxCacheAdmin, QuoteMuxCaptureAdmin


def get_admin_cache_policies() -> tuple[dict[str, object], ...]:
    return QuoteMuxCacheAdmin().list_policies()


def get_admin_cache_policy(capability_id: str) -> dict[str, object]:
    return QuoteMuxCacheAdmin().get_policy(capability_id)


def put_admin_cache_policy(capability_id: str, payload: dict[str, object]) -> dict[str, object]:
    ttl_seconds = payload.get("ttl_seconds")
    return QuoteMuxCacheAdmin().update_policy(
        CachePolicyUpdate(
            capability_id=capability_id,
            enabled=bool(payload.get("enabled", False)),
            ttl_seconds=None if ttl_seconds is None else int(ttl_seconds),
        )
    )


def get_admin_cache_status() -> tuple[dict[str, object], ...]:
    return QuoteMuxCacheAdmin().list_status()


def get_admin_cache_audit(capability_id: str = "", event_type: str = "", limit: int = 100) -> tuple[dict[str, object], ...]:
    return QuoteMuxCacheAdmin().list_audit(capability_id, event_type, limit)


def get_admin_capture_policies() -> tuple[dict[str, object], ...]:
    return QuoteMuxCaptureAdmin().list_policies()


def get_admin_capture_overview() -> tuple[dict[str, object], ...]:
    return QuoteMuxCaptureAdmin().list_overview()


def get_admin_capture_policy(capability_id: str) -> dict[str, object]:
    return QuoteMuxCaptureAdmin().get_policy(capability_id)


def put_admin_capture_policy(capability_id: str, payload: dict[str, object]) -> dict[str, object]:
    admin = QuoteMuxCaptureAdmin()
    current = admin.get_policy(capability_id)
    run_time_value = payload.get("run_time", current["run_time"])
    run_time = _parse_run_time(str(run_time_value))
    return admin.update_policy(
        CapturePolicyPayload(
            capability_id=capability_id,
            enabled=bool(payload.get("enabled", current["enabled"])),
            cadence=str(payload.get("cadence", current["cadence"])),
            run_time=run_time,
            timezone=str(payload.get("timezone", current["timezone"])),
            weekday=_optional_int(payload.get("weekday", current["weekday"])),
            month=_optional_int(payload.get("month", current["month"])),
            month_day=_optional_int(payload.get("month_day", current["month_day"])),
            scope_profile=str(payload.get("scope_profile", current["scope_profile"])),
            window_count=int(payload.get("window_count", current["window_count"])),
            batch_size=int(payload.get("batch_size", current["batch_size"])),
            notes=str(payload.get("notes", current["notes"])),
        )
    )


def get_admin_capture_runs(capability_id: str = "", status: str = "", limit: int = 100) -> tuple[dict[str, object], ...]:
    return QuoteMuxCaptureAdmin().list_runs(capability_id, status, limit)


def post_admin_run_capture(capability_id: str) -> dict[str, object]:
    return QuoteMuxCaptureAdmin().run_capture(capability_id)


def post_admin_run_due_captures() -> tuple[dict[str, object], ...]:
    return QuoteMuxCaptureAdmin().run_due_captures()


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    return int(value)


def _parse_run_time(value: str) -> time:
    parts = value.split(":")
    if len(parts) == 2:
        return time(int(parts[0]), int(parts[1]))
    return time(int(parts[0]), int(parts[1]), int(parts[2]))
