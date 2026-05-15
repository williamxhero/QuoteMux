from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
import json
from pathlib import Path

import pandas as pd

from quotemux.infra.config import DATALAKE_ROOT


AUDIT_ROOT = DATALAKE_ROOT / "type=cache" / "service=fallback"


def _serialize_value(value: object) -> object:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _serialize_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_serialize_value(item) for item in value]
    return value


def record_provider_event(contract_name: str, provider: str, status: str, detail: dict[str, object]) -> None:
    day_text = datetime.now().strftime("%Y%m%d")
    path = AUDIT_ROOT / "audit" / f"date={day_text}" / "events.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "logged_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "contract_name": contract_name,
        "provider": provider,
        "status": status,
        "detail": _serialize_value(detail),
    }
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def write_stage_frame(stage_name: str, contract_name: str, identity: dict[str, str], df: pd.DataFrame) -> Path:
    path = AUDIT_ROOT / stage_name / f"contract={contract_name}"
    for key, value in identity.items():
        path = path / f"{key}={value}"
    path.mkdir(parents=True, exist_ok=True)
    file_path = path / "data.parquet"
    df.to_parquet(file_path, index=False)
    return file_path


def read_fallback_summary(day_text: str = "") -> dict[str, object]:
    actual_day = day_text or datetime.now().strftime("%Y%m%d")
    path = AUDIT_ROOT / "audit" / f"date={actual_day}" / "events.jsonl"
    summary: dict[str, object] = {
        "date": actual_day,
        "event_count": 0,
        "status_counts": {},
        "provider_counts": {},
        "source_instance_counts": {},
        "contract_counts": {},
        "capability_counts": {},
        "conflict_count": 0,
        "quarantine_count": 0,
        "store_counts": {"hit": 0, "miss": 0, "write": 0},
    }
    if not path.exists():
        return summary

    status_counts: dict[str, int] = defaultdict(int)
    provider_counts: dict[str, dict[str, int]] = {}
    source_instance_counts: dict[str, dict[str, object]] = {}
    contract_counts: dict[str, dict[str, int]] = {}
    store_counts = {"hit": 0, "miss": 0, "write": 0}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        if raw_line == "":
            continue
        try:
            payload = json.loads(raw_line)
        except json.JSONDecodeError:
            continue
        status = str(payload.get("status", ""))
        provider = str(payload.get("provider", ""))
        contract_name = str(payload.get("contract_name", ""))
        detail = payload.get("detail")
        detail_dict = detail if isinstance(detail, dict) else {}
        status_counts[status] += 1
        provider_entry = provider_counts.setdefault(provider, defaultdict(int))
        provider_entry["event_count"] += 1
        provider_entry[status] += 1
        if bool(detail_dict.get("provider_hit", False)):
            provider_entry["provider_hit_count"] += 1
        package_id = str(detail_dict.get("package_id", provider))
        source_instance_id = str(detail_dict.get("source_instance_id", provider))
        handler = str(detail_dict.get("handler", ""))
        profile_id = str(detail_dict.get("profile_id", ""))
        profile_version = str(detail_dict.get("profile_version", ""))
        instance_key = "|".join([profile_id, contract_name, package_id, source_instance_id, handler])
        instance_entry = source_instance_counts.setdefault(
            instance_key,
            {
                "profile_id": profile_id,
                "profile_version": profile_version,
                "contract_name": contract_name,
                "package_id": package_id,
                "source_instance_id": source_instance_id,
                "handler": handler,
                "event_count": 0,
                "request_count": 0,
                "success_count": 0,
                "error_count": 0,
                "elapsed_ms": 0.0,
            },
        )
        instance_entry["event_count"] = int(instance_entry["event_count"]) + 1
        instance_entry["request_count"] = int(instance_entry["request_count"]) + int(detail_dict.get("request_count", 0))
        if status == "success":
            instance_entry["success_count"] = int(instance_entry["success_count"]) + int(detail_dict.get("request_count", 0))
        if status == "error":
            instance_entry["error_count"] = int(instance_entry["error_count"]) + 1
        instance_entry["elapsed_ms"] = round(float(instance_entry["elapsed_ms"]) + float(detail_dict.get("elapsed_ms", 0.0)), 3)
        contract_entry = contract_counts.setdefault(contract_name, defaultdict(int))
        contract_entry["event_count"] += 1
        contract_entry[status] += 1
        if bool(detail_dict.get("provider_hit", False)):
            contract_entry["provider_hit_count"] += 1
        if status == "store_hit":
            store_counts["hit"] += 1
        if status == "store_miss":
            store_counts["miss"] += 1
        if status == "store_write":
            store_counts["write"] += 1

    summary["event_count"] = int(sum(status_counts.values()))
    summary["status_counts"] = dict(status_counts)
    summary["provider_counts"] = {
        key: dict(value)
        for key, value in sorted(provider_counts.items())
    }
    summary["source_instance_counts"] = {
        key: value
        for key, value in sorted(source_instance_counts.items())
    }
    summary["contract_counts"] = {
        key: dict(value)
        for key, value in sorted(contract_counts.items())
    }
    summary["capability_counts"] = dict(summary["contract_counts"])
    summary["conflict_count"] = int(status_counts.get("conflict", 0))
    summary["quarantine_count"] = int(status_counts.get("quarantine", 0))
    summary["store_counts"] = store_counts
    return summary

