from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import time

from quotemux.store.admin import CachePolicyUpdate, CapturePolicyPayload, QuoteMuxCacheAdmin, QuoteMuxCaptureAdmin


CADENCE_DAILY = "daily"
CADENCE_WEEKLY = "weekly"
CADENCE_MONTHLY = "monthly"
CADENCE_YEARLY = "yearly"
TIMEZONE = "Asia/Shanghai"
DEFAULT_RUN_TIME = time(0, 0)


@dataclass(frozen=True)
class FirstBatchCapturePolicy:
    capability_id: str
    cadence: str
    run_time: time
    weekday: int | None
    month: int | None
    month_day: int | None
    window_count: int
    batch_size: int
    notes: str


FIRST_BATCH_CAPTURE_POLICIES = (
    FirstBatchCapturePolicy("stocks.quotes.intraday", CADENCE_DAILY, DEFAULT_RUN_TIME, None, None, None, 5, 100, "第一批：分钟 K 线，provider 历史窗口有限，每天补最近 5 个交易日"),
    FirstBatchCapturePolicy("stocks.quotes.daily", CADENCE_DAILY, DEFAULT_RUN_TIME, None, None, None, 30, 100, "第一批：股票日线，每天补最近 30 个交易日"),
    FirstBatchCapturePolicy("stocks.quotes.daily_snapshot", CADENCE_DAILY, DEFAULT_RUN_TIME, None, None, None, 5, 1, "第一批：股票全市场日快照，每天补最近 5 个交易日"),
    FirstBatchCapturePolicy("indexes.quotes.daily", CADENCE_DAILY, DEFAULT_RUN_TIME, None, None, None, 30, 100, "第一批：指数日线，每天补最近 30 个交易日"),
    FirstBatchCapturePolicy("boards.quotes.daily", CADENCE_DAILY, DEFAULT_RUN_TIME, None, None, None, 30, 100, "第一批：板块日线，每天补最近 30 个交易日"),
    FirstBatchCapturePolicy("markets.calendar.trading", CADENCE_MONTHLY, DEFAULT_RUN_TIME, None, None, 31, 2, 1, "第一批：交易日历，每月最后一天维护当前和下一年度窗口"),
    FirstBatchCapturePolicy("markets.calendar.trading.previous", CADENCE_DAILY, DEFAULT_RUN_TIME, None, None, None, 30, 1, "第一批：最近历史交易日，每天维护采集窗口"),
    FirstBatchCapturePolicy("markets.calendar.trading.next", CADENCE_DAILY, DEFAULT_RUN_TIME, None, None, None, 30, 1, "第一批：未来交易日，每天维护调度和展示窗口"),
    FirstBatchCapturePolicy("markets.calendar.trading.yearly", CADENCE_YEARLY, DEFAULT_RUN_TIME, None, 12, 31, 2, 1, "第一批：年度交易日历，每年最后一天维护"),
    FirstBatchCapturePolicy("markets.trading.sessions", CADENCE_MONTHLY, DEFAULT_RUN_TIME, None, None, 31, 1, 1, "第一批：交易时段，低频参考数据"),
    FirstBatchCapturePolicy("stocks.catalog", CADENCE_MONTHLY, DEFAULT_RUN_TIME, None, None, 31, 1, 1, "第一批：股票目录，采集股票范围依赖"),
    FirstBatchCapturePolicy("stocks.catalog.archive", CADENCE_MONTHLY, DEFAULT_RUN_TIME, None, None, 31, 1, 1, "第一批：股票目录归档，采集股票范围依赖"),
    FirstBatchCapturePolicy("stocks.profile.basic", CADENCE_MONTHLY, DEFAULT_RUN_TIME, None, None, 31, 1, 100, "第一批：股票基础资料，目录和展示依赖"),
    FirstBatchCapturePolicy("stocks.reference.bse_code_mappings", CADENCE_MONTHLY, DEFAULT_RUN_TIME, None, None, 31, 1, 1, "第一批：北交所代码映射，低频参考数据"),
    FirstBatchCapturePolicy("stocks.reference.hk_connect_targets", CADENCE_MONTHLY, DEFAULT_RUN_TIME, None, None, 31, 1, 1, "第一批：沪深港通标的，低频参考数据"),
    FirstBatchCapturePolicy("indexes.catalog", CADENCE_MONTHLY, DEFAULT_RUN_TIME, None, None, 31, 1, 1, "第一批：指数目录，指数日线采集依赖"),
    FirstBatchCapturePolicy("indexes.profile", CADENCE_MONTHLY, DEFAULT_RUN_TIME, None, None, 31, 1, 100, "第一批：指数资料，指数展示依赖"),
    FirstBatchCapturePolicy("indexes.members", CADENCE_WEEKLY, DEFAULT_RUN_TIME, 6, None, None, 1, 100, "第一批：指数成分，每周日维护"),
    FirstBatchCapturePolicy("boards.catalog", CADENCE_MONTHLY, DEFAULT_RUN_TIME, None, None, 31, 1, 1, "第一批：板块目录，板块日线采集依赖"),
    FirstBatchCapturePolicy("boards.reference.categories", CADENCE_MONTHLY, DEFAULT_RUN_TIME, None, None, 31, 1, 1, "第一批：板块分类，板块展示依赖"),
    FirstBatchCapturePolicy("boards.profile", CADENCE_MONTHLY, DEFAULT_RUN_TIME, None, None, 31, 1, 100, "第一批：板块资料，板块展示依赖"),
    FirstBatchCapturePolicy("boards.members", CADENCE_WEEKLY, DEFAULT_RUN_TIME, 6, None, None, 1, 100, "第一批：板块成分，每周日维护"),
    FirstBatchCapturePolicy("boards.members.history", CADENCE_WEEKLY, DEFAULT_RUN_TIME, 6, None, None, 30, 100, "第一批：板块成分历史，每周日维护最近窗口"),
)


def first_batch_capability_ids() -> tuple[str, ...]:
    return tuple(policy.capability_id for policy in FIRST_BATCH_CAPTURE_POLICIES)


def apply_first_batch_capture_policies() -> tuple[dict[str, object], ...]:
    cache_admin = QuoteMuxCacheAdmin()
    capture_admin = QuoteMuxCaptureAdmin()
    first_batch = {policy.capability_id: policy for policy in FIRST_BATCH_CAPTURE_POLICIES}
    results: list[dict[str, object]] = []
    for current in capture_admin.list_policies():
        capability_id = str(current["capability_id"])
        first_batch_policy = first_batch.get(capability_id)
        if first_batch_policy is None:
            updated = _disable_capture_policy(capture_admin, current)
            results.append({"capability_id": capability_id, "capture_enabled": False, "cache_changed": False, "policy": updated})
            continue
        cache_policy = cache_admin.update_policy(CachePolicyUpdate(capability_id, True, None))
        updated = _enable_first_batch_policy(capture_admin, current, first_batch_policy)
        results.append({"capability_id": capability_id, "capture_enabled": True, "cache_changed": True, "cache_policy": cache_policy, "policy": updated})
    return tuple(results)


def _disable_capture_policy(capture_admin: QuoteMuxCaptureAdmin, current: dict[str, object]) -> dict[str, object]:
    return capture_admin.update_policy(
        CapturePolicyPayload(
            capability_id=str(current["capability_id"]),
            enabled=False,
            cadence=str(current["cadence"]),
            run_time=_time_from_text(str(current["run_time"])),
            timezone=str(current["timezone"]),
            weekday=_optional_int(current["weekday"]),
            month=_optional_int(current["month"]),
            month_day=_optional_int(current["month_day"]),
            scope_profile=str(current["scope_profile"]),
            window_count=int(current["window_count"]),
            batch_size=int(current["batch_size"]),
            notes=str(current["notes"]),
        )
    )


def _enable_first_batch_policy(capture_admin: QuoteMuxCaptureAdmin, current: dict[str, object], first_batch_policy: FirstBatchCapturePolicy) -> dict[str, object]:
    return capture_admin.update_policy(
        CapturePolicyPayload(
            capability_id=first_batch_policy.capability_id,
            enabled=True,
            cadence=first_batch_policy.cadence,
            run_time=first_batch_policy.run_time,
            timezone=TIMEZONE,
            weekday=first_batch_policy.weekday,
            month=first_batch_policy.month,
            month_day=first_batch_policy.month_day,
            scope_profile=str(current["scope_profile"]),
            window_count=first_batch_policy.window_count,
            batch_size=first_batch_policy.batch_size,
            notes=first_batch_policy.notes,
        )
    )


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    return int(value)


def _time_from_text(value: str) -> time:
    parts = value.split(":")
    if len(parts) == 2:
        return time(int(parts[0]), int(parts[1]))
    return time(int(parts[0]), int(parts[1]), int(parts[2]))


def main() -> None:
    print(json.dumps(apply_first_batch_capture_policies(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
