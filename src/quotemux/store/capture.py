from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
import hashlib
import json
from typing import Callable, Sequence
from zoneinfo import ZoneInfo

import pandas as pd
import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from quotemux.infra.common import format_date_value
from quotemux.infra.db.client import execute_many, execute_sql, query_dataframe
from quotemux.infra.db.config import DB_CONNECT_TIMEOUT, DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER
from quotemux.infra.db.reference_reads import load_concept_catalog_frame, load_derivable_concept_ids_frame, load_index_catalog_frame, load_stock_active_codes_frame, load_trade_calendar_frame
from quotemux.capabilities import get_capability_config_root, is_independently_configurable_capability_id
from quotemux.capabilities.inventory import list_capability_ids
from quotemux.concepts import QuoteMuxConcepts
from quotemux.fact_ref_writes import get_fact_ref_writer
from quotemux.reports import ContractReport
from quotemux.requests.indexes import IndexMembersRequest, IndexQuotesRequest
from quotemux.requests.markets import TradingCalendarRequest
from quotemux.requests.stocks import StockDailySnapshotRequest, StockQuotesRequest
from quotemux.store.planner import CacheMissingPlanner, CacheMissingRange
from quotemux.store.default_update_policy import get_capability_update_policy_default
from quotemux.store.postgres import _ensure_schema, _field_values, _is_fresh, _time_range_from_request, build_scope_identity, get_postgres_cache_store
from quotemux.store.runtime import store_result
from quotemux.store.capture_gaps import CaptureGap, CaptureGapRepository
from quotemux.source_packages.registry import get_default_source_package_registry


CAPTURE_RUNNING = "running"
CAPTURE_SUCCESS = "success"
CAPTURE_PARTIAL = "partial"
CAPTURE_FAILED = "failed"
CAPTURE_SKIPPED = "skipped"
CAPTURE_MARKET_DATA_READY_TIME = time(16, 0)

CADENCE_DAILY = "daily"
CADENCE_WEEKLY = "weekly"
CADENCE_MONTHLY = "monthly"
CADENCE_YEARLY = "yearly"
VALID_CADENCES = (CADENCE_DAILY, CADENCE_WEEKLY, CADENCE_MONTHLY, CADENCE_YEARLY)

PROFILE_ACTIVE_STOCKS_RECENT_TRADING_DAYS = "active_stocks_recent_trading_days"
PROFILE_INDEXES_RECENT_TRADING_DAYS = "indexes_recent_trading_days"
PROFILE_DAILY_SNAPSHOT_RECENT_TRADING_DAYS = "daily_snapshot_recent_trading_days"
PROFILE_TRADING_CALENDAR_YEAR_WINDOW = "trading_calendar_year_window"
PROFILE_CONCEPTS_RECENT_TRADING_DAYS = "concepts_recent_trading_days"
PROFILE_BOARDS_RECENT_TRADING_DAYS = "boards_recent_trading_days"
PROFILE_CATALOG_SNAPSHOT = "catalog_snapshot"
PROFILE_SINGLE_ENTITY_SNAPSHOT = "single_entity_snapshot"
PROFILE_MARKET_RECENT_TRADING_DAYS = "market_recent_trading_days"
PROFILE_ACTIVE_STOCKS_RECENT_REPORT_PERIODS = "active_stocks_recent_report_periods"
PROFILE_CORPORATE_ACTIONS_RECENT_ANNOUNCEMENTS = "corporate_actions_recent_announcements"
PROFILE_OWNERSHIP_RECENT_TRADING_DAYS = "ownership_recent_trading_days"
PROFILE_RESEARCH_RECENT_DATES = "research_recent_dates"
PROFILE_RESEARCH_RECENT_MONTHS = "research_recent_months"
PROFILE_TRADING_SESSIONS_SNAPSHOT = "trading_sessions_snapshot"
PROFILE_STOCK_REFERENCE_SNAPSHOT = "stock_reference_snapshot"
PROFILE_NEWS_EVENT_UPDATE = "news_event_update"

PROFILE_LABELS = {
    PROFILE_ACTIVE_STOCKS_RECENT_TRADING_DAYS: "活跃股票最近交易日",
    PROFILE_INDEXES_RECENT_TRADING_DAYS: "指数最近交易日",
    PROFILE_DAILY_SNAPSHOT_RECENT_TRADING_DAYS: "股票全市场日快照",
    PROFILE_TRADING_CALENDAR_YEAR_WINDOW: "交易日历年度窗口",
    PROFILE_CONCEPTS_RECENT_TRADING_DAYS: "题材概念最近交易日",
    PROFILE_BOARDS_RECENT_TRADING_DAYS: "行业板块最近交易日",
    PROFILE_CATALOG_SNAPSHOT: "目录快照",
    PROFILE_SINGLE_ENTITY_SNAPSHOT: "单实体快照",
    PROFILE_MARKET_RECENT_TRADING_DAYS: "市场最近交易日",
    PROFILE_ACTIVE_STOCKS_RECENT_REPORT_PERIODS: "活跃股票最近报告期",
    PROFILE_CORPORATE_ACTIONS_RECENT_ANNOUNCEMENTS: "企业行为公告窗口",
    PROFILE_OWNERSHIP_RECENT_TRADING_DAYS: "股东持仓最近交易日",
    PROFILE_RESEARCH_RECENT_DATES: "研究数据最近日期",
    PROFILE_RESEARCH_RECENT_MONTHS: "研究排行最近月份",
    PROFILE_TRADING_SESSIONS_SNAPSHOT: "交易时段快照",
    PROFILE_STOCK_REFERENCE_SNAPSHOT: "股票参考快照",
    PROFILE_NEWS_EVENT_UPDATE: "新闻专用更新",
}


@dataclass(frozen=True)
class CapturePolicy:
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


@dataclass(frozen=True)
class CaptureRun:
    id: int
    capability_id: str
    status: str
    planned_time: datetime
    started_at: datetime
    finished_at: datetime | None
    row_count: int
    coverage_count: int
    error_message: str
    detail_json: dict[str, object]


@dataclass(frozen=True)
class CapturePolicyUpdate:
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


@dataclass(frozen=True)
class DefaultCapturePolicySpec:
    capability_id: str
    enabled: bool
    cadence: str
    run_time: time
    timezone: str
    scope_profile: str
    window_count: int
    batch_size: int


@dataclass(frozen=True)
class CaptureRequest:
    capability_id: str
    request_identity: dict[str, object]


@dataclass(frozen=True)
class CaptureExecutionResult:
    row_count: int
    coverage_count: int
    partial_batches: tuple[dict[str, object], ...]
    failed_batches: tuple[dict[str, object], ...]


@dataclass(frozen=True)
class _CaptureRuntimeReport:
    contract_name: str
    store_write_count: int = 1


@dataclass(frozen=True)
class _CaptureBatchResult:
    items: tuple[object, ...]
    store_write_count: int
    partial_issues: tuple[str, ...] = ()
    row_count_override: int | None = None


def _default_profile_for_capability(capability_id: str) -> str:
    if capability_id == "futures.quotes.main_continuous.1m":
        return PROFILE_MARKET_RECENT_TRADING_DAYS
    if capability_id in {"stocks.quotes.daily", "stocks.quotes.intraday"}:
        return PROFILE_ACTIVE_STOCKS_RECENT_TRADING_DAYS
    if capability_id == "stocks.quotes.daily_snapshot":
        return PROFILE_DAILY_SNAPSHOT_RECENT_TRADING_DAYS
    if capability_id in {"indexes.quotes.daily", "indexes.members"}:
        return PROFILE_INDEXES_RECENT_TRADING_DAYS
    if capability_id in {"concepts.quotes.daily", "concepts.members", "concepts.members.history", "concepts.indicators.money_flow"}:
        return PROFILE_CONCEPTS_RECENT_TRADING_DAYS
    if capability_id == "boards.quotes.daily":
        return PROFILE_BOARDS_RECENT_TRADING_DAYS
    if capability_id == "markets.calendar.trading":
        return PROFILE_TRADING_CALENDAR_YEAR_WINDOW
    if capability_id in {"futures.contracts.catalog", "futures.contracts.main_mapping", "stocks.catalog", "stocks.catalog.archive", "indexes.catalog", "concepts.catalog", "concepts.reference.categories", "markets.participants.hot_money"}:
        return PROFILE_CATALOG_SNAPSHOT
    if capability_id in {"stocks.profile.basic", "stocks.profile.company", "stocks.profile.managers", "stocks.profile.management_rewards", "stocks.profile.name_history", "indexes.profile", "concepts.profile"}:
        return PROFILE_SINGLE_ENTITY_SNAPSHOT
    if capability_id.startswith("stocks.reference."):
        return PROFILE_STOCK_REFERENCE_SNAPSHOT
    if capability_id in {"markets.trading.sessions"}:
        return PROFILE_TRADING_SESSIONS_SNAPSHOT
    if capability_id.startswith("stocks.finance.") or capability_id in {"stocks.ownership.shareholders.top10", "stocks.ownership.shareholders.top10_float"}:
        return PROFILE_ACTIVE_STOCKS_RECENT_REPORT_PERIODS
    if capability_id.startswith("stocks.corporate_actions."):
        return PROFILE_CORPORATE_ACTIONS_RECENT_ANNOUNCEMENTS
    if capability_id.startswith("stocks.ownership."):
        return PROFILE_OWNERSHIP_RECENT_TRADING_DAYS
    if capability_id in {"stocks.research.reports", "stocks.research.surveys", "rankings.research.reports"}:
        return PROFILE_RESEARCH_RECENT_DATES
    if capability_id == "rankings.research.broker_monthly_picks":
        return PROFILE_RESEARCH_RECENT_MONTHS
    if capability_id == "markets.events.news":
        return PROFILE_NEWS_EVENT_UPDATE
    if capability_id.startswith("markets.") or capability_id == "concepts.indicators.money_flow.snapshot":
        return PROFILE_MARKET_RECENT_TRADING_DAYS
    if capability_id.startswith("stocks."):
        return PROFILE_ACTIVE_STOCKS_RECENT_TRADING_DAYS
    return PROFILE_MARKET_RECENT_TRADING_DAYS


def _default_cadence_for_profile(scope_profile: str) -> str:
    if scope_profile in {PROFILE_CATALOG_SNAPSHOT, PROFILE_SINGLE_ENTITY_SNAPSHOT, PROFILE_STOCK_REFERENCE_SNAPSHOT, PROFILE_TRADING_SESSIONS_SNAPSHOT, PROFILE_TRADING_CALENDAR_YEAR_WINDOW}:
        return CADENCE_MONTHLY
    if scope_profile in {PROFILE_ACTIVE_STOCKS_RECENT_REPORT_PERIODS, PROFILE_RESEARCH_RECENT_MONTHS}:
        return CADENCE_WEEKLY
    return CADENCE_DAILY


def _default_window_for_profile(scope_profile: str, capability_id: str) -> int:
    if capability_id == "futures.quotes.main_continuous.1m":
        return 2
    if capability_id == "stocks.quotes.intraday":
        return 5
    if scope_profile in {PROFILE_ACTIVE_STOCKS_RECENT_TRADING_DAYS, PROFILE_INDEXES_RECENT_TRADING_DAYS, PROFILE_CONCEPTS_RECENT_TRADING_DAYS, PROFILE_BOARDS_RECENT_TRADING_DAYS, PROFILE_MARKET_RECENT_TRADING_DAYS, PROFILE_OWNERSHIP_RECENT_TRADING_DAYS, PROFILE_RESEARCH_RECENT_DATES, PROFILE_CORPORATE_ACTIONS_RECENT_ANNOUNCEMENTS}:
        return 30
    if scope_profile == PROFILE_DAILY_SNAPSHOT_RECENT_TRADING_DAYS:
        return 5
    if scope_profile == PROFILE_ACTIVE_STOCKS_RECENT_REPORT_PERIODS:
        return 8
    if scope_profile == PROFILE_RESEARCH_RECENT_MONTHS:
        return 6
    if scope_profile == PROFILE_TRADING_CALENDAR_YEAR_WINDOW:
        return 2
    return 1


def _default_batch_size_for_profile(scope_profile: str) -> int:
    if scope_profile in {PROFILE_CATALOG_SNAPSHOT, PROFILE_DAILY_SNAPSHOT_RECENT_TRADING_DAYS, PROFILE_TRADING_CALENDAR_YEAR_WINDOW, PROFILE_TRADING_SESSIONS_SNAPSHOT, PROFILE_STOCK_REFERENCE_SNAPSHOT, PROFILE_MARKET_RECENT_TRADING_DAYS, PROFILE_RESEARCH_RECENT_MONTHS, PROFILE_NEWS_EVENT_UPDATE}:
        return 1
    return 100


def _build_default_capture_policy_specs() -> tuple[DefaultCapturePolicySpec, ...]:
    specs: list[DefaultCapturePolicySpec] = []
    for capability_id in list_capability_ids():
        if not is_independently_configurable_capability_id(capability_id):
            continue
        scope_profile = _default_profile_for_capability(capability_id)
        policy_default = get_capability_update_policy_default(capability_id)
        run_time = time(0, 30) if capability_id == "futures.quotes.main_continuous.1m" else time(18, 0)
        specs.append(
            DefaultCapturePolicySpec(
                capability_id,
                policy_default.capture_enabled,
                policy_default.capture_cadence,
                run_time,
                "Asia/Shanghai",
                scope_profile,
                _default_window_for_profile(scope_profile, capability_id),
                _default_batch_size_for_profile(scope_profile),
            )
        )
    return tuple(specs)


DEFAULT_CAPTURE_POLICY_SPECS: tuple[DefaultCapturePolicySpec, ...] = _build_default_capture_policy_specs()


CAPTURE_SCHEMA_SQL = (
    """
    create table if not exists capability_capture_policy (
        capability_id text primary key references capability_cache_policy(capability_id),
        enabled boolean not null default false,
        cadence text not null,
        run_time time not null,
        timezone text not null default 'Asia/Shanghai',
        weekday integer,
        month integer,
        month_day integer,
        scope_profile text not null,
        window_count integer not null,
        batch_size integer not null,
        notes text not null default '',
        managed_by_default boolean not null default true,
        created_at timestamp without time zone not null default now(),
        updated_at timestamp without time zone not null default now()
    )
    """,
    "alter table capability_capture_policy add column if not exists month integer",
    "alter table capability_capture_policy add column if not exists managed_by_default boolean",
    "create unique index if not exists idx_capture_policy_capability_id_unique on capability_capture_policy (capability_id)",
    """
    create table if not exists capability_capture_runs (
        id bigserial primary key,
        capability_id text not null,
        status text not null,
        planned_time timestamp without time zone not null,
        started_at timestamp without time zone not null default now(),
        finished_at timestamp without time zone,
        row_count integer not null default 0,
        coverage_count integer not null default 0,
        error_message text not null default '',
        detail_json jsonb not null default '{}'::jsonb
    )
    """,
    "create index if not exists idx_capture_runs_capability_time on capability_capture_runs (capability_id, started_at desc)",
    "create index if not exists idx_capture_runs_status_time on capability_capture_runs (status, started_at desc)",
)


_CACHE_MISSING_PLANNER = CacheMissingPlanner()


_CAPTURE_SCHEMA_READY = False
_CAPTURE_SCHEMA_FAILED = False

_LEGACY_CAPTURE_ENABLED_OVERRIDES = {
    # This was enabled before the batch endpoint became the scheduled default.
    "stocks.indicators.money_flow": True,
}


def _ensure_capture_schema() -> bool:
    global _CAPTURE_SCHEMA_FAILED, _CAPTURE_SCHEMA_READY
    if _CAPTURE_SCHEMA_READY:
        return True
    if _CAPTURE_SCHEMA_FAILED:
        return False
    if not _ensure_schema():
        _CAPTURE_SCHEMA_FAILED = True
        return False
    for statement in CAPTURE_SCHEMA_SQL:
        if not execute_sql(statement):
            _CAPTURE_SCHEMA_FAILED = True
            return False
    # Existing rows predate provenance. Only rows that still exactly match the
    # former shipped defaults are safe to keep managing; every other row is a
    # user configuration and must survive future default changes untouched.
    legacy_params = [
        (
            spec.capability_id,
            _LEGACY_CAPTURE_ENABLED_OVERRIDES.get(spec.capability_id, spec.enabled),
            spec.cadence,
            spec.run_time,
            spec.timezone,
            spec.scope_profile,
            spec.window_count,
            spec.batch_size,
        )
        for spec in DEFAULT_CAPTURE_POLICY_SPECS
    ]
    if not execute_many(
        """
        update capability_capture_policy
        set managed_by_default = true
        where managed_by_default is null
          and capability_id = %s
          and enabled = %s
          and cadence = %s
          and run_time = %s
          and timezone = %s
          and weekday is null
          and month is null
          and month_day is null
          and scope_profile = %s
          and window_count = %s
          and batch_size = %s
          and notes = ''
        """,
        legacy_params,
    ):
        _CAPTURE_SCHEMA_FAILED = True
        return False
    for statement in (
        "update capability_capture_policy set managed_by_default = false where managed_by_default is null",
        "alter table capability_capture_policy alter column managed_by_default set default true, alter column managed_by_default set not null",
    ):
        if not execute_sql(statement):
            _CAPTURE_SCHEMA_FAILED = True
            return False
    params = [
        (
            spec.capability_id,
            spec.enabled,
            spec.cadence,
            spec.run_time,
            spec.timezone,
            None,
            None,
            None,
            spec.scope_profile,
            spec.window_count,
            spec.batch_size,
            "",
            True,
        )
        for spec in DEFAULT_CAPTURE_POLICY_SPECS
    ]
    ok = execute_many(
        """
        insert into capability_capture_policy (
            capability_id, enabled, cadence, run_time, timezone, weekday,
            month, month_day, scope_profile, window_count, batch_size, notes,
            managed_by_default
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        on conflict (capability_id) do update set
            scope_profile = case
                when capability_capture_policy.managed_by_default
                then excluded.scope_profile
                else capability_capture_policy.scope_profile
            end,
            cadence = capability_capture_policy.cadence,
            updated_at = now()
        """,
        params,
    )
    if ok:
        ok = execute_sql(
            """
            update capability_capture_policy single_policy
            set enabled = false,
                updated_at = now()
            where single_policy.capability_id = 'stocks.indicators.money_flow'
              and single_policy.managed_by_default
              and exists (
                  select 1
                  from capability_capture_policy batch_policy
                  where batch_policy.capability_id = 'stocks.indicators.money_flow.batch'
                    and batch_policy.enabled
              )
            """
        )
    _CAPTURE_SCHEMA_READY = ok
    _CAPTURE_SCHEMA_FAILED = not ok
    return ok


def _is_empty_dataframe(frame: pd.DataFrame) -> bool:
    return frame.empty


def _datetime_from_value(value: object) -> datetime:
    return pd.Timestamp(value).to_pydatetime()


def _time_from_value(value: object) -> time:
    if isinstance(value, time):
        return value
    if isinstance(value, str):
        parts = value.split(":")
        if len(parts) == 2:
            return time(int(parts[0]), int(parts[1]))
        return time(int(parts[0]), int(parts[1]), int(float(parts[2])))
    if isinstance(value, timedelta):
        total_seconds = int(value.total_seconds())
        return time(total_seconds // 3600, total_seconds % 3600 // 60, total_seconds % 60)
    return pd.Timestamp(value).time()


def _policy_from_row(row: dict[str, object]) -> CapturePolicy:
    return CapturePolicy(
        capability_id=str(row["capability_id"]),
        enabled=bool(row["enabled"]),
        cadence=str(row["cadence"]),
        run_time=_time_from_value(row["run_time"]),
        timezone=str(row["timezone"]),
        weekday=None if pd.isna(row["weekday"]) else int(row["weekday"]),
        month=None if pd.isna(row["month"]) else int(row["month"]),
        month_day=None if pd.isna(row["month_day"]) else int(row["month_day"]),
        scope_profile=str(row["scope_profile"]),
        window_count=int(row["window_count"]),
        batch_size=int(row["batch_size"]),
        notes=str(row["notes"]),
    )


def _run_from_row(row: dict[str, object]) -> CaptureRun:
    detail_json = row["detail_json"] if isinstance(row["detail_json"], dict) else {}
    finished_at = None if pd.isna(row["finished_at"]) else _datetime_from_value(row["finished_at"])
    return CaptureRun(
        id=int(row["id"]),
        capability_id=str(row["capability_id"]),
        status=str(row["status"]),
        planned_time=_datetime_from_value(row["planned_time"]),
        started_at=_datetime_from_value(row["started_at"]),
        finished_at=finished_at,
        row_count=int(row["row_count"]),
        coverage_count=int(row["coverage_count"]),
        error_message=str(row["error_message"]),
        detail_json=detail_json,
    )


def _serialize_value(value: object) -> object:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, dict):
        return {str(key): _serialize_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_serialize_value(item) for item in value]
    return value


def _validate_capture_policy(policy: CapturePolicy) -> None:
    if policy.cadence not in VALID_CADENCES:
        raise ValueError(f"未知 capture 周期: {policy.cadence}")
    if policy.timezone == "":
        raise ValueError("capture timezone 不能为空")
    if policy.cadence == CADENCE_WEEKLY and policy.weekday is not None and not 0 <= policy.weekday <= 6:
        raise ValueError("weekly weekday 必须在 0 到 6 之间")
    if policy.cadence == CADENCE_YEARLY and policy.month is not None and not 1 <= policy.month <= 12:
        raise ValueError("yearly month 必须在 1 到 12 之间")
    if policy.cadence in {CADENCE_MONTHLY, CADENCE_YEARLY} and policy.month_day is not None and not 1 <= policy.month_day <= 31:
        raise ValueError("month_day 必须在 1 到 31 之间")
    if policy.window_count < 1:
        raise ValueError("window_count 必须大于 0")
    if policy.batch_size < 1:
        raise ValueError("batch_size 必须大于 0")


class CapturePolicyRepository:
    def list(self) -> tuple[CapturePolicy, ...]:
        if not _ensure_capture_schema():
            return ()
        frame = query_dataframe(
            """
            select capability_id, enabled, cadence, run_time, timezone, weekday,
                   month, month_day, scope_profile, window_count, batch_size, notes
            from capability_capture_policy
            order by capability_id asc
            """,
            (),
        )
        if _is_empty_dataframe(frame):
            return ()
        return tuple(_policy_from_row(row) for row in frame.to_dict("records") if is_independently_configurable_capability_id(str(row["capability_id"])))

    def get(self, capability_id: str) -> CapturePolicy | None:
        if not _ensure_capture_schema():
            return None
        root_capability_id = get_capability_config_root(capability_id)
        frame = query_dataframe(
            """
            select capability_id, enabled, cadence, run_time, timezone, weekday,
                   month, month_day, scope_profile, window_count, batch_size, notes
            from capability_capture_policy
            where capability_id = %s
            """,
            (root_capability_id,),
        )
        if _is_empty_dataframe(frame):
            return None
        return _policy_from_row(frame.iloc[0].to_dict())

    def update(self, policy: CapturePolicy) -> bool:
        if not _ensure_capture_schema():
            return False
        root_capability_id = get_capability_config_root(policy.capability_id)
        return execute_sql(
            """
            update capability_capture_policy
            set enabled = %s,
                cadence = %s,
                run_time = %s,
                timezone = %s,
                weekday = %s,
                month = %s,
                month_day = %s,
                scope_profile = %s,
                window_count = %s,
                batch_size = %s,
                notes = %s,
                managed_by_default = false,
                updated_at = now()
            where capability_id = %s
            """,
            (
                policy.enabled,
                policy.cadence,
                policy.run_time,
                policy.timezone,
                policy.weekday,
                policy.month,
                policy.month_day,
                policy.scope_profile,
                policy.window_count,
                policy.batch_size,
                policy.notes,
                root_capability_id,
            ),
        )


class CaptureRunRepository:
    def get_by_id(self, run_id: int) -> CaptureRun | None:
        if not _ensure_capture_schema():
            return None
        frame = query_dataframe(
            """
            select id, capability_id, status, planned_time, started_at, finished_at,
                   row_count, coverage_count, error_message, detail_json
            from capability_capture_runs
            where id = %s
            limit 1
            """,
            (run_id,),
        )
        if _is_empty_dataframe(frame):
            return None
        return _run_from_row(frame.iloc[0].to_dict())

    def list(self, capability_id: str = "", status: str = "", limit: int = 100) -> tuple[CaptureRun, ...]:
        if not _ensure_capture_schema():
            return ()
        clauses: list[str] = []
        params: list[object] = []
        if capability_id != "":
            clauses.append("capability_id = %s")
            params.append(get_capability_config_root(capability_id))
        if status != "":
            clauses.append("status = %s")
            params.append(status)
        where_sql = " where " + " and ".join(clauses) if clauses else ""
        params.append(max(1, min(limit, 1000)))
        frame = query_dataframe(
            f"""
            select id, capability_id, status, planned_time, started_at, finished_at,
                   row_count, coverage_count, error_message, detail_json
            from capability_capture_runs
            {where_sql}
            order by started_at desc
            limit %s
            """,
            tuple(params),
        )
        if _is_empty_dataframe(frame):
            return ()
        return tuple(_run_from_row(row) for row in frame.to_dict("records"))

    def latest_for_planned_time(self, capability_id: str, planned_time: datetime) -> CaptureRun | None:
        if not _ensure_capture_schema():
            return None
        root_capability_id = get_capability_config_root(capability_id)
        frame = query_dataframe(
            """
            select id, capability_id, status, planned_time, started_at, finished_at,
                   row_count, coverage_count, error_message, detail_json
            from capability_capture_runs
            where capability_id = %s and planned_time = %s
            order by started_at desc
            limit 1
            """,
            (root_capability_id, planned_time),
        )
        if _is_empty_dataframe(frame):
            return None
        return _run_from_row(frame.iloc[0].to_dict())

    def latest_success_for_repair_fingerprint(self, capability_id: str, fingerprint: str) -> CaptureRun | None:
        if not _ensure_capture_schema():
            return None
        frame = query_dataframe(
            """
            select id, capability_id, status, planned_time, started_at, finished_at,
                   row_count, coverage_count, error_message, detail_json
            from capability_capture_runs
            where capability_id = %s
              and status = 'success'
              and detail_json ->> 'repair_fingerprint' = %s
            order by started_at desc
            limit 1
            """,
            (get_capability_config_root(capability_id), fingerprint),
        )
        if _is_empty_dataframe(frame):
            return None
        return _run_from_row(frame.iloc[0].to_dict())

    def list_running_started_before(self, started_before: datetime, capability_id: str = "") -> tuple[CaptureRun, ...]:
        if not _ensure_capture_schema():
            raise RuntimeError("capture schema 初始化失败")
        clauses = ["status = 'running'"]
        params: list[object] = []
        if capability_id != "":
            clauses.append("capability_id = %s")
            params.append(get_capability_config_root(capability_id))
        clauses.append("started_at <= %s")
        params.append(started_before)
        frame = query_dataframe(
            f"""
            select id, capability_id, status, planned_time, started_at, finished_at,
                   row_count, coverage_count, error_message, detail_json
            from capability_capture_runs
            where {" and ".join(clauses)}
            order by capability_id asc, started_at asc
            """,
            tuple(params),
        )
        if _is_empty_dataframe(frame):
            return ()
        return tuple(_run_from_row(row) for row in frame.to_dict("records"))

    def fail_stale_running(self, run_ids: tuple[int, ...], detail_json: dict[str, object]) -> bool:
        if run_ids == ():
            return True
        if not _ensure_capture_schema():
            return False
        return execute_sql(
            """
            update capability_capture_runs
            set status = 'failed',
                finished_at = now(),
                error_message = 'stale-reconciled: capture 进程已退出且 capability advisory lock 空闲',
                detail_json = coalesce(detail_json, '{}'::jsonb) || %s
            where id = any(%s) and status = 'running'
            """,
            (Jsonb(detail_json), list(run_ids)),
        )

    def create(self, capability_id: str, status: str, planned_time: datetime, detail_json: dict[str, object]) -> CaptureRun:
        if not _ensure_capture_schema():
            raise RuntimeError("capture schema 初始化失败")
        root_capability_id = get_capability_config_root(capability_id)
        if status == CAPTURE_RUNNING:
            execute_sql(
                """
                update capability_capture_runs
                set status = 'failed',
                    finished_at = now(),
                    error_message = 'auto-superseded: 新 capture run 启动，自动作废残留 running 行'
                where capability_id = %s and status = 'running'
                """,
                (root_capability_id,),
            )
        ok = execute_sql(
            """
            insert into capability_capture_runs (capability_id, status, planned_time, detail_json)
            values (%s, %s, %s, %s)
            """,
            (root_capability_id, status, planned_time, Jsonb(detail_json)),
        )
        if not ok:
            raise RuntimeError("capture run 创建失败")
        frame = query_dataframe(
            """
            select id, capability_id, status, planned_time, started_at, finished_at,
                   row_count, coverage_count, error_message, detail_json
            from capability_capture_runs
            where capability_id = %s and planned_time = %s
            order by started_at desc
            limit 1
            """,
            (root_capability_id, planned_time),
        )
        if _is_empty_dataframe(frame):
            raise RuntimeError("capture run 创建失败")
        return _run_from_row(frame.iloc[0].to_dict())

    def finish(self, run_id: int, status: str, row_count: int, coverage_count: int, error_message: str, detail_json: dict[str, object]) -> bool:
        if not _ensure_capture_schema():
            return False
        return execute_sql(
            """
            update capability_capture_runs
            set status = %s,
                finished_at = now(),
                row_count = %s,
                coverage_count = %s,
                error_message = %s,
                detail_json = %s
            where id = %s
            """,
            (status, row_count, coverage_count, error_message, Jsonb(detail_json), run_id),
        )


class PostgresAdvisoryLock:
    def __init__(self, capability_id: str) -> None:
        self._capability_id = capability_id
        self._connection: psycopg.Connection | None = None

    def acquire(self) -> bool:
        connection = psycopg.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            connect_timeout=DB_CONNECT_TIMEOUT,
            row_factory=dict_row,
        )
        with connection.cursor() as cursor:
            cursor.execute("select pg_try_advisory_lock(hashtext(%s)) as locked", (self._capability_id,))
            row = cursor.fetchone()
        connection.commit()
        locked = bool(row["locked"]) if isinstance(row, dict) else False
        if not locked:
            connection.close()
            return False
        self._connection = connection
        return True

    def release(self) -> None:
        if self._connection is None:
            return
        try:
            with self._connection.cursor() as cursor:
                cursor.execute("select pg_advisory_unlock(hashtext(%s))", (self._capability_id,))
            self._connection.commit()
        finally:
            self._connection.close()
            self._connection = None


class PostgresAdvisoryLockFactory:
    def create(self, capability_id: str) -> PostgresAdvisoryLock:
        return PostgresAdvisoryLock(capability_id)


class CaptureRunMaintenance:
    """Reconcile orphaned run rows without overriding a live lock owner."""

    def __init__(
        self,
        runs: CaptureRunRepository | None = None,
        locks: PostgresAdvisoryLockFactory | None = None,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        self._runs = runs or CaptureRunRepository()
        self._locks = locks or PostgresAdvisoryLockFactory()
        self._now_provider = now_provider or _current_datetime

    def reconcile_stale_running(self, started_before: datetime | None = None) -> dict[str, object]:
        cutoff = (started_before or self._now_provider()).replace(tzinfo=None)
        candidates = self._runs.list_running_started_before(cutoff)
        capability_ids = sorted({run.capability_id for run in candidates})
        reconciled_run_ids: list[int] = []
        active_capability_ids: list[str] = []

        for capability_id in capability_ids:
            lock = self._locks.create(capability_id)
            if not lock.acquire():
                active_capability_ids.append(capability_id)
                continue
            try:
                # A run may finish between the initial scan and lock acquisition.
                # Re-read while holding the same lock every capture job holds.
                stale_runs = self._runs.list_running_started_before(cutoff, capability_id)
                stale_run_ids = tuple(run.id for run in stale_runs)
                if stale_run_ids == ():
                    continue
                detail_json = {
                    "phase": "maintenance",
                    "reason": "stale_running_after_process_exit",
                    "reconciled_at": _serialize_value(cutoff),
                }
                if not self._runs.fail_stale_running(stale_run_ids, detail_json):
                    raise RuntimeError(f"capture running 状态修复失败: {capability_id}")
                reconciled_run_ids.extend(stale_run_ids)
            finally:
                lock.release()

        return {
            "started_before": _serialize_value(cutoff),
            "candidate_count": len(candidates),
            "reconciled_run_ids": reconciled_run_ids,
            "active_capability_ids": active_capability_ids,
        }


def _chunk(items: Sequence[str], size: int) -> tuple[tuple[str, ...], ...]:
    actual_size = max(1, size)
    return tuple(tuple(items[index: index + actual_size]) for index in range(0, len(items), actual_size))


def _normalized_capture_items(capability_id: str, request_identity: dict[str, object], items: Sequence[object]) -> list[object]:
    if capability_id == "concepts.members":
        trade_date = format_date_value(request_identity.get("trade_date", ""))
        return [item.model_copy(update={"join_date": trade_date}) if getattr(item, "join_date", "") == "" and trade_date != "" and hasattr(item, "model_copy") else item for item in items]
    return list(items)


def _date_range_end_text(now: datetime) -> str:
    return now.strftime("%Y-%m-%d")


def _recent_trading_days(window_count: int, now: datetime) -> tuple[str, ...]:
    end_day = now.date() if now.time() >= CAPTURE_MARKET_DATA_READY_TIME else now.date() - timedelta(days=1)
    end_text = end_day.strftime("%Y-%m-%d")
    start_day = end_day - timedelta(days=max(10, window_count * 3))
    frame = load_trade_calendar_frame("SSE", start_day.strftime("%Y-%m-%d"), end_text, True)
    if _is_empty_dataframe(frame):
        return ()
    values = [format_date_value(row["trade_date"]) for row in frame.to_dict("records")]
    return tuple(item for item in values if item != "")[-window_count:]


def _recent_calendar_days(window_count: int, now: datetime) -> tuple[str, ...]:
    if window_count < 1:
        return ()
    start_day = now.date() - timedelta(days=window_count - 1)
    return tuple((start_day + timedelta(days=offset)).strftime("%Y-%m-%d") for offset in range(window_count))


def _active_stock_codes(trade_date: str) -> tuple[str, ...]:
    frame = load_stock_active_codes_frame(trade_date)
    if _is_empty_dataframe(frame):
        return ()
    return tuple(str(row["code"]) for row in frame.to_dict("records") if str(row["code"]) != "")


def _intraday_missing_stock_codes(trade_date: str) -> tuple[str, ...]:
    frame = query_dataframe(
        """
        with expected_codes as (
            select distinct code
            from fact.stock_daily_1d
            where trade_date = %s
              and not coalesce(is_suspended, false)
        ), standard_coverage as (
            select code, count(*) as bar_count
            from fact.stock_bar_1m
            where bar_time >= %s::date
              and bar_time < %s::date + interval '1 day'
              and (
                    bar_time::time between time '09:31:00' and time '11:30:00'
                 or bar_time::time between time '13:01:00' and time '15:00:00'
              )
            group by code
        )
        select expected.code
        from expected_codes expected
        left join standard_coverage coverage on coverage.code = expected.code
        where coalesce(coverage.bar_count, 0) < 240
        order by expected.code
        """,
        (trade_date, trade_date, trade_date),
    )
    if _is_empty_dataframe(frame):
        return ()
    return tuple(str(row["code"]) for row in frame.to_dict("records") if str(row["code"]) != "")


def _intraday_missing_universe_dates(trading_days: Sequence[str]) -> tuple[str, ...]:
    if trading_days == ():
        return ()
    frame = query_dataframe(
        """
        select requested.trade_date
        from unnest(%s::date[]) as requested(trade_date)
        left join fact.stock_daily_1d daily
          on daily.trade_date = requested.trade_date
         and not coalesce(daily.is_suspended, false)
        group by requested.trade_date
        having count(daily.code) = 0
        order by requested.trade_date
        """,
        (list(trading_days),),
    )
    if _is_empty_dataframe(frame):
        return ()
    return tuple(format_date_value(row["trade_date"]) for row in frame.to_dict("records") if format_date_value(row["trade_date"]) != "")


def _intraday_request_identity(codes: Sequence[str], trade_date: str) -> dict[str, object]:
    return {
        "codes": list(codes),
        "freq": "1m",
        "trade_date": "",
        "start_date": trade_date,
        "end_date": trade_date,
        "start_time": "",
        "end_time": "",
        "count": None,
        "adjust": "none",
        "limit": max(5000, len(codes) * 240),
    }


def _intraday_gap_requests(policy: CapturePolicy, gaps: Sequence[CaptureGap]) -> tuple[CaptureRequest, ...]:
    codes_by_date: dict[str, list[str]] = {}
    for gap in gaps:
        codes_by_date.setdefault(gap.trade_date, []).append(gap.code)
    requests: list[CaptureRequest] = []
    for trade_date in sorted(codes_by_date):
        for batch in _chunk(tuple(dict.fromkeys(codes_by_date[trade_date])), policy.batch_size):
            requests.append(CaptureRequest(policy.capability_id, _intraday_request_identity(batch, trade_date)))
    return tuple(requests)


def _index_codes() -> tuple[str, ...]:
    frame = load_index_catalog_frame([])
    if _is_empty_dataframe(frame):
        return ()
    return tuple(str(row["index_code"]) for row in frame.to_dict("records") if str(row["index_code"]) != "")


def _scope_identities(capability_id: str, request_identity: dict[str, object]) -> tuple[str, ...]:
    policy = get_postgres_cache_store().get_policy(capability_id)
    if policy is None:
        return ("",)
    if policy.request_scope_fields == ():
        return ("",)
    scope_ids: list[str] = [""]
    for field in policy.request_scope_fields:
        next_scope_ids: list[str] = []
        for value in _field_values(request_identity, field):
            for current_scope_id in scope_ids:
                if str(value) == "":
                    next_scope_ids.append(current_scope_id)
                    continue
                criteria: dict[str, object] = {}
                if current_scope_id != "":
                    for pair in current_scope_id.split("|"):
                        if pair == "" or "=" not in pair:
                            continue
                        key, current_value = pair.split("=", 1)
                        criteria[key] = current_value
                criteria[field] = value
                next_scope_ids.append(build_scope_identity(criteria, policy.request_scope_fields))
        scope_ids = next_scope_ids
    unique_scope_ids = tuple(dict.fromkeys(scope_ids))
    return unique_scope_ids if unique_scope_ids != () else ("",)


def _cached_coverages(capability_id: str, request_identity: dict[str, object]) -> tuple[CacheMissingRange, ...]:
    policy = get_postgres_cache_store().get_policy(capability_id)
    if policy is None:
        return ()
    time_start, time_end = _time_range_from_request(request_identity)
    now = datetime.now()
    coverage_ranges: list[CacheMissingRange] = []
    for scope_identity in _scope_identities(capability_id, request_identity):
        coverages = get_postgres_cache_store().coverage.find_for_scope(capability_id, scope_identity)
        for coverage in coverages:
            if not _is_fresh(policy, coverage.fresh_until, now):
                continue
            if coverage.time_end < time_start or coverage.time_start > time_end:
                continue
            coverage_ranges.append(
                CacheMissingRange(
                    max(time_start, coverage.time_start),
                    min(time_end, coverage.time_end),
                )
            )
    return tuple(coverage_ranges)


def _date_missing_ranges(capability_id: str, request_identity: dict[str, object], expected_dates: Sequence[str]) -> tuple[tuple[str, str], ...]:
    time_start, time_end = _time_range_from_request(request_identity)
    missing_ranges = _CACHE_MISSING_PLANNER.plan(
        "date_range",
        time_start,
        time_end,
        _cached_coverages(capability_id, request_identity),
        tuple(datetime.strptime(item, "%Y-%m-%d") for item in expected_dates if item != ""),
    )
    return tuple((item.time_start.strftime("%Y-%m-%d"), item.time_end.strftime("%Y-%m-%d")) for item in missing_ranges)


def _single_date_missing(capability_id: str, request_identity: dict[str, object]) -> bool:
    trade_date = format_date_value(request_identity.get("trade_date", ""))
    if trade_date == "":
        return True
    return _date_missing_ranges(capability_id, request_identity, (trade_date,)) != ()


def _single_point_missing(capability_id: str, request_identity: dict[str, object]) -> bool:
    time_start, time_end = _time_range_from_request(request_identity)
    if time_start != time_end:
        return True
    return _date_missing_ranges(capability_id, request_identity, (time_start.strftime("%Y-%m-%d"),)) != ()


def _fact_daily_count(table_name: str, trade_date: str, where_sql: str = "") -> int:
    actual_trade_date = format_date_value(trade_date)
    if actual_trade_date == "":
        return 0
    query = f"select count(*) as row_count from {table_name} where trade_date = %s::date {where_sql}"
    frame = query_dataframe(query, (actual_trade_date,))
    if _is_empty_dataframe(frame):
        return 0
    return int(frame.iloc[0].to_dict().get("row_count", 0) or 0)


def _complete_board_daily_count(trade_date: str) -> int:
    actual_trade_date = format_date_value(trade_date)
    if actual_trade_date == "":
        return 0
    frame = query_dataframe(
        """
        select count(*) as row_count
        from fact.board_daily_1d
        where trade_date = %s::date
          and left(board_code, 9) = 'INDUSTRY:'
          and open is not null
          and high is not null
          and low is not null
          and close is not null
          and pre_close is not null
          and change is not null
        """,
        (actual_trade_date,),
    )
    if _is_empty_dataframe(frame):
        return 0
    return int(frame.iloc[0].to_dict().get("row_count", 0) or 0)


def _daily_count_complete(actual_count: int, expected_count: int) -> bool:
    if actual_count <= 0:
        return False
    if expected_count <= 0:
        return True
    return actual_count >= int(expected_count * 0.9)


def _concept_daily_fact_missing(trade_date: str) -> bool:
    expected_ids = _derivable_concept_ids(trade_date)
    if expected_ids == ():
        return False
    frame = query_dataframe(
        """
        select concept_id
        from fact.concept_daily_1d
        where trade_date = %s::date
          and concept_id = any(%s)
          and amount is not null
          and pct_chg is not null
        """,
        (trade_date, list(expected_ids)),
    )
    actual_ids = set() if _is_empty_dataframe(frame) else {str(row["concept_id"]) for row in frame.to_dict("records")}
    return actual_ids != set(expected_ids)


def _complete_stock_daily_count(trade_date: str) -> int:
    actual_trade_date = format_date_value(trade_date)
    if actual_trade_date == "":
        return 0
    frame = query_dataframe(
        """
        select count(*) as row_count
        from fact.stock_daily_1d
        where trade_date = %s::date
          and open is not null
          and high is not null
          and low is not null
          and close is not null
          and pre_close is not null
          and pct_chg is not null
          and volume is not null
          and amount is not null
        """,
        (actual_trade_date,),
    )
    if _is_empty_dataframe(frame):
        return 0
    return int(frame.iloc[0].to_dict().get("row_count", 0) or 0)


def _recent_stock_daily_count(trade_date: str) -> int:
    actual_trade_date = format_date_value(trade_date)
    if actual_trade_date == "":
        return 0
    frame = query_dataframe(
        """
        select coalesce(max(day_count), 0) as row_count
        from (
            select count(*) as day_count
            from fact.stock_daily_1d
            where trade_date < %s::date
              and trade_date >= %s::date - interval '45 days'
              and open is not null
              and high is not null
              and low is not null
              and close is not null
              and pre_close is not null
              and pct_chg is not null
              and volume is not null
              and amount is not null
            group by trade_date
        ) daily_counts
        """,
        (actual_trade_date, actual_trade_date),
    )
    if _is_empty_dataframe(frame):
        return 0
    return int(frame.iloc[0].to_dict().get("row_count", 0) or 0)


def _stock_daily_fact_missing(trade_date: str) -> bool:
    actual_count = _complete_stock_daily_count(trade_date)
    expected_count = _recent_stock_daily_count(trade_date)
    return not _daily_count_complete(actual_count, expected_count)


def _board_daily_fact_missing(trade_date: str) -> bool:
    return not _daily_count_complete(_complete_board_daily_count(trade_date), _industry_count())


def _report_period_dates(periods: Sequence[str]) -> tuple[str, ...]:
    return tuple(format_date_value(item) for item in periods if format_date_value(item) != "")


def _report_period_missing_periods(capability_id: str, request_identity: dict[str, object], periods: Sequence[str]) -> tuple[str, ...]:
    expected_dates = _report_period_dates(periods)
    if expected_dates == ():
        return ()
    missing_ranges = _date_missing_ranges(capability_id, request_identity, expected_dates)
    missing_periods: list[str] = []
    for range_start, range_end in missing_ranges:
        for expected_date in expected_dates:
            if range_start <= expected_date <= range_end:
                missing_periods.append(expected_date.replace("-", ""))
    return tuple(dict.fromkeys(missing_periods))


def _month_first_days(months: Sequence[str]) -> tuple[str, ...]:
    values: list[str] = []
    for month_text in months:
        if len(month_text) != 6 or not month_text.isdigit():
            continue
        values.append(f"{month_text[:4]}-{month_text[4:6]}-01")
    return tuple(values)


def _missing_months(capability_id: str, months: Sequence[str]) -> tuple[str, ...]:
    expected_days = _month_first_days(months)
    if expected_days == ():
        return ()
    missing_ranges = _date_missing_ranges(capability_id, {"start_date": expected_days[0], "end_date": expected_days[-1]}, expected_days)
    missing_days: list[str] = []
    for range_start, range_end in missing_ranges:
        for expected_day in expected_days:
            if range_start <= expected_day <= range_end:
                missing_days.append(expected_day)
    missing_months = [item[:4] + item[5:7] for item in missing_days]
    return tuple(dict.fromkeys(missing_months))


def _append_missing_range_requests(
    requests: list[CaptureRequest],
    capability_id: str,
    request_identity: dict[str, object],
    expected_dates: Sequence[str],
) -> None:
    for missing_start, missing_end in _date_missing_ranges(capability_id, request_identity, expected_dates):
        requests.append(CaptureRequest(capability_id, {**request_identity, "start_date": missing_start, "end_date": missing_end}))


def _concept_ids() -> tuple[str, ...]:
    return tuple(group.concept_id for group in QuoteMuxConcepts().list_alias_groups("") if group.concept_id != "")


def _derivable_concept_ids(trade_date: str, concept_ids: Sequence[str] | None = None) -> tuple[str, ...]:
    requested_ids = list(dict.fromkeys(concept_ids or _concept_ids()))
    frame = load_derivable_concept_ids_frame(requested_ids, trade_date)
    if _is_empty_dataframe(frame):
        return ()
    return tuple(str(row["concept_id"]) for row in frame.to_dict("records") if str(row["concept_id"]) != "")


def _missing_derivable_concept_ids(items: Sequence[object], expected_ids: Sequence[str]) -> tuple[str, ...]:
    complete_ids = {
        str(getattr(item, "concept_id", ""))
        for item in items
        if getattr(item, "amount", None) is not None and getattr(item, "pct_chg", None) is not None
    }
    return tuple(sorted(set(expected_ids) - complete_ids))


def _industry_count() -> int:
    frame = query_dataframe("select count(distinct industry) as industry_count from ref.stock where industry <> ''", ())
    if _is_empty_dataframe(frame):
        return 0
    return int(frame.iloc[0].to_dict().get("industry_count", 0) or 0)


def _active_stock_requests(policy: CapturePolicy, capability_id: str, now: datetime) -> tuple[CaptureRequest, ...]:
    trading_days = _recent_trading_days(policy.window_count, now)
    if trading_days == ():
        return ()
    freq = "1d" if capability_id == "stocks.quotes.daily" else "1m"
    requests: list[CaptureRequest] = []
    if capability_id == "stocks.quotes.intraday":
        missing_universe_dates = _intraday_missing_universe_dates(trading_days)
        if missing_universe_dates != ():
            raise RuntimeError(f"股票 1m 分钟线缺少日线股票池: trade_dates={','.join(missing_universe_dates)}")
        for trade_date in reversed(trading_days):
            codes = _intraday_missing_stock_codes(trade_date)
            for batch in _chunk(codes, policy.batch_size):
                requests.append(CaptureRequest(capability_id, _intraday_request_identity(batch, trade_date)))
        return tuple(requests)
    codes = _active_stock_codes(trading_days[-1])
    if codes == ():
        return ()
    for batch in _chunk(codes, policy.batch_size):
        request_identity = {
            "codes": list(batch),
            "freq": freq,
            "trade_date": "",
            "start_date": trading_days[0],
            "end_date": trading_days[-1],
            "start_time": "",
            "end_time": "",
            "count": None,
            "adjust": "none",
            "limit": 5000,
        }
        for missing_start, missing_end in _date_missing_ranges(capability_id, request_identity, trading_days):
            requests.append(
                CaptureRequest(
                    capability_id,
                    {
                        **request_identity,
                        "start_date": missing_start,
                        "end_date": missing_end,
                    },
                )
            )
    return tuple(requests)


def _index_quote_requests(policy: CapturePolicy, capability_id: str, now: datetime) -> tuple[CaptureRequest, ...]:
    trading_days = _recent_trading_days(policy.window_count, now)
    if trading_days == ():
        return ()
    codes = _index_codes()
    if codes == ():
        return ()
    requests: list[CaptureRequest] = []
    for batch in _chunk(codes, policy.batch_size):
        request_identity = {
            "index_codes": list(batch),
            "freq": "1d",
            "trade_date": "",
            "start_date": trading_days[0],
            "end_date": trading_days[-1],
            "count": None,
            "limit": 5000,
        }
        for missing_start, missing_end in _date_missing_ranges(capability_id, request_identity, trading_days):
            requests.append(
                CaptureRequest(
                    capability_id,
                    {
                        **request_identity,
                        "start_date": missing_start,
                        "end_date": missing_end,
                    },
                )
            )
    return tuple(requests)


def _daily_snapshot_requests(policy: CapturePolicy, capability_id: str, now: datetime) -> tuple[CaptureRequest, ...]:
    trading_days = _recent_trading_days(policy.window_count, now)
    return tuple(
        CaptureRequest(capability_id, {"trade_date": trade_date, "limit": 10000, "offset": 0})
        for trade_date in trading_days
        if _stock_daily_fact_missing(trade_date)
    )


def _trading_calendar_requests(policy: CapturePolicy, capability_id: str, now: datetime) -> tuple[CaptureRequest, ...]:
    start_year = now.year
    end_year = start_year + max(1, policy.window_count) - 1
    return (
        CaptureRequest(
            capability_id,
            {
                "exchange": "SSE",
                "start_date": f"{start_year}-01-01",
                "end_date": f"{end_year}-12-31",
                "is_open": None,
            },
        ),
    )


def _concept_quote_requests(policy: CapturePolicy, capability_id: str, now: datetime) -> tuple[CaptureRequest, ...]:
    trading_days = _recent_trading_days(policy.window_count, now)
    requests: list[CaptureRequest] = []
    for trade_date in trading_days:
        if not _concept_daily_fact_missing(trade_date):
            continue
        requests.append(
            CaptureRequest(
                capability_id,
                {
                    "trade_date": trade_date,
                    "limit": 5000,
                    "offset": 0,
                },
            )
        )
    return tuple(requests)


def _board_quote_requests(policy: CapturePolicy, capability_id: str, now: datetime) -> tuple[CaptureRequest, ...]:
    trading_days = _recent_trading_days(policy.window_count, now)
    return tuple(
        CaptureRequest(
            capability_id,
            {
                "board_codes": [],
                "freq": "1d",
                "trade_date": trade_date,
                "start_date": "",
                "end_date": "",
                "start_time": "",
                "end_time": "",
                "count": None,
            },
        )
        for trade_date in trading_days
        if _board_daily_fact_missing(trade_date)
    )


def _index_member_requests(policy: CapturePolicy, capability_id: str, now: datetime) -> tuple[CaptureRequest, ...]:
    trading_days = _recent_trading_days(policy.window_count, now)
    if trading_days == ():
        return ()
    index_codes = _index_codes()
    return tuple(
        CaptureRequest(capability_id, {"index_code": index_code, "trade_date": trade_date})
        for trade_date in trading_days
        for index_code in index_codes
        if _single_date_missing(capability_id, {"index_code": index_code, "trade_date": trade_date})
    )


def _concept_member_requests(policy: CapturePolicy, capability_id: str, now: datetime) -> tuple[CaptureRequest, ...]:
    trading_days = _recent_trading_days(policy.window_count, now)
    if trading_days == ():
        return ()
    concept_ids = _concept_ids()
    return tuple(
        CaptureRequest(capability_id, {"concept_id": concept_id, "trade_date": trade_date})
        for trade_date in trading_days
        for concept_id in concept_ids
    )


def _date_window(policy: CapturePolicy, now: datetime) -> tuple[str, str]:
    trading_days = _recent_trading_days(policy.window_count, now)
    if trading_days == ():
        end_date = _date_range_end_text(now)
        start_date = (now.date() - timedelta(days=max(1, policy.window_count))).strftime("%Y-%m-%d")
        return start_date, end_date
    return trading_days[0], trading_days[-1]


def _recent_report_periods(window_count: int, now: datetime) -> tuple[str, ...]:
    quarter_days = ((3, 31), (6, 30), (9, 30), (12, 31))
    periods: list[str] = []
    year = now.year
    while len(periods) < window_count:
        for month, day in reversed(quarter_days):
            candidate = date(year, month, day)
            if candidate <= now.date():
                periods.append(candidate.strftime("%Y%m%d"))
                if len(periods) == window_count:
                    break
        year -= 1
    return tuple(periods)


def _recent_months(window_count: int, now: datetime) -> tuple[str, ...]:
    months: list[str] = []
    year = now.year
    month = now.month
    while len(months) < window_count:
        months.append(f"{year:04d}{month:02d}")
        month -= 1
        if month == 0:
            year -= 1
            month = 12
    return tuple(months)


def _catalog_snapshot_requests(policy: CapturePolicy, capability_id: str, now: datetime) -> tuple[CaptureRequest, ...]:
    start_date, end_date = _date_window(policy, now)
    identities = {
        "futures.contracts.catalog": {"codes": [], "include_expired": False},
        "futures.contracts.main_mapping": {"codes": []},
        "stocks.catalog": {"codes": [], "name": "", "exchange": "", "list_status": "", "include_delisted": True, "limit": 10000, "offset": 0, "refresh": True},
        "stocks.catalog.archive": {"trade_date": end_date, "code": "", "name": "", "industry": "", "area": "", "limit": 10000, "offset": 0},
        "indexes.catalog": {"category": "", "market": "", "publisher": "", "status": "active", "limit": 10000, "offset": 0},
        "concepts.catalog": {"category": "", "market": "", "status": "active", "limit": 10000, "offset": 0},
        "concepts.reference.categories": {"parent_code": "", "level": None},
        "markets.participants.hot_money": {"name": "", "tag": "", "limit": 10000, "offset": 0},
    }
    identity = identities.get(capability_id)
    if identity is None:
        return ()
    return (CaptureRequest(capability_id, identity),)


def _single_entity_snapshot_requests(policy: CapturePolicy, capability_id: str, now: datetime) -> tuple[CaptureRequest, ...]:
    start_date, end_date = _date_window(policy, now)
    if capability_id.startswith("stocks.profile."):
        if capability_id == "stocks.profile.name_history":
            return (CaptureRequest(capability_id, {"code": "", "start_date": "", "end_date": ""}),)
        trading_days = _recent_trading_days(1, now)
        codes = _active_stock_codes(trading_days[-1]) if trading_days != () else ()
        if capability_id == "stocks.profile.basic":
            return tuple(CaptureRequest(capability_id, {"code": code}) for code in codes)
        if capability_id == "stocks.profile.company":
            return tuple(CaptureRequest(capability_id, {"code": code}) for code in codes)
        if capability_id == "stocks.profile.managers":
            return tuple(CaptureRequest(capability_id, {"code": code}) for code in codes)
        if capability_id == "stocks.profile.management_rewards":
            return tuple(CaptureRequest(capability_id, {"code": code, "start_date": start_date, "end_date": end_date}) for code in codes)
    if capability_id == "indexes.profile":
        return tuple(CaptureRequest(capability_id, {"index_code": index_code}) for index_code in _index_codes())
    if capability_id == "concepts.profile":
        return tuple(CaptureRequest(capability_id, {"concept_id": concept_id}) for concept_id in _concept_ids())
    return ()


def _market_recent_trading_day_requests(policy: CapturePolicy, capability_id: str, now: datetime) -> tuple[CaptureRequest, ...]:
    start_date, end_date = _date_window(policy, now)
    recent_days = _recent_trading_days(policy.window_count, now)
    if capability_id == "concepts.indicators.money_flow.snapshot":
        return tuple(
            CaptureRequest(capability_id, {"trade_date": trade_date, "scope": "", "limit": 10000, "offset": 0})
            for trade_date in recent_days
            if _single_date_missing(capability_id, {"trade_date": trade_date, "scope": "", "limit": 10000, "offset": 0})
        )
    identities = {
        "markets.indicators.main_capital_flow": {"trade_date": "", "start_date": start_date, "end_date": end_date},
        "markets.connect.capital_flow": {"trade_date": "", "start_date": start_date, "end_date": end_date},
        "markets.connect.quotas": {"trade_date": "", "start_date": start_date, "end_date": end_date, "market_type": ""},
        "markets.connect.active_top10": {"trade_date": "", "start_date": start_date, "end_date": end_date, "market_type": "", "limit": 10000},
        "markets.events.block_trades": {"trade_date": "", "start_date": start_date, "end_date": end_date, "code": "", "limit": 10000},
        "markets.participants.dragon_tiger": {"trade_date": "", "start_date": start_date, "end_date": end_date, "code": "", "limit": 10000},
        "markets.participants.dragon_tiger.institutions": {"trade_date": "", "start_date": start_date, "end_date": end_date, "code": "", "limit": 10000},
        "markets.participants.hot_money.details": {"trade_date": "", "start_date": start_date, "end_date": end_date, "name": "", "limit": 10000, "offset": 0},
    }
    if capability_id == "markets.trading.open_auctions":
        return tuple(
            CaptureRequest(capability_id, {"codes": "", "trade_date": trade_date})
            for trade_date in recent_days
            if _single_date_missing(capability_id, {"codes": "", "trade_date": trade_date})
        )
    identity = identities.get(capability_id)
    if identity is None:
        return ()
    if recent_days == ():
        return (CaptureRequest(capability_id, identity),)
    return tuple(
        CaptureRequest(capability_id, {**identity, "start_date": missing_start, "end_date": missing_end})
        for missing_start, missing_end in _date_missing_ranges(capability_id, identity, recent_days)
    )


def _stock_trading_day_requests(policy: CapturePolicy, capability_id: str, now: datetime) -> tuple[CaptureRequest, ...]:
    start_date, end_date = _date_window(policy, now)
    recent_days = _recent_trading_days(policy.window_count, now)
    active_days = _recent_trading_days(1, now)
    codes = _active_stock_codes(active_days[-1]) if active_days != () else ()
    if codes == ():
        return ()
    if capability_id == "stocks.indicators.risk_flags":
        request_identity = {"trade_date": "", "start_date": start_date, "end_date": end_date, "flag_type": "", "status": "", "limit": 10000, "offset": 0}
        if recent_days == ():
            return (CaptureRequest(capability_id, request_identity),)
        return tuple(
            CaptureRequest(capability_id, {**request_identity, "start_date": missing_start, "end_date": missing_end})
            for missing_start, missing_end in _date_missing_ranges(capability_id, request_identity, recent_days)
        )
    if capability_id == "stocks.quotes.auctions":
        # Tushare's auction endpoints are market-wide per trade date. Splitting
        # them by stock multiplied one 30-day refresh into thousands of identical
        # source requests and kept the scheduled capture lock for hours.
        request_identity = {"code": "", "session": "", "trade_date": "", "start_date": start_date, "end_date": end_date}
        if recent_days == ():
            return (CaptureRequest(capability_id, request_identity),)
        requests: list[CaptureRequest] = []
        _append_missing_range_requests(requests, capability_id, request_identity, recent_days)
        return tuple(requests)
    batch_requests = {
        "stocks.indicators.daily_basic": lambda batch: {"code": "", "codes": ",".join(batch), "trade_date": "", "start_date": start_date, "end_date": end_date},
        "stocks.indicators.daily_valuation": lambda batch: {"code": "", "codes": ",".join(batch), "trade_date": "", "start_date": start_date, "end_date": end_date},
        "stocks.indicators.daily_market_value": lambda batch: {"code": "", "codes": ",".join(batch), "trade_date": "", "start_date": start_date, "end_date": end_date},
        "stocks.indicators.money_flow.batch": lambda batch: {"codes": ",".join(batch), "trade_date": start_date, "view": "main"},
    }
    batch_builder = batch_requests.get(capability_id)
    if batch_builder is not None:
        requests: list[CaptureRequest] = []
        for batch in _chunk(codes, policy.batch_size):
            if capability_id == "stocks.indicators.money_flow.batch":
                for trade_date in recent_days:
                    request_identity = {"codes": ",".join(batch), "trade_date": trade_date, "view": "main"}
                    if _single_date_missing(capability_id, request_identity):
                        requests.append(CaptureRequest(capability_id, request_identity))
                continue
            request_identity = batch_builder(batch)
            if recent_days == ():
                requests.append(CaptureRequest(capability_id, request_identity))
                continue
            _append_missing_range_requests(requests, capability_id, request_identity, recent_days)
        return tuple(requests)
    per_code = {
        "stocks.factors.adj": lambda code: {"code": code, "start_date": start_date, "end_date": end_date, "base_date": ""},
        "stocks.factors.technical": lambda code: {"code": code, "trade_date": "", "start_date": start_date, "end_date": end_date, "adjust": "none"},
        "stocks.indicators.money_flow": lambda code: {"code": code, "trade_date": "", "start_date": start_date, "end_date": end_date, "view": ""},
        "stocks.indicators.premarket": lambda code: {"code": code, "trade_date": "", "start_date": start_date, "end_date": end_date},
        "stocks.indicators.chip_distribution": lambda code: {"code": code, "trade_date": "", "start_date": start_date, "end_date": end_date},
        "stocks.indicators.chip_performance": lambda code: {"code": code, "trade_date": "", "start_date": start_date, "end_date": end_date},
        "stocks.indicators.ah_comparisons": lambda code: {"code": code, "trade_date": "", "start_date": start_date, "end_date": end_date, "limit": 10000, "offset": 0},
        "stocks.signals.hl": lambda code: {"code": code, "trade_date": "", "start_date": start_date, "end_date": end_date},
        "stocks.signals.nine_turn": lambda code: {"code": code, "freq": "D", "trade_date": "", "start_date": start_date, "end_date": end_date},
    }
    builder = per_code.get(capability_id)
    if builder is None:
        return ()
    requests: list[CaptureRequest] = []
    for code in codes:
        request_identity = builder(code)
        if recent_days == ():
            requests.append(CaptureRequest(capability_id, request_identity))
            continue
        _append_missing_range_requests(requests, capability_id, request_identity, recent_days)
    return tuple(requests)


def _concept_member_history_requests(policy: CapturePolicy, capability_id: str, now: datetime) -> tuple[CaptureRequest, ...]:
    trading_days = _recent_trading_days(policy.window_count, now)
    if trading_days == ():
        return ()
    requests: list[CaptureRequest] = []
    for concept_id in _concept_ids():
        request_identity = {"concept_id": concept_id, "start_date": trading_days[0], "end_date": trading_days[-1]}
        _append_missing_range_requests(requests, capability_id, request_identity, trading_days)
    return tuple(requests)


def _concept_money_flow_requests(policy: CapturePolicy, capability_id: str, now: datetime) -> tuple[CaptureRequest, ...]:
    trading_days = _recent_trading_days(policy.window_count, now)
    if trading_days == ():
        return ()
    requests: list[CaptureRequest] = []
    for concept_id in _concept_ids():
        request_identity = {"concept_id": concept_id, "trade_date": "", "start_date": trading_days[0], "end_date": trading_days[-1], "scope": "concept"}
        _append_missing_range_requests(requests, capability_id, request_identity, trading_days)
    return tuple(requests)


def _report_period_requests(policy: CapturePolicy, capability_id: str, now: datetime) -> tuple[CaptureRequest, ...]:
    trading_days = _recent_trading_days(1, now)
    codes = _active_stock_codes(trading_days[-1]) if trading_days != () else ()
    periods = _recent_report_periods(policy.window_count, now)
    if codes == () or periods == ():
        return ()
    window_start = periods[-1]
    window_end = periods[0]
    if capability_id == "stocks.finance.statements":
        requests: list[CaptureRequest] = []
        for batch in _chunk(codes, policy.batch_size):
            request_identity = {"codes": list(batch), "report_period": "", "start_period": window_start, "end_period": window_end, "report_type": ""}
            for report_period in _report_period_missing_periods(capability_id, request_identity, periods):
                requests.append(CaptureRequest(capability_id, {"codes": list(batch), "report_period": report_period, "start_period": "", "end_period": "", "report_type": ""}))
        return tuple(requests)
    if capability_id == "stocks.finance.indicators":
        requests: list[CaptureRequest] = []
        for batch in _chunk(codes, policy.batch_size):
            request_identity = {"code": "", "codes": ",".join(batch), "report_period": "", "start_period": window_start, "end_period": window_end}
            for report_period in _report_period_missing_periods(capability_id, request_identity, periods):
                requests.append(CaptureRequest(capability_id, {"code": "", "codes": ",".join(batch), "report_period": report_period, "start_period": "", "end_period": ""}))
        return tuple(requests)
    per_code = {
        "stocks.finance.audits": lambda code: {"code": code, "report_period": "", "start_period": window_start, "end_period": window_end},
        "stocks.finance.disclosure_dates": lambda code: {"code": code, "report_period": "", "start_period": window_start, "end_period": window_end},
        "stocks.finance.express": lambda code: {"code": code, "report_period": "", "start_period": window_start, "end_period": window_end},
        "stocks.finance.forecasts": lambda code: {"code": code, "report_period": "", "start_period": window_start, "end_period": window_end},
        "stocks.finance.main_business": lambda code: {"code": code, "report_period": "", "start_period": window_start, "end_period": window_end, "classification": ""},
        "stocks.ownership.shareholders.top10": lambda code: {"code": code, "report_period": "", "start_period": window_start, "end_period": window_end},
        "stocks.ownership.shareholders.top10_float": lambda code: {"code": code, "report_period": "", "start_period": window_start, "end_period": window_end},
    }
    builder = per_code.get(capability_id)
    if builder is None:
        return ()
    requests: list[CaptureRequest] = []
    for code in codes:
        request_identity = builder(code)
        for report_period in _report_period_missing_periods(capability_id, request_identity, periods):
            requests.append(CaptureRequest(capability_id, {**request_identity, "report_period": report_period, "start_period": "", "end_period": ""}))
    return tuple(requests)


def _corporate_action_requests(policy: CapturePolicy, capability_id: str, now: datetime) -> tuple[CaptureRequest, ...]:
    recent_days = _recent_calendar_days(policy.window_count, now)
    if recent_days == ():
        return ()
    start_date, end_date = recent_days[0], recent_days[-1]
    trading_days = _recent_trading_days(1, now)
    codes = _active_stock_codes(trading_days[-1]) if trading_days != () else ()
    builders = {
        "stocks.corporate_actions.dividends": lambda code: {"code": code, "start_date": start_date, "end_date": end_date},
        "stocks.corporate_actions.repurchases": lambda code: {"code": code, "start_date": start_date, "end_date": end_date},
        "stocks.corporate_actions.rights_issues": lambda code: {"code": code, "start_date": start_date, "end_date": end_date},
        "stocks.corporate_actions.share_changes": lambda code: {"code": code, "trade_date": "", "start_date": start_date, "end_date": end_date},
        "stocks.corporate_actions.unlock_schedules": lambda code: {"code": code, "unlock_date": "", "start_date": start_date, "end_date": end_date},
    }
    builder = builders.get(capability_id)
    if builder is None:
        return ()
    requests: list[CaptureRequest] = []
    for code in codes:
        request_identity = builder(code)
        _append_missing_range_requests(requests, capability_id, request_identity, recent_days)
    return tuple(requests)


def _ownership_trading_day_requests(policy: CapturePolicy, capability_id: str, now: datetime) -> tuple[CaptureRequest, ...]:
    recent_trading_days = _recent_trading_days(policy.window_count, now)
    recent_calendar_days = _recent_calendar_days(policy.window_count, now)
    start_date, end_date = _date_window(policy, now)
    trading_days = _recent_trading_days(1, now)
    codes = _active_stock_codes(trading_days[-1]) if trading_days != () else ()
    range_builders = {
        "stocks.ownership.ccass_holdings": lambda code: {"code": code, "trade_date": "", "start_date": start_date, "end_date": end_date},
        "stocks.ownership.ccass_holding_details": lambda code: {"code": code, "trade_date": "", "start_date": start_date, "end_date": end_date},
        "stocks.ownership.hk_connect_holdings": lambda code: {"code": code, "trade_date": "", "start_date": start_date, "end_date": end_date},
        "stocks.ownership.pledges.stats": lambda code: {"code": code, "trade_date": "", "start_date": start_date, "end_date": end_date},
        "stocks.ownership.shareholders.count": lambda code: {"code": code, "trade_date": "", "start_date": start_date, "end_date": end_date},
        "stocks.ownership.shareholders.changes": lambda code: {"code": code, "trade_date": "", "start_date": start_date, "end_date": end_date},
    }
    if capability_id in range_builders:
        if recent_trading_days == ():
            return ()
        requests: list[CaptureRequest] = []
        builder = range_builders[capability_id]
        for code in codes:
            request_identity = builder(code)
            _append_missing_range_requests(requests, capability_id, request_identity, recent_trading_days)
        return tuple(requests)
    if capability_id == "stocks.ownership.pledges.details":
        if recent_calendar_days == ():
            return ()
        requests: list[CaptureRequest] = []
        for code in codes:
            request_identity = {"code": code, "start_date": recent_calendar_days[0], "end_date": recent_calendar_days[-1], "status": ""}
            _append_missing_range_requests(requests, capability_id, request_identity, recent_calendar_days)
        return tuple(requests)
    return ()


def _research_date_requests(policy: CapturePolicy, capability_id: str, now: datetime) -> tuple[CaptureRequest, ...]:
    recent_days = _recent_calendar_days(policy.window_count, now)
    if recent_days == ():
        return ()
    start_date, end_date = recent_days[0], recent_days[-1]
    if capability_id == "rankings.research.reports":
        requests: list[CaptureRequest] = []
        request_identity = {"trade_date": "", "start_date": start_date, "end_date": end_date, "limit": 10000}
        _append_missing_range_requests(requests, capability_id, request_identity, recent_days)
        return tuple(requests)
    trading_days = _recent_trading_days(1, now)
    codes = _active_stock_codes(trading_days[-1]) if trading_days != () else ()
    builders = {
        "stocks.research.reports": lambda code: {"code": code, "report_date": "", "start_date": start_date, "end_date": end_date},
        "stocks.research.surveys": lambda code: {"code": code, "survey_date": "", "start_date": start_date, "end_date": end_date},
    }
    builder = builders.get(capability_id)
    if builder is None:
        return ()
    requests: list[CaptureRequest] = []
    for code in codes:
        request_identity = builder(code)
        _append_missing_range_requests(requests, capability_id, request_identity, recent_days)
    return tuple(requests)


def _research_month_requests(policy: CapturePolicy, capability_id: str, now: datetime) -> tuple[CaptureRequest, ...]:
    if capability_id != "rankings.research.broker_monthly_picks":
        return ()
    recent_months = _recent_months(policy.window_count, now)
    return tuple(CaptureRequest(capability_id, {"trade_month": month_text, "limit": 10000}) for month_text in _missing_months(capability_id, recent_months))


def _stock_reference_requests(policy: CapturePolicy, capability_id: str, now: datetime) -> tuple[CaptureRequest, ...]:
    if capability_id == "stocks.reference.bse_code_mappings":
        return (CaptureRequest(capability_id, {"old_code": "", "new_code": "", "status": ""}),)
    if capability_id == "stocks.reference.hk_connect_targets":
        return (CaptureRequest(capability_id, {"direction": "", "status": "", "effective_date": ""}),)
    return ()


def _trading_session_requests(policy: CapturePolicy, capability_id: str, now: datetime) -> tuple[CaptureRequest, ...]:
    if capability_id == "markets.trading.sessions":
        return (CaptureRequest(capability_id, {"codes": ""}),)
    return ()


def _news_event_requests(policy: CapturePolicy, capability_id: str, now: datetime) -> tuple[CaptureRequest, ...]:
    if capability_id != "markets.events.news":
        return ()
    recent_days = _recent_calendar_days(policy.window_count, now)
    return tuple(
        CaptureRequest(
            capability_id,
            {
                "trade_date": trade_date,
                "announcement_date": "",
                "crawl_date": "",
                "stock_code": "",
                "event_type": "",
                "min_importance_score": None,
                "sort_by": "announcement_time",
                "limit": 10000,
                "offset": 0,
                "include_sources": True,
                "include_content_text": False,
            },
        )
        for trade_date in recent_days
        if _single_point_missing(
            capability_id,
            {
                "trade_date": trade_date,
                "announcement_date": "",
                "crawl_date": "",
                "stock_code": "",
                "event_type": "",
                "min_importance_score": None,
                "sort_by": "announcement_time",
                "limit": 10000,
                "offset": 0,
                "include_sources": True,
                "include_content_text": False,
            },
        )
    )


def build_capture_requests(policy: CapturePolicy, now: datetime) -> tuple[CaptureRequest, ...]:
    if policy.capability_id == "futures.quotes.main_continuous.1m":
        return (CaptureRequest(policy.capability_id, {"overlap_days": max(1, policy.window_count)}),)
    if policy.scope_profile == PROFILE_ACTIVE_STOCKS_RECENT_TRADING_DAYS and policy.capability_id in {"stocks.quotes.daily", "stocks.quotes.intraday"}:
        return _active_stock_requests(policy, policy.capability_id, now)
    if policy.scope_profile == PROFILE_ACTIVE_STOCKS_RECENT_TRADING_DAYS:
        return _stock_trading_day_requests(policy, policy.capability_id, now)
    if policy.scope_profile == PROFILE_INDEXES_RECENT_TRADING_DAYS and policy.capability_id == "indexes.quotes.daily":
        return _index_quote_requests(policy, policy.capability_id, now)
    if policy.scope_profile == PROFILE_DAILY_SNAPSHOT_RECENT_TRADING_DAYS and policy.capability_id == "stocks.quotes.daily_snapshot":
        return _daily_snapshot_requests(policy, policy.capability_id, now)
    if policy.scope_profile == PROFILE_TRADING_CALENDAR_YEAR_WINDOW and policy.capability_id == "markets.calendar.trading":
        return _trading_calendar_requests(policy, policy.capability_id, now)
    if policy.scope_profile == PROFILE_CONCEPTS_RECENT_TRADING_DAYS and policy.capability_id == "concepts.quotes.daily":
        return _concept_quote_requests(policy, policy.capability_id, now)
    if policy.scope_profile == PROFILE_BOARDS_RECENT_TRADING_DAYS and policy.capability_id == "boards.quotes.daily":
        return _board_quote_requests(policy, policy.capability_id, now)
    if policy.scope_profile == PROFILE_INDEXES_RECENT_TRADING_DAYS and policy.capability_id == "indexes.members":
        return _index_member_requests(policy, policy.capability_id, now)
    if policy.scope_profile == PROFILE_CONCEPTS_RECENT_TRADING_DAYS and policy.capability_id == "concepts.members":
        return _concept_member_requests(policy, policy.capability_id, now)
    if policy.scope_profile == PROFILE_CONCEPTS_RECENT_TRADING_DAYS and policy.capability_id == "concepts.members.history":
        return _concept_member_history_requests(policy, policy.capability_id, now)
    if policy.scope_profile == PROFILE_CONCEPTS_RECENT_TRADING_DAYS and policy.capability_id == "concepts.indicators.money_flow":
        return _concept_money_flow_requests(policy, policy.capability_id, now)
    if policy.scope_profile == PROFILE_CATALOG_SNAPSHOT:
        return _catalog_snapshot_requests(policy, policy.capability_id, now)
    if policy.scope_profile == PROFILE_SINGLE_ENTITY_SNAPSHOT:
        return _single_entity_snapshot_requests(policy, policy.capability_id, now)
    if policy.scope_profile == PROFILE_MARKET_RECENT_TRADING_DAYS:
        return _market_recent_trading_day_requests(policy, policy.capability_id, now)
    if policy.scope_profile == PROFILE_ACTIVE_STOCKS_RECENT_REPORT_PERIODS:
        return _report_period_requests(policy, policy.capability_id, now)
    if policy.scope_profile == PROFILE_CORPORATE_ACTIONS_RECENT_ANNOUNCEMENTS:
        return _corporate_action_requests(policy, policy.capability_id, now)
    if policy.scope_profile == PROFILE_OWNERSHIP_RECENT_TRADING_DAYS:
        return _ownership_trading_day_requests(policy, policy.capability_id, now)
    if policy.scope_profile == PROFILE_RESEARCH_RECENT_DATES:
        return _research_date_requests(policy, policy.capability_id, now)
    if policy.scope_profile == PROFILE_RESEARCH_RECENT_MONTHS:
        return _research_month_requests(policy, policy.capability_id, now)
    if policy.scope_profile == PROFILE_TRADING_SESSIONS_SNAPSHOT:
        return _trading_session_requests(policy, policy.capability_id, now)
    if policy.scope_profile == PROFILE_STOCK_REFERENCE_SNAPSHOT:
        return _stock_reference_requests(policy, policy.capability_id, now)
    if policy.scope_profile == PROFILE_NEWS_EVENT_UPDATE:
        return _news_event_requests(policy, policy.capability_id, now)
    return ()


def _scheduled_time(policy: CapturePolicy, now: datetime) -> datetime | None:
    local_now = now.astimezone(ZoneInfo(policy.timezone)) if now.tzinfo is not None else now.replace(tzinfo=ZoneInfo(policy.timezone))
    local_date = local_now.date()
    timezone = ZoneInfo(policy.timezone)

    def occurrence(year: int, month: int, day: int) -> datetime:
        return datetime.combine(date(year, month, day), policy.run_time, timezone)

    if policy.cadence == CADENCE_DAILY:
        scheduled = occurrence(local_date.year, local_date.month, local_date.day)
        if scheduled > local_now:
            previous_day = local_date - timedelta(days=1)
            scheduled = occurrence(previous_day.year, previous_day.month, previous_day.day)
    elif policy.cadence == CADENCE_WEEKLY:
        expected_weekday = 6 if policy.weekday is None else policy.weekday
        scheduled_day = local_date - timedelta(days=(local_date.weekday() - expected_weekday) % 7)
        scheduled = occurrence(scheduled_day.year, scheduled_day.month, scheduled_day.day)
        if scheduled > local_now:
            scheduled_day -= timedelta(days=7)
            scheduled = occurrence(scheduled_day.year, scheduled_day.month, scheduled_day.day)
    elif policy.cadence == CADENCE_MONTHLY:
        expected_day = monthrange(local_date.year, local_date.month)[1] if policy.month_day is None else min(policy.month_day, monthrange(local_date.year, local_date.month)[1])
        scheduled = occurrence(local_date.year, local_date.month, expected_day)
        if scheduled > local_now:
            previous_year = local_date.year if local_date.month > 1 else local_date.year - 1
            previous_month = local_date.month - 1 if local_date.month > 1 else 12
            previous_day = monthrange(previous_year, previous_month)[1] if policy.month_day is None else min(policy.month_day, monthrange(previous_year, previous_month)[1])
            scheduled = occurrence(previous_year, previous_month, previous_day)
    elif policy.cadence == CADENCE_YEARLY:
        expected_month = 12 if policy.month is None else policy.month
        expected_day = monthrange(local_date.year, expected_month)[1] if policy.month_day is None else min(policy.month_day, monthrange(local_date.year, expected_month)[1])
        scheduled = occurrence(local_date.year, expected_month, expected_day)
        if scheduled > local_now:
            previous_year = local_date.year - 1
            previous_day = monthrange(previous_year, expected_month)[1] if policy.month_day is None else min(policy.month_day, monthrange(previous_year, expected_month)[1])
            scheduled = occurrence(previous_year, expected_month, previous_day)
    else:
        scheduled = occurrence(local_date.year, local_date.month, local_date.day)
        if scheduled > local_now:
            previous_day = local_date - timedelta(days=1)
            scheduled = occurrence(previous_day.year, previous_day.month, previous_day.day)
    return scheduled.replace(tzinfo=None)


def is_capture_due(policy: CapturePolicy, runs: CaptureRunRepository, now: datetime) -> bool:
    if not policy.enabled:
        return False
    planned_time = _scheduled_time(policy, now)
    if planned_time is None:
        return False
    previous = runs.latest_for_planned_time(policy.capability_id, planned_time)
    return previous is None or previous.status in {CAPTURE_PARTIAL, CAPTURE_FAILED, CAPTURE_SKIPPED}


CAPTURE_DUE_PRIORITY: dict[str, int] = {
    "futures.quotes.main_continuous.1m": 0,
    "stocks.quotes.intraday": 0,
    "stocks.quotes.daily_snapshot": 1,
    "stocks.quotes.daily": 2,
    "boards.quotes.daily": 3,
    "concepts.members.history": 3,
    "concepts.members": 4,
    "concepts.quotes.daily": 5,
    "indexes.quotes.daily": 6,
    "markets.calendar.trading": 7,
}


def _due_policy_sort_key(policy: CapturePolicy) -> tuple[int, str]:
    return (CAPTURE_DUE_PRIORITY.get(policy.capability_id, 100), policy.capability_id)


def _current_datetime() -> datetime:
    return datetime.now().astimezone()


def _policy_local_now(policy: CapturePolicy, now: datetime) -> datetime:
    if now.tzinfo is None:
        now = now.replace(tzinfo=ZoneInfo(policy.timezone))
    return now.astimezone(ZoneInfo(policy.timezone))


RUNTIME_METHODS: dict[str, tuple[str, str]] = {
    "futures.contracts.catalog": ("futures", "get_contract_catalog"),
    "futures.contracts.main_mapping": ("futures", "get_main_contract_mappings"),
    "concepts.catalog": ("concepts", "get_catalog"),
    "concepts.indicators.money_flow": ("concepts", "get_money_flow"),
    "concepts.indicators.money_flow.snapshot": ("concepts", "get_market_money_flow"),
    "concepts.members.history": ("concepts", "get_member_history"),
    "concepts.profile": ("concepts", "get_profile"),
    "concepts.reference.categories": ("concepts", "get_categories"),
    "indexes.catalog": ("indexes", "get_catalog"),
    "indexes.profile": ("indexes", "get_profile"),
    "markets.connect.active_top10": ("markets", "get_connect_active_top10"),
    "markets.connect.capital_flow": ("markets", "get_connect_capital_flow"),
    "markets.connect.quotas": ("markets", "get_connect_quotas"),
    "markets.events.block_trades": ("markets", "get_block_trades"),
    "markets.indicators.main_capital_flow": ("markets", "get_main_capital_flow"),
    "markets.participants.dragon_tiger": ("markets", "get_dragon_tiger"),
    "markets.participants.dragon_tiger.institutions": ("markets", "get_dragon_tiger_institutions"),
    "markets.participants.hot_money": ("markets", "get_hot_money"),
    "markets.participants.hot_money.details": ("markets", "get_hot_money_details"),
    "markets.trading.open_auctions": ("markets", "get_open_auctions"),
    "markets.trading.sessions": ("markets", "get_sessions"),
    "rankings.research.broker_monthly_picks": ("rankings", "get_broker_monthly_picks"),
    "rankings.research.reports": ("rankings", "get_research_reports"),
    "stocks.catalog": ("stocks", "get_catalog"),
    "stocks.catalog.archive": ("stocks", "get_archive"),
    "stocks.corporate_actions.dividends": ("stocks", "get_dividends"),
    "stocks.corporate_actions.repurchases": ("stocks", "get_repurchases"),
    "stocks.corporate_actions.rights_issues": ("stocks", "get_rights_issues"),
    "stocks.corporate_actions.share_changes": ("stocks", "get_share_changes"),
    "stocks.corporate_actions.unlock_schedules": ("stocks", "get_unlock_schedules"),
    "stocks.factors.adj": ("stocks", "get_adj_factors"),
    "stocks.factors.technical": ("stocks", "get_technical_factors"),
    "stocks.finance.audits": ("stocks", "get_audits"),
    "stocks.finance.disclosure_dates": ("stocks", "get_disclosure_dates"),
    "stocks.finance.express": ("stocks", "get_express"),
    "stocks.finance.forecasts": ("stocks", "get_forecasts"),
    "stocks.finance.indicators": ("stocks", "get_finance_indicators"),
    "stocks.finance.main_business": ("stocks", "get_main_business"),
    "stocks.finance.statements": ("stocks", "get_financial_statements"),
    "stocks.indicators.ah_comparisons": ("stocks", "get_ah_comparisons"),
    "stocks.indicators.chip_distribution": ("stocks", "get_chip_distribution"),
    "stocks.indicators.chip_performance": ("stocks", "get_chip_performance"),
    "stocks.indicators.daily_basic": ("stocks", "get_daily_basic"),
    "stocks.indicators.daily_market_value": ("stocks", "get_daily_market_value"),
    "stocks.indicators.daily_valuation": ("stocks", "get_daily_valuation"),
    "stocks.indicators.money_flow": ("stocks", "get_money_flow"),
    "stocks.indicators.money_flow.batch": ("stocks", "get_money_flow_batch"),
    "stocks.indicators.premarket": ("stocks", "get_premarket"),
    "stocks.indicators.risk_flags": ("stocks", "get_risk_flags"),
    "stocks.ownership.ccass_holding_details": ("stocks", "get_ccass_holding_details"),
    "stocks.ownership.ccass_holdings": ("stocks", "get_ccass_holdings"),
    "stocks.ownership.hk_connect_holdings": ("stocks", "get_hk_connect_holdings"),
    "stocks.ownership.pledges.details": ("stocks", "get_pledge_details"),
    "stocks.ownership.pledges.stats": ("stocks", "get_pledge_stats"),
    "stocks.ownership.shareholders.changes": ("stocks", "get_shareholder_changes"),
    "stocks.ownership.shareholders.count": ("stocks", "get_shareholder_count"),
    "stocks.ownership.shareholders.top10": ("stocks", "get_shareholder_top10"),
    "stocks.ownership.shareholders.top10_float": ("stocks", "get_shareholder_top10_float"),
    "stocks.profile.basic": ("stocks", "get_basic"),
    "stocks.profile.company": ("stocks", "get_profile"),
    "stocks.profile.management_rewards": ("stocks", "get_management_rewards"),
    "stocks.profile.managers": ("stocks", "get_managers"),
    "stocks.profile.name_history": ("stocks", "get_name_history"),
    "stocks.quotes.auctions": ("stocks", "get_auctions"),
    "stocks.reference.bse_code_mappings": ("stocks", "get_bse_code_mappings"),
    "stocks.reference.hk_connect_targets": ("stocks", "get_hk_connect_targets"),
    "stocks.research.reports": ("stocks", "get_research_reports"),
    "stocks.research.surveys": ("stocks", "get_surveys"),
    "stocks.signals.hl": ("stocks", "get_hl_signal"),
    "stocks.signals.nine_turn": ("stocks", "get_nine_turn"),
}


class QuoteMuxCaptureJob:
    def __init__(
        self,
        runtime: object | None = None,
        policies: CapturePolicyRepository | None = None,
        runs: CaptureRunRepository | None = None,
        locks: PostgresAdvisoryLockFactory | None = None,
        now_provider: Callable[[], datetime] | None = None,
        cache_store: object | None = None,
        gaps: CaptureGapRepository | None = None,
    ) -> None:
        if runtime is None:
            from quotemux.runtime import QuoteMux

            self._runtime = QuoteMux()
        else:
            self._runtime = runtime
        self._policies = policies or CapturePolicyRepository()
        self._runs = runs or CaptureRunRepository()
        self._locks = locks or PostgresAdvisoryLockFactory()
        self._now_provider = now_provider or _current_datetime
        self._cache_store = cache_store or get_postgres_cache_store()
        self._gaps = gaps or CaptureGapRepository()

    def list_policies(self) -> tuple[dict[str, object], ...]:
        return tuple(self._policy_to_dict(policy) for policy in self._policies.list())

    def get_policy(self, capability_id: str) -> dict[str, object]:
        root_capability_id = get_capability_config_root(capability_id)
        policy = self._get_policy(root_capability_id)
        return self._policy_to_dict(policy)

    def update_policy(self, update: CapturePolicyUpdate) -> dict[str, object]:
        current = self._get_policy(update.capability_id)
        policy = CapturePolicy(
            capability_id=current.capability_id,
            enabled=update.enabled,
            cadence=update.cadence,
            run_time=update.run_time,
            timezone=update.timezone,
            weekday=update.weekday,
            month=update.month,
            month_day=update.month_day,
            scope_profile=update.scope_profile,
            window_count=update.window_count,
            batch_size=update.batch_size,
            notes=update.notes,
        )
        _validate_capture_policy(policy)
        if not self._policies.update(policy):
            raise RuntimeError(f"capture 策略更新失败: {update.capability_id}")
        return self._policy_to_dict(policy)

    def list_runs(self, capability_id: str = "", status: str = "", limit: int = 100) -> tuple[dict[str, object], ...]:
        return tuple(self._run_to_dict(run) for run in self._runs.list(capability_id, status, limit))

    def list_gaps(self, capability_id: str = "", status: str = "", limit: int = 500) -> tuple[dict[str, object], ...]:
        return self._gaps.list(capability_id, status, limit)

    def audit_intraday_gaps(self, window_count: int = 30) -> dict[str, object]:
        return self._gaps.audit_intraday(window_count).to_dict()

    def retry_intraday_gaps(self, window_count: int = 30) -> dict[str, object]:
        policy = self._get_policy("stocks.quotes.intraday")
        audit_before = self._gaps.audit_intraday(window_count)
        gaps_before = self._gaps.list_retryable(policy.capability_id, window_count)
        if gaps_before == ():
            return {
                "status": CAPTURE_SUCCESS,
                "audit_before": audit_before.to_dict(),
                "capture_run": {},
                "audit_after": audit_before.to_dict(),
                "resolved_trade_dates": [],
            }
        requests = _intraday_gap_requests(policy, gaps_before)
        capture_run = self._run_capture_requests(policy, requests, {"mode": "gap_only", "gap_count": len(gaps_before)})
        audit_after = self._gaps.audit_intraday(window_count)
        unresolved_after = {
            (gap.code, gap.trade_date)
            for gap in self._gaps.list_unresolved(policy.capability_id, window_count)
        }
        resolved_trade_dates = sorted({
            gap.trade_date
            for gap in gaps_before
            if (gap.code, gap.trade_date) not in unresolved_after
        })
        return {
            "status": str(capture_run["status"]),
            "audit_before": audit_before.to_dict(),
            "capture_run": capture_run,
            "audit_after": audit_after.to_dict(),
            "resolved_trade_dates": resolved_trade_dates,
        }

    def run_due_captures(self) -> tuple[dict[str, object], ...]:
        now = self._now_provider()
        runs: list[dict[str, object]] = []
        due_policies: list[tuple[CapturePolicy, datetime]] = []
        for policy in self._policies.list():
            planned_time = _scheduled_time(policy, now)
            if planned_time is None or not is_capture_due(policy, self._runs, now):
                continue
            due_policies.append((policy, planned_time))
        for policy, planned_time in sorted(due_policies, key=lambda item: _due_policy_sort_key(item[0])):
            try:
                runs.append(self.run_capture(policy.capability_id, planned_time))
            except BaseException as exc:
                runs.append({"capability_id": policy.capability_id, "status": "failed", "error": str(exc), "error_type": type(exc).__name__})
        return tuple(runs)

    def run_capture(self, capability_id: str, planned_time: datetime | None = None) -> dict[str, object]:
        root_capability_id = get_capability_config_root(capability_id)
        policy = self._get_policy(root_capability_id)
        actual_planned_time = planned_time or self._now_provider().replace(tzinfo=None)
        skipped = self._precheck_skip(policy, actual_planned_time)
        if skipped is not None:
            return self._run_to_dict(skipped)
        lock = self._locks.create(root_capability_id)
        if not lock.acquire():
            run = self._create_finished_run(policy, actual_planned_time, CAPTURE_SKIPPED, 0, 0, "", {"reason": "advisory_lock_busy"})
            return self._run_to_dict(run)
        return self._run_capture_requests(policy, None, {"mode": "scheduled"}, actual_planned_time, lock)

    @staticmethod
    def repair_fingerprint(dataset: str, scope: dict[str, object], dataset_version: str = "") -> str:
        root_capability_id = get_capability_config_root(dataset)
        canonical_scope, actual_dataset_version = _repair_scope_and_version(scope, dataset_version)
        payload = json.dumps(
            {"dataset": root_capability_id, "dataset_version": actual_dataset_version, "scope": canonical_scope},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def run_repair(self, dataset: str, scope: dict[str, object], dataset_version: str = "") -> dict[str, object]:
        """Run one explicit, idempotent repair through the existing capture executor."""
        root_capability_id = get_capability_config_root(dataset)
        policy = self._get_policy(root_capability_id)
        canonical_scope, actual_dataset_version = _repair_scope_and_version(scope, dataset_version)
        fingerprint = self.repair_fingerprint(root_capability_id, canonical_scope, actual_dataset_version)
        planned_time = self._now_provider().replace(tzinfo=None)
        detail = {
            "mode": "repair",
            "repair_dataset": root_capability_id,
            "repair_scope": canonical_scope,
            "repair_fingerprint": fingerprint,
            "repair_dataset_version": actual_dataset_version,
        }
        lock = self._locks.create(root_capability_id)
        if not lock.acquire():
            run = self._create_finished_run(
                policy,
                planned_time,
                CAPTURE_SKIPPED,
                0,
                0,
                "",
                {**detail, "reason": "advisory_lock_busy"},
            )
            return self._run_to_dict(run)
        try:
            existing = self._runs.latest_success_for_repair_fingerprint(root_capability_id, fingerprint)
            if existing is not None:
                lock.release()
                return {**self._run_to_dict(existing), "repair_reused": True}
            skipped = self._precheck_repair_skip(policy, planned_time)
            if skipped is not None:
                lock.release()
                return {**self._run_to_dict(skipped), "repair_fingerprint": fingerprint}
        except BaseException:
            lock.release()
            raise
        try:
            return self._run_capture_requests(
                policy,
                (CaptureRequest(root_capability_id, canonical_scope),),
                detail,
                planned_time,
                lock,
            )
        except BaseException:
            lock.release()
            raise

    def get_repair_run(self, run_id: int) -> dict[str, object]:
        run = self._runs.get_by_id(run_id)
        if run is None or run.detail_json.get("mode") != "repair":
            raise KeyError(f"未知 repair run: {run_id}")
        return self._run_to_dict(run)

    def _run_capture_requests(
        self,
        policy: CapturePolicy,
        requests: Sequence[CaptureRequest] | None,
        run_detail: dict[str, object],
        planned_time: datetime | None = None,
        acquired_lock: object | None = None,
    ) -> dict[str, object]:
        actual_planned_time = planned_time or self._now_provider().replace(tzinfo=None)
        lock = acquired_lock or self._locks.create(policy.capability_id)
        if acquired_lock is None and not lock.acquire():
            run = self._create_finished_run(policy, actual_planned_time, CAPTURE_SKIPPED, 0, 0, "", {"reason": "advisory_lock_busy"})
            return self._run_to_dict(run)
        run = self._runs.create(policy.capability_id, CAPTURE_RUNNING, actual_planned_time, {"phase": "预处理", **run_detail})
        try:
            actual_requests = build_capture_requests(policy, _policy_local_now(policy, self._now_provider())) if requests is None else tuple(requests)
            result = self._execute_requests(policy, actual_requests)
            if run_detail.get("mode") == "repair":
                result = self._require_repair_write_result(result, actual_requests)
            status = self._execution_status(result)
            error_message = ""
            if status == CAPTURE_FAILED:
                error_message = "部分 batch 采集失败"
            elif status == CAPTURE_PARTIAL:
                error_message = "存在未解决数据缺口"
            detail_json = {
                "phase": "后处理",
                **run_detail,
                "partial_batches": list(result.partial_batches),
                "failed_batches": list(result.failed_batches),
            }
            self._runs.finish(run.id, status, result.row_count, result.coverage_count, error_message, detail_json)
            return self._run_to_dict(self._merge_finished_run(run, status, result.row_count, result.coverage_count, error_message, detail_json))
        except BaseException as exc:
            is_base = not isinstance(exc, Exception)
            prefix = "interrupted" if is_base else "exception"
            error_message = f"{prefix}({type(exc).__name__}): {exc}"[:1000]
            detail_json = {"phase": "后处理", "error": str(exc), "error_type": type(exc).__name__, "is_base_exception": is_base}
            self._runs.finish(run.id, CAPTURE_FAILED, 0, 0, error_message, detail_json)
            return self._run_to_dict(self._merge_finished_run(run, CAPTURE_FAILED, 0, 0, error_message, detail_json))
        finally:
            lock.release()

    def _execute(self, policy: CapturePolicy) -> CaptureExecutionResult:
        policy_now = _policy_local_now(policy, self._now_provider())
        return self._execute_requests(policy, build_capture_requests(policy, policy_now))

    def _execute_requests(self, policy: CapturePolicy, requests: Sequence[CaptureRequest]) -> CaptureExecutionResult:
        row_count = 0
        coverage_count = 0
        partial_batches: list[dict[str, object]] = []
        failed_batches: list[dict[str, object]] = []
        for request in requests:
            try:
                batch_result = self._run_capture_batch(request)
                row_count += batch_result.row_count_override if batch_result.row_count_override is not None else len(batch_result.items)
                coverage_count += batch_result.store_write_count
                for issue in batch_result.partial_issues:
                    partial_batches.append({"request_identity": request.request_identity, "error": issue})
            except Exception as exc:
                if request.capability_id == "stocks.quotes.intraday":
                    trade_date = format_date_value(request.request_identity.get("start_date", request.request_identity.get("trade_date", "")))
                    for code in request.request_identity.get("codes", []):
                        self._gaps.record_system_failure(request.capability_id, str(code), trade_date, str(exc))
                failed_batches.append({"request_identity": request.request_identity, "error": str(exc)})
        if policy.capability_id == "stocks.quotes.intraday" and requests != () and row_count == 0 and partial_batches == () and failed_batches == ():
            failed_batches.append({"request_identity": {}, "error": "股票 1m 分钟线本轮未获取到任何数据"})
        if policy.capability_id == "stocks.quotes.intraday" and row_count > 0 and coverage_count == 0:
            failed_batches.append({"request_identity": {}, "error": "股票 1m 分钟线本轮未写入 fact.stock_bar_1m"})
        if policy.capability_id == "stocks.catalog" and requests != () and row_count == 0:
            failed_batches.append({"request_identity": {}, "error": "股票目录权威刷新未返回任何数据"})
        if policy.capability_id in {"concepts.quotes.daily", "boards.quotes.daily"} and requests != () and row_count == 0:
            failed_batches.append({"request_identity": {}, "error": f"{policy.capability_id} 本轮未获取到任何数据"})
        if policy.capability_id in {"concepts.quotes.daily", "boards.quotes.daily"} and row_count > 0 and coverage_count == 0:
            failed_batches.append({"request_identity": {}, "error": f"{policy.capability_id} 本轮未写入事实表"})
        return CaptureExecutionResult(row_count, coverage_count, tuple(partial_batches), tuple(failed_batches))

    def _execution_status(self, result: CaptureExecutionResult) -> str:
        if result.failed_batches != ():
            return CAPTURE_FAILED
        if result.partial_batches != ():
            return CAPTURE_PARTIAL
        return CAPTURE_SUCCESS

    @staticmethod
    def _require_repair_write_result(
        result: CaptureExecutionResult,
        requests: Sequence[CaptureRequest],
    ) -> CaptureExecutionResult:
        """An explicit repair is not successful until it has changed published storage."""
        failures = list(result.failed_batches)
        request_identity = requests[0].request_identity if requests else {}
        if result.row_count <= 0:
            failures.append({"request_identity": request_identity, "error": "repair returned zero rows"})
        if result.coverage_count <= 0:
            failures.append({"request_identity": request_identity, "error": "repair wrote zero rows"})
        return CaptureExecutionResult(
            result.row_count,
            result.coverage_count,
            result.partial_batches,
            tuple(failures),
        )

    def _run_capture_batch(self, request: CaptureRequest) -> _CaptureBatchResult:
        if request.capability_id == "futures.quotes.main_continuous.1m":
            result = self._runtime.futures.update_main_continuous(**request.request_identity)
            errors = tuple(str(item.get("error", "")) for item in result.get("errors", []) if isinstance(item, dict))
            return _CaptureBatchResult(
                items=(),
                store_write_count=int(result.get("updated_products", 0)),
                partial_issues=errors,
                row_count_override=int(result.get("fetched_rows", 0)),
            )
        if request.capability_id == "stocks.quotes.intraday":
            return self._run_intraday_capture_batch(request)
        items, report = self._run_runtime_request(request)
        normalized_items = tuple(self._normalize_runtime_items(items))
        return _CaptureBatchResult(normalized_items, int(getattr(report, "store_write_count", 0)))

    def _run_intraday_capture_batch(self, request: CaptureRequest) -> _CaptureBatchResult:
        result, report = self._runtime.stocks.get_quotes_query_result_with_report(
            StockQuotesRequest(**request.request_identity),
            write_fact_ref=False,
        )
        incomplete_codes = tuple(item.code for item in result.meta.codes if not item.complete)
        if incomplete_codes == ():
            complete_items = tuple(result.items)
        else:
            incomplete_code_set = set(incomplete_codes)
            complete_items = tuple(item for item in result.items if item.code not in incomplete_code_set)
        write_count = self._write_fact_ref_items(request.capability_id, complete_items)
        trade_date = format_date_value(request.request_identity.get("start_date", request.request_identity.get("trade_date", "")))
        provider_results = report.to_dict()
        provider_success_count = sum(int(item.get("success_count", 0) or 0) for item in report.package_reports())
        system_failed = report.source_error_count > 0 and provider_success_count == 0 and sum(report.source_hit_counts.values()) == 0
        for summary in result.meta.codes:
            if summary.complete:
                if write_count > 0 and trade_date != "":
                    self._gaps.resolve(request.capability_id, summary.code, trade_date, summary.actual_bar_count)
                continue
            if trade_date == "":
                raise RuntimeError(f"股票 1m 缺口缺少交易日: code={summary.code}")
            error = "所有 provider 调用失败" if system_failed else "所有可用 provider 均未返回完整 240 根分钟线"
            self._gaps.record_incomplete(
                request.capability_id,
                summary.code,
                trade_date,
                summary.expected_bar_count,
                summary.actual_bar_count,
                provider_results,
                system_failed,
                error,
            )
        if incomplete_codes == ():
            return _CaptureBatchResult(complete_items, write_count)
        sample_codes = ",".join(incomplete_codes[:10])
        issue = f"股票 1m 分钟线覆盖不完整: incomplete_codes={len(incomplete_codes)} sample={sample_codes}"
        return _CaptureBatchResult(complete_items, write_count, (issue,))

    def _run_runtime_request(self, request: CaptureRequest):
        if request.capability_id == "stocks.quotes.daily":
            return self._runtime.stocks.get_quotes_with_report(StockQuotesRequest(**request.request_identity))
        if request.capability_id == "stocks.quotes.daily_snapshot":
            return self._runtime.stocks.get_daily_snapshot_with_report(StockDailySnapshotRequest(**request.request_identity))
        if request.capability_id == "indexes.quotes.daily":
            return self._runtime.indexes.get_quotes_with_report(IndexQuotesRequest(**request.request_identity))
        if request.capability_id == "markets.calendar.trading":
            return self._runtime.markets.get_trading_calendar_with_report(TradingCalendarRequest(**request.request_identity))
        if request.capability_id == "indexes.members":
            return self._runtime.indexes.get_members_with_report(IndexMembersRequest(**request.request_identity))
        if request.capability_id == "concepts.quotes.daily":
            if "concept_ids" in request.request_identity:
                items = self._normalize_runtime_items(self._runtime.concepts.get_quotes(**request.request_identity))
            else:
                items = self._normalize_runtime_items(self._runtime.concepts.get_market_daily_snapshot(**request.request_identity))
            trade_date = format_date_value(request.request_identity.get("trade_date", ""))
            requested_ids = request.request_identity.get("concept_ids")
            candidate_ids = tuple(str(item) for item in requested_ids) if isinstance(requested_ids, list) else _concept_ids()
            expected_ids = _derivable_concept_ids(trade_date, candidate_ids)
            missing_ids = _missing_derivable_concept_ids(items, expected_ids)
            if missing_ids != ():
                sample = ",".join(missing_ids[:10])
                raise RuntimeError(
                    f"概念日线快照覆盖不完整: expected={len(expected_ids)} "
                    f"actual={len(expected_ids) - len(missing_ids)} missing={len(missing_ids)} sample={sample}"
                )
            normalized_items = _normalized_capture_items(request.capability_id, request.request_identity, items)
            store_result(request.capability_id, request.request_identity, normalized_items, ContractReport(contract_name=request.capability_id))
            fact_write_count = self._write_fact_ref_items(request.capability_id, normalized_items)
            return normalized_items, _CaptureRuntimeReport("concepts.quotes.daily", fact_write_count)
        if request.capability_id == "boards.quotes.daily":
            handler = get_default_source_package_registry().get_handler("derived_core", "get_industry_board_quotes")
            items = self._normalize_runtime_items(handler(**request.request_identity))
            expected_count = _industry_count()
            if expected_count > 0 and len(items) < int(expected_count * 0.9):
                raise RuntimeError(f"行业板块日线快照覆盖不完整: expected={expected_count} actual={len(items)}")
            normalized_items = _normalized_capture_items(request.capability_id, request.request_identity, items)
            store_result(request.capability_id, request.request_identity, normalized_items, ContractReport(contract_name=request.capability_id))
            fact_write_count = self._write_fact_ref_items(request.capability_id, normalized_items)
            return normalized_items, _CaptureRuntimeReport("boards.quotes.daily", fact_write_count)
        if request.capability_id == "concepts.members":
            refresh_members = getattr(self._runtime.concepts, "refresh_members", self._runtime.concepts.get_members)
            items = self._normalize_runtime_items(refresh_members(**request.request_identity))
            normalized_items = _normalized_capture_items(request.capability_id, request.request_identity, items)
            write_result = store_result(request.capability_id, request.request_identity, normalized_items, ContractReport(contract_name=request.capability_id))
            self._write_fact_ref_items(request.capability_id, normalized_items)
            return normalized_items, _CaptureRuntimeReport("concepts.members", write_result.coverage_count)
        if request.capability_id == "markets.events.news":
            return self._run_news_update(request)
        method_spec = RUNTIME_METHODS.get(request.capability_id)
        if method_spec is not None:
            component_name, method_name = method_spec
            component = getattr(self._runtime, component_name)
            items = getattr(component, method_name)(**request.request_identity)
            if request.capability_id == "concepts.members.history":
                return items, _CaptureRuntimeReport(request.capability_id, self._write_fact_ref_items(request.capability_id, items))
            normalized_items = self._normalize_runtime_items(items)
            write_result = store_result(request.capability_id, request.request_identity, normalized_items, ContractReport(contract_name=request.capability_id))
            return normalized_items, _CaptureRuntimeReport(request.capability_id, write_result.coverage_count)
        raise ValueError(f"未支持 capture capability: {request.capability_id}")

    def _run_news_update(self, request: CaptureRequest):
        updater = getattr(self._runtime.news, "update_events_capture", None)
        if updater is not None:
            return updater(**request.request_identity)
        result = self._runtime.news.get_events(**request.request_identity)
        items = self._normalize_runtime_items(result)
        write_result = store_result(request.capability_id, request.request_identity, items, ContractReport(contract_name=request.capability_id))
        return items, _CaptureRuntimeReport(request.capability_id, write_result.coverage_count)

    def _normalize_runtime_items(self, value: object) -> list[object]:
        if value is None:
            return []
        events = getattr(value, "events", None)
        if isinstance(events, list):
            return list(events)
        if isinstance(value, list):
            return value
        if isinstance(value, tuple):
            return list(value)
        return [value]

    def _write_fact_ref_items(self, capability_id: str, items: object) -> int:
        normalized_items = self._normalize_runtime_items(items)
        if normalized_items == []:
            return 0
        writer = get_fact_ref_writer(capability_id)
        if writer is None:
            return 0
        return len(normalized_items) if writer(normalized_items) else 0

    def _precheck_skip(self, policy: CapturePolicy, planned_time: datetime) -> CaptureRun | None:
        if not policy.enabled:
            return self._create_finished_run(policy, planned_time, CAPTURE_SKIPPED, 0, 0, "", {"reason": "capture_policy_disabled"})
        cache_policy = self._cache_store.get_policy(policy.capability_id)
        if cache_policy is None or not cache_policy.write_enabled:
            return self._create_finished_run(policy, planned_time, CAPTURE_SKIPPED, 0, 0, "", {"reason": "cache_policy_disabled"})
        if policy.window_count < 1:
            return self._create_finished_run(policy, planned_time, CAPTURE_SKIPPED, 0, 0, "", {"reason": "empty_window"})
        if policy.batch_size < 1:
            return self._create_finished_run(policy, planned_time, CAPTURE_SKIPPED, 0, 0, "", {"reason": "empty_batch_size"})
        return None

    def _precheck_repair_skip(self, policy: CapturePolicy, planned_time: datetime) -> CaptureRun | None:
        """Keep explicit repair independent of scheduler enablement, but never weaken write gates."""
        cache_policy = self._cache_store.get_policy(policy.capability_id)
        if cache_policy is None or not cache_policy.write_enabled:
            return self._create_finished_run(policy, planned_time, CAPTURE_SKIPPED, 0, 0, "", {"reason": "cache_policy_disabled"})
        if not self._has_executable_repair_path(policy.capability_id):
            return self._create_finished_run(policy, planned_time, CAPTURE_SKIPPED, 0, 0, "", {"reason": "repair_path_unavailable"})
        if policy.window_count < 1:
            return self._create_finished_run(policy, planned_time, CAPTURE_SKIPPED, 0, 0, "", {"reason": "empty_window"})
        if policy.batch_size < 1:
            return self._create_finished_run(policy, planned_time, CAPTURE_SKIPPED, 0, 0, "", {"reason": "empty_batch_size"})
        return None

    @staticmethod
    def _has_executable_repair_path(capability_id: str) -> bool:
        return capability_id in {
            "futures.quotes.main_continuous.1m",
            "stocks.quotes.intraday",
            "indexes.members",
            "concepts.quotes.daily",
            "boards.quotes.daily",
            "concepts.members",
            "markets.events.news",
        } or capability_id in RUNTIME_METHODS

    def _create_finished_run(
        self,
        policy: CapturePolicy,
        planned_time: datetime,
        status: str,
        row_count: int,
        coverage_count: int,
        error_message: str,
        detail_json: dict[str, object],
    ) -> CaptureRun:
        run = self._runs.create(policy.capability_id, status, planned_time, detail_json)
        self._runs.finish(run.id, status, row_count, coverage_count, error_message, detail_json)
        return self._merge_finished_run(run, status, row_count, coverage_count, error_message, detail_json)

    def _merge_finished_run(self, run: CaptureRun, status: str, row_count: int, coverage_count: int, error_message: str, detail_json: dict[str, object]) -> CaptureRun:
        return CaptureRun(
            id=run.id,
            capability_id=run.capability_id,
            status=status,
            planned_time=run.planned_time,
            started_at=run.started_at,
            finished_at=self._now_provider().replace(tzinfo=None),
            row_count=row_count,
            coverage_count=coverage_count,
            error_message=error_message,
            detail_json=detail_json,
        )

    def _get_policy(self, capability_id: str) -> CapturePolicy:
        root_capability_id = get_capability_config_root(capability_id)
        policy = self._policies.get(root_capability_id)
        if policy is None:
            raise KeyError(f"未知 capture 策略: {capability_id}")
        return policy

    def _policy_to_dict(self, policy: CapturePolicy) -> dict[str, object]:
        return {
            "capability_id": policy.capability_id,
            "enabled": policy.enabled,
            "cadence": policy.cadence,
            "run_time": policy.run_time.strftime("%H:%M:%S"),
            "timezone": policy.timezone,
            "weekday": policy.weekday,
            "month": policy.month,
            "month_day": policy.month_day,
            "scope_profile": policy.scope_profile,
            "scope_profile_label": PROFILE_LABELS.get(policy.scope_profile, policy.scope_profile),
            "window_count": policy.window_count,
            "batch_size": policy.batch_size,
            "notes": policy.notes,
        }

    def _run_to_dict(self, run: CaptureRun) -> dict[str, object]:
        return {
            "id": run.id,
            "capability_id": run.capability_id,
            "status": run.status,
            "planned_time": _serialize_value(run.planned_time),
            "started_at": _serialize_value(run.started_at),
            "finished_at": _serialize_value(run.finished_at),
            "row_count": run.row_count,
            "coverage_count": run.coverage_count,
            "error_message": run.error_message,
            "detail_json": _serialize_value(run.detail_json),
        }


def _canonical_repair_scope(scope: dict[str, object]) -> dict[str, object]:
    if not isinstance(scope, dict):
        raise TypeError("repair scope must be an object")

    def normalize(value: object, key: str = "") -> object:
        if isinstance(value, dict):
            return {
                str(item_key): normalize(item_value, str(item_key))
                for item_key, item_value in sorted(value.items(), key=lambda item: str(item[0]))
            }
        if isinstance(value, (list, tuple)):
            items = [normalize(item) for item in value]
            if key in {"codes", "concept_ids", "index_codes", "product_codes"}:
                return sorted(set(str(item) for item in items))
            return items
        if isinstance(value, (date, datetime)):
            return value.isoformat()
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        raise TypeError(f"repair scope contains unsupported value: {type(value).__name__}")

    return normalize(scope)  # type: ignore[return-value]


def _repair_scope_and_version(scope: dict[str, object], dataset_version: str) -> tuple[dict[str, object], str]:
    raw_scope = dict(scope)
    scope_version = str(raw_scope.pop("dataset_version", ""))
    explicit_version = str(dataset_version)
    if explicit_version != "" and scope_version != "" and explicit_version != scope_version:
        raise ValueError("conflicting repair dataset_version values")
    return _canonical_repair_scope(raw_scope), explicit_version or scope_version


def run_due_captures() -> tuple[dict[str, object], ...]:
    return QuoteMuxCaptureJob().run_due_captures()


def run_capture(capability_id: str) -> dict[str, object]:
    return QuoteMuxCaptureJob().run_capture(capability_id)


def run_repair(dataset: str, scope: dict[str, object], dataset_version: str = "") -> dict[str, object]:
    return QuoteMuxCaptureJob().run_repair(dataset, scope, dataset_version)


def reconcile_stale_capture_runs(started_before: datetime | None = None) -> dict[str, object]:
    """Fail orphaned running rows whose capability advisory lock is free."""
    return CaptureRunMaintenance().reconcile_stale_running(started_before)
