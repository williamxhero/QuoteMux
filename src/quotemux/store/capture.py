from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Callable, Sequence
from zoneinfo import ZoneInfo

import pandas as pd
import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from quotemux.infra.common import format_date_value
from quotemux.infra.db.client import execute_many, execute_sql, query_dataframe
from quotemux.infra.db.config import DL_DB_CONNECT_TIMEOUT, DL_DB_HOST, DL_DB_NAME, DL_DB_PASSWORD, DL_DB_PORT, DL_DB_USER
from quotemux.infra.db.reference_reads import load_board_catalog_frame, load_index_catalog_frame, load_stock_active_codes_frame, load_trade_calendar_frame
from quotemux.capabilities import get_capability_config_root, is_independently_configurable_capability_id
from quotemux.capabilities.inventory import list_capability_ids
from quotemux.reports import ContractReport
from quotemux.requests.indexes import IndexMembersRequest, IndexQuotesRequest
from quotemux.requests.markets import TradingCalendarRequest
from quotemux.requests.stocks import StockDailySnapshotRequest, StockQuotesRequest
from quotemux.store.default_update_policy import get_capability_update_policy_default
from quotemux.store.postgres import _ensure_schema, get_postgres_cache_store
from quotemux.store.runtime import store_result


CAPTURE_RUNNING = "running"
CAPTURE_SUCCESS = "success"
CAPTURE_FAILED = "failed"
CAPTURE_SKIPPED = "skipped"

CADENCE_DAILY = "daily"
CADENCE_WEEKLY = "weekly"
CADENCE_MONTHLY = "monthly"
CADENCE_YEARLY = "yearly"
VALID_CADENCES = (CADENCE_DAILY, CADENCE_WEEKLY, CADENCE_MONTHLY, CADENCE_YEARLY)

PROFILE_ACTIVE_STOCKS_RECENT_TRADING_DAYS = "active_stocks_recent_trading_days"
PROFILE_INDEXES_RECENT_TRADING_DAYS = "indexes_recent_trading_days"
PROFILE_DAILY_SNAPSHOT_RECENT_TRADING_DAYS = "daily_snapshot_recent_trading_days"
PROFILE_TRADING_CALENDAR_YEAR_WINDOW = "trading_calendar_year_window"
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
    PROFILE_BOARDS_RECENT_TRADING_DAYS: "板块最近交易日",
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
    failed_batches: tuple[dict[str, object], ...]


@dataclass(frozen=True)
class _CaptureRuntimeReport:
    contract_name: str
    store_write_count: int = 1


def _default_profile_for_capability(capability_id: str) -> str:
    if capability_id in {"stocks.quotes.daily", "stocks.quotes.intraday"}:
        return PROFILE_ACTIVE_STOCKS_RECENT_TRADING_DAYS
    if capability_id == "stocks.quotes.daily_snapshot":
        return PROFILE_DAILY_SNAPSHOT_RECENT_TRADING_DAYS
    if capability_id in {"indexes.quotes.daily", "indexes.members"}:
        return PROFILE_INDEXES_RECENT_TRADING_DAYS
    if capability_id in {"boards.quotes.daily", "boards.members", "boards.members.history", "boards.indicators.money_flow"}:
        return PROFILE_BOARDS_RECENT_TRADING_DAYS
    if capability_id == "markets.calendar.trading":
        return PROFILE_TRADING_CALENDAR_YEAR_WINDOW
    if capability_id in {"stocks.catalog", "stocks.catalog.archive", "indexes.catalog", "boards.catalog", "boards.reference.categories", "markets.participants.hot_money"}:
        return PROFILE_CATALOG_SNAPSHOT
    if capability_id in {"stocks.profile.basic", "stocks.profile.company", "stocks.profile.managers", "stocks.profile.management_rewards", "stocks.profile.name_history", "indexes.profile", "boards.profile"}:
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
    if capability_id.startswith("markets.") or capability_id == "boards.indicators.money_flow.snapshot":
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
    if capability_id == "stocks.quotes.intraday":
        return 5
    if scope_profile in {PROFILE_ACTIVE_STOCKS_RECENT_TRADING_DAYS, PROFILE_INDEXES_RECENT_TRADING_DAYS, PROFILE_BOARDS_RECENT_TRADING_DAYS, PROFILE_MARKET_RECENT_TRADING_DAYS, PROFILE_OWNERSHIP_RECENT_TRADING_DAYS, PROFILE_RESEARCH_RECENT_DATES, PROFILE_CORPORATE_ACTIONS_RECENT_ANNOUNCEMENTS}:
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
        specs.append(
            DefaultCapturePolicySpec(
                capability_id,
                policy_default.capture_enabled,
                policy_default.capture_cadence,
                time(18, 0),
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
        created_at timestamp without time zone not null default now(),
        updated_at timestamp without time zone not null default now()
    )
    """,
    "alter table capability_capture_policy add column if not exists month integer",
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


_CAPTURE_SCHEMA_READY = False
_CAPTURE_SCHEMA_FAILED = False


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
        )
        for spec in DEFAULT_CAPTURE_POLICY_SPECS
    ]
    ok = execute_many(
        """
        insert into capability_capture_policy (
            capability_id, enabled, cadence, run_time, timezone, weekday,
            month, month_day, scope_profile, window_count, batch_size, notes
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        on conflict (capability_id) do update set
            scope_profile = excluded.scope_profile,
            cadence = capability_capture_policy.cadence,
            updated_at = now()
        """,
        params,
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

    def create(self, capability_id: str, status: str, planned_time: datetime, detail_json: dict[str, object]) -> CaptureRun:
        if not _ensure_capture_schema():
            raise RuntimeError("capture schema 初始化失败")
        root_capability_id = get_capability_config_root(capability_id)
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
            host=DL_DB_HOST,
            port=DL_DB_PORT,
            dbname=DL_DB_NAME,
            user=DL_DB_USER,
            password=DL_DB_PASSWORD,
            connect_timeout=DL_DB_CONNECT_TIMEOUT,
            row_factory=dict_row,
        )
        with connection.cursor() as cursor:
            cursor.execute("select pg_try_advisory_lock(hashtext(%s)) as locked", (self._capability_id,))
            row = cursor.fetchone()
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


def _chunk(items: Sequence[str], size: int) -> tuple[tuple[str, ...], ...]:
    actual_size = max(1, size)
    return tuple(tuple(items[index: index + actual_size]) for index in range(0, len(items), actual_size))


def _date_range_end_text(now: datetime) -> str:
    return now.strftime("%Y-%m-%d")


def _recent_trading_days(window_count: int, now: datetime) -> tuple[str, ...]:
    end_text = _date_range_end_text(now)
    start_day = now.date() - timedelta(days=max(10, window_count * 3))
    frame = load_trade_calendar_frame(start_day.strftime("%Y-%m-%d"), end_text, True)
    if _is_empty_dataframe(frame):
        return ()
    values = [format_date_value(row["trade_date"]) for row in frame.to_dict("records")]
    return tuple(item for item in values if item != "")[-window_count:]


def _active_stock_codes(trade_date: str) -> tuple[str, ...]:
    frame = load_stock_active_codes_frame(trade_date)
    if _is_empty_dataframe(frame):
        return ()
    return tuple(str(row["code"]) for row in frame.to_dict("records") if str(row["code"]) != "")


def _index_codes() -> tuple[str, ...]:
    frame = load_index_catalog_frame([])
    if _is_empty_dataframe(frame):
        return ()
    return tuple(str(row["index_code"]) for row in frame.to_dict("records") if str(row["index_code"]) != "")


def _board_codes() -> tuple[str, ...]:
    frame = load_board_catalog_frame("active")
    if _is_empty_dataframe(frame):
        return ()
    return tuple(str(row["board_code"]) for row in frame.to_dict("records") if str(row["board_code"]) != "")


def _active_stock_requests(policy: CapturePolicy, capability_id: str, now: datetime) -> tuple[CaptureRequest, ...]:
    trading_days = _recent_trading_days(policy.window_count, now)
    if trading_days == ():
        return ()
    codes = _active_stock_codes(trading_days[-1])
    if codes == ():
        return ()
    start_date = trading_days[0]
    end_date = trading_days[-1]
    freq = "1d" if capability_id == "stocks.quotes.daily" else "1m"
    return tuple(
        CaptureRequest(
            capability_id,
            {
                "codes": list(batch),
                "freq": freq,
                "trade_date": "",
                "start_date": start_date,
                "end_date": end_date,
                "start_time": "",
                "end_time": "",
                "count": None,
                "adjust": "none",
                "limit": 5000,
            },
        )
        for batch in _chunk(codes, policy.batch_size)
    )


def _index_quote_requests(policy: CapturePolicy, capability_id: str, now: datetime) -> tuple[CaptureRequest, ...]:
    trading_days = _recent_trading_days(policy.window_count, now)
    if trading_days == ():
        return ()
    codes = _index_codes()
    if codes == ():
        return ()
    start_date = trading_days[0]
    end_date = trading_days[-1]
    return tuple(
        CaptureRequest(
            capability_id,
            {
                "index_codes": list(batch),
                "freq": "1d",
                "trade_date": "",
                "start_date": start_date,
                "end_date": end_date,
                "count": None,
                "limit": 5000,
            },
        )
        for batch in _chunk(codes, policy.batch_size)
    )


def _daily_snapshot_requests(policy: CapturePolicy, capability_id: str, now: datetime) -> tuple[CaptureRequest, ...]:
    trading_days = _recent_trading_days(policy.window_count, now)
    return tuple(CaptureRequest(capability_id, {"trade_date": trade_date, "limit": 10000, "offset": 0}) for trade_date in trading_days)


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


def _board_quote_requests(policy: CapturePolicy, capability_id: str, now: datetime) -> tuple[CaptureRequest, ...]:
    trading_days = _recent_trading_days(policy.window_count, now)
    if trading_days == ():
        return ()
    board_codes = _board_codes()
    if board_codes == ():
        return ()
    start_date = trading_days[0]
    end_date = trading_days[-1]
    return tuple(
        CaptureRequest(
            capability_id,
            {
                "board_codes": list(batch),
                "freq": "1d",
                "trade_date": "",
                "start_date": start_date,
                "end_date": end_date,
                "start_time": "",
                "end_time": "",
                "count": None,
                "limit": 5000,
            },
        )
        for batch in _chunk(board_codes, policy.batch_size)
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
    )


def _board_member_requests(policy: CapturePolicy, capability_id: str, now: datetime) -> tuple[CaptureRequest, ...]:
    trading_days = _recent_trading_days(policy.window_count, now)
    if trading_days == ():
        return ()
    board_codes = _board_codes()
    return tuple(
        CaptureRequest(capability_id, {"board_code": board_code, "trade_date": trade_date})
        for trade_date in trading_days
        for board_code in board_codes
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
        "stocks.catalog": {"codes": [], "name": "", "exchange": "", "list_status": "L", "include_delisted": False, "limit": 10000, "offset": 0},
        "stocks.catalog.archive": {"trade_date": end_date, "code": "", "name": "", "industry": "", "area": "", "limit": 10000, "offset": 0},
        "indexes.catalog": {"category": "", "market": "", "publisher": "", "status": "active", "limit": 10000, "offset": 0},
        "boards.catalog": {"category": "", "market": "", "status": "active", "limit": 10000, "offset": 0},
        "boards.reference.categories": {"parent_code": "", "level": None},
        "markets.participants.hot_money": {"name": "", "tag": "", "limit": 10000, "offset": 0},
    }
    identity = identities.get(capability_id)
    if identity is None:
        return ()
    return (CaptureRequest(capability_id, identity),)


def _single_entity_snapshot_requests(policy: CapturePolicy, capability_id: str, now: datetime) -> tuple[CaptureRequest, ...]:
    start_date, end_date = _date_window(policy, now)
    if capability_id.startswith("stocks.profile."):
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
        if capability_id == "stocks.profile.name_history":
            return tuple(CaptureRequest(capability_id, {"code": code, "start_date": start_date, "end_date": end_date}) for code in codes)
    if capability_id == "indexes.profile":
        return tuple(CaptureRequest(capability_id, {"index_code": index_code}) for index_code in _index_codes())
    if capability_id == "boards.profile":
        return tuple(CaptureRequest(capability_id, {"board_code": board_code}) for board_code in _board_codes())
    return ()


def _market_recent_trading_day_requests(policy: CapturePolicy, capability_id: str, now: datetime) -> tuple[CaptureRequest, ...]:
    start_date, end_date = _date_window(policy, now)
    if capability_id == "boards.indicators.money_flow.snapshot":
        return tuple(CaptureRequest(capability_id, {"trade_date": trade_date, "scope": "", "limit": 10000, "offset": 0}) for trade_date in _recent_trading_days(policy.window_count, now))
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
        return tuple(CaptureRequest(capability_id, {"codes": "", "trade_date": trade_date}) for trade_date in _recent_trading_days(policy.window_count, now))
    identity = identities.get(capability_id)
    if identity is None:
        return ()
    return (CaptureRequest(capability_id, identity),)


def _stock_trading_day_requests(policy: CapturePolicy, capability_id: str, now: datetime) -> tuple[CaptureRequest, ...]:
    start_date, end_date = _date_window(policy, now)
    trading_days = _recent_trading_days(1, now)
    codes = _active_stock_codes(trading_days[-1]) if trading_days != () else ()
    if codes == ():
        return ()
    batch_requests = {
        "stocks.indicators.daily_basic": lambda batch: {"code": "", "codes": ",".join(batch), "trade_date": "", "start_date": start_date, "end_date": end_date},
        "stocks.indicators.daily_valuation": lambda batch: {"code": "", "codes": ",".join(batch), "trade_date": "", "start_date": start_date, "end_date": end_date},
        "stocks.indicators.daily_market_value": lambda batch: {"code": "", "codes": ",".join(batch), "trade_date": "", "start_date": start_date, "end_date": end_date},
    }
    batch_builder = batch_requests.get(capability_id)
    if batch_builder is not None:
        return tuple(CaptureRequest(capability_id, batch_builder(batch)) for batch in _chunk(codes, policy.batch_size))
    per_code = {
        "stocks.quotes.auctions": lambda code: {"code": code, "session": "", "trade_date": "", "start_date": start_date, "end_date": end_date},
        "stocks.factors.adj": lambda code: {"code": code, "start_date": start_date, "end_date": end_date, "base_date": ""},
        "stocks.factors.technical": lambda code: {"code": code, "trade_date": "", "start_date": start_date, "end_date": end_date, "adjust": "none"},
        "stocks.indicators.money_flow": lambda code: {"code": code, "trade_date": "", "start_date": start_date, "end_date": end_date, "view": ""},
        "stocks.indicators.premarket": lambda code: {"code": code, "trade_date": "", "start_date": start_date, "end_date": end_date},
        "stocks.indicators.chip_distribution": lambda code: {"code": code, "trade_date": "", "start_date": start_date, "end_date": end_date},
        "stocks.indicators.chip_performance": lambda code: {"code": code, "trade_date": "", "start_date": start_date, "end_date": end_date},
        "stocks.indicators.ah_comparisons": lambda code: {"code": code, "trade_date": "", "start_date": start_date, "end_date": end_date, "limit": 10000, "offset": 0},
        "stocks.signals.hl": lambda code: {"code": code, "trade_date": "", "start_date": start_date, "end_date": end_date},
        "stocks.signals.nine_turn": lambda code: {"code": code, "freq": "D", "trade_date": "", "start_date": start_date, "end_date": end_date},
        "stocks.indicators.risk_flags": lambda code: {"trade_date": "", "start_date": start_date, "end_date": end_date, "flag_type": "", "status": "", "limit": 10000, "offset": 0},
    }
    builder = per_code.get(capability_id)
    if builder is None:
        return ()
    if capability_id == "stocks.indicators.risk_flags":
        return (CaptureRequest(capability_id, builder("")),)
    return tuple(CaptureRequest(capability_id, builder(code)) for code in codes)


def _report_period_requests(policy: CapturePolicy, capability_id: str, now: datetime) -> tuple[CaptureRequest, ...]:
    trading_days = _recent_trading_days(1, now)
    codes = _active_stock_codes(trading_days[-1]) if trading_days != () else ()
    periods = _recent_report_periods(policy.window_count, now)
    if codes == () or periods == ():
        return ()
    start_period = periods[-1]
    end_period = periods[0]
    if capability_id == "stocks.finance.statements":
        return tuple(CaptureRequest(capability_id, {"codes": list(batch), "report_period": "", "start_period": start_period, "end_period": end_period, "report_type": ""}) for batch in _chunk(codes, policy.batch_size))
    if capability_id == "stocks.finance.indicators":
        return tuple(CaptureRequest(capability_id, {"code": "", "codes": ",".join(batch), "report_period": "", "start_period": start_period, "end_period": end_period}) for batch in _chunk(codes, policy.batch_size))
    per_code = {
        "stocks.finance.audits": lambda code: {"code": code, "report_period": "", "start_period": start_period, "end_period": end_period},
        "stocks.finance.disclosure_dates": lambda code: {"code": code, "report_period": "", "start_period": start_period, "end_period": end_period},
        "stocks.finance.express": lambda code: {"code": code, "report_period": "", "start_period": start_period, "end_period": end_period},
        "stocks.finance.forecasts": lambda code: {"code": code, "report_period": "", "start_period": start_period, "end_period": end_period},
        "stocks.finance.main_business": lambda code: {"code": code, "report_period": "", "start_period": start_period, "end_period": end_period, "classification": ""},
        "stocks.ownership.shareholders.top10": lambda code: {"code": code, "report_period": "", "start_period": start_period, "end_period": end_period},
        "stocks.ownership.shareholders.top10_float": lambda code: {"code": code, "report_period": "", "start_period": start_period, "end_period": end_period},
    }
    builder = per_code.get(capability_id)
    if builder is None:
        return ()
    return tuple(CaptureRequest(capability_id, builder(code)) for code in codes)


def _corporate_action_requests(policy: CapturePolicy, capability_id: str, now: datetime) -> tuple[CaptureRequest, ...]:
    start_date, end_date = _date_window(policy, now)
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
    return tuple(CaptureRequest(capability_id, builder(code)) for code in codes)


def _ownership_trading_day_requests(policy: CapturePolicy, capability_id: str, now: datetime) -> tuple[CaptureRequest, ...]:
    start_date, end_date = _date_window(policy, now)
    trading_days = _recent_trading_days(1, now)
    codes = _active_stock_codes(trading_days[-1]) if trading_days != () else ()
    builders = {
        "stocks.ownership.ccass_holdings": lambda code: {"code": code, "trade_date": "", "start_date": start_date, "end_date": end_date},
        "stocks.ownership.ccass_holding_details": lambda code: {"code": code, "trade_date": "", "start_date": start_date, "end_date": end_date},
        "stocks.ownership.hk_connect_holdings": lambda code: {"code": code, "trade_date": "", "start_date": start_date, "end_date": end_date},
        "stocks.ownership.pledges.stats": lambda code: {"code": code, "trade_date": "", "start_date": start_date, "end_date": end_date},
        "stocks.ownership.pledges.details": lambda code: {"code": code, "start_date": start_date, "end_date": end_date, "status": ""},
        "stocks.ownership.shareholders.count": lambda code: {"code": code, "trade_date": "", "start_date": start_date, "end_date": end_date},
        "stocks.ownership.shareholders.changes": lambda code: {"code": code, "trade_date": "", "start_date": start_date, "end_date": end_date},
    }
    builder = builders.get(capability_id)
    if builder is None:
        return ()
    return tuple(CaptureRequest(capability_id, builder(code)) for code in codes)


def _research_date_requests(policy: CapturePolicy, capability_id: str, now: datetime) -> tuple[CaptureRequest, ...]:
    start_date, end_date = _date_window(policy, now)
    if capability_id == "rankings.research.reports":
        return (CaptureRequest(capability_id, {"trade_date": "", "start_date": start_date, "end_date": end_date, "limit": 10000}),)
    trading_days = _recent_trading_days(1, now)
    codes = _active_stock_codes(trading_days[-1]) if trading_days != () else ()
    builders = {
        "stocks.research.reports": lambda code: {"code": code, "report_date": "", "start_date": start_date, "end_date": end_date},
        "stocks.research.surveys": lambda code: {"code": code, "survey_date": "", "start_date": start_date, "end_date": end_date},
    }
    builder = builders.get(capability_id)
    if builder is None:
        return ()
    return tuple(CaptureRequest(capability_id, builder(code)) for code in codes)


def _research_month_requests(policy: CapturePolicy, capability_id: str, now: datetime) -> tuple[CaptureRequest, ...]:
    if capability_id != "rankings.research.broker_monthly_picks":
        return ()
    return tuple(CaptureRequest(capability_id, {"trade_month": month_text, "limit": 10000}) for month_text in _recent_months(policy.window_count, now))


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
        for trade_date in _recent_trading_days(policy.window_count, now)
    )


def build_capture_requests(policy: CapturePolicy, now: datetime) -> tuple[CaptureRequest, ...]:
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
    if policy.scope_profile == PROFILE_BOARDS_RECENT_TRADING_DAYS and policy.capability_id == "boards.quotes.daily":
        return _board_quote_requests(policy, policy.capability_id, now)
    if policy.scope_profile == PROFILE_INDEXES_RECENT_TRADING_DAYS and policy.capability_id == "indexes.members":
        return _index_member_requests(policy, policy.capability_id, now)
    if policy.scope_profile == PROFILE_BOARDS_RECENT_TRADING_DAYS and policy.capability_id == "boards.members":
        return _board_member_requests(policy, policy.capability_id, now)
    if policy.scope_profile == PROFILE_BOARDS_RECENT_TRADING_DAYS and policy.capability_id == "boards.members.history":
        start_date, end_date = _date_window(policy, now)
        return tuple(CaptureRequest(policy.capability_id, {"board_code": board_code, "start_date": start_date, "end_date": end_date}) for board_code in _board_codes())
    if policy.scope_profile == PROFILE_BOARDS_RECENT_TRADING_DAYS and policy.capability_id == "boards.indicators.money_flow":
        start_date, end_date = _date_window(policy, now)
        return tuple(CaptureRequest(policy.capability_id, {"board_code": board_code, "trade_date": "", "start_date": start_date, "end_date": end_date, "scope": ""}) for board_code in _board_codes())
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
    if policy.cadence == CADENCE_DAILY:
        scheduled_day = local_date
    elif policy.cadence == CADENCE_WEEKLY:
        if local_date.weekday() != 6:
            return None
        scheduled_day = local_date
    elif policy.cadence == CADENCE_MONTHLY:
        if local_date.day != monthrange(local_date.year, local_date.month)[1]:
            return None
        scheduled_day = local_date
    elif policy.cadence == CADENCE_YEARLY:
        if local_date.month != 12 or local_date.day != 31:
            return None
        scheduled_day = local_date
    else:
        scheduled_day = local_date
    scheduled = datetime.combine(scheduled_day, time(0, 0), ZoneInfo(policy.timezone))
    return scheduled.replace(tzinfo=None)


def is_capture_due(policy: CapturePolicy, runs: CaptureRunRepository, now: datetime) -> bool:
    if not policy.enabled:
        return False
    planned_time = _scheduled_time(policy, now)
    if planned_time is None:
        return False
    previous = runs.latest_for_planned_time(policy.capability_id, planned_time)
    return previous is None or previous.status == CAPTURE_SKIPPED


RUNTIME_METHODS: dict[str, tuple[str, str]] = {
    "boards.catalog": ("boards", "get_catalog"),
    "boards.indicators.money_flow": ("boards", "get_money_flow"),
    "boards.indicators.money_flow.snapshot": ("boards", "get_market_money_flow"),
    "boards.members.history": ("boards", "get_member_history"),
    "boards.profile": ("boards", "get_profile"),
    "boards.reference.categories": ("boards", "get_categories"),
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
    ) -> None:
        if runtime is None:
            from quotemux.runtime import QuoteMux

            self._runtime = QuoteMux()
        else:
            self._runtime = runtime
        self._policies = policies or CapturePolicyRepository()
        self._runs = runs or CaptureRunRepository()
        self._locks = locks or PostgresAdvisoryLockFactory()
        self._now_provider = now_provider or datetime.now
        self._cache_store = cache_store or get_postgres_cache_store()

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

    def run_due_captures(self) -> tuple[dict[str, object], ...]:
        now = self._now_provider()
        runs: list[dict[str, object]] = []
        for policy in self._policies.list():
            planned_time = _scheduled_time(policy, now)
            if planned_time is not None and is_capture_due(policy, self._runs, now):
                runs.append(self.run_capture(policy.capability_id, planned_time))
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
        run = self._runs.create(root_capability_id, CAPTURE_RUNNING, actual_planned_time, {"phase": "预处理"})
        try:
            result = self._execute(policy)
            status = CAPTURE_SUCCESS if result.failed_batches == () else CAPTURE_FAILED
            error_message = "" if result.failed_batches == () else "部分 batch 采集失败"
            detail_json = {"phase": "后处理", "failed_batches": list(result.failed_batches)}
            self._runs.finish(run.id, status, result.row_count, result.coverage_count, error_message, detail_json)
            return self._run_to_dict(self._merge_finished_run(run, status, result.row_count, result.coverage_count, error_message, detail_json))
        except Exception as exc:
            detail_json = {"phase": "后处理", "error": str(exc)}
            self._runs.finish(run.id, CAPTURE_FAILED, 0, 0, str(exc), detail_json)
            return self._run_to_dict(self._merge_finished_run(run, CAPTURE_FAILED, 0, 0, str(exc), detail_json))
        finally:
            lock.release()

    def _execute(self, policy: CapturePolicy) -> CaptureExecutionResult:
        requests = build_capture_requests(policy, self._now_provider())
        row_count = 0
        coverage_count = 0
        failed_batches: list[dict[str, object]] = []
        for request in requests:
            try:
                items, report = self._run_runtime_request(request)
                row_count += len(items)
                coverage_count += int(getattr(report, "store_write_count", 0))
            except Exception as exc:
                failed_batches.append({"request_identity": request.request_identity, "error": str(exc)})
        return CaptureExecutionResult(row_count, coverage_count, tuple(failed_batches))

    def _run_runtime_request(self, request: CaptureRequest):
        if request.capability_id in {"stocks.quotes.daily", "stocks.quotes.intraday"}:
            return self._runtime.stocks.get_quotes_with_report(StockQuotesRequest(**request.request_identity))
        if request.capability_id == "stocks.quotes.daily_snapshot":
            return self._runtime.stocks.get_daily_snapshot_with_report(StockDailySnapshotRequest(**request.request_identity))
        if request.capability_id == "indexes.quotes.daily":
            return self._runtime.indexes.get_quotes_with_report(IndexQuotesRequest(**request.request_identity))
        if request.capability_id == "markets.calendar.trading":
            return self._runtime.markets.get_trading_calendar_with_report(TradingCalendarRequest(**request.request_identity))
        if request.capability_id == "indexes.members":
            return self._runtime.indexes.get_members_with_report(IndexMembersRequest(**request.request_identity))
        if request.capability_id == "boards.quotes.daily":
            items = self._runtime.boards.get_quotes(**request.request_identity)
            return items, _CaptureRuntimeReport("boards.quotes.daily")
        if request.capability_id == "boards.members":
            items = self._runtime.boards.get_members(**request.request_identity)
            return items, _CaptureRuntimeReport("boards.members")
        if request.capability_id == "markets.events.news":
            return self._run_news_update(request)
        method_spec = RUNTIME_METHODS.get(request.capability_id)
        if method_spec is not None:
            component_name, method_name = method_spec
            component = getattr(self._runtime, component_name)
            items = getattr(component, method_name)(**request.request_identity)
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


def run_due_captures() -> tuple[dict[str, object], ...]:
    return QuoteMuxCaptureJob().run_due_captures()


def run_capture(capability_id: str) -> dict[str, object]:
    return QuoteMuxCaptureJob().run_capture(capability_id)
