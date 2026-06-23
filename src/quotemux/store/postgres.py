from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Sequence

import pandas as pd
from psycopg.types.json import Jsonb
from pydantic import BaseModel

from quotemux.capabilities import get_capability_config_root, get_capability_definition, is_independently_configurable_capability_id, list_capability_definitions
from quotemux.reports import ContractReport
from quotemux.store.cache_db import execute_many, execute_sql, query_dataframe
from quotemux.store.default_update_policy import cache_enabled_from_ttl_days, get_capability_update_policy_default, ttl_seconds_from_days
from quotemux.store.payload_store import CachePayloadRef, get_payload, put_payload


CACHE_HIT = "hit"
CACHE_PARTIAL_HIT = "partial_hit"
CACHE_MISS = "miss"
CACHE_STALE = "stale"
CACHE_SKIP = "skip"
CACHE_NEVER_EXPIRE_TTL_SECONDS = -1
CACHE_NEVER_EXPIRE_UNTIL = datetime.max


@dataclass(frozen=True)
class CachePolicy:
    capability_id: str
    enabled: bool
    read_enabled: bool
    write_enabled: bool
    ttl_seconds: int
    time_field: str
    key_fields: tuple[str, ...]
    request_scope_fields: tuple[str, ...]
    coverage_mode: str


@dataclass(frozen=True)
class CacheScope:
    scope_identity: str
    criteria: dict[str, object]
    time_start: datetime
    time_end: datetime


@dataclass(frozen=True)
class CacheCoverage:
    scope_identity: str
    time_start: datetime
    time_end: datetime
    fresh_until: datetime
    row_count: int
    source_json: dict[str, object]


@dataclass(frozen=True)
class CacheReadResult:
    status: str
    items: tuple[dict[str, object], ...]
    scope_identity: str
    time_start: datetime | None
    time_end: datetime | None
    detail: dict[str, object]

    @property
    def hit(self) -> bool:
        return self.status == CACHE_HIT

    @property
    def partial_hit(self) -> bool:
        return self.status == CACHE_PARTIAL_HIT


@dataclass(frozen=True)
class CacheWriteResult:
    status: str
    row_count: int
    coverage_count: int


@dataclass(frozen=True)
class DefaultCachePolicySpec:
    capability_id: str
    time_field: str
    key_fields: tuple[str, ...]
    request_scope_fields: tuple[str, ...]
    coverage_mode: str
    ttl_seconds: int
    enabled: bool = True
    read_enabled: bool = True
    write_enabled: bool = True


def _time_field_for_capability(capability_id: str) -> str:
    if capability_id in {"markets.trading.open_auctions", "stocks.quotes.auctions"}:
        return "trade_date"
    if capability_id in {"stocks.quotes.daily", "stocks.quotes.intraday", "stocks.quotes.daily_snapshot", "indexes.quotes.daily"}:
        return "trade_time"
    if capability_id.startswith("boards.quotes.") or capability_id.startswith("markets.trading.open_auctions"):
        return "trade_time"
    if capability_id in {"boards.indicators.money_flow", "boards.indicators.money_flow.snapshot", "stocks.indicators.daily_basic", "stocks.indicators.daily_market_value", "stocks.indicators.daily_valuation", "stocks.indicators.money_flow", "stocks.indicators.money_flow.batch"}:
        return "trade_date"
    if capability_id == "stocks.indicators.risk_flags":
        return "start_date"
    if capability_id.startswith("markets.calendar.") or capability_id.startswith("markets.indicators.") or capability_id.startswith("markets.connect.capital_flow") or capability_id.startswith("markets.connect.active_top10") or capability_id.startswith("markets.events.block_trades") or capability_id.startswith("markets.participants.dragon_tiger") or capability_id == "markets.participants.hot_money.details":
        return "trade_date"
    if capability_id.startswith("stocks.finance."):
        return "report_period"
    if capability_id in {"stocks.corporate_actions.dividends", "stocks.corporate_actions.repurchases", "stocks.corporate_actions.rights_issues"}:
        return "announce_date"
    if capability_id == "stocks.corporate_actions.share_changes":
        return "change_date"
    if capability_id == "stocks.corporate_actions.unlock_schedules":
        return "unlock_date"
    if capability_id == "stocks.catalog" or capability_id == "stocks.profile.basic" or capability_id.startswith("indexes.catalog") or capability_id.startswith("indexes.profile"):
        return "list_date"
    if capability_id == "stocks.catalog.archive" or capability_id == "stocks.factors.adj":
        return "trade_date"
    if capability_id == "stocks.factors.technical":
        return "trade_date"
    if capability_id in {"stocks.indicators.ah_comparisons", "stocks.indicators.chip_distribution", "stocks.indicators.chip_performance", "stocks.indicators.premarket"}:
        return "trade_date"
    if capability_id == "stocks.ownership.shareholders.changes":
        return "trade_date"
    if capability_id == "stocks.profile.managers":
        return "as_of_date"
    if capability_id == "stocks.profile.management_rewards":
        return "ann_date"
    if capability_id in {"stocks.signals.hl", "stocks.signals.limit_order_amount"}:
        return "trade_date"
    if capability_id == "stocks.signals.nine_turn":
        return "trade_time"
    if capability_id == "stocks.profile.name_history":
        return "start_date"
    if capability_id == "indexes.members" or capability_id.startswith("stocks.ownership.ccass_") or capability_id == "stocks.ownership.hk_connect_holdings" or capability_id == "stocks.ownership.pledges.stats" or capability_id == "stocks.ownership.shareholders.count":
        return "trade_date"
    if capability_id == "stocks.ownership.pledges.details":
        return "start_date"
    if capability_id.startswith("stocks.ownership.shareholders.top10"):
        return "report_period"
    if capability_id == "boards.members":
        return "join_date"
    if capability_id == "stocks.research.reports":
        return "report_date"
    if capability_id == "stocks.research.surveys":
        return "survey_date"
    if capability_id == "markets.events.news":
        return "announcement_time"
    if capability_id.startswith("boards.members"):
        return "effective_date"
    if capability_id.startswith("stocks.reference."):
        return "effective_date"
    if capability_id == "markets.connect.quotas":
        return "trade_date"
    if capability_id == "rankings.research.broker_monthly_picks":
        return "trade_month"
    if capability_id.startswith("rankings.research."):
        return "trade_date"
    return "as_of_date"


def _key_fields_for_capability(capability_id: str) -> tuple[str, ...]:
    if capability_id.startswith("stocks.quotes."):
        return ("code", "freq", "adjust")
    if capability_id.startswith("indexes.quotes."):
        return ("index_code", "freq")
    if capability_id.startswith("boards.quotes."):
        return ("board_code", "freq")
    if capability_id.startswith("boards.members"):
        return ("board_code", "code")
    if capability_id == "indexes.members":
        return ("index_code", "code")
    if capability_id.startswith("markets.calendar."):
        return ("exchange", "trade_date")
    if capability_id == "boards.reference.categories":
        return ("category_code",)
    if capability_id == "markets.trading.sessions":
        return ("code",)
    if capability_id == "markets.events.news":
        return ("event_id",)
    if capability_id.startswith("stocks.finance.statements"):
        return ("code", "report_period", "report_type", "statement_type")
    if capability_id == "stocks.finance.indicators":
        return ("code", "report_period")
    if capability_id == "stocks.finance.audits":
        return ("code", "report_period", "announce_date")
    if capability_id == "stocks.finance.disclosure_dates":
        return ("code", "report_period", "plan_date", "actual_date")
    if capability_id == "stocks.finance.express":
        return ("code", "report_period", "announce_date")
    if capability_id == "stocks.finance.forecasts":
        return ("code", "report_period", "forecast_type")
    if capability_id == "stocks.finance.main_business":
        return ("code", "report_period", "classification", "segment_name")
    if capability_id == "stocks.corporate_actions.dividends":
        return ("code", "announce_date", "record_date", "ex_date")
    if capability_id == "stocks.corporate_actions.repurchases":
        return ("code", "announce_date", "progress")
    if capability_id == "stocks.corporate_actions.rights_issues":
        return ("code", "announce_date", "record_date")
    if capability_id == "stocks.corporate_actions.share_changes":
        return ("code", "change_date", "reason")
    if capability_id == "stocks.corporate_actions.unlock_schedules":
        return ("code", "unlock_date", "holder_type", "share_type")
    if capability_id in {"markets.trading.open_auctions", "stocks.quotes.auctions"}:
        return ("code", "trade_date", "auction_time", "session")
    if capability_id == "stocks.catalog.archive":
        return ("code", "trade_date")
    if capability_id == "stocks.factors.technical":
        return ("code", "trade_date", "adjust")
    if capability_id == "stocks.indicators.ah_comparisons":
        return ("code", "trade_date")
    if capability_id == "stocks.indicators.chip_distribution":
        return ("code", "trade_date", "price")
    if capability_id in {"stocks.indicators.chip_performance", "stocks.indicators.premarket"}:
        return ("code", "trade_date")
    if capability_id == "stocks.ownership.shareholders.changes":
        return ("code", "trade_date")
    if capability_id == "stocks.profile.managers":
        return ("code", "name", "title", "begin_date")
    if capability_id == "stocks.profile.management_rewards":
        return ("code", "ann_date", "name", "title")
    if capability_id == "stocks.signals.hl":
        return ("code", "trade_date", "signal", "first_extreme")
    if capability_id == "stocks.signals.limit_order_amount":
        return ("code", "trade_date", "limit_side")
    if capability_id == "stocks.signals.nine_turn":
        return ("code", "trade_time", "freq")
    if capability_id == "stocks.ownership.ccass_holdings":
        return ("code", "trade_date")
    if capability_id == "stocks.ownership.ccass_holding_details":
        return ("code", "trade_date", "participant_id")
    if capability_id == "stocks.ownership.hk_connect_holdings":
        return ("code", "trade_date")
    if capability_id == "stocks.ownership.pledges.stats":
        return ("code", "trade_date")
    if capability_id == "stocks.ownership.pledges.details":
        return ("code", "holder_name", "start_date", "end_date", "status")
    if capability_id == "stocks.ownership.shareholders.count":
        return ("code", "trade_date")
    if capability_id in {"stocks.indicators.daily_basic", "stocks.indicators.daily_market_value", "stocks.indicators.daily_valuation", "stocks.indicators.money_flow", "stocks.indicators.money_flow.batch"}:
        return ("code", "trade_date")
    if capability_id == "stocks.indicators.risk_flags":
        return ("code", "flag_type", "start_date", "end_date", "status")
    if capability_id in {"boards.indicators.money_flow", "boards.indicators.money_flow.snapshot"}:
        return ("board_code", "trade_date", "scope")
    if capability_id.startswith("stocks.ownership.shareholders.top10"):
        return ("code", "report_period", "rank", "shareholder_name")
    if capability_id == "stocks.research.reports":
        return ("code", "report_date", "institution", "title")
    if capability_id == "stocks.research.surveys":
        return ("code", "survey_date", "org_name", "announcement_date")
    if capability_id == "stocks.reference.bse_code_mappings":
        return ("old_code", "new_code", "effective_date")
    if capability_id == "stocks.reference.hk_connect_targets":
        return ("code", "direction", "effective_date")
    if capability_id.startswith("stocks."):
        return ("code",)
    if capability_id.startswith("boards."):
        if capability_id == "boards.reference.categories":
            return ("parent_code",)
        return ("board_code",)
    if capability_id.startswith("indexes."):
        return ("index_code",)
    if capability_id == "markets.indicators.main_capital_flow":
        return ("market", "trade_date")
    if capability_id in {"markets.connect.capital_flow", "markets.connect.quotas"}:
        return ("market", "trade_date")
    if capability_id == "markets.connect.active_top10":
        return ("market", "trade_date", "code", "rank")
    if capability_id == "markets.events.block_trades":
        return ("trade_date", "code", "buyer", "seller")
    if capability_id == "markets.participants.dragon_tiger":
        return ("trade_date", "code", "reason")
    if capability_id == "markets.participants.dragon_tiger.institutions":
        return ("trade_date", "code", "institution_count")
    if capability_id == "markets.participants.hot_money":
        return ("name",)
    if capability_id == "markets.participants.hot_money.details":
        return ("trade_date", "name", "code")
    if capability_id.startswith("rankings.research."):
        if capability_id == "rankings.research.broker_monthly_picks":
            return ("trade_month", "code", "institution")
        return ("trade_date", "code", "institution", "title")
    return ("id",)


def _request_scope_fields_for_capability(capability_id: str) -> tuple[str, ...]:
    if capability_id == "stocks.catalog":
        return ("codes", "name", "exchange", "list_status", "include_delisted")
    if capability_id.startswith("stocks.quotes."):
        if capability_id == "stocks.quotes.daily_snapshot":
            return ("trade_date",)
        return ("code", "freq", "adjust")
    if capability_id.startswith("indexes.quotes."):
        return ("index_code", "freq")
    if capability_id.startswith("boards.quotes."):
        return ("board_code", "freq")
    if capability_id.startswith("boards.members"):
        return ("board_code",)
    if capability_id == "indexes.members":
        return ("index_code",)
    if capability_id == "markets.calendar.trading":
        return ("exchange", "is_open")
    if capability_id == "markets.events.news":
        return ("event_type", "stock_code", "sort_by", "include_sources", "include_content_text")
    if capability_id in {"markets.trading.open_auctions", "stocks.catalog.archive", "stocks.factors.technical", "stocks.indicators.ah_comparisons", "stocks.indicators.chip_distribution", "stocks.indicators.chip_performance", "stocks.indicators.premarket", "stocks.ownership.shareholders.changes", "stocks.profile.management_rewards", "stocks.profile.managers", "stocks.quotes.auctions", "stocks.signals.hl"}:
        return ("code",)
    if capability_id == "stocks.signals.limit_order_amount":
        return ()
    if capability_id == "stocks.signals.nine_turn":
        return ("code", "freq")
    if capability_id in {"stocks.indicators.daily_basic", "stocks.indicators.daily_market_value", "stocks.indicators.daily_valuation"}:
        return ("code",)
    if capability_id == "stocks.indicators.money_flow":
        return ("code", "view")
    if capability_id == "stocks.indicators.money_flow.batch":
        return ("codes", "view")
    if capability_id == "stocks.indicators.risk_flags":
        return ("flag_type", "status")
    if capability_id in {"boards.indicators.money_flow", "boards.indicators.money_flow.snapshot"}:
        return ("board_code", "scope")
    if capability_id.startswith("stocks.finance.statements"):
        return ("code", "report_type")
    if capability_id == "stocks.finance.main_business":
        return ("code", "classification")
    if capability_id.startswith("stocks."):
        return ("code",)
    if capability_id.startswith("boards."):
        return ("board_code",)
    if capability_id.startswith("indexes."):
        return ("index_code",)
    if capability_id == "markets.connect.quotas":
        return ("market",)
    if capability_id == "markets.connect.active_top10":
        return ("market",)
    if capability_id == "markets.events.block_trades":
        return ("code",)
    if capability_id.startswith("markets.participants.dragon_tiger"):
        return ("code",)
    if capability_id == "markets.participants.hot_money":
        return ("name", "tag")
    if capability_id == "markets.participants.hot_money.details":
        return ("name",)
    if capability_id == "rankings.research.broker_monthly_picks":
        return ("trade_month",)
    if capability_id == "markets.trading.sessions":
        return ("codes",)
    return ()


def _coverage_mode_for_capability(capability_id: str) -> str:
    if capability_id == "stocks.catalog":
        return "snapshot"
    if capability_id == "stocks.quotes.intraday":
        return "minute_range"
    if capability_id in {"stocks.quotes.daily", "indexes.quotes.daily", "boards.quotes.daily"}:
        return "trading_day_range"
    if capability_id.startswith("stocks.finance."):
        return "period_range"
    if capability_id.startswith("markets.events.") or capability_id.startswith("stocks.research."):
        return "event_range"
    if capability_id.endswith(".snapshot") or capability_id == "stocks.quotes.daily_snapshot":
        return "snapshot"
    return "date_range"


def _build_default_policy_specs() -> tuple[DefaultCachePolicySpec, ...]:
    specs: list[DefaultCachePolicySpec] = []
    for definition in list_capability_definitions():
        capability_id = definition.capability_id
        if not is_independently_configurable_capability_id(capability_id):
            continue
        policy_default = get_capability_update_policy_default(capability_id)
        cache_enabled = cache_enabled_from_ttl_days(policy_default.cache_ttl_days)
        specs.append(
            DefaultCachePolicySpec(
                capability_id=capability_id,
                time_field=_time_field_for_capability(capability_id),
                key_fields=_key_fields_for_capability(capability_id),
                request_scope_fields=_request_scope_fields_for_capability(capability_id),
                coverage_mode=_coverage_mode_for_capability(capability_id),
                ttl_seconds=ttl_seconds_from_days(policy_default.cache_ttl_days),
                enabled=cache_enabled,
                read_enabled=cache_enabled,
                write_enabled=cache_enabled,
            )
        )
    return tuple(specs)


DEFAULT_POLICY_SPECS: tuple[DefaultCachePolicySpec, ...] = _build_default_policy_specs()


SCHEMA_SQL = (
    """
    create table if not exists capability_cache_policy (
        capability_id text primary key,
        enabled boolean not null default true,
        read_enabled boolean not null default true,
        write_enabled boolean not null default true,
        ttl_seconds integer not null,
        time_field text not null,
        key_fields text[] not null,
        request_scope_fields text[] not null default '{}',
        coverage_mode text not null,
        notes text not null default '',
        created_at timestamp without time zone not null default now(),
        updated_at timestamp without time zone not null default now()
    )
    """,
    """
    create table if not exists capability_cache_rows (
        id bigserial primary key,
        capability_id text not null references capability_cache_policy(capability_id),
        time_key timestamp without time zone not null,
        identity_value text not null,
        payload_sha256 text not null,
        payload_path text not null,
        source_sha256 text not null,
        source_path text not null,
        fresh_until timestamp without time zone not null,
        created_at timestamp without time zone not null default now(),
        updated_at timestamp without time zone not null default now(),
        unique (capability_id, time_key, identity_value)
    )
    """,
    "alter table capability_cache_rows add column if not exists payload_sha256 text not null default ''",
    "alter table capability_cache_rows add column if not exists payload_path text not null default ''",
    "alter table capability_cache_rows add column if not exists source_sha256 text not null default ''",
    "alter table capability_cache_rows add column if not exists source_path text not null default ''",
    """
    do $$
    begin
        if exists (
            select 1
            from information_schema.columns
            where table_schema = 'public' and table_name = 'capability_cache_rows' and column_name = 'payload_json'
        ) then
            alter table capability_cache_rows alter column payload_json drop not null;
        end if;
    end $$;
    """,
    "create index if not exists idx_cache_rows_capability_time on capability_cache_rows (capability_id, time_key)",
    "create index if not exists idx_cache_rows_capability_fresh_until on capability_cache_rows (capability_id, fresh_until)",
    "drop index if exists idx_cache_rows_payload_gin",
    """
    create table if not exists capability_cache_coverage (
        id bigserial primary key,
        capability_id text not null references capability_cache_policy(capability_id),
        scope_identity text not null,
        time_start timestamp without time zone not null,
        time_end timestamp without time zone not null,
        fresh_until timestamp without time zone not null,
        row_count integer not null default 0,
        source_json jsonb not null default '{}'::jsonb,
        created_at timestamp without time zone not null default now(),
        updated_at timestamp without time zone not null default now(),
        unique (capability_id, scope_identity, time_start, time_end)
    )
    """,
    "create index if not exists idx_cache_coverage_capability_time on capability_cache_coverage (capability_id, time_start, time_end)",
    "create index if not exists idx_cache_coverage_capability_fresh_until on capability_cache_coverage (capability_id, fresh_until)",
    """
    create table if not exists capability_cache_audit (
        id bigserial primary key,
        capability_id text not null,
        event_type text not null,
        scope_identity text not null default '',
        time_start timestamp without time zone,
        time_end timestamp without time zone,
        package_id text not null default '',
        source_instance_id text not null default '',
        detail_json jsonb not null default '{}'::jsonb,
        created_at timestamp without time zone not null default now()
    )
    """,
    "create index if not exists idx_cache_audit_capability_time on capability_cache_audit (capability_id, created_at desc)",
    "create index if not exists idx_cache_audit_event_type_time on capability_cache_audit (event_type, created_at desc)",
)


_SCHEMA_READY = False
_SCHEMA_FAILED = False


def _serialize_value(value: object) -> object:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, BaseModel):
        return {key: _serialize_value(item) for key, item in value.model_dump().items()}
    if isinstance(value, dict):
        return {str(key): _serialize_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_serialize_value(item) for item in value]
    return value


def _normalize_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    return str(value)


def _parse_time_key(value: object) -> datetime:
    text = _normalize_text(value)
    if text == "":
        raise ValueError("缓存时间字段不能为空")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d", "%Y%m%d", "%Y%m", "%Y-%m"):
        try:
            parsed = datetime.strptime(text, fmt)
            if fmt in {"%Y-%m-%d", "%Y%m%d"}:
                return datetime.combine(parsed.date(), time.min)
            if fmt in {"%Y%m", "%Y-%m"}:
                return datetime.combine(parsed.date().replace(day=1), time.min)
            return parsed
        except ValueError:
            continue
    parsed = datetime.fromisoformat(text)
    return parsed.replace(tzinfo=None)


def _datetime_from_date_text(text: str) -> datetime:
    return datetime.combine(_parse_time_key(text).date(), time.min)


def _month_range_from_text(text: str) -> tuple[datetime, datetime]:
    month_start = _parse_time_key(text)
    if month_start.month == 12:
        next_month = datetime(month_start.year + 1, 1, 1)
    else:
        next_month = datetime(month_start.year, month_start.month + 1, 1)
    return month_start, next_month - timedelta(days=1)


def build_identity_value(payload: dict[str, object], fields: Sequence[str]) -> str:
    return "|".join(f"{field}={_normalize_text(payload.get(field, ''))}" for field in fields)


def build_time_key(payload: dict[str, object], time_field: str) -> datetime:
    return _parse_time_key(payload.get(time_field, ""))


def build_scope_identity(payload: dict[str, object], fields: Sequence[str]) -> str:
    return build_identity_value(payload, fields)


def _fresh_until_from_ttl(written_at: datetime, ttl_seconds: int) -> datetime:
    if ttl_seconds == CACHE_NEVER_EXPIRE_TTL_SECONDS:
        return CACHE_NEVER_EXPIRE_UNTIL
    return written_at + timedelta(seconds=ttl_seconds)


def _is_fresh(policy: CachePolicy, fresh_until: datetime, now: datetime) -> bool:
    return _policy_ignores_ttl(policy) or fresh_until > now


def _policy_ignores_ttl(policy: CachePolicy) -> bool:
    return policy.ttl_seconds == CACHE_NEVER_EXPIRE_TTL_SECONDS or (policy.ttl_seconds == 0 and policy.enabled and policy.read_enabled and policy.write_enabled)


def _policy_from_row(row: dict[str, object]) -> CachePolicy:
    return CachePolicy(
        capability_id=str(row["capability_id"]),
        enabled=bool(row["enabled"]),
        read_enabled=bool(row["read_enabled"]),
        write_enabled=bool(row["write_enabled"]),
        ttl_seconds=int(row["ttl_seconds"]),
        time_field=str(row["time_field"]),
        key_fields=tuple(str(item) for item in row["key_fields"]),
        request_scope_fields=tuple(str(item) for item in row["request_scope_fields"]),
        coverage_mode=str(row["coverage_mode"]),
    )


def _is_empty_dataframe(frame: pd.DataFrame) -> bool:
    return frame.empty


def _ensure_schema() -> bool:
    global _SCHEMA_FAILED, _SCHEMA_READY
    if _SCHEMA_READY:
        return True
    if _SCHEMA_FAILED:
        return False
    for statement in SCHEMA_SQL:
        if not execute_sql(statement):
            _SCHEMA_FAILED = True
            return False
    params = [
        (
            spec.capability_id,
            spec.enabled,
            spec.read_enabled,
            spec.write_enabled,
            spec.ttl_seconds,
            spec.time_field,
            list(spec.key_fields),
            list(spec.request_scope_fields),
            spec.coverage_mode,
            "",
        )
        for spec in DEFAULT_POLICY_SPECS
    ]
    ok = execute_many(
        """
        insert into capability_cache_policy (
            capability_id, enabled, read_enabled, write_enabled, ttl_seconds,
            time_field, key_fields, request_scope_fields, coverage_mode, notes
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        on conflict (capability_id) do update set
            time_field = excluded.time_field,
            key_fields = excluded.key_fields,
            request_scope_fields = excluded.request_scope_fields,
            coverage_mode = excluded.coverage_mode,
            updated_at = now()
        """,
        params,
    )
    _SCHEMA_READY = ok
    _SCHEMA_FAILED = not ok
    return ok


class CachePolicyRepository:
    def list(self) -> tuple[CachePolicy, ...]:
        if not _ensure_schema():
            return ()
        frame = query_dataframe(
            """
            select capability_id, enabled, read_enabled, write_enabled, ttl_seconds,
                   time_field, key_fields, request_scope_fields, coverage_mode
            from capability_cache_policy
            order by capability_id asc
            """,
            (),
        )
        if _is_empty_dataframe(frame):
            return ()
        return tuple(
            _policy_from_row(row)
            for row in frame.to_dict("records")
            if is_independently_configurable_capability_id(str(row["capability_id"]))
        )

    def get(self, capability_id: str) -> CachePolicy | None:
        if not _ensure_schema():
            return None
        actual_capability_id = get_capability_config_root(capability_id)
        frame = query_dataframe(
            """
            select capability_id, enabled, read_enabled, write_enabled, ttl_seconds,
                   time_field, key_fields, request_scope_fields, coverage_mode
            from capability_cache_policy
            where capability_id = %s
            """,
            (actual_capability_id,),
        )
        if _is_empty_dataframe(frame):
            return None
        return _policy_from_row(frame.iloc[0].to_dict())

    def update(self, policy: CachePolicy) -> bool:
        if not _ensure_schema():
            return False
        actual_capability_id = get_capability_config_root(policy.capability_id)
        return execute_sql(
            """
            update capability_cache_policy
            set enabled = %s,
                read_enabled = %s,
                write_enabled = %s,
                ttl_seconds = %s,
                time_field = %s,
                key_fields = %s,
                request_scope_fields = %s,
                coverage_mode = %s,
                updated_at = now()
            where capability_id = %s
            """,
            (
                policy.enabled,
                policy.read_enabled,
                policy.write_enabled,
                policy.ttl_seconds,
                policy.time_field,
                list(policy.key_fields),
                list(policy.request_scope_fields),
                policy.coverage_mode,
                actual_capability_id,
            ),
        )


class CacheAuditRepository:
    def list(self, capability_id: str = "", event_type: str = "", limit: int = 100) -> tuple[dict[str, object], ...]:
        if not _ensure_schema():
            return ()
        clauses: list[str] = []
        params: list[object] = []
        if capability_id != "":
            clauses.append("capability_id = %s")
            params.append(capability_id)
        if event_type != "":
            clauses.append("event_type = %s")
            params.append(event_type)
        where_sql = " where " + " and ".join(clauses) if clauses else ""
        params.append(max(1, min(limit, 1000)))
        frame = query_dataframe(
            f"""
            select capability_id, event_type, scope_identity, time_start, time_end,
                   package_id, source_instance_id, detail_json, created_at
            from capability_cache_audit
            {where_sql}
            order by created_at desc
            limit %s
            """,
            tuple(params),
        )
        if _is_empty_dataframe(frame):
            return ()
        return tuple(_serialize_value(row) for row in frame.to_dict("records") if isinstance(row, dict))

    def write(
        self,
        capability_id: str,
        event_type: str,
        scope_identity: str,
        time_start: datetime | None,
        time_end: datetime | None,
        detail: dict[str, object],
    ) -> None:
        if not _ensure_schema():
            return
        execute_sql(
            """
            insert into capability_cache_audit (
                capability_id, event_type, scope_identity, time_start, time_end,
                package_id, source_instance_id, detail_json
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                capability_id,
                event_type,
                scope_identity,
                time_start,
                time_end,
                str(detail.get("package_id", "")),
                str(detail.get("source_instance_id", "")),
                Jsonb(detail),
            ),
        )


class CacheCoverageRepository:
    def find_for_scope(self, capability_id: str, scope_identity: str) -> tuple[CacheCoverage, ...]:
        if not _ensure_schema():
            return ()
        frame = query_dataframe(
            """
            select scope_identity, time_start, time_end, fresh_until, row_count, source_json
            from capability_cache_coverage
            where capability_id = %s and scope_identity = %s
            order by time_start asc
            """,
            (capability_id, scope_identity),
        )
        if _is_empty_dataframe(frame):
            return ()
        return tuple(
            CacheCoverage(
                scope_identity=str(row["scope_identity"]),
                time_start=pd.Timestamp(row["time_start"]).to_pydatetime(),
                time_end=pd.Timestamp(row["time_end"]).to_pydatetime(),
                fresh_until=pd.Timestamp(row["fresh_until"]).to_pydatetime(),
                row_count=int(row["row_count"]),
                source_json=row["source_json"] if isinstance(row["source_json"], dict) else {},
            )
            for row in frame.to_dict("records")
        )

    def upsert_many(self, capability_id: str, coverages: Sequence[tuple[CacheScope, int, datetime, dict[str, object]]]) -> bool:
        if not _ensure_schema():
            return False
        params = [
            (capability_id, scope.scope_identity, scope.time_start, scope.time_end, fresh_until, row_count, Jsonb(source_json))
            for scope, row_count, fresh_until, source_json in coverages
        ]
        return execute_many(
            """
            insert into capability_cache_coverage (
                capability_id, scope_identity, time_start, time_end,
                fresh_until, row_count, source_json
            )
            values (%s, %s, %s, %s, %s, %s, %s)
            on conflict (capability_id, scope_identity, time_start, time_end) do update set
                fresh_until = excluded.fresh_until,
                row_count = excluded.row_count,
                source_json = excluded.source_json,
                updated_at = now()
            """,
            params,
        )


class CacheStatusRepository:
    def list(self) -> tuple[dict[str, object], ...]:
        if not _ensure_schema():
            return ()
        frame = query_dataframe(
            """
            select
                p.capability_id,
                p.enabled,
                p.read_enabled,
                p.write_enabled,
                p.ttl_seconds,
                p.time_field,
                p.key_fields,
                p.request_scope_fields,
                p.coverage_mode,
                coalesce(r.row_count, 0) as row_count,
                coalesce(c.coverage_count, 0) as coverage_count,
                a.last_hit_at,
                a.last_miss_at,
                a.last_write_at
            from capability_cache_policy p
            left join (
                select capability_id, count(*) as row_count
                from capability_cache_rows
                group by capability_id
            ) r on r.capability_id = p.capability_id
            left join (
                select capability_id, count(*) as coverage_count
                from capability_cache_coverage
                group by capability_id
            ) c on c.capability_id = p.capability_id
            left join (
                select
                    capability_id,
                    max(created_at) filter (where event_type = 'cache_hit') as last_hit_at,
                    max(created_at) filter (where event_type = 'cache_miss') as last_miss_at,
                    max(created_at) filter (where event_type = 'cache_write') as last_write_at
                from capability_cache_audit
                group by capability_id
            ) a on a.capability_id = p.capability_id
            order by p.capability_id asc
            """,
            (),
        )
        if _is_empty_dataframe(frame):
            return ()
        return tuple(_serialize_value(row) for row in frame.to_dict("records") if isinstance(row, dict))


class CacheRowRepository:
    def read(self, capability_id: str, time_start: datetime, time_end: datetime, never_expires: bool) -> tuple[dict[str, object], ...]:
        if not _ensure_schema():
            return ()
        frame = query_dataframe(
            """
            select payload_sha256, payload_path, source_sha256, source_path
            from capability_cache_rows
            where capability_id = %s
              and time_key >= %s
              and time_key <= %s
              and (%s or fresh_until > now())
              and payload_path <> ''
            order by time_key asc, identity_value asc
            """,
            (capability_id, time_start, time_end, never_expires),
        )
        if _is_empty_dataframe(frame):
            return ()
        payloads: list[dict[str, object]] = []
        for row in frame.to_dict("records"):
            payload = get_payload(
                CachePayloadRef(
                    str(row["payload_sha256"]),
                    str(row["payload_path"]),
                    str(row["source_sha256"]),
                    str(row["source_path"]),
                )
            )
            if payload is not None:
                payloads.append(payload)
        return tuple(payloads)

    def upsert_many(self, capability_id: str, rows: Sequence[tuple[datetime, str, dict[str, object], dict[str, object], datetime]]) -> bool:
        if not _ensure_schema():
            return False
        params = [
            (
                capability_id,
                time_key,
                identity_value,
                payload_ref.payload_sha256,
                payload_ref.payload_path,
                payload_ref.source_sha256,
                payload_ref.source_path,
                fresh_until,
            )
            for time_key, identity_value, payload_json, source_json, fresh_until in rows
            for payload_ref in (put_payload(capability_id, time_key, payload_json, source_json),)
        ]
        return execute_many(
            """
            insert into capability_cache_rows (
                capability_id, time_key, identity_value,
                payload_sha256, payload_path, source_sha256, source_path,
                fresh_until
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s)
            on conflict (capability_id, time_key, identity_value) do update set
                payload_sha256 = excluded.payload_sha256,
                payload_path = excluded.payload_path,
                source_sha256 = excluded.source_sha256,
                source_path = excluded.source_path,
                fresh_until = excluded.fresh_until,
                updated_at = now()
            """,
            params,
        )


def _field_values(request_identity: dict[str, object], field: str) -> tuple[object, ...]:
    value = request_identity.get(field, "")
    if value == "" and field == "code":
        value = request_identity.get("codes", "")
    if value == "" and field == "index_code":
        value = request_identity.get("index_codes", "")
    if value == "" and field == "market":
        value = request_identity.get("market_type", "")
    if isinstance(value, list):
        if value == []:
            return ("",)
        return tuple(value)
    if isinstance(value, tuple):
        if value == ():
            return ("",)
        return value
    return (value,)


def _time_range_from_request(request_identity: dict[str, object]) -> tuple[datetime, datetime]:
    trade_date = _normalize_text(request_identity.get("trade_date", ""))
    if trade_date != "":
        current = _datetime_from_date_text(trade_date)
        return current, current
    report_period = _normalize_text(request_identity.get("report_period", ""))
    if report_period != "":
        current = _parse_time_key(report_period)
        return current, current
    report_date = _normalize_text(request_identity.get("report_date", ""))
    if report_date != "":
        current = _datetime_from_date_text(report_date)
        return current, current
    announcement_date = _normalize_text(request_identity.get("announcement_date", ""))
    if announcement_date != "":
        current = _datetime_from_date_text(announcement_date)
        return current, current
    crawl_date = _normalize_text(request_identity.get("crawl_date", ""))
    if crawl_date != "":
        current = _datetime_from_date_text(crawl_date)
        return current, current
    survey_date = _normalize_text(request_identity.get("survey_date", ""))
    if survey_date != "":
        current = _datetime_from_date_text(survey_date)
        return current, current
    effective_date = _normalize_text(request_identity.get("effective_date", ""))
    if effective_date != "":
        current = _datetime_from_date_text(effective_date)
        return current, current
    announce_date = _normalize_text(request_identity.get("announce_date", ""))
    if announce_date != "":
        current = _datetime_from_date_text(announce_date)
        return current, current
    unlock_date = _normalize_text(request_identity.get("unlock_date", ""))
    if unlock_date != "":
        current = _datetime_from_date_text(unlock_date)
        return current, current
    change_date = _normalize_text(request_identity.get("change_date", ""))
    if change_date != "":
        current = _datetime_from_date_text(change_date)
        return current, current
    trade_month = _normalize_text(request_identity.get("trade_month", ""))
    if trade_month != "":
        return _month_range_from_text(trade_month)
    start_time = _normalize_text(request_identity.get("start_time", ""))
    end_time = _normalize_text(request_identity.get("end_time", ""))
    if start_time != "" or end_time != "":
        start = _parse_time_key(start_time or end_time)
        end = _parse_time_key(end_time or start_time)
        return start, end
    start_period = _normalize_text(request_identity.get("start_period", ""))
    end_period = _normalize_text(request_identity.get("end_period", ""))
    if start_period != "" or end_period != "":
        start = _parse_time_key(start_period or end_period)
        end = _parse_time_key(end_period or start_period)
        return start, end
    start_date = _normalize_text(request_identity.get("start_date", ""))
    end_date = _normalize_text(request_identity.get("end_date", ""))
    if start_date != "" or end_date != "":
        start = _datetime_from_date_text(start_date or end_date)
        end = _datetime_from_date_text(end_date or start_date)
        return start, end
    start_year = _normalize_text(request_identity.get("start_year", ""))
    end_year = _normalize_text(request_identity.get("end_year", ""))
    if start_year != "" or end_year != "":
        start = _datetime_from_date_text(f"{start_year or end_year}-01-01")
        end = _datetime_from_date_text(f"{end_year or start_year}-12-31")
        return start, end
    current = datetime.combine(datetime.now().date(), time.min)
    return current, current


def _request_scopes(policy: CachePolicy, request_identity: dict[str, object]) -> tuple[CacheScope, ...]:
    if (
        policy.capability_id == "stocks.finance.main_business"
        and _normalize_text(request_identity.get("report_period", "")) == ""
        and _normalize_text(request_identity.get("start_period", "")) == ""
        and _normalize_text(request_identity.get("end_period", "")) == ""
    ):
        time_start, time_end = datetime.min, datetime.max
    else:
        time_start, time_end = _time_range_from_request(request_identity)
    if policy.request_scope_fields == ():
        return (CacheScope("", {}, time_start, time_end),)
    scopes: list[CacheScope] = [CacheScope("", {}, time_start, time_end)]
    for field in policy.request_scope_fields:
        next_scopes: list[CacheScope] = []
        for value in _field_values(request_identity, field):
            for scope in scopes:
                criteria = {**scope.criteria, field: value}
                scope_identity = build_scope_identity(criteria, policy.request_scope_fields)
                next_scopes.append(CacheScope(scope_identity, criteria, time_start, time_end))
        scopes = next_scopes
    return tuple(scopes)


def _payload_matches_scope(payload: dict[str, object], scope: CacheScope) -> bool:
    for field, value in scope.criteria.items():
        if _normalize_text(value) == "":
            continue
        if field in {"start_year", "end_year", "is_open", "sort_by", "limit", "offset", "include_delisted", "include_sources", "include_content_text"}:
            continue
        if field in {"event_type", "stock_code"} and _normalize_text(value) == "":
            continue
        payload_value = payload.get(field, "")
        if field == "codes":
            payload_value = payload.get("code", "")
        if field == "trade_date":
            if "request_trade_date" in payload:
                payload_value = payload["request_trade_date"]
            elif "trade_time" in payload:
                payload_value = str(payload["trade_time"])[:10]
        if field == "n" and "request_n" in payload:
            payload_value = payload["request_n"]
        if field == "stock_code":
            related_codes = payload.get("related_stock_codes", [])
            if isinstance(related_codes, list):
                if _normalize_text(value) not in {_normalize_text(item) for item in related_codes}:
                    return False
                continue
        if field == "market" and payload_value == "":
            payload_value = payload.get("market_type", "")
        if _normalize_text(payload_value) != _normalize_text(value):
            return False
    return True


def _filter_payloads(payloads: Sequence[dict[str, object]], scopes: Sequence[CacheScope]) -> tuple[dict[str, object], ...]:
    if scopes == ():
        return tuple(payloads)
    result: list[dict[str, object]] = []
    seen: set[str] = set()
    for payload in payloads:
        if not any(_payload_matches_scope(payload, scope) for scope in scopes):
            continue
        marker = repr(sorted(payload.items()))
        if marker in seen:
            continue
        seen.add(marker)
        result.append(payload)
    return tuple(result)


def _build_actual_coverage_rows(
    policy: CachePolicy,
    scopes: Sequence[CacheScope],
    payloads: Sequence[dict[str, object]],
    fresh_until: datetime,
    source_json: dict[str, object],
) -> list[tuple[CacheScope, int, datetime, dict[str, object]]]:
    if policy.capability_id == "stocks.finance.main_business" and payloads != []:
        rows: list[tuple[CacheScope, int, datetime, dict[str, object]]] = []
        for scope in scopes:
            matched_payloads = [payload for payload in payloads if _payload_matches_scope(payload, scope)]
            if matched_payloads == []:
                continue
            time_keys = [build_time_key(payload, policy.time_field) for payload in matched_payloads]
            actual_scope = CacheScope(scope.scope_identity, scope.criteria, min(time_keys), max(time_keys))
            rows.append((actual_scope, len(matched_payloads), fresh_until, source_json))
        return rows
    if payloads == [] or not policy.capability_id.startswith("stocks.quotes."):
        return [(scope, sum(1 for payload in payloads if _payload_matches_scope(payload, scope)), fresh_until, source_json) for scope in scopes]
    rows: list[tuple[CacheScope, int, datetime, dict[str, object]]] = []
    for scope in scopes:
        matched_payloads = [payload for payload in payloads if _payload_matches_scope(payload, scope)]
        if matched_payloads == []:
            continue
        time_keys = [build_time_key(payload, policy.time_field) for payload in matched_payloads]
        actual_scope = CacheScope(scope.scope_identity, scope.criteria, min(time_keys), max(time_keys))
        rows.append((actual_scope, len(matched_payloads), fresh_until, source_json))
    return rows


def _coverage_covers(policy: CachePolicy, coverage: CacheCoverage, scope: CacheScope, now: datetime) -> bool:
    if not _is_fresh(policy, coverage.fresh_until, now):
        return False
    if policy.coverage_mode == "snapshot":
        return coverage.scope_identity == scope.scope_identity and coverage.time_start == scope.time_start and coverage.time_end == scope.time_end
    return coverage.time_start <= scope.time_start and coverage.time_end >= scope.time_end


def _coverage_overlaps(policy: CachePolicy, coverage: CacheCoverage, scope: CacheScope, now: datetime) -> bool:
    if not _is_fresh(policy, coverage.fresh_until, now):
        return False
    if policy.coverage_mode == "snapshot":
        return coverage.scope_identity == scope.scope_identity and coverage.time_start == scope.time_start and coverage.time_end == scope.time_end
    return coverage.time_start <= scope.time_end and coverage.time_end >= scope.time_start


def _coverage_read_range(policy: CachePolicy, coverage: CacheCoverage, scope: CacheScope) -> tuple[datetime, datetime]:
    if policy.coverage_mode == "snapshot":
        return scope.time_start, scope.time_end
    start = max(coverage.time_start, scope.time_start)
    end = min(coverage.time_end, scope.time_end)
    if policy.coverage_mode == "event_range" and start == end and start.time() == time.min:
        end = end + timedelta(days=1) - timedelta(microseconds=1)
    return start, end


def _source_json(report: ContractReport) -> dict[str, object]:
    definition = get_capability_definition(report.contract_name)
    packages = sorted(report.source_request_counts)
    source_instances = [item.source_instance_id for item in report.source_instance_reports if item.request_count > 0]
    return {
        "packages": packages,
        "source_instances": source_instances,
        "merge_strategy": definition.default_merge_strategy,
        "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "report": report.to_dict(),
    }


class UnifiedPostgresCacheStore:
    def __init__(self) -> None:
        self.policies = CachePolicyRepository()
        self.rows = CacheRowRepository()
        self.coverage = CacheCoverageRepository()
        self.audit = CacheAuditRepository()
        self.status = CacheStatusRepository()

    def read(self, capability_id: str, request_identity: dict[str, object]) -> CacheReadResult:
        policy = self.policies.get(capability_id)
        if policy is None or not policy.enabled or not policy.read_enabled:
            self.audit.write(capability_id, "cache_skip", "", None, None, {"reason": "policy_disabled"})
            return CacheReadResult(CACHE_SKIP, (), "", None, None, {"reason": "policy_disabled"})
        scopes = _request_scopes(policy, request_identity)
        now = datetime.now()
        full_coverages: list[tuple[CacheScope, CacheCoverage]] = []
        partial_coverages: list[tuple[CacheScope, CacheCoverage]] = []
        stale_count = 0
        for scope in scopes:
            coverages = self.coverage.find_for_scope(capability_id, scope.scope_identity)
            stale_count += sum(1 for item in coverages if not _is_fresh(policy, item.fresh_until, now))
            full = next((item for item in coverages if _coverage_covers(policy, item, scope, now)), None)
            if full is not None:
                full_coverages.append((scope, full))
                continue
            partial_coverages.extend((scope, item) for item in coverages if _coverage_overlaps(policy, item, scope, now))
        if len(full_coverages) == len(scopes):
            payloads = self._read_covered_payloads(policy, capability_id, full_coverages)
            items = _filter_payloads(payloads, scopes)
            scope_identity = "|".join(scope.scope_identity for scope in scopes)
            first_scope = scopes[0]
            self.audit.write(capability_id, "cache_hit", scope_identity, first_scope.time_start, first_scope.time_end, {"row_count": len(items)})
            return CacheReadResult(CACHE_HIT, items, scope_identity, first_scope.time_start, first_scope.time_end, {"row_count": len(items)})
        if partial_coverages != []:
            payloads = self._read_covered_payloads(policy, capability_id, partial_coverages)
            items = _filter_payloads(payloads, scopes)
            first_scope = scopes[0]
            scope_identity = "|".join(scope.scope_identity for scope in scopes)
            self.audit.write(capability_id, "cache_partial_hit", scope_identity, first_scope.time_start, first_scope.time_end, {"row_count": len(items)})
            return CacheReadResult(CACHE_PARTIAL_HIT, items, scope_identity, first_scope.time_start, first_scope.time_end, {"row_count": len(items)})
        first_scope = scopes[0]
        scope_identity = "|".join(scope.scope_identity for scope in scopes)
        event_type = "cache_stale" if stale_count else "cache_miss"
        self.audit.write(capability_id, event_type, scope_identity, first_scope.time_start, first_scope.time_end, {"stale_count": stale_count})
        status = CACHE_STALE if stale_count else CACHE_MISS
        return CacheReadResult(status, (), scope_identity, first_scope.time_start, first_scope.time_end, {"stale_count": stale_count})

    def _read_covered_payloads(self, policy: CachePolicy, capability_id: str, coverages: Sequence[tuple[CacheScope, CacheCoverage]]) -> tuple[dict[str, object], ...]:
        payloads: list[dict[str, object]] = []
        seen: set[str] = set()
        never_expires = _policy_ignores_ttl(policy)
        for scope, coverage in coverages:
            start, end = _coverage_read_range(policy, coverage, scope)
            for payload in self.rows.read(capability_id, start, end, never_expires):
                marker = repr(sorted(payload.items()))
                if marker in seen:
                    continue
                seen.add(marker)
                payloads.append(payload)
        return tuple(payloads)

    def write(self, capability_id: str, request_identity: dict[str, object], items: Sequence[object], report: ContractReport) -> CacheWriteResult:
        policy = self.policies.get(capability_id)
        if policy is None or not policy.write_enabled:
            self.audit.write(capability_id, "cache_skip", "", None, None, {"reason": "policy_disabled"})
            return CacheWriteResult(CACHE_SKIP, 0, 0)
        written_at = datetime.now()
        fresh_until = _fresh_until_from_ttl(written_at, policy.ttl_seconds)
        source_json = _source_json(report)
        payloads = [_serialize_value(item) for item in items]
        typed_payloads = [payload for payload in payloads if isinstance(payload, dict)]
        rows = [
            (build_time_key(payload, policy.time_field), build_identity_value(payload, policy.key_fields), payload, source_json, fresh_until)
            for payload in typed_payloads
        ]
        rows_ok = self.rows.upsert_many(capability_id, rows)
        scopes = _request_scopes(policy, request_identity)
        coverages = _build_actual_coverage_rows(policy, scopes, typed_payloads, fresh_until, source_json)
        coverage_ok = self.coverage.upsert_many(capability_id, coverages)
        status = "write" if rows_ok and coverage_ok else CACHE_SKIP
        first_scope = scopes[0]
        self.audit.write(
            capability_id,
            "cache_write" if status == "write" else "cache_skip",
            "|".join(scope.scope_identity for scope in scopes),
            first_scope.time_start,
            first_scope.time_end,
            {"row_count": len(rows), "coverage_count": len(coverages)},
        )
        return CacheWriteResult(status, len(rows), len(coverages))

    def list_policies(self) -> tuple[CachePolicy, ...]:
        return self.policies.list()

    def get_policy(self, capability_id: str) -> CachePolicy | None:
        return self.policies.get(capability_id)

    def update_policy(self, policy: CachePolicy) -> bool:
        return self.policies.update(policy)

    def list_status(self) -> tuple[dict[str, object], ...]:
        return self.status.list()

    def list_audit(self, capability_id: str = "", event_type: str = "", limit: int = 100) -> tuple[dict[str, object], ...]:
        return self.audit.list(capability_id, event_type, limit)


_STORE = UnifiedPostgresCacheStore()


def get_postgres_cache_store() -> UnifiedPostgresCacheStore:
    return _STORE
