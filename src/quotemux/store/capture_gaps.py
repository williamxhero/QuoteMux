from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from psycopg.types.json import Jsonb

from quotemux.infra.common import format_date_value
from quotemux.infra.db.client import execute_many, execute_sql, query_dataframe


GAP_PENDING = "pending"
GAP_RETRYING = "retrying"
GAP_PROVIDER_EMPTY = "provider_empty"
GAP_SYSTEM_FAILED = "system_failed"
GAP_RESOLVED = "resolved"

INTRADAY_CAPABILITY_ID = "stocks.quotes.intraday"


@dataclass(frozen=True)
class CaptureGap:
    capability_id: str
    market: str
    code: str
    trade_date: str
    expected_count: int
    actual_count: int
    missing_count: int
    status: str
    provider_results: dict[str, object]
    retry_count: int
    first_seen_at: str
    last_seen_at: str
    last_attempt_at: str
    resolved_at: str
    last_error: str


@dataclass(frozen=True)
class CaptureGapAuditResult:
    capability_id: str
    window_count: int
    audited_trade_dates: int
    unresolved_count: int
    resolved_count: int
    scanned_trade_dates: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "capability_id": self.capability_id,
            "window_count": self.window_count,
            "audited_trade_dates": self.audited_trade_dates,
            "unresolved_count": self.unresolved_count,
            "resolved_count": self.resolved_count,
            "scanned_trade_dates": list(self.scanned_trade_dates),
        }


CAPTURE_GAP_SCHEMA_SQL = """
create table if not exists market_data_capture_gaps (
    id bigserial primary key,
    capability_id text not null,
    market text not null default '',
    code text not null,
    trade_date date not null,
    expected_count integer not null,
    actual_count integer not null,
    missing_count integer not null,
    status text not null,
    provider_results jsonb not null default '{}'::jsonb,
    retry_count integer not null default 0,
    first_seen_at timestamp not null default now(),
    last_seen_at timestamp not null default now(),
    last_attempt_at timestamp,
    resolved_at timestamp,
    last_error text not null default '',
    unique (capability_id, code, trade_date)
);
create index if not exists idx_market_data_capture_gaps_unresolved
    on market_data_capture_gaps (capability_id, trade_date desc, status)
    where status <> 'resolved';
create table if not exists market_data_intraday_coverage_daily (
    capability_id text not null,
    market text not null default '',
    code text not null,
    trade_date date not null,
    expected_count integer not null,
    actual_count integer not null,
    audited_at timestamp not null default now(),
    primary key (capability_id, code, trade_date)
);
create table if not exists market_data_capture_gap_audits (
    capability_id text not null,
    trade_date date not null,
    expected_count integer not null,
    complete_count integer not null,
    gap_count integer not null,
    audited_at timestamp not null default now(),
    primary key (capability_id, trade_date)
);
"""


def _timestamp_text(value: object) -> str:
    if value is None:
        return ""
    return str(value)


def _is_missing_value(value: object) -> bool:
    return value is None or value != value


def _gap_from_row(row: dict[str, object]) -> CaptureGap:
    provider_results = row.get("provider_results", {})
    return CaptureGap(
        capability_id=str(row.get("capability_id", "")),
        market=str(row.get("market", "")),
        code=str(row.get("code", "")),
        trade_date=format_date_value(row.get("trade_date", "")),
        expected_count=int(row.get("expected_count", 0) or 0),
        actual_count=int(row.get("actual_count", 0) or 0),
        missing_count=int(row.get("missing_count", 0) or 0),
        status=str(row.get("status", "")),
        provider_results=dict(provider_results) if isinstance(provider_results, dict) else {},
        retry_count=int(row.get("retry_count", 0) or 0),
        first_seen_at=_timestamp_text(row.get("first_seen_at")),
        last_seen_at=_timestamp_text(row.get("last_seen_at")),
        last_attempt_at=_timestamp_text(row.get("last_attempt_at")),
        resolved_at=_timestamp_text(row.get("resolved_at")),
        last_error=str(row.get("last_error", "")),
    )


def _gap_to_dict(gap: CaptureGap) -> dict[str, object]:
    return {
        "capability_id": gap.capability_id,
        "market": gap.market,
        "code": gap.code,
        "trade_date": gap.trade_date,
        "expected_count": gap.expected_count,
        "actual_count": gap.actual_count,
        "missing_count": gap.missing_count,
        "status": gap.status,
        "provider_results": gap.provider_results,
        "retry_count": gap.retry_count,
        "first_seen_at": gap.first_seen_at,
        "last_seen_at": gap.last_seen_at,
        "last_attempt_at": gap.last_attempt_at,
        "resolved_at": gap.resolved_at,
        "last_error": gap.last_error,
    }


class CaptureGapRepository:
    """持久化行情缺口，并负责按交易日窗口审计。"""

    def __init__(self) -> None:
        self._schema_ready = False

    def list(self, capability_id: str = "", status: str = "", limit: int = 500) -> tuple[dict[str, object], ...]:
        self._ensure_schema()
        conditions: list[str] = []
        params: list[object] = []
        if capability_id != "":
            conditions.append("capability_id = %s")
            params.append(capability_id)
        if status != "":
            conditions.append("status = %s")
            params.append(status)
        where_sql = "" if conditions == [] else "where " + " and ".join(conditions)
        frame = query_dataframe(
            f"""
            select capability_id, market, code, trade_date, expected_count, actual_count,
                   missing_count, status, provider_results, retry_count, first_seen_at,
                   last_seen_at, last_attempt_at, resolved_at, last_error
            from market_data_capture_gaps
            {where_sql}
            order by trade_date desc, code
            limit %s
            """,
            tuple([*params, max(1, limit)]),
        )
        if frame.empty:
            return ()
        return tuple(_gap_to_dict(_gap_from_row(row)) for row in frame.to_dict("records"))

    def list_unresolved(self, capability_id: str, window_count: int) -> tuple[CaptureGap, ...]:
        self._ensure_schema()
        frame = query_dataframe(
            """
            with selected_dates as (
                select distinct trade_date
                from fact.stock_daily_1d
                order by trade_date desc
                limit %s
            )
            select gaps.capability_id, gaps.market, gaps.code, gaps.trade_date,
                   gaps.expected_count, gaps.actual_count, gaps.missing_count, gaps.status,
                   gaps.provider_results, gaps.retry_count, gaps.first_seen_at,
                   gaps.last_seen_at, gaps.last_attempt_at, gaps.resolved_at, gaps.last_error
            from market_data_capture_gaps gaps
            join selected_dates dates on dates.trade_date = gaps.trade_date
            where gaps.capability_id = %s
              and gaps.status <> %s
            order by gaps.trade_date desc, gaps.missing_count desc, gaps.code
            """,
            (max(1, window_count), capability_id, GAP_RESOLVED),
        )
        if frame.empty:
            return ()
        return tuple(_gap_from_row(row) for row in frame.to_dict("records"))

    def list_retryable(self, capability_id: str, window_count: int) -> tuple[CaptureGap, ...]:
        self._ensure_schema()
        frame = query_dataframe(
            """
            with selected_dates as (
                select distinct trade_date
                from fact.stock_daily_1d
                order by trade_date desc
                limit %s
            ), recent_dates as (
                select trade_date
                from selected_dates
                order by trade_date desc
                limit 5
            )
            select gaps.capability_id, gaps.market, gaps.code, gaps.trade_date,
                   gaps.expected_count, gaps.actual_count, gaps.missing_count, gaps.status,
                   gaps.provider_results, gaps.retry_count, gaps.first_seen_at,
                   gaps.last_seen_at, gaps.last_attempt_at, gaps.resolved_at, gaps.last_error
            from market_data_capture_gaps gaps
            join selected_dates dates on dates.trade_date = gaps.trade_date
            where gaps.capability_id = %s
              and gaps.status <> %s
              and (
                    gaps.status = %s
                 or gaps.last_attempt_at is null
                 or (
                        gaps.trade_date in (select trade_date from recent_dates)
                    and gaps.last_attempt_at < now() - interval '2 hours'
                 )
                 or (
                        gaps.status = %s
                    and gaps.last_attempt_at < now() - interval '15 minutes'
                 )
                 or (
                        gaps.status = %s
                    and gaps.retry_count < 3
                    and gaps.last_attempt_at < now() - interval '20 hours'
                 )
              )
            -- 先恢复最新交易日，避免历史补偿长期占用当日收盘后的回补窗口。
            -- 同一交易日优先处理缺失更多的股票，尽快降低该日的整体覆盖风险。
            order by gaps.trade_date desc, gaps.missing_count desc, gaps.code
            """,
            (
                max(1, window_count),
                capability_id,
                GAP_RESOLVED,
                GAP_PENDING,
                GAP_SYSTEM_FAILED,
                GAP_PROVIDER_EMPTY,
            ),
        )
        if frame.empty:
            return ()
        return tuple(_gap_from_row(row) for row in frame.to_dict("records"))

    def audit_intraday(self, window_count: int = 30) -> CaptureGapAuditResult:
        self._ensure_schema()
        actual_window_count = max(1, window_count)
        selected_dates = self._selected_intraday_dates(actual_window_count)
        dates_to_scan = self._intraday_dates_to_scan(selected_dates)
        for trade_date in dates_to_scan:
            self._audit_intraday_date(trade_date)
        if not execute_sql(
            """
            insert into market_data_capture_gaps (
                capability_id, market, code, trade_date, expected_count, actual_count,
                missing_count, status, last_seen_at, resolved_at
            )
            select coverage.capability_id, coverage.market, coverage.code, coverage.trade_date,
                   coverage.expected_count, coverage.actual_count,
                   greatest(coverage.expected_count - coverage.actual_count, 0), %s, now(), null
            from market_data_intraday_coverage_daily coverage
            where coverage.capability_id = %s
              and coverage.trade_date = any(%s::date[])
              and coverage.actual_count <> coverage.expected_count
            on conflict (capability_id, code, trade_date) do update
            set market = excluded.market,
                expected_count = excluded.expected_count,
                actual_count = excluded.actual_count,
                missing_count = excluded.missing_count,
                status = case
                    when market_data_capture_gaps.status = %s then %s
                    else market_data_capture_gaps.status
                end,
                last_seen_at = now(),
                resolved_at = null
            """,
            (GAP_PENDING, INTRADAY_CAPABILITY_ID, list(selected_dates), GAP_RESOLVED, GAP_PENDING),
        ):
            raise RuntimeError("股票 1m 历史缺口审计写入失败")
        if not execute_sql(
            """
            update market_data_capture_gaps gaps
            set status = %s,
                actual_count = coverage.actual_count,
                missing_count = 0,
                last_seen_at = now(),
                resolved_at = coalesce(gaps.resolved_at, now()),
                last_error = ''
            from market_data_intraday_coverage_daily coverage
            where gaps.capability_id = %s
              and coverage.capability_id = gaps.capability_id
              and coverage.code = gaps.code
              and coverage.trade_date = gaps.trade_date
              and coverage.trade_date = any(%s::date[])
              and coverage.actual_count = coverage.expected_count
              and gaps.status <> %s
            """,
            (GAP_RESOLVED, INTRADAY_CAPABILITY_ID, list(selected_dates), GAP_RESOLVED),
        ):
            raise RuntimeError("股票 1m 历史缺口解决状态更新失败")
        unresolved = self.list_unresolved(INTRADAY_CAPABILITY_ID, actual_window_count)
        summary_frame = query_dataframe(
            """
            select count(distinct dates.trade_date) filter (where audits.capability_id is not null) as audited_count,
                   count(*) filter (where gaps.status = %s) as resolved_count
            from unnest(%s::date[]) dates(trade_date)
            left join market_data_capture_gap_audits audits
              on audits.capability_id = %s and audits.trade_date = dates.trade_date
            left join market_data_capture_gaps gaps
              on gaps.capability_id = %s and gaps.trade_date = dates.trade_date
            """,
            (GAP_RESOLVED, list(selected_dates), INTRADAY_CAPABILITY_ID, INTRADAY_CAPABILITY_ID),
        )
        summary = {} if summary_frame.empty else summary_frame.iloc[0].to_dict()
        return CaptureGapAuditResult(
            INTRADAY_CAPABILITY_ID,
            actual_window_count,
            int(summary.get("audited_count", 0) or 0),
            len(unresolved),
            int(summary.get("resolved_count", 0) or 0),
            dates_to_scan,
        )

    def _selected_intraday_dates(self, window_count: int) -> tuple[str, ...]:
        frame = query_dataframe(
            """
            select distinct trade_date
            from fact.stock_daily_1d
            order by trade_date desc
            limit %s
            """,
            (window_count,),
        )
        if frame.empty:
            return ()
        return tuple(format_date_value(row["trade_date"]) for row in frame.to_dict("records"))

    def _intraday_dates_to_scan(self, selected_dates: Sequence[str]) -> tuple[str, ...]:
        if selected_dates == ():
            return ()
        frame = query_dataframe(
            """
            select requested.trade_date, audits.audited_at
            from unnest(%s::date[]) with ordinality requested(trade_date, position)
            left join market_data_capture_gap_audits audits
              on audits.capability_id = %s
             and audits.trade_date = requested.trade_date
            order by requested.position
            """,
            (list(selected_dates), INTRADAY_CAPABILITY_ID),
        )
        rows = [] if frame.empty else frame.to_dict("records")
        unaudited = [format_date_value(row["trade_date"]) for row in rows if _is_missing_value(row.get("audited_at"))]
        if unaudited != []:
            return tuple(unaudited[:5])
        latest_date = selected_dates[0]
        historical = [row for row in rows if format_date_value(row["trade_date"]) != latest_date]
        oldest = min(historical, key=lambda row: str(row.get("audited_at", "")), default=None)
        if oldest is None:
            return (latest_date,)
        return (latest_date, format_date_value(oldest["trade_date"]))

    def _audit_intraday_date(self, trade_date: str) -> None:
        if not execute_sql(
            """
            with expected as (
                select daily.market, daily.code
                from fact.stock_daily_1d daily
                where daily.trade_date = %s::date
                  and not coalesce(daily.is_suspended, false)
            ), actual as (
                select bars.code, count(*)::integer as bar_count
                from fact.stock_bar_1m bars
                where bars.bar_time >= %s::date
                  and bars.bar_time < %s::date + interval '1 day'
                  and (
                        bars.bar_time::time between time '09:31:00' and time '11:30:00'
                     or bars.bar_time::time between time '13:01:00' and time '15:00:00'
                  )
                  and bars.open is not null
                  and bars.high is not null
                  and bars.low is not null
                  and bars.close is not null
                group by bars.code
            )
            insert into market_data_intraday_coverage_daily (
                capability_id, market, code, trade_date, expected_count, actual_count, audited_at
            )
            select %s, expected.market, expected.code, %s::date, 240,
                   coalesce(actual.bar_count, 0), now()
            from expected
            left join actual on actual.code = expected.code
            on conflict (capability_id, code, trade_date) do update
            set market = excluded.market,
                expected_count = excluded.expected_count,
                actual_count = excluded.actual_count,
                audited_at = now()
            """,
            (trade_date, trade_date, trade_date, INTRADAY_CAPABILITY_ID, trade_date),
        ):
            raise RuntimeError(f"股票 1m 单日覆盖审计失败: trade_date={trade_date}")
        if not execute_sql(
            """
            insert into market_data_capture_gap_audits (
                capability_id, trade_date, expected_count, complete_count, gap_count, audited_at
            )
            select %s, %s::date, count(*),
                   count(*) filter (where actual_count = expected_count),
                   count(*) filter (where actual_count <> expected_count), now()
            from market_data_intraday_coverage_daily
            where capability_id = %s and trade_date = %s::date
            on conflict (capability_id, trade_date) do update
            set expected_count = excluded.expected_count,
                complete_count = excluded.complete_count,
                gap_count = excluded.gap_count,
                audited_at = now()
            """,
            (INTRADAY_CAPABILITY_ID, trade_date, INTRADAY_CAPABILITY_ID, trade_date),
        ):
            raise RuntimeError(f"股票 1m 单日审计汇总失败: trade_date={trade_date}")

    def mark_retrying(self, gaps: Sequence[CaptureGap]) -> None:
        self._ensure_schema()
        if gaps == ():
            return
        params = [
            (GAP_RETRYING, gap.capability_id, gap.code, gap.trade_date)
            for gap in gaps
        ]
        if not execute_many(
            """
            update market_data_capture_gaps
            set status = %s,
                last_attempt_at = now(),
                last_error = ''
            where capability_id = %s and code = %s and trade_date = %s::date
            """,
            params,
        ):
            raise RuntimeError("行情缺口重试状态更新失败")

    def record_incomplete(
        self,
        capability_id: str,
        code: str,
        trade_date: str,
        expected_count: int,
        actual_count: int,
        provider_results: dict[str, object],
        system_failed: bool,
        last_error: str,
    ) -> None:
        self._ensure_schema()
        status = GAP_SYSTEM_FAILED if system_failed else GAP_PROVIDER_EMPTY
        if not execute_sql(
            """
            insert into market_data_capture_gaps (
                capability_id, market, code, trade_date, expected_count, actual_count,
                missing_count, status, provider_results, retry_count, first_seen_at,
                last_seen_at, last_attempt_at, last_error
            )
            values (%s, '', %s, %s::date, %s, %s, %s, %s, %s, 1, now(), now(), now(), %s)
            on conflict (capability_id, code, trade_date) do update
            set expected_count = excluded.expected_count,
                actual_count = excluded.actual_count,
                missing_count = excluded.missing_count,
                status = excluded.status,
                provider_results = excluded.provider_results,
                retry_count = market_data_capture_gaps.retry_count + 1,
                last_seen_at = now(),
                last_attempt_at = now(),
                resolved_at = null,
                last_error = excluded.last_error
            """,
            (
                capability_id,
                code,
                trade_date,
                expected_count,
                actual_count,
                max(expected_count - actual_count, 0),
                status,
                Jsonb(provider_results),
                last_error,
            ),
        ):
            raise RuntimeError(f"行情缺口记录失败: code={code} trade_date={trade_date}")

    def record_system_failure(self, capability_id: str, code: str, trade_date: str, last_error: str) -> None:
        self._ensure_schema()
        if not execute_sql(
            """
            insert into market_data_capture_gaps (
                capability_id, market, code, trade_date, expected_count, actual_count,
                missing_count, status, provider_results, retry_count, first_seen_at,
                last_seen_at, last_attempt_at, last_error
            )
            values (%s, '', %s, %s::date, 240, 0, 240, %s, %s, 1, now(), now(), now(), %s)
            on conflict (capability_id, code, trade_date) do update
            set status = excluded.status,
                provider_results = excluded.provider_results,
                retry_count = market_data_capture_gaps.retry_count + 1,
                last_seen_at = now(),
                last_attempt_at = now(),
                resolved_at = null,
                last_error = excluded.last_error
            """,
            (
                capability_id,
                code,
                trade_date,
                GAP_SYSTEM_FAILED,
                Jsonb({"error_type": "system_failure", "error": last_error}),
                last_error,
            ),
        ):
            raise RuntimeError(f"行情系统失败记录失败: code={code} trade_date={trade_date}")

    def resolve(self, capability_id: str, code: str, trade_date: str, actual_count: int) -> None:
        self._ensure_schema()
        if not execute_sql(
            """
            update market_data_capture_gaps
            set actual_count = %s,
                missing_count = 0,
                status = %s,
                last_seen_at = now(),
                last_attempt_at = now(),
                resolved_at = coalesce(resolved_at, now()),
                last_error = ''
            where capability_id = %s and code = %s and trade_date = %s::date
            """,
            (actual_count, GAP_RESOLVED, capability_id, code, trade_date),
        ):
            raise RuntimeError(f"行情缺口解决状态更新失败: code={code} trade_date={trade_date}")

    def _ensure_schema(self) -> None:
        if self._schema_ready:
            return
        if not execute_sql(CAPTURE_GAP_SCHEMA_SQL):
            raise RuntimeError("行情缺口表初始化失败")
        self._schema_ready = True
