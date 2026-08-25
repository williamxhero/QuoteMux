from __future__ import annotations

from collections.abc import Generator, Iterable
from datetime import date, datetime
import sys
from typing import Any

from quotemux.infra.db.read_client import QueryBatch, ReadOnlyClient, StageCallback


_CANONICAL_MARKET = """
(
    (day_rows.market = 'SHSE' and (left(day_rows.code, 1) in ('5', '6') or left(day_rows.code, 3) = '900'))
    or (day_rows.market = 'BJSE' and (left(day_rows.code, 1) in ('4', '8') or left(day_rows.code, 3) = '920'))
    or (day_rows.market = 'SZSE' and left(day_rows.code, 1) not in ('4', '5', '6', '8', '9'))
)
"""


def _daily_metric_selects() -> str:
    previous_close = "coalesce(day_rows.pre_close, previous_day.previous_close)"
    change = f"coalesce(day_rows.change, day_rows.close - {previous_close})"
    return f"""
        {previous_close} as pre_close,
        {change} as change,
        coalesce(day_rows.pct_chg, {change} / nullif({previous_close}, 0) * 100) as pct_chg,
    """


def _daily_target_query(*, snapshot: bool, skip_suspended: bool, skip_st: bool) -> str:
    where = [
        "day_rows.trade_date = %s::date" if snapshot else "day_rows.trade_date >= %s::date and day_rows.trade_date <= %s::date",
        _CANONICAL_MARKET,
    ]
    if skip_suspended:
        where.append("day_rows.is_suspended is not true")
    if skip_st:
        if snapshot:
            where.append("day_rows.is_st is not true")
        else:
            where.append("""
                not exists (
                    select 1
                    from fact.stock_daily_1d st_rows
                    where st_rows.market = day_rows.market
                      and st_rows.code = day_rows.code
                      and st_rows.trade_date >= %s::date
                      and st_rows.trade_date <= %s::date
                      and st_rows.is_st is true
                )
            """)
    ordering = "day_rows.code" if snapshot else "day_rows.trade_date, day_rows.code"
    return f"""
        with target_rows as materialized (
            select day_rows.market, day_rows.code, day_rows.trade_date,
                   day_rows.open, day_rows.high, day_rows.low, day_rows.close,
                   day_rows.pre_close, day_rows.change, day_rows.pct_chg,
                   day_rows.volume, day_rows.amount,
                   day_rows.is_suspended, day_rows.is_st
            from fact.stock_daily_1d day_rows
            where {' and '.join(where)}
            order by {ordering}
            limit %s
            offset %s
        )
        select day_rows.code,
               day_rows.trade_date::text as trade_time,
               day_rows.open, day_rows.high, day_rows.low, day_rows.close,
               {_daily_metric_selects()}
               day_rows.volume, day_rows.amount,
               day_rows.is_suspended, day_rows.is_st
        from target_rows day_rows
        left join lateral (
            select previous_rows.close as previous_close
            from fact.stock_daily_1d previous_rows
            where previous_rows.market = day_rows.market
              and previous_rows.code = day_rows.code
              and previous_rows.trade_date < day_rows.trade_date
              and day_rows.pre_close is null
            order by previous_rows.trade_date desc
            limit 1
        ) previous_day on true
        order by {ordering}
    """


_STOCK_1M_QUERY = """
    select bars.code, bars.bar_time as trade_time,
           bars.open, bars.high, bars.low, bars.close, bars.volume, bars.amount
    from fact.stock_bar_1m bars
    where bars.code = any(%s::character(6)[])
      and bars.bar_time >= %s::timestamp
      and bars.bar_time <= %s::timestamp
    order by bars.code, bars.bar_time
"""

_STOCK_1M_COVERAGE_QUERY = """
    select coverage.code,
           sum(coverage.row_count)::bigint as row_count,
           min(coverage.first_bar_time) as first_trade_time,
           max(coverage.last_bar_time) as last_trade_time
    from readmodel.stock_bar_1m_daily_coverage coverage
    where coverage.code = any(%s::character(6)[])
      and coverage.trade_date >= (%s::timestamp)::date
      and coverage.trade_date <= (%s::timestamp)::date
    group by coverage.code
    order by coverage.code
"""

_FUTURES_SERIES_STORAGE = {
    "back_adjusted_continuous": "apex_l0_adjusted",
    "main_continuous": "main_continuous",
}

_FUTURES_PRODUCT_CODES = (
    "IF", "IH", "IC", "IM", "T", "TF", "TS", "TL",
    "ad", "ag", "al", "ao", "au", "br", "bu", "cu", "fu", "hc", "ni", "op", "pb", "rb", "ru", "sn", "sp", "ss", "wr", "zn",
    "a", "b", "bz", "c", "cs", "eb", "eg", "fb", "i", "j", "jd", "jm", "l", "lg", "lh", "m", "p", "pg", "PL", "pp", "rr", "v", "y",
    "AP", "CF", "CJ", "CY", "FG", "JR", "MA", "OI", "PF", "PK", "PR", "PX", "RM", "RS", "SA", "SF", "SH", "SM", "SR", "TA", "UR", "WH", "ZC",
    "bc", "ec", "lu", "nr", "sc", "lc", "pd", "ps", "pt", "si",
)
_FUTURES_CANONICAL_CODES = {code.lower(): code for code in _FUTURES_PRODUCT_CODES}
_MAX_FUTURES_1M_LIMIT = 500_000

_FUTURES_1M_QUERY = """
    select bars.product_code,
           bars.exchange,
           case when bars.series_type = 'apex_l0_adjusted'
                then 'back_adjusted_continuous'
                else bars.series_type
           end as series_type,
           bars.bar_time::text as bar_time,
           bars.open, bars.high, bars.low, bars.close,
           bars.volume, bars.open_interest, bars.adjustment_offset
    from fact.future_bar_1m bars
    where bars.product_code = any(%s::text[])
      and bars.series_type = %s
      and bars.bar_time >= %s::timestamp
      and bars.bar_time <= %s::timestamp
    order by bars.bar_time, bars.product_code
    limit %s
"""

_FUTURES_COVERAGE_QUERY = """
    select coverage.product_code,
           coverage.exchange,
           case when coverage.series_type = 'apex_l0_adjusted'
                then 'back_adjusted_continuous'
                else coverage.series_type
           end as series_type,
           coverage.row_count,
           coverage.first_bar_time::text as first_bar_time,
           coverage.last_bar_time::text as last_bar_time
    from fact.future_bar_1m_coverage coverage
    where (%s = '' or coverage.series_type = %s)
    order by coverage.series_type, coverage.exchange, coverage.product_code
"""


def _date_text(value: str, field_name: str) -> str:
    text = str(value).strip()
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise ValueError(f"{field_name} must be YYYY-MM-DD") from exc


def _timestamp_value(value: str | datetime, field_name: str) -> str | datetime:
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    try:
        datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO timestamp") from exc
    return text


def _stock_code(value: object) -> str:
    text = str(value).strip().upper()
    if "." in text:
        left, right = text.split(".", 1)
        text = right if left in {"SH", "SZ", "BJ", "SHSE", "SZSE", "BJSE"} else left
    if len(text) != 6 or not text.isdigit():
        raise ValueError(f"invalid stock code: {value}")
    return text


def _stock_codes(values: list[str]) -> list[str]:
    return sorted({_stock_code(value) for value in values})


def _future_codes(values: str | Iterable[str]) -> list[str]:
    raw_values = values.split(",") if isinstance(values, str) else values
    codes: set[str] = set()
    for value in raw_values:
        text = str(value).strip()
        if text == "":
            continue
        code = _FUTURES_CANONICAL_CODES.get(text.lower())
        if code is None:
            raise ValueError(f"unknown futures product code: {text}")
        codes.add(code)
    return sorted(codes)


class Stock1mBatchStream:
    def __init__(
        self,
        client: Any,
        codes: list[str],
        start_time: str | datetime,
        end_time: str | datetime,
        batch_size: int,
    ) -> None:
        self._client = client
        self._params = (codes, start_time, end_time)
        self._batch_size = batch_size
        self._snapshot_context = None
        self._snapshot = None
        self._iterator = None
        self._coverage: QueryBatch | None = None

    @property
    def coverage(self) -> QueryBatch:
        if self._coverage is None:
            raise RuntimeError("Stock1mBatchStream must be entered before reading coverage")
        return self._coverage

    def __enter__(self) -> Stock1mBatchStream:
        self._snapshot_context = self._client.snapshot()
        self._snapshot = self._snapshot_context.__enter__()
        try:
            self._coverage = self._snapshot.query_batch(_STOCK_1M_COVERAGE_QUERY, self._params, stage="stock_1m_coverage")
            self._iterator = self._snapshot.stream_batches(
                _STOCK_1M_QUERY,
                self._params,
                batch_size=self._batch_size,
                stage="stock_1m_stream",
            )
            return self
        except BaseException:
            self._snapshot_context.__exit__(*sys.exc_info())
            raise

    def __iter__(self):
        if self._iterator is None:
            raise RuntimeError("Stock1mBatchStream must be used as a context manager")
        return self._iterator

    def __exit__(self, exc_type, exc, traceback) -> bool:
        try:
            if self._iterator is not None:
                self._iterator.close()
        finally:
            if self._snapshot_context is not None:
                return bool(self._snapshot_context.__exit__(exc_type, exc, traceback))
        return False


class QuoteMuxPublicReader:
    """Strictly local, read-only market-data API for HTTP query serving."""

    def __init__(self, client: Any | None = None, stage_callback: StageCallback | None = None) -> None:
        self._client = client or ReadOnlyClient(stage_callback)

    def get_stock_daily_snapshot_batch(
        self,
        trade_date: str,
        *,
        limit: int = 200,
        offset: int = 0,
        skip_suspended: bool = True,
        skip_st: bool = False,
    ) -> QueryBatch:
        actual_date = _date_text(trade_date, "trade_date")
        self._validate_page(limit, offset)
        query = _daily_target_query(snapshot=True, skip_suspended=skip_suspended, skip_st=skip_st)
        return self._client.query_batch(query, (actual_date, limit, offset), stage="daily_snapshot")

    def get_stock_daily_local_window_batch(
        self,
        start_date: str,
        end_date: str,
        *,
        limit: int = 50_000,
        offset: int = 0,
        skip_suspended: bool = True,
        skip_st: bool = False,
    ) -> QueryBatch:
        actual_start = _date_text(start_date, "start_date")
        actual_end = _date_text(end_date, "end_date")
        if actual_start > actual_end:
            raise ValueError("start_date must not be after end_date")
        self._validate_page(limit, offset)
        query = _daily_target_query(snapshot=False, skip_suspended=skip_suspended, skip_st=skip_st)
        params: tuple[object, ...] = (actual_start, actual_end, limit, offset)
        if skip_st:
            params = (actual_start, actual_end, actual_start, actual_end, limit, offset)
        return self._client.query_batch(query, params, stage="daily_local_window")

    def open_stock_1m_batch_stream(
        self,
        codes: list[str],
        start_time: str | datetime,
        end_time: str | datetime,
        *,
        batch_size: int = 4_096,
    ) -> Stock1mBatchStream:
        normalized_codes, actual_start, actual_end = self._stock_1m_inputs(codes, start_time, end_time, batch_size)
        return Stock1mBatchStream(self._client, normalized_codes, actual_start, actual_end, batch_size)

    def stream_stock_1m_batches(
        self,
        codes: list[str],
        start_time: str | datetime,
        end_time: str | datetime,
        *,
        batch_size: int = 4_096,
    ) -> Generator[QueryBatch, None, None]:
        normalized_codes, actual_start, actual_end = self._stock_1m_inputs(codes, start_time, end_time, batch_size)
        return self._client.stream_batches(
            _STOCK_1M_QUERY,
            (normalized_codes, actual_start, actual_end),
            batch_size=batch_size,
            stage="stock_1m_stream",
        )

    def get_stock_1m_coverage_batch(
        self,
        codes: list[str],
        start_time: str | datetime,
        end_time: str | datetime,
    ) -> QueryBatch:
        normalized_codes, actual_start, actual_end = self._stock_1m_inputs(codes, start_time, end_time, 1)
        return self._client.query_batch(
            _STOCK_1M_COVERAGE_QUERY,
            (normalized_codes, actual_start, actual_end),
            stage="stock_1m_coverage",
        )

    def get_futures_quotes_1m_batch(
        self,
        codes: str | Iterable[str],
        series_type: str,
        start_time: str | datetime,
        end_time: str | datetime,
        *,
        limit: int = 10_000,
    ) -> QueryBatch:
        """Read published futures 1m bars without invoking the writer runtime."""
        normalized_codes = _future_codes(codes)
        if not normalized_codes:
            raise ValueError("codes must not be empty")
        storage_series_type = _FUTURES_SERIES_STORAGE.get(series_type)
        if storage_series_type is None:
            supported = ", ".join(_FUTURES_SERIES_STORAGE)
            raise ValueError(f"series_type must be one of: {supported}")
        actual_start = _timestamp_value(start_time, "start_time")
        actual_end = _timestamp_value(end_time, "end_time")
        start_key = actual_start if isinstance(actual_start, datetime) else datetime.fromisoformat(actual_start)
        end_key = actual_end if isinstance(actual_end, datetime) else datetime.fromisoformat(actual_end)
        if start_key > end_key:
            raise ValueError("start_time must not be after end_time")
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise ValueError("limit must be a positive integer")
        return self._client.query_batch(
            _FUTURES_1M_QUERY,
            (normalized_codes, storage_series_type, actual_start, actual_end, min(limit, _MAX_FUTURES_1M_LIMIT)),
            stage="futures_1m",
        )

    def list_futures_coverage_batch(self, series_type: str = "") -> QueryBatch:
        if series_type and series_type not in _FUTURES_SERIES_STORAGE:
            supported = ", ".join(_FUTURES_SERIES_STORAGE)
            raise ValueError(f"series_type must be one of: {supported}")
        storage_series_type = _FUTURES_SERIES_STORAGE.get(series_type, "")
        return self._client.query_batch(
            _FUTURES_COVERAGE_QUERY,
            (storage_series_type, storage_series_type),
            stage="futures_coverage",
        )

    @staticmethod
    def _stock_1m_inputs(
        codes: list[str],
        start_time: str | datetime,
        end_time: str | datetime,
        batch_size: int,
    ) -> tuple[list[str], str | datetime, str | datetime]:
        normalized_codes = _stock_codes(codes)
        if not normalized_codes:
            raise ValueError("codes must not be empty")
        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
            raise ValueError("batch_size must be a positive integer")
        actual_start = _timestamp_value(start_time, "start_time")
        actual_end = _timestamp_value(end_time, "end_time")
        start_key = actual_start if isinstance(actual_start, datetime) else datetime.fromisoformat(actual_start)
        end_key = actual_end if isinstance(actual_end, datetime) else datetime.fromisoformat(actual_end)
        if start_key > end_key:
            raise ValueError("start_time must not be after end_time")
        return normalized_codes, actual_start, actual_end

    @staticmethod
    def _validate_page(limit: int, offset: int) -> None:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise ValueError("limit must be a positive integer")
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
            raise ValueError("offset must be a non-negative integer")
