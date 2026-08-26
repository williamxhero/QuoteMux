from __future__ import annotations

from collections.abc import Generator, Iterable
from datetime import date, datetime
import base64
import hashlib
import json
import sys
from typing import Any
from contextlib import nullcontext
from zoneinfo import ZoneInfo

from quotemux.infra.db.read_client import QueryBatch, ReadOnlyClient, StageCallback
from quotemux.futures_partial_contract import DATASET_ID as _CONTRACT_PARTIAL_DATASET_ID, admitted_rows_cte


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

_PARTIAL_DATASET_ID = _CONTRACT_PARTIAL_DATASET_ID
_PARTIAL_PRODUCTS = frozenset(("ag", "al", "AP", "CF", "cu", "hc", "i", "j", "m", "MA", "ni", "p", "ru", "sc", "T", "TA", "TF", "v", "y", "lh", "SA", "ao", "si"))


class FuturesPartialPublicationQueryError(ValueError):
    """The caller supplied malformed partial-query parameters or cursor."""


class FuturesPartialPublicationStaleError(ValueError):
    """A qmp/qmc/qmg identity is absent, malformed, or no longer coherent."""

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

_FUTURES_SERIES_STATE_QUERY = """
    select distinct on (state.series_type)
           case when state.series_type = 'apex_l0_adjusted'
                then 'back_adjusted_continuous'
                else state.series_type
           end as series_type,
           state.generation, state.row_count,
           state.first_bar_time::text as first_bar_time,
           state.last_bar_time::text as last_bar_time,
           state.transaction_id, state.operation, state.delta_fingerprint,
           state.recorded_at::text as recorded_at
    from audit.future_bar_1m_series_generation state
    where (%s = '' or state.series_type = %s)
    order by state.series_type, state.generation desc
"""

_FUTURES_PARTIAL_BARS_QUERY = admitted_rows_cte(
    qmi_expression="select publication.payload_json->>'qmi_id' from audit.future_bar_1m_partial_publication publication where publication.qmp_id=%s"
) + """
    select bars.product_code, bars.exchange, 'back_adjusted_continuous' as series_type,
           bars.bar_time::text as bar_time, bars.open, bars.high, bars.low, bars.close,
           bars.volume, bars.open_interest, bars.adjustment_offset,
           array_agg(distinct boundary.boundary_id order by boundary.boundary_id) as boundary_ids,
           array_agg(distinct bars.source_key order by bars.source_key) as source_keys
    from admitted_rows bars
    join audit.future_bar_1m_partial_source_boundary boundary
      on boundary.qmp_id = %s and boundary.product_code = bars.product_code
     and boundary.exchange = bars.exchange and boundary.series_type = bars.series_type
     and boundary.source_key = bars.source_key and bars.bar_time between boundary.start_time and boundary.end_time
    join audit.future_bar_1m_partial_revision revision on revision.qmc_id = %s and revision.qmp_id = boundary.qmp_id
    join audit.future_bar_1m_partial_revision_interval interval_row
      on interval_row.qmc_id = revision.qmc_id and interval_row.product_code = bars.product_code
     and interval_row.status = 'accepted' and bars.bar_time between interval_row.start_time and interval_row.end_time
    where bars.product_code = any(%s::text[])
      and bars.bar_time >= %s::timestamp and bars.bar_time <= %s::timestamp
      and (%s::timestamp is null or (bars.bar_time, bars.product_code) > (%s::timestamp, %s::text))
    group by bars.product_code, bars.exchange, bars.bar_time, bars.open, bars.high, bars.low, bars.close,
             bars.volume, bars.open_interest, bars.adjustment_offset
    order by bars.bar_time, bars.product_code
    limit %s
"""

_FUTURES_PARTIAL_COVERAGE_QUERY = """
    with clipped_intervals as (
        select interval_row.product_code, interval_row.exchange,
               greatest(interval_row.start_time, %s::timestamp) as start_time,
               least(interval_row.end_time, %s::timestamp) as end_time,
               interval_row.status, interval_row.interval_id, interval_row.residual_json
        from audit.future_bar_1m_partial_revision_interval interval_row
        join audit.future_bar_1m_partial_revision revision on revision.qmc_id = interval_row.qmc_id
        where revision.qmp_id = %s and interval_row.qmc_id = %s
          and interval_row.product_code = any(%s::text[])
          and interval_row.end_time >= %s::timestamp and interval_row.start_time <= %s::timestamp
    )
    select product_code, exchange, start_time::text, end_time::text, status,
           ((extract(epoch from end_time - start_time)/60)::bigint + 1) as observed_count,
           interval_id, residual_json
    from clipped_intervals
    where (%s::text is null or (product_code, start_time, end_time, status, interval_id) > (%s::text, %s::timestamp, %s::timestamp, %s::text, %s::text))
    order by product_code, start_time, end_time, status, interval_id
    limit %s
"""

_FUTURES_PARTIAL_IDENTITY_QUERY = """
    select publication.payload_json, publication.payload_sha256, revision.payload_json, revision.payload_sha256, generation.generation, generation.row_count,
           generation.first_bar_time::text, generation.last_bar_time::text
    from audit.future_bar_1m_partial_publication publication
    join audit.future_bar_1m_partial_revision revision on revision.qmp_id=publication.qmp_id and revision.qmc_id=%s
    join lateral (select generation,row_count,first_bar_time,last_bar_time from audit.future_bar_1m_series_generation
                  where series_type='apex_l0_adjusted' order by generation desc limit 1) generation on true
    where publication.qmp_id=%s
"""


def _date_text(value: str, field_name: str) -> str:
    text = str(value).strip()
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise ValueError(f"{field_name} must be YYYY-MM-DD") from exc


def _timestamp_value(value: str | datetime, field_name: str) -> str | datetime:
    parsed = value if isinstance(value, datetime) else None
    text = str(value).strip() if parsed is None else ""
    try:
        parsed = parsed or datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO timestamp") from exc
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(ZoneInfo("Asia/Shanghai")).replace(tzinfo=None)
    # PostgreSQL facts deliberately use local-naive Asia/Shanghai timestamps.
    return parsed.isoformat(sep=" ")


def _partial_minute_timestamp(value: str | datetime, field_name: str) -> str | datetime:
    actual = _timestamp_value(value, field_name); parsed = datetime.fromisoformat(str(actual))
    if parsed.second or parsed.microsecond: raise FuturesPartialPublicationQueryError(f"{field_name} must be minute-aligned")
    return actual


def _partial_cursor_encode(kind: str, payload: dict[str, object]) -> str:
    canonical = json.dumps({"kind": kind, **payload}, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    return base64.urlsafe_b64encode(canonical).rstrip(b"=").decode("ascii")


def canonical_identity_for_reader(payload: dict[str, object]) -> str:
    """Mirror the metadata identity algorithm without importing writer modules."""
    return "qmg-v1-" + hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def _partial_identity(prefix: str, payload: dict[str, object]) -> str:
    return f"{prefix}-v1-" + hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def _partial_cursor_decode(value: str | None, kind: str, query_hash: str) -> dict[str, object] | None:
    if value in (None, ""):
        return None
    try:
        raw = base64.urlsafe_b64decode(str(value) + "=" * (-len(str(value)) % 4))
        payload = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise FuturesPartialPublicationQueryError("invalid partial cursor") from exc
    if not isinstance(payload, dict) or payload.get("kind") != kind or payload.get("query_hash") != query_hash:
        raise FuturesPartialPublicationQueryError("partial cursor does not match query")
    common = {"kind", "dataset_id", "qmp_id", "qmc_id", "qmg_id", "query_hash"}
    specific = {"partial-bars": {"bar_time", "product_code"}, "partial-coverage": {"product_code", "start_time", "end_time", "status", "interval_id"}}
    if set(payload) != common | specific[kind] or payload.get("dataset_id") != _PARTIAL_DATASET_ID:
        raise FuturesPartialPublicationQueryError("partial cursor has invalid fields")
    try:
        if kind == "partial-bars":
            if payload["product_code"] not in _PARTIAL_PRODUCTS: raise ValueError
            _timestamp_value(str(payload["bar_time"]), "cursor.bar_time")
        else:
            if payload["product_code"] not in _PARTIAL_PRODUCTS or payload["status"] != "accepted" or not isinstance(payload["interval_id"], str) or not payload["interval_id"].startswith("qci-v1-") or len(payload["interval_id"]) != 71: raise ValueError
            if datetime.fromisoformat(str(_timestamp_value(str(payload["start_time"]), "cursor.start_time"))) > datetime.fromisoformat(str(_timestamp_value(str(payload["end_time"]), "cursor.end_time"))): raise ValueError
    except (TypeError, ValueError):
        raise FuturesPartialPublicationQueryError("partial cursor has invalid values") from None
    return payload


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

    def list_futures_series_state_batch(self, series_type: str = "") -> QueryBatch:
        if series_type and series_type not in _FUTURES_SERIES_STORAGE:
            supported = ", ".join(_FUTURES_SERIES_STORAGE)
            raise ValueError(f"series_type must be one of: {supported}")
        storage_series_type = _FUTURES_SERIES_STORAGE.get(series_type, "")
        return self._client.query_batch(
            _FUTURES_SERIES_STATE_QUERY,
            (storage_series_type, storage_series_type),
            stage="futures_series_state",
        )

    def read_futures_1m_partial_page(
        self, codes: str | Iterable[str], start_time: str | datetime, end_time: str | datetime,
        *, qmp_id: str, qmc_id: str, qmg_id: str, cursor: str | None = None, limit: int = 10_000,
    ) -> tuple[QueryBatch, str | None]:
        """Read only admitted bars; output includes ``boundary_ids`` and ``source_keys`` evidence."""
        from quotemux.store.futures_partial_publication import validate_identity
        try:
            validate_identity(qmp_id, "qmp"); validate_identity(qmc_id, "qmc"); validate_identity(qmg_id, "qmg")
        except ValueError as exc:
            raise FuturesPartialPublicationStaleError(str(exc)) from exc
        normalized_codes = _future_codes(codes)
        if not normalized_codes or any(code not in _PARTIAL_PRODUCTS for code in normalized_codes):
            raise FuturesPartialPublicationQueryError("codes must be non-empty S000012 products; TL is not accepted")
        start, end = _partial_minute_timestamp(start_time, "start_time"), _partial_minute_timestamp(end_time, "end_time")
        if datetime.fromisoformat(str(start)) > datetime.fromisoformat(str(end)):
            raise FuturesPartialPublicationQueryError("start_time must not be after end_time")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 0 < limit <= _MAX_FUTURES_1M_LIMIT:
            raise FuturesPartialPublicationQueryError("limit must be a positive bounded integer")
        query_hash = hashlib.sha256(json.dumps([_PARTIAL_DATASET_ID, qmp_id, qmc_id, qmg_id, normalized_codes, str(start), str(end)], separators=(",", ":")).encode()).hexdigest()
        snapshot_context = self._client.snapshot() if hasattr(self._client, "snapshot") else nullcontext(self._client)
        with snapshot_context as snapshot:
            self._verify_futures_partial_identity(qmp_id, qmc_id, qmg_id, snapshot)
            prior = _partial_cursor_decode(cursor, "partial-bars", query_hash)
            prior_time = prior.get("bar_time") if prior else None
            prior_code = prior.get("product_code") if prior else None
            batch = snapshot.query_batch(_FUTURES_PARTIAL_BARS_QUERY, (qmp_id, qmp_id, qmp_id, qmc_id, normalized_codes, start, end, prior_time, prior_time, prior_code, limit + 1), stage="futures_partial_1m")
        rows = batch.rows[:limit]
        output = QueryBatch(batch.columns, rows)
        next_cursor = None
        if len(batch.rows) > limit:
            last = rows[-1]
            next_cursor = _partial_cursor_encode("partial-bars", {"dataset_id": _PARTIAL_DATASET_ID, "qmp_id": qmp_id, "qmc_id": qmc_id, "qmg_id": qmg_id, "query_hash": query_hash, "bar_time": str(last[3]), "product_code": str(last[0])})
        return output, next_cursor

    def read_futures_1m_partial_coverage_page(
        self, codes: str | Iterable[str], start_time: str | datetime, end_time: str | datetime, *, qmp_id: str, qmc_id: str, qmg_id: str, cursor: str | None = None, limit: int = 1_000,
    ) -> tuple[QueryBatch, str | None]:
        """Return observed accepted intervals. Missing bars are intentionally represented as skip residuals."""
        from quotemux.store.futures_partial_publication import validate_identity
        try:
            validate_identity(qmp_id, "qmp"); validate_identity(qmc_id, "qmc"); validate_identity(qmg_id, "qmg")
        except ValueError as exc:
            raise FuturesPartialPublicationStaleError(str(exc)) from exc
        normalized_codes = _future_codes(codes)
        if not normalized_codes or any(code not in _PARTIAL_PRODUCTS for code in normalized_codes):
            raise FuturesPartialPublicationQueryError("codes must be non-empty S000012 products; TL is not accepted")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 0 < limit <= 10_000:
            raise FuturesPartialPublicationQueryError("limit must be a positive bounded integer")
        start, end = _partial_minute_timestamp(start_time, "start_time"), _partial_minute_timestamp(end_time, "end_time")
        if datetime.fromisoformat(str(start)) > datetime.fromisoformat(str(end)):
            raise FuturesPartialPublicationQueryError("start_time must not be after end_time")
        query_hash = hashlib.sha256(json.dumps([_PARTIAL_DATASET_ID, qmp_id, qmc_id, qmg_id, normalized_codes, str(start), str(end)], separators=(",", ":")).encode()).hexdigest()
        snapshot_context = self._client.snapshot() if hasattr(self._client, "snapshot") else nullcontext(self._client)
        with snapshot_context as snapshot:
            self._verify_futures_partial_identity(qmp_id, qmc_id, qmg_id, snapshot)
            prior = _partial_cursor_decode(cursor, "partial-coverage", query_hash)
            params = (start, end, qmp_id, qmc_id, normalized_codes, start, end, prior.get("product_code") if prior else None, *( [prior.get(k) for k in ("product_code", "start_time", "end_time", "status", "interval_id")] if prior else [None] * 5), limit + 1)
            batch = snapshot.query_batch(_FUTURES_PARTIAL_COVERAGE_QUERY, params, stage="futures_partial_coverage")
        rows = batch.rows[:limit]; output = QueryBatch(batch.columns, rows)
        next_cursor = None
        if len(batch.rows) > limit:
            last = rows[-1]
            next_cursor = _partial_cursor_encode("partial-coverage", {"dataset_id": _PARTIAL_DATASET_ID, "qmp_id": qmp_id, "qmc_id": qmc_id, "qmg_id": qmg_id, "query_hash": query_hash, "product_code": str(last[0]), "start_time": str(last[2]), "end_time": str(last[3]), "status": str(last[4]), "interval_id": str(last[6])})
        return output, next_cursor

    def _verify_futures_partial_identity(self, qmp_id: str, qmc_id: str, qmg_id: str, client: Any) -> None:
        """Reject stale publications before returning any bar. Called inside the reader path."""
        batch = client.query_batch(_FUTURES_PARTIAL_IDENTITY_QUERY, (qmc_id, qmp_id), stage="futures_partial_identity")
        if len(batch.rows) != 1:
            raise FuturesPartialPublicationStaleError("partial publication identity is absent or incoherent")
        row = batch.rows[0]
        try:
            payload = row[0] if isinstance(row[0], dict) else json.loads(str(row[0]))
            revision = row[2] if isinstance(row[2], dict) else json.loads(str(row[2]))
            actual_qmp = _partial_identity("qmp", payload); actual_qmc = _partial_identity("qmc", revision)
            payload_sha = hashlib.sha256(json.dumps(payload,ensure_ascii=False,sort_keys=True,separators=(",",":"),allow_nan=False).encode()).hexdigest()
            revision_sha = hashlib.sha256(json.dumps(revision,ensure_ascii=False,sort_keys=True,separators=(",",":"),allow_nan=False).encode()).hexdigest()
            actual_qmg = canonical_identity_for_reader({"dataset_id": _PARTIAL_DATASET_ID, "series_type": "apex_l0_adjusted", "generation": int(row[4]), "row_count": int(row[5]), "first_bar_time": str(row[6]), "last_bar_time": str(row[7])})
        except Exception as exc:
            raise FuturesPartialPublicationStaleError("partial publication identity is malformed") from exc
        if payload.get("dataset_id") != _PARTIAL_DATASET_ID or revision.get("dataset_id") != _PARTIAL_DATASET_ID or actual_qmp != qmp_id or actual_qmc != qmc_id or row[1] != payload_sha or row[3] != revision_sha or payload.get("qmg_id") != qmg_id or revision.get("qmp_id") != qmp_id or revision.get("qmg_id") != qmg_id or actual_qmg != qmg_id:
            raise FuturesPartialPublicationStaleError("partial publication generation is stale")

    def get_futures_1m_partial_metadata(self, *, qmp_id: str, qmc_id: str, qmg_id: str) -> dict[str, object]:
        """Return verified immutable contract metadata for the MarketHub facade.

        A facade must not infer completeness from a 200 page; it can only state
        the partial contract after this identity/generation check passes.
        """
        from quotemux.store.futures_partial_publication import validate_identity
        try:
            validate_identity(qmp_id, "qmp"); validate_identity(qmc_id, "qmc"); validate_identity(qmg_id, "qmg")
        except ValueError as exc:
            raise FuturesPartialPublicationStaleError(str(exc)) from exc
        snapshot_context = self._client.snapshot() if hasattr(self._client, "snapshot") else nullcontext(self._client)
        with snapshot_context as snapshot:
            batch = snapshot.query_batch(_FUTURES_PARTIAL_IDENTITY_QUERY, (qmc_id, qmp_id), stage="futures_partial_metadata")
            self._verify_futures_partial_identity(qmp_id, qmc_id, qmg_id, snapshot)
        row = batch.rows[0]
        try:
            publication = row[0] if isinstance(row[0], dict) else json.loads(str(row[0]))
            revision = row[2] if isinstance(row[2], dict) else json.loads(str(row[2]))
            warmup = revision.get("warmup", {})
            return {
                "dataset_id": _PARTIAL_DATASET_ID, "qmp_id": qmp_id, "qmc_id": qmc_id, "qmg_id": qmg_id,
                "qmi_id": publication["qmi_id"], "catalog_identity": publication["catalog_identity"],
                "publication_verified": True, "timezone": revision["timezone"], "interval_bounds": revision["interval_bounds"],
                "coverage_semantics": revision["coverage_semantics"], "missing_bar_semantics": revision["missing_bar_semantics"],
                "open_interest": revision["open_interest"], "session_grid": revision["session_grid"],
                "sources": publication["sources"], "source_boundary_manifest": publication["source_boundary_manifest"],
                "warmup": warmup, "residual_semantics": warmup.get("residual_semantics"),
                "lineage_limitations": publication["lineage_limitations"],
            }
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise FuturesPartialPublicationStaleError("partial publication metadata is malformed") from exc

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
