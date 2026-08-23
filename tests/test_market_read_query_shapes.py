from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import time

import pandas as pd
import pytest

from platform_models import StockQuoteItem
from quotemux import futures
from quotemux import stocks
from quotemux.infra.db import market_reads


STOCK_DAILY_COLUMNS = {
    "code",
    "market",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "change",
    "pct_chg",
    "volume",
    "amount",
    "is_suspended",
    "is_st",
}


def test_daily_snapshot_pages_target_day_before_previous_close_lookup(monkeypatch) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(market_reads, "_existing_columns", lambda *_args: STOCK_DAILY_COLUMNS)

    def fake_query(query: str, params: tuple[object, ...]) -> pd.DataFrame:
        captured["query"] = query
        captured["params"] = params
        return pd.DataFrame()

    monkeypatch.setattr(market_reads, "query_dataframe", fake_query)

    market_reads.load_stock_daily_snapshot_frame("2026-08-21", 100, 200)

    query = str(captured["query"]).lower()
    target_rows = query.index("target_rows as materialized")
    target_date = query.index("day_rows.trade_date = %s::date", target_rows)
    target_limit = query.index("limit %s", target_date)
    previous_lookup = query.index("left join lateral", target_limit)
    assert target_rows < target_date < target_limit < previous_lookup
    assert "lag(" not in query
    assert captured["params"] == ("2026-08-21", 100, 200)


def test_daily_snapshot_previous_close_lookup_is_same_market_and_strictly_earlier(monkeypatch) -> None:
    monkeypatch.setattr(market_reads, "_existing_columns", lambda *_args: STOCK_DAILY_COLUMNS)

    query = market_reads._stock_daily_snapshot_query().lower()

    assert "previous_rows.market = day_rows.market" in query
    assert "previous_rows.code = day_rows.code" in query
    assert "previous_rows.trade_date < day_rows.trade_date" in query
    assert "order by previous_rows.trade_date desc" in query
    assert "limit 1" in query
    assert "and day_rows.pre_close is null" in query


def test_stock_1m_batch_query_uses_chunk_pruned_any_query(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_query(query: str, params: tuple[object, ...]) -> pd.DataFrame:
        captured["query"] = query
        captured["params"] = params
        return pd.DataFrame()

    monkeypatch.setattr(market_reads, "query_dataframe", fake_query)

    market_reads.load_stock_intraday_frame(
        ["600001", "000001", "600001"],
        "2026-08-21 09:30:00",
        "2026-08-21 15:00:00",
    )

    query = str(captured["query"]).lower()
    assert "bars.code = any(%s::character(6)[])" in query
    assert "bars.bar_time >= %s::timestamp" in query
    assert "bars.bar_time <= %s::timestamp" in query
    assert "cross join lateral" not in query
    assert "order by bars.code, bars.bar_time" in query
    assert captured["params"] == (["000001", "600001"], "2026-08-21 09:30:00", "2026-08-21 15:00:00")


def test_quote_item_grouping_is_single_pass() -> None:
    class CountingList(list[StockQuoteItem]):
        iterations = 0

        def __iter__(self):
            self.iterations += 1
            return super().__iter__()

    items = CountingList(
        [
            StockQuoteItem(code="600000", trade_time="2026-08-21 09:31:00", freq="1m"),
            StockQuoteItem(code="000001", trade_time="2026-08-21 09:31:00", freq="1m"),
            StockQuoteItem(code="600000", trade_time="2026-08-21 09:32:00", freq="1m"),
        ]
    )

    grouped = stocks._group_quote_items_by_code(items, "1m")

    assert items.iterations == 1
    assert [item.trade_time for item in grouped["600000"]] == ["2026-08-21 09:31:00", "2026-08-21 09:32:00"]


def test_canonical_minute_time_key_skips_reparsing(monkeypatch) -> None:
    monkeypatch.setattr(stocks, "format_datetime_value", lambda *_args: pytest.fail("canonical timestamp must not be reparsed"))
    item = StockQuoteItem(code="600000", trade_time="2026-08-21 09:31:00", freq="1m")

    assert stocks._quote_time_key(item) == "2026-08-21 09:31:00"


def test_daily_snapshot_accepts_small_gap_after_large_market_coverage(monkeypatch) -> None:
    active = pd.DataFrame([{"code": f"{index:06d}"} for index in range(100)])
    local_items = [
        StockQuoteItem(
            code=f"{index:06d}",
            trade_time="2026-08-21",
            freq="1d",
            close=1.0,
            pre_close=1.0,
            pct_chg=0.0,
            amount=1.0,
        )
        for index in range(99)
    ]
    monkeypatch.setattr(stocks, "load_stock_active_codes_frame", lambda _trade_date: active)

    assert stocks._build_snapshot_requests("2026-08-21", local_items) == []


def test_futures_coverage_reads_maintained_summary_without_fact_aggregate(monkeypatch) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(futures, "ensure_future_schema", lambda: None)

    def fake_query(query: str, params: tuple[object, ...]) -> pd.DataFrame:
        captured["query"] = query
        captured["params"] = params
        return pd.DataFrame()

    monkeypatch.setattr(futures, "query_dataframe", fake_query)

    futures.QuoteMuxFutures().list_coverage("main_continuous")

    query = str(captured["query"]).lower()
    assert "from fact.future_bar_1m_coverage" in query
    assert "from fact.future_bar_1m\n" not in query
    assert "group by" not in query
    assert captured["params"] == ("main_continuous", "main_continuous")


def test_futures_coverage_schema_has_one_time_backfill_and_write_maintenance() -> None:
    schema = "\n".join(futures.FUTURE_SCHEMA_SQL).lower()

    assert "create table if not exists fact.future_bar_1m_coverage" in schema
    assert "where not exists (select 1 from fact.future_bar_1m_coverage)" in schema
    assert "group by bars.product_code, bars.exchange, bars.series_type" in schema
    assert "after insert on fact.future_bar_1m" in schema
    assert "referencing new table as inserted_rows" in schema
    assert "for each statement execute function fact.maintain_future_bar_1m_coverage_after_insert" in schema
    assert "after delete on fact.future_bar_1m" in schema
    assert "referencing old table as deleted_rows" in schema
    assert "after update on fact.future_bar_1m" in schema
    assert "referencing old table as updated_old_rows new table as updated_new_rows" in schema
    assert "for each statement execute function fact.maintain_future_bar_1m_coverage_after_update" in schema
    assert "for each row execute function fact.maintain_future_bar_1m_coverage" not in schema
    assert "except" in schema


def test_future_schema_initializes_once_across_concurrent_callers(monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(futures, "_FUTURE_SCHEMA_READY", False)

    def fake_execute(statement: str) -> bool:
        time.sleep(0.001)
        calls.append(statement)
        return True

    monkeypatch.setattr(futures, "execute_sql", fake_execute)

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(lambda _index: futures.ensure_future_schema(), range(16)))

    assert calls == list(futures.FUTURE_SCHEMA_SQL)
    assert futures._FUTURE_SCHEMA_READY is True


def test_future_schema_failure_remains_retryable(monkeypatch) -> None:
    calls: list[str] = []
    should_fail = True
    monkeypatch.setattr(futures, "_FUTURE_SCHEMA_READY", False)

    def fake_execute(statement: str) -> bool:
        nonlocal should_fail
        calls.append(statement)
        if should_fail:
            should_fail = False
            return False
        return True

    monkeypatch.setattr(futures, "execute_sql", fake_execute)

    with pytest.raises(RuntimeError):
        futures.ensure_future_schema()
    assert futures._FUTURE_SCHEMA_READY is False

    futures.ensure_future_schema()

    assert calls == [futures.FUTURE_SCHEMA_SQL[0], *futures.FUTURE_SCHEMA_SQL]
    assert futures._FUTURE_SCHEMA_READY is True
