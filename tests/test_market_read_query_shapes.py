from __future__ import annotations

import pandas as pd

from quotemux import futures
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


def test_stock_1m_batch_query_uses_per_code_lateral_time_ranges(monkeypatch) -> None:
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
    assert "unnest(%s::character(6)[])" in query
    assert "cross join lateral" in query
    assert "bars.code = requested.code" in query
    assert "bars.bar_time >= %s::timestamp" in query
    assert "bars.bar_time <= %s::timestamp" in query
    assert "order by bars.bar_time" in query
    assert captured["params"] == (["000001", "600001"], "2026-08-21 09:30:00", "2026-08-21 15:00:00")


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
