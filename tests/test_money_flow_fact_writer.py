from __future__ import annotations

from platform_models import StockMoneyFlowItem, StockQuoteItem
from quotemux.fact_ref_writes import EXPECTED_INTRADAY_BAR_TIMES, _upsert_stock_intraday, _upsert_stock_money_flow_snapshot


def test_money_flow_snapshot_writer_preserves_existing_non_null_facts(monkeypatch) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr("quotemux.fact_ref_writes._table_exists", lambda schema, table: True)

    def capture(query: str, params: list[tuple[object, ...]]) -> bool:
        captured["query"] = query
        captured["params"] = params
        return True

    monkeypatch.setattr("quotemux.fact_ref_writes.execute_many", capture)
    item = StockMoneyFlowItem(
        code="600000",
        trade_date="2026-07-21",
        view="main",
        main_inflow=10.0,
        main_outflow=7.0,
        net_inflow=3.0,
        active_buy_amount=15.0,
    )

    assert _upsert_stock_money_flow_snapshot([item]) is True
    assert "coalesce(fact.stock_money_flow_daily.main_inflow, excluded.main_inflow)" in str(captured["query"])
    assert captured["params"] == [("SHSE", "600000", "2026-07-21", 10.0, 7.0, 3.0, "tushare", 15.0)]


def _minute_items(*, null_amount_at: str | None = None) -> list[StockQuoteItem]:
    return [
        StockQuoteItem(
            code="600000",
            trade_time=f"2026-08-14 {bar_time}",
            freq="1m",
            open=10.0,
            high=10.1,
            low=9.9,
            close=10.0,
            volume=100,
            amount=None if bar_time == null_amount_at else 1000.0,
            adjust="none",
        )
        for bar_time in EXPECTED_INTRADAY_BAR_TIMES["1m"]
    ]


def test_intraday_writer_records_complete_amount_backed_day(monkeypatch) -> None:
    calls: list[tuple[str, list[tuple[object, ...]]]] = []

    def capture(query: str, params: list[tuple[object, ...]], **_kwargs: object) -> bool:
        calls.append((query, params))
        return True

    monkeypatch.setattr("quotemux.fact_ref_writes.execute_many", capture)
    monkeypatch.setattr("quotemux.fact_ref_writes.execute_many_with_migration_journal", capture)
    monkeypatch.setattr("quotemux.fact_ref_writes.execute_sql", lambda query, params: True)

    assert _upsert_stock_intraday(_minute_items()) is True
    assert len(calls[0][1]) == 240
    assert calls[2][1] == [
        (
            "quotemux.fact_ref_writer.complete_standard_grid",
            "2026-08-14 09:31:00",
            "2026-08-14 15:00:00",
            240,
        )
    ]


def test_intraday_writer_rejects_day_with_any_null_amount(monkeypatch) -> None:
    calls: list[tuple[str, list[tuple[object, ...]]]] = []

    def capture(query: str, params: list[tuple[object, ...]], **_kwargs: object) -> bool:
        calls.append((query, params))
        return True

    monkeypatch.setattr("quotemux.fact_ref_writes.execute_many", capture)
    monkeypatch.setattr("quotemux.fact_ref_writes.execute_many_with_migration_journal", capture)
    monkeypatch.setattr("quotemux.fact_ref_writes.execute_sql", lambda query, params: True)

    assert _upsert_stock_intraday(_minute_items(null_amount_at="14:59:00")) is True
    assert calls[0][1] == []
    assert len(calls) == 2
