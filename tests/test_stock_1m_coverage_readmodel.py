from __future__ import annotations

from platform_models import StockQuoteItem

from quotemux import fact_ref_writes
from quotemux.common import EXPECTED_INTRADAY_BAR_TIMES


def _day_items(code: str, trade_date: str) -> list[StockQuoteItem]:
    return [
        StockQuoteItem(
            code=code,
            trade_time=f"{trade_date} {bar_time}",
            freq="1m",
            open=10.0,
            high=10.0,
            low=10.0,
            close=10.0,
            volume=1.0,
            amount=10.0,
        )
        for bar_time in EXPECTED_INTRADAY_BAR_TIMES["1m"]
    ]


def test_stock_1m_batch_writer_refreshes_only_distinct_affected_daily_groups(monkeypatch) -> None:
    coverage_calls: list[tuple[str, tuple[object, ...]]] = []
    audit_calls: list[tuple[str, list[tuple[object, ...]]]] = []
    monkeypatch.setattr(fact_ref_writes, "execute_many_with_migration_journal", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(fact_ref_writes, "execute_sql", lambda query, params=(): coverage_calls.append((query, params)) or True)
    monkeypatch.setattr(fact_ref_writes, "execute_many", lambda query, params: audit_calls.append((query, params)) or True)
    items = [
        *_day_items("600000", "2026-08-20"),
        *_day_items("600000", "2026-08-21"),
        *_day_items("600000", "2026-08-21"),
    ]

    assert fact_ref_writes._upsert_stock_intraday(items) is True

    assert len(coverage_calls) == 1
    query, params = coverage_calls[0]
    normalized = query.lower()
    assert "insert into readmodel.stock_bar_1m_daily_coverage" in normalized
    assert "unnest(%s::text[], %s::character(6)[], %s::date[])" in normalized
    assert "join affected" in normalized
    assert "bars.bar_time >= affected.trade_date::timestamp" in normalized
    assert "bars.bar_time < affected.trade_date::timestamp + interval '1 day'" in normalized
    assert "group by bars.market, bars.code, affected.trade_date" in normalized
    assert "on conflict (market, code, trade_date) do update" in normalized
    assert params == (["SHSE", "SHSE"], ["600000", "600000"], ["2026-08-20", "2026-08-21"])
    assert len(audit_calls) == 1


def test_stock_1m_coverage_is_not_refreshed_when_fact_batch_fails(monkeypatch) -> None:
    monkeypatch.setattr(fact_ref_writes, "execute_many_with_migration_journal", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(fact_ref_writes, "execute_sql", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("coverage must not write")))

    assert fact_ref_writes._upsert_stock_intraday(_day_items("600000", "2026-08-21")) is False
