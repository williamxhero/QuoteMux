from __future__ import annotations

from platform_models import StockRiskFlagItem

import quotemux.stocks as stocks_module
from quotemux.reports import ContractReport
from quotemux.settings import QuoteMuxSettings
from quotemux.stocks import QuoteMuxStocks


def _items(count: int) -> list[StockRiskFlagItem]:
    return [
        StockRiskFlagItem(
            code=f"{index:06d}",
            name=f"stock-{index}",
            flag_type="st",
            start_date="2026-01-02",
            end_date="",
            status="active",
        )
        for index in range(count)
    ]


def _get_page(monkeypatch, items: list[StockRiskFlagItem], report: ContractReport, limit: int, offset: int) -> list[StockRiskFlagItem]:
    monkeypatch.setattr(stocks_module, "execute_capability_query", lambda spec: (items, report))
    return QuoteMuxStocks(QuoteMuxSettings(enabled_sources=("tushare",))).get_risk_flags(
        "",
        "2026-01-02",
        "2026-01-02",
        "",
        "",
        limit,
        offset,
    )


def test_risk_flags_cache_hit_applies_limit_and_offset(monkeypatch) -> None:
    cached = _items(6)
    report = ContractReport("stocks.indicators.risk_flags").with_store_stats(hit=True)

    first_page = _get_page(monkeypatch, cached, report, limit=2, offset=0)
    second_page = _get_page(monkeypatch, cached, report, limit=2, offset=2)

    assert [item.code for item in first_page] == ["000000", "000001"]
    assert [item.code for item in second_page] == ["000002", "000003"]
    assert set(item.code for item in first_page).isdisjoint(item.code for item in second_page)
    assert len(first_page) <= 2
    assert len(second_page) <= 2


def test_risk_flags_cache_hit_returns_boundary_page_and_empty_past_end(monkeypatch) -> None:
    cached = _items(5)
    report = ContractReport("stocks.indicators.risk_flags").with_store_stats(partial_hit=True)

    boundary_page = _get_page(monkeypatch, cached, report, limit=2, offset=4)
    past_end_page = _get_page(monkeypatch, cached, report, limit=2, offset=5)

    assert [item.code for item in boundary_page] == ["000004"]
    assert past_end_page == []


def test_risk_flags_provider_page_is_not_offset_twice(monkeypatch) -> None:
    provider_page = _items(2)
    report = ContractReport("stocks.indicators.risk_flags").with_store_stats(miss=True)

    result = _get_page(monkeypatch, provider_page, report, limit=2, offset=4)

    assert result == provider_page
    assert len(result) <= 2
