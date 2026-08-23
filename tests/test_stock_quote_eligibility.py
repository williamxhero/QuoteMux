from __future__ import annotations

import pandas as pd

from platform_models import StockQuoteItem
from quotemux.settings import QuoteMuxSettings
from quotemux.stocks import _build_missing_quote_requests, _build_stock_quotes_query_result


def _without_suspensions(monkeypatch) -> None:
    monkeypatch.setattr(
        "quotemux.stocks.load_stock_suspension_history_frame",
        lambda *args: pd.DataFrame(),
    )


def test_pre_listing_day_is_complete_without_a_quote_or_provider_fetch(monkeypatch) -> None:
    _without_suspensions(monkeypatch)
    monkeypatch.setattr("quotemux.stocks._expected_trade_dates", lambda *args: ["2025-09-03"])
    monkeypatch.setattr(
        "quotemux.stocks.load_stock_catalog_frame",
        lambda *args: pd.DataFrame.from_records(
            [{"code": "001233", "listed_date": "2025-11-25", "delisted_date": ""}]
        ),
    )

    requests = _build_missing_quote_requests(
        ["001233"], [], "1d", "", "2025-09-03", "2025-09-03", "", "", None, QuoteMuxSettings()
    )
    result = _build_stock_quotes_query_result(
        ["001233"], [], [], [], "1d", None, ["2025-09-03"], None, set()
    )

    summary = result.meta.codes[0]
    assert requests == []
    assert result.items == []
    assert result.meta.complete is True
    assert summary.expected_bar_count == 0
    assert summary.actual_bar_count == 0
    assert summary.complete is True
    assert summary.missing_trade_dates == []


def test_listed_trading_day_counts_daily_coverage(monkeypatch) -> None:
    _without_suspensions(monkeypatch)
    monkeypatch.setattr(
        "quotemux.stocks.load_stock_catalog_frame",
        lambda *args: pd.DataFrame.from_records(
            [{"code": "001233", "listed_date": "2025-11-25", "delisted_date": ""}]
        ),
    )
    item = StockQuoteItem(code="001233", trade_time="2025-11-25", freq="1d", close=83.52)

    result = _build_stock_quotes_query_result(
        ["001233"], [item], [item], [item], "1d", None, ["2025-11-25"], None, set()
    )

    summary = result.meta.codes[0]
    assert summary.expected_bar_count == 1
    assert summary.actual_bar_count == 1
    assert summary.complete is True


def test_suspended_days_are_not_expected_or_requested(monkeypatch) -> None:
    monkeypatch.setattr("quotemux.stocks._expected_trade_dates", lambda *args: ["2024-03-05", "2024-03-06"])
    monkeypatch.setattr(
        "quotemux.stocks.load_stock_catalog_frame",
        lambda *args: pd.DataFrame.from_records(
            [{"code": "000005", "listed_date": "1990-12-10", "delisted_date": "2024-04-26"}]
        ),
    )
    monkeypatch.setattr(
        "quotemux.stocks.load_stock_suspension_history_frame",
        lambda *args: pd.DataFrame.from_records(
            [{"code": "000005", "suspend_start_date": "2024-03-06", "suspend_end_date": "2024-04-25"}]
        ),
    )
    active_item = StockQuoteItem(code="000005", trade_time="2024-03-05", freq="1d", close=0.83)

    requests = _build_missing_quote_requests(
        ["000005"], [active_item], "1d", "", "2024-03-05", "2024-03-06", "", "", None, QuoteMuxSettings()
    )
    result = _build_stock_quotes_query_result(
        ["000005"], [active_item], [active_item], [active_item], "1d", None, ["2024-03-05", "2024-03-06"], None, set()
    )

    summary = result.meta.codes[0]
    assert requests == []
    assert summary.expected_bar_count == 1
    assert summary.actual_bar_count == 1
    assert summary.missing_trade_dates == []
    assert summary.complete is True


def test_delisting_date_is_outside_quote_eligibility(monkeypatch) -> None:
    _without_suspensions(monkeypatch)
    monkeypatch.setattr(
        "quotemux.stocks.load_stock_catalog_frame",
        lambda *args: pd.DataFrame.from_records(
            [{"code": "000584", "listed_date": "1995-11-28", "delisted_date": "2025-07-11"}]
        ),
    )
    last_item = StockQuoteItem(code="000584", trade_time="2025-07-10", freq="1d", close=0.27)

    result = _build_stock_quotes_query_result(
        ["000584"], [last_item], [last_item], [last_item], "1d", None, ["2025-07-10", "2025-07-11"], None, set()
    )

    summary = result.meta.codes[0]
    assert summary.expected_bar_count == 1
    assert summary.actual_bar_count == 1
    assert summary.missing_trade_dates == []
    assert summary.complete is True


def test_suspended_coverage_row_is_not_counted_as_an_expected_bar(monkeypatch) -> None:
    monkeypatch.setattr(
        "quotemux.stocks.load_stock_catalog_frame",
        lambda *args: pd.DataFrame.from_records(
            [{"code": "000046", "listed_date": "1994-09-12", "delisted_date": "2024-02-07"}]
        ),
    )
    monkeypatch.setattr(
        "quotemux.stocks.load_stock_suspension_history_frame",
        lambda *args: pd.DataFrame.from_records(
            [{"code": "000046", "suspend_start_date": "2024-01-02", "suspend_end_date": "2024-02-06"}]
        ),
    )
    suspended_item = StockQuoteItem(
        code="000046", trade_time="2024-01-02", freq="1d", close=0.38, is_suspended=True
    )

    result = _build_stock_quotes_query_result(
        ["000046"], [], [], [suspended_item], "1d", None, ["2024-01-02"], None, set()
    )

    summary = result.meta.codes[0]
    assert summary.expected_bar_count == 0
    assert summary.actual_bar_count == 0
    assert summary.missing_count == 0
    assert summary.complete is True
