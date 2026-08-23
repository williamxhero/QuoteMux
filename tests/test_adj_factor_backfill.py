from __future__ import annotations

import pandas as pd

from quotemux import QuoteMux
from platform_models import AdjFactorItem
from quotemux.fact_ref_writes import _upsert_stock_adj_factors
from quotemux.local_daily import _apply_daily_adjustment, get_stock_codes_missing_adj_factors


def test_missing_adj_factor_codes_include_partially_covered_active_windows(monkeypatch) -> None:
    frame = pd.DataFrame(
        [
            {"code": "000001", "trade_time": "2024-06-03", "adj_factor": 2.0, "is_suspended": False},
            {"code": "000001", "trade_time": "2024-06-04", "adj_factor": None, "is_suspended": False},
            {"code": "000002", "trade_time": "2024-06-03", "adj_factor": None, "is_suspended": False},
            {"code": "000003", "trade_time": "2024-06-03", "adj_factor": 0.0, "is_suspended": False},
            {"code": "000004", "trade_time": "2024-06-03", "adj_factor": 3.0, "is_suspended": False},
            {"code": "000004", "trade_time": "2024-06-04", "adj_factor": None, "is_suspended": True},
        ]
    )
    monkeypatch.setattr("quotemux.local_daily.load_stock_daily_frame", lambda codes, start_date, end_date: frame)

    result = get_stock_codes_missing_adj_factors(
        ["000001", "000002", "000003", "000004"], "2024-06-01", "2024-06-30"
    )

    assert result == ["000001", "000002", "000003"]


def test_adj_factor_writer_only_updates_null_fact_values(monkeypatch) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        "quotemux.fact_ref_writes._existing_columns",
        lambda schema, table: {"adj_factor"},
    )

    def capture_execute_many(query: str, params: list[tuple[object, ...]]) -> bool:
        captured["query"] = query
        captured["params"] = params
        return True

    monkeypatch.setattr("quotemux.fact_ref_writes.execute_many", capture_execute_many)

    result = _upsert_stock_adj_factors(
        [
            AdjFactorItem(code="000971", trade_date="20240603", adj_factor=1.25),
            AdjFactorItem(code="000982", trade_date="20240604", adj_factor=None),
        ]
    )

    assert result is True
    assert "daily_rows.adj_factor is null" in str(captured["query"])
    assert captured["params"] == [("SZSE", "000971", "2024-06-03", 1.25)]


def test_qfq_does_not_manufacture_rows_without_any_factor() -> None:
    frame = pd.DataFrame(
        [
            {
                "code": "000971",
                "trade_time": "2024-06-03",
                "open": 4.0,
                "high": 4.2,
                "low": 3.9,
                "close": 4.1,
                "adj_factor": None,
            }
        ]
    )

    assert _apply_daily_adjustment(frame, "qfq", {}).empty


def test_qfq_uses_provider_factor_after_backfill() -> None:
    frame = pd.DataFrame(
        [
            {
                "code": "000971",
                "trade_time": "2024-06-03",
                "open": 4.0,
                "high": 4.2,
                "low": 3.9,
                "close": 4.1,
                "adj_factor": 1.25,
            },
            {
                "code": "000971",
                "trade_time": "2024-06-04",
                "open": 4.1,
                "high": 4.3,
                "low": 4.0,
                "close": 4.2,
                "adj_factor": 1.25,
            },
        ]
    )

    result = _apply_daily_adjustment(frame, "qfq", {"000971": 1.25})

    assert list(result["trade_time"].dt.strftime("%Y-%m-%d")) == ["2024-06-03", "2024-06-04"]
    assert list(result["close"]) == [4.1, 4.2]
