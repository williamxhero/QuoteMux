from __future__ import annotations

import json
from pathlib import Path
import sqlite3

import pandas as pd
import pytest

from quotemux.capabilities import get_capability_definition
from quotemux.models import StockStrategyFactorItem


def _eligible_identity_keys(
    identity_join_sql: str,
    *,
    codes: str,
    start_date: str,
    end_date: str,
    stock_rows: list[tuple[str, str, str, str | None]],
    daily_rows: list[tuple[str, str, str]],
) -> list[tuple[str, str]]:
    requested_codes = [item for item in codes.split(",") if item]
    placeholders = ", ".join("?" for _ in requested_codes) or "null"
    query = f"""
        select day_rows.code, day_rows.trade_date
        from fact.stock_daily_1d day_rows
        {identity_join_sql}
        where day_rows.trade_date between ? and ?
          and (? = '' or day_rows.code in ({placeholders}))
        order by day_rows.trade_date, day_rows.code
    """
    connection = sqlite3.connect(":memory:")
    try:
        connection.execute("attach database ':memory:' as fact")
        connection.execute("attach database ':memory:' as ref")
        connection.execute("create table fact.stock_daily_1d (market text, code text, trade_date text)")
        connection.execute("create table ref.stock (market text, code text, listed_date text, delisted_date text)")
        connection.executemany("insert into fact.stock_daily_1d values (?, ?, ?)", daily_rows)
        connection.executemany("insert into ref.stock values (?, ?, ?, ?)", stock_rows)
        return connection.execute(
            query,
            (start_date, end_date, codes, *requested_codes),
        ).fetchall()
    finally:
        connection.close()


@pytest.mark.parametrize(
    ("legacy_code", "canonical_code", "effective_date", "end_date"),
    [
        ("837403", "920403", "2024-01-18", "2024-01-19"),
        ("873690", "920690", "2024-01-05", "2024-01-19"),
        ("873806", "920806", "2024-01-11", "2024-01-19"),
    ],
)
def test_strategy_factor_identity_excludes_legacy_rows_from_the_mapping_effective_date(
    legacy_code: str,
    canonical_code: str,
    effective_date: str,
    end_date: str,
) -> None:
    from quotemux_packages.derived_core.source import _STRATEGY_FACTOR_IDENTITY_JOIN_SQL

    daily_rows = [
        ("BJSE", code, trade_date.strftime("%Y-%m-%d"))
        for trade_date in pd.date_range(effective_date, end_date, freq="B")
        for code in (legacy_code, canonical_code)
    ]
    stock_rows = [
        ("BJSE", legacy_code, effective_date, effective_date),
        ("BJSE", canonical_code, effective_date, None),
    ]

    legacy_keys = _eligible_identity_keys(
        _STRATEGY_FACTOR_IDENTITY_JOIN_SQL,
        codes=legacy_code,
        start_date=effective_date,
        end_date=end_date,
        stock_rows=stock_rows,
        daily_rows=daily_rows,
    )
    canonical_keys = _eligible_identity_keys(
        _STRATEGY_FACTOR_IDENTITY_JOIN_SQL,
        codes=canonical_code,
        start_date=effective_date,
        end_date=end_date,
        stock_rows=stock_rows,
        daily_rows=daily_rows,
    )
    mixed_keys = _eligible_identity_keys(
        _STRATEGY_FACTOR_IDENTITY_JOIN_SQL,
        codes=f"{legacy_code},{canonical_code}",
        start_date=effective_date,
        end_date=end_date,
        stock_rows=stock_rows,
        daily_rows=daily_rows,
    )

    assert legacy_keys == []
    assert canonical_keys != []
    assert mixed_keys == canonical_keys
    assert len(mixed_keys) == len(set(mixed_keys))


def test_strategy_factor_identity_preserves_valid_legacy_history_before_delisting() -> None:
    from quotemux_packages.derived_core.source import _STRATEGY_FACTOR_IDENTITY_JOIN_SQL

    keys = _eligible_identity_keys(
        _STRATEGY_FACTOR_IDENTITY_JOIN_SQL,
        codes="430489",
        start_date="2020-07-24",
        end_date="2020-07-27",
        stock_rows=[("BJSE", "430489", "2015-01-01", "2020-07-27")],
        daily_rows=[
            ("BJSE", "430489", "2020-07-24"),
            ("BJSE", "430489", "2020-07-27"),
        ],
    )

    assert keys == [("430489", "2020-07-24")]


def test_strategy_factor_model_keeps_the_stable_contract() -> None:
    item = StockStrategyFactorItem(
        trade_date="2025-01-02",
        code="600000",
        listing_board="main_board",
        free_float_shares=70000.0,
        active_buy_amount=330000.0,
        active_buy_amount_proportion_all=0.001,
        volume_shares=68538701.0,
        mean_volume_5d_shares=60000000.0,
        mean_volume_5d_to_free_float_shares=0.005,
        ma40=9.0,
        price_band_state="inside_band",
    )

    assert item.model_dump()["free_float_shares"] == 70000.0
    assert item.model_dump()["active_buy_amount"] == 330000.0
    assert item.active_buy_amount_proportion_all == 0.001
    assert item.volume_shares == 68538701.0
    assert item.mean_volume_5d_to_free_float_shares == 0.005
    assert item.price_band_state == "inside_band"


def test_strategy_factor_capability_has_one_derived_source() -> None:
    definition = get_capability_definition("stocks.factors.strategy_window")
    manifest_path = Path(__file__).resolve().parents[2] / "QuoteMux_Packages" / "packages" / "derived_core" / "quotemux_package.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert definition.api_paths == ("/api/stocks/factors/strategy-window",)
    assert definition.allowed_packages == ("derived_core",)
    assert definition.default_source_order == ("derived_core",)
    assert manifest["handler_targets"]["get_strategy_factor_window"] == "quotemux_packages.derived_core.source:get_strategy_factor_window"
    tushare_manifest_path = Path(__file__).resolve().parents[2] / "QuoteMux_Packages" / "packages" / "tushare" / "quotemux_package.json"
    tushare_manifest = json.loads(tushare_manifest_path.read_text(encoding="utf-8"))
    assert tushare_manifest["handler_targets"]["get_stock_financial_pit_period"] == "quotemux_packages.tushare.source:get_stock_financial_pit_period"
    pit_definition = get_capability_definition("stocks.finance.pit.raw.period")
    assert pit_definition.allowed_packages == ("tushare",)


def test_strategy_factor_handler_uses_one_local_fact_query(monkeypatch) -> None:
    from quotemux.infra.db import client as db_client
    from quotemux_packages.derived_core import source

    calls: list[tuple[str, tuple[object, ...]]] = []

    def fake_query_dataframe(query: str, params: tuple[object, ...]) -> pd.DataFrame:
        calls.append((query, params))
        return pd.DataFrame(
            [
                {
                    "trade_date": "2025-01-02",
                    "code": "600000",
                    "listing_board": "main_board",
                    "close": 10.0,
                    "dividend_yield_pct": 3.2,
                    "float_market_cap": 120000.0,
                    "circulating_shares": 80000.0,
                    "free_float_shares": 70000.0,
                    "active_buy_amount": 330000.0,
                    "active_buy_amount_proportion_all": 0.000275,
                    "volume_shares": 68538701.0,
                    "mean_volume_5d_shares": 60000000.0,
                    "mean_volume_5d_to_free_float_shares": 0.00574,
                    "ma10": 9.8,
                    "ma20": 9.5,
                    "ma40": 9.0,
                    "is_st": False,
                    "is_suspended": False,
                    "upper_limit": 11.0,
                    "lower_limit": 9.0,
                    "price_band_state": "inside_band",
                }
            ]
        )

    monkeypatch.setattr(db_client, "query_dataframe", fake_query_dataframe)
    items = source.get_strategy_factor_window("2025-01-02", "2025-01-03")

    assert len(calls) == 1
    assert calls[0][1] == ("2025-01-02", "2025-01-03", "", [], "2025-01-02", "2025-01-02", "2025-01-03")
    assert "limit 39" in calls[0][0].lower()
    assert items[0].ma40 == 9.0
    assert items[0].active_buy_amount_proportion_all == 0.000275


def test_strategy_factor_handler_rejects_an_inverted_range() -> None:
    from quotemux_packages.derived_core import source

    with pytest.raises(ValueError, match="start_date"):
        source.get_strategy_factor_window("2025-01-03", "2025-01-02")


def test_strategy_factor_window_does_not_hide_provider_errors(monkeypatch) -> None:
    from quotemux.runtime_core.executor import FallbackReport, ProviderMergeStats
    from quotemux.settings import QuoteMuxSettings
    from quotemux.stocks import QuoteMuxStocks

    report = FallbackReport(
        contract_name="stocks.factors.strategy_window",
        profile_id="profile",
        profile_version="v1",
        steps=(
            ProviderMergeStats(
                name="derived_core",
                package_id="derived_core",
                source_instance_id="derived_core-default",
                handler="get_strategy_factor_window",
                request_count=1,
                fetched_row_count=0,
                added_count=0,
                filled_field_count=0,
                conflict_count=0,
                skipped_count=0,
                error_count=1,
                elapsed_ms=1.0,
            ),
        ),
    )
    monkeypatch.setattr(
        "quotemux.stocks.run_fallback_chain_with_report", lambda *args, **kwargs: ([], report)
    )
    monkeypatch.setattr(
        "quotemux.stocks.SourceInstanceExecutor.build_steps", lambda *args, **kwargs: ()
    )

    with pytest.raises(RuntimeError, match="provider 执行失败"):
        QuoteMuxStocks(QuoteMuxSettings()).get_strategy_factor_window(
            "2025-01-02", "2025-01-02"
        )


def test_strategy_factor_handler_converts_nan_to_null(monkeypatch) -> None:
    from quotemux.infra.db import client as db_client
    from quotemux_packages.derived_core import source

    monkeypatch.setattr(
        db_client,
        "query_dataframe",
        lambda query, params: pd.DataFrame(
            [
                {
                    "trade_date": "2025-01-02",
                    "code": "600000",
                    "listing_board": "main_board",
                    "close": float("nan"),
                    "is_st": False,
                    "is_suspended": False,
                    "price_band_state": "",
                }
            ]
        ),
    )

    items = source.get_strategy_factor_window("2025-01-02", "2025-01-02")

    assert items[0].close is None


def test_strategy_factor_handler_normalizes_financial_date_strings(monkeypatch) -> None:
    from datetime import date

    from quotemux.infra.db import client as db_client
    from quotemux_packages.derived_core import source

    monkeypatch.setattr(
        db_client,
        "query_dataframe",
        lambda query, params: pd.DataFrame(
            [
                {
                    "trade_date": "2025-01-02",
                    "code": "600000",
                    "listing_board": "main_board",
                    "financial_formula_version": "market-hub-v1",
                    "financial_announcement_date": date(2025, 1, 30),
                    "financial_report_period": date(2024, 12, 31),
                    "is_st": False,
                    "is_suspended": False,
                    "price_band_state": "",
                }
            ]
        ),
    )

    items = source.get_strategy_factor_window("2025-01-02", "2025-01-02")

    assert items[0].financial_formula_version == "market-hub-v1"
    assert items[0].financial_announcement_date == "2025-01-30"
    assert items[0].financial_report_period == "2024-12-31"


def test_tushare_money_flow_derives_active_buy_amount(monkeypatch) -> None:
    from quotemux_packages.tushare import source

    class Pro:
        moneyflow = object()

    monkeypatch.setattr(source, "get_ts_pro", lambda: Pro())
    monkeypatch.setattr(
        source,
        "call_tushare_api",
        lambda *args, **kwargs: pd.DataFrame(
            [
                {
                    "trade_date": "20250102",
                    "buy_sm_amount": 1.0,
                    "buy_md_amount": 2.0,
                    "buy_lg_amount": 10.0,
                    "buy_elg_amount": 20.0,
                    "sell_lg_amount": 5.0,
                    "sell_elg_amount": 7.0,
                    "net_mf_amount": 18.0,
                }
            ]
        ),
    )

    frame = source._fetch_money_flow_frame("600000", "20250102", "20250102", "main")

    assert frame["active_buy_amount"].iloc[0] == pytest.approx(330000.0)


def test_tushare_daily_basic_keeps_free_share(monkeypatch) -> None:
    from quotemux_packages.tushare import stocks

    monkeypatch.setattr(
        stocks,
        "_build_daily_market_frames",
        lambda trade_date: pd.DataFrame(
            [
                {
                    "code": "600000",
                    "trade_date": trade_date,
                    "turnover_rate": 1.0,
                    "volume_ratio": 1.2,
                    "pe": 8.0,
                    "pb": 1.0,
                    "total_share": 100000.0,
                    "float_share": 80000.0,
                    "free_share": 70000.0,
                }
            ]
        ),
    )

    items = stocks.get_stock_daily_basic("", "", "2025-01-02", "", "")

    assert items[0].free_share == 70000.0


def test_tushare_daily_market_cache_refetches_missing_strategy_columns(monkeypatch) -> None:
    from quotemux_packages.tushare import stocks

    cached = pd.DataFrame(
        [
            {
                "code": "600000",
                "trade_date": "2025-01-02",
                "float_share": None,
                "free_share": None,
                "circ_mv": None,
            }
        ]
    )
    fetched = pd.DataFrame(
        [
            {
                "code": "600000",
                "trade_date": "2025-01-02",
                "turnover_rate": 1.0,
                "volume_ratio": 1.2,
                "pe": 8.0,
                "pb": 1.0,
                "ps": 1.0,
                "pcf": 1.0,
                "dv_ratio": 3.0,
                "dv_ttm": 2.5,
                "total_share": 100000.0,
                "float_share": 80000.0,
                "free_share": 70000.0,
                "total_mv": 1000000.0,
                "circ_mv": 800000.0,
            }
        ]
    )
    writes: list[pd.DataFrame] = []

    monkeypatch.setattr(stocks, "build_cache_path", lambda *args, **kwargs: "cache.parquet")
    monkeypatch.setattr(stocks, "read_cache_frame", lambda path: cached)
    monkeypatch.setattr(stocks, "_fetch_daily_basic_market_frame", lambda start, end: fetched)
    monkeypatch.setattr(stocks, "write_cache_frame", lambda path, frame: writes.append(frame))

    frame = stocks._build_daily_market_frames("2025-01-02")

    assert frame.iloc[0]["free_share"] == 70000.0
    assert frame.iloc[0]["dv_ttm"] == 2.5
    assert len(writes) == 1


def test_tushare_money_flow_snapshot_refetches_null_active_buy_cache(monkeypatch) -> None:
    from quotemux_packages.tushare import source

    cached = pd.DataFrame(
        [
            {
                "code": "600000",
                "trade_date": "2025-01-02",
                "view": "main",
                "main_inflow": 1.0,
                "main_outflow": 1.0,
                "net_inflow": 0.0,
                "active_buy_amount": None,
            }
        ]
    )
    fetched = cached.copy()
    fetched.loc[0, "active_buy_amount"] = 330000.0
    writes: list[pd.DataFrame] = []

    monkeypatch.setattr(source, "build_cache_path", lambda *args, **kwargs: "cache.parquet")
    monkeypatch.setattr(source, "read_cache_frame", lambda path: cached)
    monkeypatch.setattr(source, "_fetch_money_flow_daily_frame", lambda day, view: fetched)
    monkeypatch.setattr(source, "write_cache_frame", lambda path, frame: writes.append(frame))

    items = source.get_stock_money_flow_snapshot("2025-01-02", "main")

    assert items[0].active_buy_amount == 330000.0
    assert len(writes) == 1


def test_tushare_daily_snapshot_converts_hands_to_shares(monkeypatch) -> None:
    from quotemux_packages.tushare import source

    class Pro:
        daily = object()

    monkeypatch.setattr(source, "get_ts_pro", lambda: Pro())
    monkeypatch.setattr(
        source,
        "call_tushare_api",
        lambda *args, **kwargs: pd.DataFrame(
            [
                {
                    "ts_code": "600000.SH",
                    "trade_date": "20250102",
                    "open": 10.0,
                    "high": 11.0,
                    "low": 9.0,
                    "close": 10.5,
                    "pre_close": 10.0,
                    "change": 0.5,
                    "pct_chg": 5.0,
                    "vol": 123.45,
                    "amount": 130.0,
                }
            ]
        ),
    )

    frame = source._fetch_stock_daily_snapshot_frame("2025-01-02")

    assert frame["volume2"].iloc[0] == pytest.approx(12345.0)
    assert frame["amount"].iloc[0] == pytest.approx(130000.0)


def test_tushare_financial_pit_keeps_announcement_and_revision_fields(monkeypatch) -> None:
    from quotemux_packages.tushare import stock_financial_pit

    indicator_frame = pd.DataFrame(
        [
            {
                "ts_code": "600000.SH",
                "ann_date": "20250430",
                "end_date": "20250331",
                "eps": 0.57,
                "gross_margin": 20.0,
                "q_dtprofit": 100.0,
                "ebit": 120.0,
                "update_flag": "1",
            }
        ]
    )
    income_frame = pd.DataFrame(
        [
            {
                "ts_code": "600000.SH",
                "ann_date": "20250430",
                "f_ann_date": "20250430",
                "end_date": "20250331",
                "report_type": "1",
                "comp_type": "2",
                "total_revenue": 1000.0,
                "ebit": 120.0,
                "n_income_attr_p": 100.0,
                "update_flag": "1",
            }
        ]
    )
    monkeypatch.setattr(
        stock_financial_pit,
        "_query_financial_frame",
        lambda api_name, fallback_api, **kwargs: indicator_frame if api_name == "fina_indicator_vip" else income_frame,
    )

    items = stock_financial_pit.get_stock_financial_pit_period("2025-03-31", strict=True)

    assert len(items) == 1
    assert items[0].actual_announcement_date == "2025-04-30"
    assert items[0].update_flag == "1"
    assert items[0].gross_profit_cumulative_cny == 200.0
    assert items[0].total_operating_revenue_cumulative_cny == 1000.0


def test_tushare_financial_pit_non_strict_missing_data_returns_empty(monkeypatch) -> None:
    from quotemux_packages.tushare import stock_financial_pit

    monkeypatch.setattr(stock_financial_pit, "query_frame", lambda api_name, **kwargs: pd.DataFrame())

    assert stock_financial_pit.get_stock_financial_pit_period("2021-03-31") == []


def test_tushare_financial_pit_falls_back_to_standard_api(monkeypatch) -> None:
    from quotemux_packages.tushare import stock_financial_pit

    calls: list[str] = []

    class Pro:
        def fina_indicator_vip(self, **kwargs):
            calls.append("fina_indicator_vip")
            raise PermissionError("vip unavailable")

        def fina_indicator(self, **kwargs):
            calls.append("fina_indicator")
            return pd.DataFrame([{"ts_code": "600000.SH"}])

    monkeypatch.setattr("quotemux_packages.tushare.source.get_ts_pro", lambda: Pro())
    monkeypatch.setattr(stock_financial_pit, "call_tushare_api", lambda api_name, fetcher, **kwargs: fetcher(**kwargs))

    frame = stock_financial_pit._query_financial_frame(
        "fina_indicator_vip",
        "fina_indicator",
        period="20250331",
        fields="ts_code",
    )

    assert calls == ["fina_indicator_vip", "fina_indicator"]
    assert frame.iloc[0]["ts_code"] == "600000.SH"


def test_tushare_financial_pit_reports_provider_errors(monkeypatch) -> None:
    from quotemux_packages.tushare import stock_financial_pit

    class Pro:
        def fina_indicator_vip(self, **kwargs):
            raise PermissionError("vip unavailable")

        def fina_indicator(self, **kwargs):
            raise RuntimeError("standard unavailable")

    monkeypatch.setattr("quotemux_packages.tushare.source.get_ts_pro", lambda: Pro())
    monkeypatch.setattr(stock_financial_pit, "call_tushare_api", lambda api_name, fetcher, **kwargs: fetcher(**kwargs))

    with pytest.raises(RuntimeError, match="fina_indicator_vip.*fina_indicator"):
        stock_financial_pit._query_financial_frame(
            "fina_indicator_vip",
            "fina_indicator",
            period="20250331",
            fields="ts_code",
        )


def test_financial_pit_requires_three_consecutive_quarters_for_minimum() -> None:
    from quotemux_packages.derived_core.financial_pit import _consecutive_three_quarter_minimum

    values = {
        "2024-03-31": 0.04,
        "2024-06-30": 0.05,
        "2024-09-30": 0.06,
        "2024-12-31": 0.07,
    }

    assert _consecutive_three_quarter_minimum(values, "2024-12-31") == 0.05
    assert _consecutive_three_quarter_minimum(values, "2025-03-31") is None
