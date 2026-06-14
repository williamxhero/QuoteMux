from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from quotemux import IndexBar1dRequest, IndexMembersRequest, IndexQuotesRequest, QuoteMux, StockBar1mRequest, StockDailyOhlcvaRepairRequest, StockDailySnapshotRequest, StockQuotesRequest, TradingCalendarRequest
from quotemux.config_runtime.models import ContractPolicyOverride, RuntimeProfile, RuntimeSnapshot, SourceInstanceConfig
from quotemux.config_runtime.runtime import QuoteMuxConfigRuntime, reset_config_runtime_cache
from quotemux.config_runtime.store import RuntimeConfigStore
from quotemux.config_runtime.validation import ConfigValidationError, validate_instance, validate_manifests, validate_profile
from quotemux.contracts.policies import get_contract_policy, list_default_contract_policies
from quotemux.contracts.registry import get_contract_allowed_merge_strategies, get_contract_result_shape, list_contract_names
from quotemux.models import BlockTradeItem, BoardCatalogItem, BoardCategoryItem, BoardMemberItem, BoardMoneyFlowItem, BoardQuoteItem, ConnectCapitalFlowItem, DisclosureDateItem, DividendItem, DragonTigerInstitutionItem, DragonTigerItem, ExpressItem, ForecastItem, HKConnectHoldingItem, IndexMemberItem, IndexQuoteItem, MainBusinessItem, MarketCapitalFlowItem, PledgeDetailItem, PledgeStatItem, RepurchaseItem, ResearchReportItem, RightsIssueItem, ShareChangeItem, ShareholderCountItem, ShareholderTop10Item, StockFinanceIndicatorItem, StockFinancialStatementItem, StockMoneyFlowItem, StockProfileItem, StockQuoteItem, SurveyItem, TradingCalendarItem, UnlockScheduleItem
from quotemux.models import HLSignalItem, ShareholderChangeItem, TechnicalFactorItem
from quotemux.runtime_core.executor import ProviderStep, SourceInstanceExecutor, run_fallback_chain_with_report
from quotemux.settings import QuoteMuxSettings
from quotemux.stocks import _build_snapshot_requests, _build_stock_quotes_query_result, _fill_suspended_daily_gaps
from quotemux.source_packages.loader import load_builtin_manifests
from quotemux.source_packages.manifest import ConfigFieldSchema, SourcePackageCapability, SourcePackageManifest
from quotemux.source_packages.registry import build_source_package_registry


@dataclass(frozen=True)
class _FakeStoreRead:
    status: str = "miss"
    hit: bool = False
    partial_hit: bool = False


@dataclass(frozen=True)
class _FakeStoreWrite:
    status: str = "skip"


@pytest.fixture(autouse=True)
def isolate_runtime_root(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("QUOTEMUX_RUNTIME_ROOT", str(tmp_path / "runtime"))
    monkeypatch.setattr("quotemux.query_engine.load_store_result", lambda capability_id, request_identity, model_type: ([], _FakeStoreRead()))
    monkeypatch.setattr("quotemux.query_engine.store_result", lambda capability_id, request_identity, items, report, quarantine_count=0: _FakeStoreWrite())
    monkeypatch.setattr("quotemux.stocks.get_fact_ref_writer", lambda capability_id: None)
    monkeypatch.setattr("quotemux.indexes.get_fact_ref_writer", lambda capability_id: None)
    monkeypatch.setattr("quotemux.boards.get_fact_ref_writer", lambda capability_id: None)
    monkeypatch.setattr("quotemux.markets.get_fact_ref_writer", lambda capability_id: None)
    monkeypatch.setattr("quotemux.stocks.get_local_stock_quotes", lambda codes, freq, trade_date, start_date, end_date, start_time, end_time, count, adjust: [])
    monkeypatch.setattr("quotemux.stocks.get_local_stock_intraday_quotes", lambda codes, freq, trade_date, start_date, end_date, start_time, end_time, count: [])
    monkeypatch.setattr("quotemux.stocks.get_local_stock_daily_snapshot_full", lambda trade_date: [])
    monkeypatch.setattr("quotemux.stocks.load_stock_active_codes_frame", lambda trade_date: pd.DataFrame())
    monkeypatch.setattr("quotemux.indexes.get_local_index_quotes", lambda index_codes, freq, trade_date, start_date, end_date, count: [])
    monkeypatch.setattr("quotemux.indexes.get_local_index_catalog", lambda index_codes: [])
    monkeypatch.setattr("quotemux.indexes.get_local_index_profile", lambda index_code: [])
    monkeypatch.setattr("quotemux.boards.get_local_board_quotes", lambda board_codes, freq, trade_date, start_date, end_date, count: [])
    monkeypatch.setattr("quotemux.boards.get_local_board_catalog", lambda status: [])
    monkeypatch.setattr("quotemux.boards.get_local_board_profile", lambda board_code: [])
    monkeypatch.setattr("quotemux.boards.get_local_board_members", lambda board_code, trade_date: [])
    monkeypatch.setattr("quotemux.boards.get_local_board_member_history", lambda board_code: [])
    monkeypatch.setattr("quotemux.markets.get_local_trading_calendar", lambda exchange, start_date, end_date, is_open: [])
    reset_config_runtime_cache()
    QuoteMuxConfigRuntime().add_import_root(str(_package_source_root()))
    yield
    reset_config_runtime_cache()


def _package_source_root() -> Path:
    return Path(__file__).resolve().parents[2] / "QuoteMux_Packages"


def _source_call_stub(
    responses: dict[tuple[str, str], object],
    calls: list[tuple[str, str, tuple[object, ...]]] | None = None,
) -> Callable[..., object]:
    sequenced_responses = {key: list(value) for key, value in responses.items() if isinstance(value, tuple)}

    def fake_call(package_id: str, handler_name: str, *args: object) -> object:
        if calls is not None:
            calls.append((package_id, handler_name, args))
        key = (package_id, handler_name)
        if key in sequenced_responses:
            value = sequenced_responses[key].pop(0)
        else:
            value = responses.get(key, [])
        if isinstance(value, BaseException):
            raise value
        if callable(value):
            return value(*args)
        return value

    return fake_call


def _manifest_with_capabilities(
    package_id: str,
    capability_ids: tuple[str, ...],
    handler_targets: tuple[tuple[str, str], ...],
    version: str = "1.0.0",
    config_schema: tuple[ConfigFieldSchema, ...] = (),
    secret_fields: tuple[str, ...] = (),
) -> SourcePackageManifest:
    handler_name = handler_targets[0][0] if handler_targets else ""
    return SourcePackageManifest(
        package_id=package_id,
        version=version,
        source_name=package_id,
        display_name=package_id,
        description="",
        capabilities=tuple(
            SourcePackageCapability(capability_id=capability_id, support_level="native", handler_name=handler_name)
            for capability_id in capability_ids
        ),
        capability_tags=(),
        config_schema=config_schema,
        secret_fields=secret_fields,
        supports_multi_instance=True,
        handler_targets=handler_targets,
    )


def test_stocks_quotes_with_report_uses_capability_runtime_and_store_writeback() -> None:
    runtime = QuoteMux()
    fake_source_call = _source_call_stub(
        {
            ("efinance", "get_stock_quotes"): [
                StockQuoteItem(code="600000", trade_time="2026-04-03", freq="1d", open=10.0, high=11.0, low=9.5, close=10.5, volume=1000.0, amount=1000000.0, adjust="none")
            ],
        }
    )
    with (
        patch("quotemux.stocks._source_package_call", side_effect=fake_source_call),
        patch("quotemux.stocks._expected_trade_dates", return_value=["2026-04-03"]),
    ):
        items, report = runtime.stocks.get_quotes_with_report(
            StockQuotesRequest(codes=["600000"], freq="1d", start_date="2026-04-03", end_date="2026-04-03")
        )

    assert len(items) == 1
    assert items[0].code == "600000"
    assert report.profile_id == "profile-default"
    assert report.profile_version == "v1"
    assert report.contract_name == "stocks.quotes.daily"
    assert report.capability_id == "stocks.quotes.daily"
    assert report.source_hit_counts["efinance"] == 1
    assert report.store_miss_count == 1
    assert report.store_write_count in {0, 1}
    report_payload = report.to_dict()
    instance_reports = report_payload["source_instance_reports"]
    assert any(item["package_id"] == "efinance" and item["source_instance_id"] == "efinance-default" and item["handler"] == "get_stock_quotes" for item in instance_reports)
    assert any(item["package_id"] == "efinance" and item["request_count"] == 1 for item in report_payload["package_reports"])


def test_daily_snapshot_with_report_fills_missing_codes_from_b3() -> None:
    runtime = QuoteMux()
    fake_source_call = _source_call_stub(
        {
            ("efinance", "get_stock_daily_snapshot_full"): [
                StockQuoteItem(code="600000", trade_time="2026-04-03", freq="1d", open=10.0, high=11.0, low=9.5, close=10.5, volume=1000.0, amount=1000000.0, adjust="none")
            ],
        }
    )
    with (
        patch("quotemux.stocks._source_package_call", side_effect=fake_source_call),
        patch("quotemux.stocks._expected_trade_dates", return_value=["2026-04-03"]),
    ):
        items, report = runtime.stocks.get_daily_snapshot_with_report(StockDailySnapshotRequest(trade_date="2026-04-03"))

    assert len(items) == 1
    assert report.source_hit_counts["efinance"] == 1


def test_daily_snapshot_requests_partial_missing_codes(monkeypatch) -> None:
    active_frame = pd.DataFrame([{"code": "600000"}, {"code": "000001"}])
    local_items = [StockQuoteItem(code="600000", trade_time="2026-04-03", freq="1d", close=10.5, adjust="none")]

    monkeypatch.setattr("quotemux.stocks.load_stock_active_codes_frame", lambda trade_date: active_frame)

    assert _build_snapshot_requests("2026-04-03", local_items) == [(["000001"], "2026-04-03")]


def test_index_quotes_with_report_uses_mootdx_when_efinance_empty() -> None:
    runtime = QuoteMux()
    fake_source_call = _source_call_stub(
        {
            ("mootdx", "get_index_quotes"): [
                IndexQuoteItem(index_code="SHSE.000001", trade_time="2026-04-03", freq="1d", open=3300.0, high=3310.0, low=3290.0, close=3305.0, amount=123000000.0)
            ],
        }
    )
    with (
        patch("quotemux.indexes._source_package_call", side_effect=fake_source_call),
        patch("quotemux.indexes._expected_trade_dates", return_value=["2026-04-03"]),
    ):
        items, report = runtime.indexes.get_quotes_with_report(
            IndexQuotesRequest(index_codes=["SHSE.000001"], start_date="2026-04-03", end_date="2026-04-03")
        )

    assert len(items) == 1
    assert report.source_hit_counts["mootdx"] == 1


def test_index_members_with_report_uses_name_map() -> None:
    runtime = QuoteMux()
    fake_source_call = _source_call_stub(
        {
            ("efinance", "get_index_members"): [IndexMemberItem(index_code="SHSE.000001", code="600000", name="浦发银行", weight=0.1, trade_date="2026-04-03")],
        }
    )
    with patch("quotemux.indexes._source_package_call", side_effect=fake_source_call):
        items, report = runtime.indexes.get_members_with_report(IndexMembersRequest(index_code="SHSE.000001", trade_date="2026-04-03"))

    assert items[0].name == "浦发银行"
    assert report.degraded is False


def test_boards_runtime_uses_akshare_source_package_capabilities() -> None:
    runtime = QuoteMux(QuoteMuxSettings(enabled_sources=("akshare",)))
    fake_source_call = _source_call_stub(
        {
            ("akshare", "get_board_catalog"): [BoardCatalogItem(board_code="BK0815", board_name="新能源车", category="concept")],
            ("akshare", "get_board_profile"): BoardCatalogItem(board_code="BK0815", board_name="新能源车", category="concept"),
            ("akshare", "get_board_members"): [BoardMemberItem(board_code="BK0815", code="600000", name="浦发银行")],
            ("akshare", "get_board_quotes"): [BoardQuoteItem(board_code="BK0815", trade_time="2026-04-03", freq="1d", close=1000.0)],
            ("akshare", "get_board_money_flow"): [BoardMoneyFlowItem(board_code="BK0815", trade_date="2026-04-03", scope="concept", net_inflow=1000000.0)],
            ("akshare", "get_board_daily_money_flow_snapshot"): [BoardMoneyFlowItem(board_code="BK0815", trade_date="2026-04-03", scope="concept", inflow=2000000.0, outflow=1000000.0, net_inflow=1000000.0)],
            ("akshare", "get_board_categories"): [BoardCategoryItem(category_code="concept", category_name="概念板块", level=1)],
        }
    )
    with patch("quotemux.boards._source_package_call", side_effect=fake_source_call):
        catalog = runtime.boards.get_catalog("concept", "a_share", "active", 10, 0)
        profile = runtime.boards.get_profile("BK0815")
        members = runtime.boards.get_members("BK0815", "2026-04-03")
        quotes = runtime.boards.get_quotes(["BK0815"], "1d", "", "2026-04-03", "2026-04-03", "", "", None, 10)
        flow = runtime.boards.get_money_flow("BK0815", "2026-04-03", "", "", "concept")
        snapshot = runtime.boards.get_market_money_flow("2026-04-03", "concept", 10, 0)
        categories = runtime.boards.get_categories("", 1)

    assert catalog[0].board_code == "BK0815"
    assert profile is not None and profile.board_name == "新能源车"
    assert members[0].code == "600000"
    assert quotes[0].close == 1000.0
    assert flow[0].net_inflow == 1000000.0
    assert snapshot[0].inflow == 2000000.0
    assert categories[0].category_code == "concept"


def test_markets_runtime_uses_akshare_wide_table_capabilities() -> None:
    runtime = QuoteMux(QuoteMuxSettings(enabled_sources=("akshare",)))
    fake_source_call = _source_call_stub(
        {
            ("akshare", "get_market_capital_flow"): [MarketCapitalFlowItem(trade_date="2026-04-03", market="all", net_inflow=1000000.0)],
            ("akshare", "get_connect_capital_flow"): [ConnectCapitalFlowItem(trade_date="2026-04-03", market="northbound", buy_amount=100.0, sell_amount=80.0, net_amount=20.0)],
            ("akshare", "get_block_trades"): [BlockTradeItem(trade_date="2026-04-03", code="600000", name="浦发银行", amount=1000000.0, buyer="买方", seller="卖方")],
            ("akshare", "get_dragon_tiger"): [DragonTigerItem(trade_date="2026-04-03", code="600000", name="浦发银行", reason="异常波动", net_amount=1000000.0)],
            ("akshare", "get_dragon_tiger_institutions"): [DragonTigerInstitutionItem(trade_date="2026-04-03", code="600000", name="浦发银行", institution_count=2, net_amount=1000000.0)],
        }
    )
    with patch("quotemux.markets._source_package_call", side_effect=fake_source_call):
        market_flow = runtime.markets.get_main_capital_flow("2026-04-03", "", "")
        connect_flow = runtime.markets.get_connect_capital_flow("2026-04-03", "", "")
        block_trades = runtime.markets.get_block_trades("2026-04-03", "", "", "600000", 10)
        dragon_tiger = runtime.markets.get_dragon_tiger("2026-04-03", "", "", "600000", 10)
        institutions = runtime.markets.get_dragon_tiger_institutions("2026-04-03", "", "", "600000", 10)

    assert market_flow[0].net_inflow == 1000000.0
    assert connect_flow[0].market == "northbound"
    assert block_trades[0].buyer == "买方"
    assert dragon_tiger[0].reason == "异常波动"
    assert institutions[0].institution_count == 2


def test_stocks_runtime_uses_akshare_shareholder_and_action_capabilities() -> None:
    runtime = QuoteMux(QuoteMuxSettings(enabled_sources=("akshare",)))
    fake_source_call = _source_call_stub(
        {
            ("akshare", "get_stock_money_flow"): [StockMoneyFlowItem(code="600000", trade_date="2026-04-03", view="main", net_inflow=1000000.0)],
            ("akshare", "get_shareholder_count"): [ShareholderCountItem(code="600000", trade_date="2026-03-31", holder_count=100000, avg_holding=5000.0)],
            ("akshare", "get_shareholder_top10"): (
                [ShareholderTop10Item(code="600000", report_period="2026-03-31", rank=1, shareholder_name="股东一", holding_volume=1000.0)],
                [ShareholderTop10Item(code="600000", report_period="2026-03-31", rank=1, shareholder_name="流通股东一", holding_volume=900.0)],
            ),
            ("akshare", "get_dividends"): [DividendItem(code="600000", announce_date="2026-04-01", record_date="2026-04-10", cash_dividend_per_share=0.1)],
            ("akshare", "get_repurchases"): [RepurchaseItem(code="600000", announce_date="2026-04-01", progress="实施中", repurchase_amount=1000000.0)],
            ("akshare", "get_rights_issues"): [RightsIssueItem(code="600000", announce_date="2026-04-01", rights_ratio=0.3, rights_price=5.0)],
            ("akshare", "get_share_changes"): [ShareChangeItem(code="600000", change_date="2026-04-01", reason="股本变动", total_share=1000000.0)],
            ("akshare", "get_unlock_schedules"): [UnlockScheduleItem(code="600000", unlock_date="2026-04-01", unlock_volume=10000.0, share_type="首发限售股")],
            ("akshare", "get_hk_connect_holdings"): [HKConnectHoldingItem(code="600000", trade_date="2026-04-03", holding_volume=1000000.0, holding_ratio=1.2)],
            ("akshare", "get_pledge_stats"): [PledgeStatItem(code="600000", trade_date="2026-04-03", pledge_volume=100000.0, pledge_ratio=2.3)],
            ("akshare", "get_pledge_details"): [PledgeDetailItem(code="600000", holder_name="股东一", start_date="2026-04-01", pledge_volume=10000.0, pledge_ratio=0.1)],
        }
    )
    with patch("quotemux.stocks._source_package_call", side_effect=fake_source_call):
        money_flow = runtime.stocks.get_money_flow("600000", "2026-04-03", "", "", "main")
        shareholder_count = runtime.stocks.get_shareholder_count("600000", "", "2026-03-31", "2026-03-31")
        top10 = runtime.stocks.get_shareholder_top10("600000", "2026-03-31", "", "")
        top10_float = runtime.stocks.get_shareholder_top10_float("600000", "2026-03-31", "", "")
        dividends = runtime.stocks.get_dividends("600000", "2026-04-01", "2026-04-30")
        repurchases = runtime.stocks.get_repurchases("600000", "2026-04-01", "2026-04-30")
        rights = runtime.stocks.get_rights_issues("600000", "2026-04-01", "2026-04-30")
        share_changes = runtime.stocks.get_share_changes("600000", "", "2026-04-01", "2026-04-30")
        unlocks = runtime.stocks.get_unlock_schedules("600000", "", "2026-04-01", "2026-04-30")
        hk_holdings = runtime.stocks.get_hk_connect_holdings("600000", "2026-04-03", "", "")
        pledge_stats = runtime.stocks.get_pledge_stats("600000", "2026-04-03", "", "")
        pledge_details = runtime.stocks.get_pledge_details("600000", "2026-04-01", "2026-04-30", "")

    assert money_flow[0].net_inflow == 1000000.0
    assert shareholder_count[0].holder_count == 100000
    assert top10[0].shareholder_name == "股东一"
    assert top10_float[0].shareholder_name == "流通股东一"
    assert dividends[0].cash_dividend_per_share == 0.1
    assert repurchases[0].progress == "实施中"
    assert rights[0].rights_price == 5.0
    assert share_changes[0].reason == "股本变动"
    assert unlocks[0].share_type == "首发限售股"
    assert hk_holdings[0].holding_ratio == 1.2
    assert pledge_stats[0].pledge_ratio == 2.3
    assert pledge_details[0].holder_name == "股东一"


def test_stocks_runtime_uses_akshare_finance_profile_and_research_capabilities() -> None:
    runtime = QuoteMux(QuoteMuxSettings(enabled_sources=("akshare",)))
    fake_source_call = _source_call_stub(
        {
            ("akshare", "get_stock_financial_statements"): [StockFinancialStatementItem(code="600000", report_period="2026-03-31", report_type="income_statement", announce_date="2026-04-01", revenue=100.0)],
            ("akshare", "get_stock_finance_indicators"): [StockFinanceIndicatorItem(code="600000", report_period="2026-03-31", roe=10.0)],
            ("akshare", "get_company_profile"): StockProfileItem(code="600000", full_name="浦发银行股份有限公司", website="https://example.com"),
            ("akshare", "get_disclosure_dates"): [DisclosureDateItem(code="600000", report_period="2026-03-31", plan_date="2026-04-01", actual_date="2026-04-02")],
            ("akshare", "get_express"): [ExpressItem(code="600000", report_period="2026-03-31", announce_date="2026-04-01", revenue=100.0)],
            ("akshare", "get_forecasts"): [ForecastItem(code="600000", report_period="2026-03-31", forecast_type="预增", forecast_summary="增长")],
            ("akshare", "get_main_business"): [MainBusinessItem(code="600000", report_period="2026-03-31", classification="product", segment_name="主营", revenue=100.0)],
            ("akshare", "get_research_reports"): [ResearchReportItem(code="600000", report_date="2026-04-01", institution="机构", title="研报")],
            ("akshare", "get_surveys"): [SurveyItem(code="600000", survey_date="2026-04-01", org_name="机构", announcement_date="2026-04-02")],
        }
    )
    with patch("quotemux.stocks._source_package_call", side_effect=fake_source_call):
        statements = runtime.stocks.get_financial_statements(["600000"], "2026-03-31", "", "", "income_statement")
        indicators = runtime.stocks.get_finance_indicators("600000", "", "2026-03-31", "", "")
        profile = runtime.stocks.get_profile("600000")
        disclosures = runtime.stocks.get_disclosure_dates("600000", "2026-03-31", "", "")
        express = runtime.stocks.get_express("600000", "2026-03-31", "", "")
        forecasts = runtime.stocks.get_forecasts("600000", "2026-03-31", "", "")
        business = runtime.stocks.get_main_business("600000", "2026-03-31", "", "", "product")
        reports = runtime.stocks.get_research_reports("600000", "2026-04-01", "", "")
        surveys = runtime.stocks.get_surveys("600000", "2026-04-01", "", "")

    assert statements[0].revenue == 100.0
    assert indicators[0].roe == 10.0
    assert profile is not None and profile.full_name == "浦发银行股份有限公司"
    assert disclosures[0].actual_date == "2026-04-02"
    assert express[0].revenue == 100.0
    assert forecasts[0].forecast_type == "预增"
    assert business[0].segment_name == "主营"
    assert reports[0].title == "研报"
    assert surveys[0].org_name == "机构"


def test_runtime_uses_efinance_wide_table_partial_capabilities() -> None:
    runtime = QuoteMux(QuoteMuxSettings(enabled_sources=("efinance",)))
    fake_source_call = _source_call_stub(
        {
            ("efinance", "get_dragon_tiger"): [DragonTigerItem(trade_date="2026-04-03", code="600000", name="浦发银行", reason="异常波动", net_amount=100.0)],
            ("efinance", "get_shareholder_count"): [ShareholderCountItem(code="600000", trade_date="2026-03-31", holder_count=100000)],
            ("efinance", "get_express"): [ExpressItem(code="600000", report_period="2026-03-31", announce_date="2026-04-01", revenue=100.0)],
            ("efinance", "get_stock_finance_indicators"): [StockFinanceIndicatorItem(code="600000", report_period="2026-03-31", gross_margin=20.0)],
        }
    )
    with (
        patch("quotemux.markets._source_package_call", side_effect=fake_source_call),
        patch("quotemux.stocks._source_package_call", side_effect=fake_source_call),
    ):
        dragon_tiger = runtime.markets.get_dragon_tiger("2026-04-03", "", "", "600000", 10)
        counts = runtime.stocks.get_shareholder_count("600000", "", "2026-03-31", "2026-03-31")
        express = runtime.stocks.get_express("600000", "2026-03-31", "", "")
        indicators = runtime.stocks.get_finance_indicators("600000", "", "2026-03-31", "", "")

    assert dragon_tiger[0].reason == "异常波动"
    assert counts[0].holder_count == 100000
    assert express[0].revenue == 100.0
    assert indicators[0].gross_margin == 20.0


def test_trading_calendar_with_report_uses_akshare_emergency() -> None:
    runtime = QuoteMux()
    fake_source_call = _source_call_stub(
        {
            ("akshare", "get_trading_calendar"): [TradingCalendarItem(exchange="SSE", trade_date="2026-04-03", is_open=True)],
        }
    )
    with patch("quotemux.markets._source_package_call", side_effect=fake_source_call):
        items, report = runtime.markets.get_trading_calendar_with_report(
            TradingCalendarRequest(exchange="SSE", start_date="2026-04-03", end_date="2026-04-03")
    )

    assert len(items) == 1
    assert report.source_hit_counts["akshare"] == 1
    assert report.degraded is False


def test_dataset_interfaces_run_through_runtime() -> None:
    runtime = QuoteMux()
    market_source_call = _source_call_stub(
        {
            ("tushare", "get_trading_calendar"): [TradingCalendarItem(exchange="SSE", trade_date="2026-04-03", is_open=True)],
        }
    )
    dataset_source_call = _source_call_stub(
        {
            ("efinance", "get_stock_quotes"): [
                StockQuoteItem(code="600000", trade_time=f"2026-04-03 09:{30 + offset // 60:02d}:{offset % 60:02d}", freq="1m", open=10.0, high=10.2, low=9.9, close=10.1, volume=100.0 + offset, amount=100000.0 + offset * 1000.0, adjust="none")
                for offset in range(242)
            ],
            ("mootdx", "get_index_quotes"): [
                IndexQuoteItem(index_code="SHSE.000001", trade_time="2026-04-03", freq="1d", open=3300.0, high=3310.0, low=3290.0, close=3305.0, amount=123000000.0)
            ],
            ("tushare", "get_stock_quotes"): [
                StockQuoteItem(code="600000", trade_time="2026-04-03", freq="1d", open=10.0, high=11.0, low=9.8, close=10.5, volume=1000.0, amount=1050000.0, adjust="none")
            ],
        }
    )
    with (
        patch("quotemux.markets._source_package_call", side_effect=market_source_call),
        patch("quotemux.datasets._source_package_call", side_effect=dataset_source_call),
    ):
        stock_frame, stock_report = runtime.datasets.get_stock_bar_1m(StockBar1mRequest(code="600000", start_date=date(2026, 4, 3), end_date=date(2026, 4, 3)), pd.DataFrame())
        index_frame, index_report = runtime.datasets.get_index_bar_1d(IndexBar1dRequest(index_code="SHSE.000001", start_date=date(2026, 4, 3), end_date=date(2026, 4, 3)), pd.DataFrame())
        repaired_frame, repair_report = runtime.datasets.repair_stock_daily_ohlcva(
            StockDailyOhlcvaRepairRequest(trade_date=date(2026, 4, 3)),
            pd.DataFrame([{"code": "600000", "is_suspended": False, "open": None, "high": None, "low": None, "close": None, "volume": None, "amount": None}]),
        )

    assert len(stock_frame) == 242
    assert len(index_frame) == 1
    assert float(repaired_frame.loc[0, "close"]) == 10.5
    assert stock_report.source_hit_counts["efinance"] == 1
    assert index_report.source_hit_counts["mootdx"] == 1
    assert repair_report.source_hit_counts["tushare"] == 1
    assert stock_report.contract_name == "stocks.quotes.intraday"
    assert index_report.contract_name == "indexes.quotes.daily"
    assert repair_report.contract_name == "stocks.quotes.daily"


def test_tushare_uses_single_top_level_source_package() -> None:
    source_root = _package_source_root() / "packages"
    source_dirs = {path.name for path in source_root.iterdir() if path.is_dir()}
    old_tushare_dirs = {
        "tushare_stocks",
        "tushare_stock_finance",
        "tushare_stock_ownership",
        "tushare_stock_chips",
        "tushare_market_topics",
    }

    assert "tushare" in source_dirs
    assert source_dirs.isdisjoint(old_tushare_dirs)

    manifests = load_builtin_manifests()
    package_ids = {manifest.package_id for manifest in manifests}
    tushare_manifest = next(manifest for manifest in manifests if manifest.package_id == "tushare")

    assert package_ids.isdisjoint(old_tushare_dirs)
    assert package_ids.issubset(source_dirs)
    assert tushare_manifest.get_handler_target("get_stock_daily_basic") == "quotemux_packages.tushare.source:get_stock_daily_basic"
    assert tushare_manifest.get_handler_target("get_connect_capital_flow") == "quotemux_packages.tushare.source:get_connect_capital_flow"


def test_local_db_capabilities_move_to_tushare_and_store_only_news() -> None:
    source_root = Path(__file__).resolve().parents[1] / "src" / "quotemux" / "sources"
    source_dirs = {path.name for path in source_root.iterdir() if path.is_dir()}
    old_datalake_dirs = {"datalake", "datalake_reference", "datalake_news", "local_topics", "static_core"}
    manifests = load_builtin_manifests()
    package_ids = {manifest.package_id for manifest in manifests}
    tushare_manifest = next(manifest for manifest in manifests if manifest.package_id == "tushare")

    assert source_dirs.isdisjoint(old_datalake_dirs)
    assert package_ids.isdisjoint(old_datalake_dirs)
    assert "datalake" not in package_ids
    assert "derived_core" in package_ids
    assert tushare_manifest.get_handler_target("get_stock_basic") == "quotemux_packages.tushare.source:get_stock_basic"
    assert tushare_manifest.get_handler_target("get_market_sessions") == "quotemux_packages.tushare.source:get_market_sessions"
    assert all("markets.events.news" not in manifest.contract_names for manifest in manifests)
    assert "markets.trading.sessions" in tushare_manifest.contract_names


def test_derived_capabilities_route_to_derived_core_provider(monkeypatch) -> None:
    class StoreRead:
        hit = False
        partial_hit = False
        status = "miss"

    runtime = QuoteMux(QuoteMuxSettings(enabled_sources=("derived_core",)))
    monkeypatch.setattr("quotemux.stocks.load_store_result", lambda capability_id, identity, model_type: ([], StoreRead()))
    monkeypatch.setattr("quotemux.stocks.store_result", lambda capability_id, identity, items, report: None)
    source_calls: list[tuple[str, str, tuple[object, ...]]] = []
    fake_source_call = _source_call_stub(
        {
            ("derived_core", "get_hl_signal"): [HLSignalItem(code="600000", trade_date="2026-04-03", first_extreme="high", signal="high_first")],
            ("derived_core", "get_technical_factors"): [TechnicalFactorItem(code="600000", trade_date="2026-04-03", adjust="none")],
            ("derived_core", "get_shareholder_changes"): [ShareholderChangeItem(code="600000", trade_date="2026-03-31", holder_count=100000)],
        },
        source_calls,
    )

    with patch("quotemux.stocks._source_package_call", side_effect=fake_source_call):
        assert runtime.stocks.get_hl_signal("600000", "2026-04-03", "", "") == [HLSignalItem(code="600000", trade_date="2026-04-03", first_extreme="high", signal="high_first")]
        assert runtime.stocks.get_technical_factors("600000", "2026-04-03", "", "", "none") == [TechnicalFactorItem(code="600000", trade_date="2026-04-03", adjust="none")]
        assert runtime.stocks.get_shareholder_changes("600000", "", "2026-01-01", "2026-03-31") == [ShareholderChangeItem(code="600000", trade_date="2026-03-31", holder_count=100000)]

    assert ("derived_core", "get_hl_signal", ("600000", "2026-04-03", "", "")) in source_calls
    assert ("derived_core", "get_technical_factors", ("600000", "2026-04-03", "", "", "none")) in source_calls
    assert ("derived_core", "get_shareholder_changes", ("600000", "", "2026-01-01", "2026-03-31")) in source_calls


def test_capability_registry_has_policy_shape_and_merge_strategy_for_every_contract() -> None:
    from quotemux.capabilities import get_capability_definition, is_independently_configurable_capability_id, list_capability_ids

    contract_names = list_contract_names()
    independently_configurable_ids = tuple(capability_id for capability_id in list_capability_ids() if is_independently_configurable_capability_id(capability_id))

    assert contract_names == independently_configurable_ids
    assert "markets.events.news" in contract_names
    news_definition = get_capability_definition("markets.events.news")
    assert news_definition.allowed_packages == ()
    assert news_definition.default_source_order == ()
    assert not any(contract_name == "updater" or contract_name.startswith("updater.") for contract_name in contract_names)
    assert "stocks.profile.basic" in contract_names
    assert "markets.calendar.trading.next" not in contract_names
    assert "markets.calendar.trading.previous" not in contract_names
    assert "markets.calendar.trading.yearly" not in contract_names
    for contract_name in contract_names:
        policy = get_contract_policy(contract_name)
        assert policy.merge_strategy in get_contract_allowed_merge_strategies(contract_name)
        assert get_contract_result_shape(contract_name) != ""


def test_default_time_series_merge_strategy_prefers_first_provider_value() -> None:
    runtime = QuoteMux()
    source_calls: list[tuple[str, str, tuple[object, ...]]] = []
    fake_source_call = _source_call_stub(
        {
            ("tushare", "get_stock_quotes"): [StockQuoteItem(code="600000", trade_time="2026-04-03", freq="1d", close=10.1, adjust="none")],
            ("efinance", "get_stock_quotes"): [StockQuoteItem(code="600000", trade_time="2026-04-03", freq="1d", close=11.0, adjust="none")],
        },
        source_calls,
    )

    with (
        patch("quotemux.stocks._source_package_call", side_effect=fake_source_call),
        patch("quotemux.stocks._expected_trade_dates", return_value=["2026-04-03"]),
    ):
        items, report = runtime.stocks.get_quotes_with_report(
            StockQuotesRequest(codes=["600000"], freq="1d", start_date="2026-04-03", end_date="2026-04-03")
        )

    assert items[0].close == 10.1
    assert [package_id for package_id, _, _ in source_calls] == ["tushare"]
    assert report.source_request_counts["tushare"] == 1
    assert report.conflict_count == 0


def test_manifest_validation_rejects_invalid_schema_and_secret_fields() -> None:
    manifest = _manifest_with_capabilities(
        "bad_schema",
        ("stocks.quotes.daily",),
        (("get_stock_quotes", "quotemux_packages.tushare.source:get_stock_quotes"),),
        config_schema=(
            ConfigFieldSchema(name="timeout", field_type="int", title="超时", default_value="abc"),
            ConfigFieldSchema(name="timeout", field_type="int", title="重复字段"),
        ),
        secret_fields=("token",),
    )

    with pytest.raises(ConfigValidationError) as exc_info:
        validate_manifests((manifest,))

    messages = [issue.message for issue in exc_info.value.issues]
    assert "timeout 默认值不符合类型: int" in messages
    assert "重复字段: timeout" in messages
    assert "密钥字段未在 config_schema 声明: token" in messages


def test_manifest_validation_rejects_unknown_contract_duplicate_contract_and_bad_version() -> None:
    manifest = _manifest_with_capabilities(
        "bad_contract",
        ("stocks.quotes.daily", "stocks.quotes.daily", "unknown.contract"),
        (("get_stock_quotes", "quotemux_packages.tushare.source:get_stock_quotes"),),
        version="2026-04-22",
    )

    with pytest.raises(ConfigValidationError) as exc_info:
        validate_manifests((manifest,))

    messages = [issue.message for issue in exc_info.value.issues]
    assert "bad_contract 版本不兼容: 2026-04-22" in messages
    assert "bad_contract 重复 capability: stocks.quotes.daily" in messages
    assert "bad_contract 未知 capability: unknown.contract" in messages


def test_manifest_validation_rejects_derived_capability_declarations() -> None:
    manifest = _manifest_with_capabilities(
        "bad_derived",
        ("markets.calendar.trading.next",),
        (("get_trading_calendar", "quotemux_packages.tushare.source:get_trading_calendar"),),
    )

    with pytest.raises(ConfigValidationError) as exc_info:
        validate_manifests((manifest,))

    messages = [issue.message for issue in exc_info.value.issues]
    assert "bad_derived 派生 capability 只能通过 DERIVED_CAPABILITY_BASE_IDS 配置: markets.calendar.trading.next" in messages


def test_manifest_validation_rejects_invalid_handler_duplicate_package_and_contract_mismatch() -> None:
    valid_manifest = _manifest_with_capabilities(
        "dup_package",
        ("stocks.quotes.daily",),
        (("get_stock_quotes", "quotemux_packages.tushare.source:get_stock_quotes"),),
    )
    bad_manifest = SourcePackageManifest(
        package_id="dup_package",
        version="1.0.0",
        source_name="dup_package",
        display_name="dup_package",
        description="",
        capabilities=(SourcePackageCapability(capability_id="markets.calendar.trading", support_level="native", handler_name="get_trading_calendar"),),
        capability_tags=(),
        config_schema=(),
        secret_fields=(),
        supports_multi_instance=True,
        handler_targets=(
            ("get_stock_quotes", "quotemux_packages.tushare.source:get_stock_quotes"),
            ("get_stock_quotes", "quotemux_packages.tushare.source:missing_handler"),
        ),
    )

    with pytest.raises(ConfigValidationError) as exc_info:
        validate_manifests((valid_manifest, bad_manifest))

    messages = [issue.message for issue in exc_info.value.issues]
    assert "重复 source package: dup_package" in messages
    assert any("get_stock_quotes" in message and "capability" in message for message in messages)
    assert "重复 handler: get_stock_quotes" in messages
    assert any("get_stock_quotes 无法加载" in message for message in messages)


def test_external_package_manifest_imports_handler_from_import_root(tmp_path: Path) -> None:
    package_root = tmp_path / "sample_package"
    package_root.mkdir()
    (package_root / "__init__.py").write_text("", encoding="utf-8")
    (package_root / "handlers.py").write_text("def get_stock_quotes(*args, **kwargs):\n    return []\n", encoding="utf-8")
    (package_root / "quotemux_package.json").write_text(
        json.dumps(
            {
                "package_id": "sample_external",
                "version": "1.0.0",
                "source_name": "sample_external",
                "display_name": "Sample External",
                "description": "",
                "contract_names": ["stocks.quotes.daily"],
                "capability_tags": ["external"],
                "config_schema": [],
                "secret_fields": [],
                "supports_multi_instance": True,
                "handler_targets": {"get_stock_quotes": "sample_package.handlers:get_stock_quotes"},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    registry = build_source_package_registry((str(tmp_path),))
    manifest = registry.get_manifest("sample_external")

    assert manifest.origin == "external"
    assert manifest.package_root == str(package_root)
    assert registry.has_handler("sample_external", "get_stock_quotes")
    assert registry.check_package_health("sample_external").status == "ok"


def test_default_runtime_profile_source_order_uses_source_instance_ids(tmp_path: Path) -> None:
    store = RuntimeConfigStore(tmp_path)
    store.ensure_initialized(load_builtin_manifests(), list_default_contract_policies())
    profile = store.read_profiles()[0]
    policy = next(item for item in profile.contract_policy_overrides if item.contract_name == "stocks.quotes.daily")

    assert "tushare-default" in policy.source_order
    assert "datalake-default" not in policy.source_order
    assert "static_core-default" not in policy.source_order
    assert "static_core" not in policy.source_order
    assert "datalake" not in policy.source_order
    assert "tushare" not in policy.source_order


def test_runtime_profile_validation_rejects_package_source_order() -> None:
    registry = build_source_package_registry(())
    profile = RuntimeProfile(
        profile_id="profile-test",
        display_name="测试 Profile",
        version="v1",
        created_at="",
        published_at="",
        note="",
        source_instances=(
            SourceInstanceConfig(
                instance_id="tushare-default",
                package_id="tushare",
                display_name="Tushare 默认实例",
                enabled=True,
                priority=1,
                timeout_seconds=None,
                config_values={},
                secret_values={},
                tags=(),
            ),
        ),
        contract_policy_overrides=(
            ContractPolicyOverride(contract_name="stocks.quotes.daily", mode="auto", source_order=("tushare",)),
        ),
    )

    with pytest.raises(ConfigValidationError) as exc_info:
        validate_profile(profile, registry)

    assert "未知 source instance: tushare" in [issue.message for issue in exc_info.value.issues]


def test_source_instance_validation_rejects_unknown_package() -> None:
    registry = build_source_package_registry(())
    unknown_instance = SourceInstanceConfig(
        instance_id="unknown-default",
        package_id="unknown",
        display_name="未知实例",
        enabled=True,
        priority=1,
        timeout_seconds=None,
        config_values={},
        secret_values={},
        tags=(),
    )

    with pytest.raises(ConfigValidationError) as unknown_exc_info:
        validate_instance(unknown_instance, registry, ())

    assert "未知 source package: unknown" in [issue.message for issue in unknown_exc_info.value.issues]


def test_runtime_snapshot_does_not_append_undeclared_fallback_instances() -> None:
    efinance_instance = SourceInstanceConfig(
        instance_id="efinance-default",
        package_id="efinance",
        display_name="EFinance 默认实例",
        enabled=True,
        priority=1,
        timeout_seconds=None,
        config_values={},
        secret_values={},
        tags=(),
    )
    tushare_instance = SourceInstanceConfig(
        instance_id="tushare-default",
        package_id="tushare",
        display_name="Tushare 默认实例",
        enabled=True,
        priority=2,
        timeout_seconds=None,
        config_values={},
        secret_values={},
        tags=(),
    )
    snapshot = RuntimeSnapshot(
        profile_id="profile-test",
        version="v1",
        published_at="",
        source_instances=(efinance_instance, tushare_instance),
        contract_policy_overrides=(
            ContractPolicyOverride(contract_name="stocks.quotes.daily", mode="auto", source_order=("tushare-default",)),
        ),
    )

    instances = snapshot.get_contract_source_instances("stocks.quotes.daily", ("efinance", "tushare"))

    assert tuple(item.instance_id for item in instances) == ("tushare-default",)


def test_source_instance_executor_uses_multi_instance_order_skips_disabled_and_passes_instance_context(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("QUOTEMUX_RUNTIME_ROOT", str(tmp_path))
    reset_config_runtime_cache()
    runtime = QuoteMuxConfigRuntime()
    runtime.ensure_initialized()
    store = RuntimeConfigStore(tmp_path)
    tushare_primary = SourceInstanceConfig(
        instance_id="tushare-primary",
        package_id="tushare",
        display_name="Tushare 主实例",
        enabled=True,
        priority=1,
        timeout_seconds=7,
        config_values={"timeout_seconds": "7"},
        secret_values={},
        tags=(),
    )
    tushare_backup = SourceInstanceConfig(
        instance_id="tushare-backup",
        package_id="tushare",
        display_name="Tushare 备用实例",
        enabled=True,
        priority=2,
        timeout_seconds=9,
        config_values={"timeout_seconds": "9"},
        secret_values={"api_key": "secret-ref"},
        tags=(),
    )
    tushare_disabled = SourceInstanceConfig(
        instance_id="tushare-disabled",
        package_id="tushare",
        display_name="Tushare 禁用实例",
        enabled=False,
        priority=3,
        timeout_seconds=None,
        config_values={},
        secret_values={},
        tags=(),
    )
    store.write_instances((tushare_primary, tushare_backup, tushare_disabled))
    store.write_draft_policies(
        (
            ContractPolicyOverride(
                contract_name="stocks.quotes.daily",
                mode="auto",
                source_order=("tushare-primary", "tushare-backup", "tushare-disabled"),
            ),
        )
    )
    runtime.publish_profile("多实例测试", "")
    captured_instances: list[SourceInstanceConfig] = []

    def build_fetcher(instance: SourceInstanceConfig):
        captured_instances.append(instance)
        return lambda: []

    steps = SourceInstanceExecutor(QuoteMuxSettings()).build_steps("stocks.quotes.daily", {"tushare": ("get_stock_quotes", build_fetcher)}, ("tushare",))

    assert tuple(step.step_id for step in steps) == ("tushare-primary", "tushare-backup")
    assert captured_instances[0].config_values["timeout_seconds"] == "7"
    assert captured_instances[1].secret_values["api_key"] == "secret-ref"


def test_fallback_chain_continues_after_handler_error() -> None:
    def broken_fetcher():
        raise RuntimeError("provider failed")

    good_item = StockQuoteItem(code="600000", trade_time="2026-04-03", freq="1d", close=10.5)
    steps = (
        ProviderStep(name="efinance", fetcher=broken_fetcher, source_instance_id="efinance-primary", handler="get_stock_quotes"),
        ProviderStep(name="efinance", fetcher=lambda: [good_item], source_instance_id="efinance-backup", handler="get_stock_quotes"),
    )

    items, report = run_fallback_chain_with_report(
        "stocks.quotes.daily",
        [],
        ("code", "trade_time", "freq"),
        lambda current_items: [()] if current_items == [] else [],
        steps,
        ("efinance-primary", "efinance-backup"),
    )

    assert items == [good_item]
    assert report.steps[0].error_count == 1
    assert report.steps[1].request_count == 1


def _publish_stock_quote_order(tmp_path: Path, source_order: tuple[str, ...], disabled_packages: tuple[str, ...] = ()) -> None:
    runtime = QuoteMuxConfigRuntime()
    runtime.ensure_initialized()
    store = RuntimeConfigStore(tmp_path)
    instances = tuple(replace(instance, enabled=False) if instance.package_id in disabled_packages else instance for instance in store.read_instances())
    store.write_instances(instances)
    store.write_draft_policies(
        (
            ContractPolicyOverride(
                contract_name="stocks.quotes.daily",
                mode="auto",
                source_order=source_order,
            ),
        )
    )
    runtime.publish_profile("stocks.quotes.daily 测试", "")


def _stock_quote_item() -> StockQuoteItem:
    return StockQuoteItem(code="600000", trade_time="2026-04-03", freq="1d", close=10.5)


def test_disabled_tushare_uses_efinance(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("QUOTEMUX_RUNTIME_ROOT", str(tmp_path))
    reset_config_runtime_cache()
    _publish_stock_quote_order(tmp_path, ("tushare-default", "efinance-default"), ("tushare",))
    runtime = QuoteMux()
    fake_source_call = _source_call_stub(
        {
            ("efinance", "get_stock_quotes"): [_stock_quote_item()],
        }
    )

    with (
        patch("quotemux.stocks._source_package_call", side_effect=fake_source_call),
        patch("quotemux.stocks._expected_trade_dates", return_value=["2026-04-03"]),
    ):
        items, report = runtime.stocks.get_quotes_with_report(
            StockQuotesRequest(codes=["600000"], freq="1d", start_date="2026-04-03", end_date="2026-04-03")
        )

    assert items == [_stock_quote_item()]
    assert any(item.source_instance_id == "efinance-default" for item in report.source_instance_reports)


def test_provider_reorder_is_reflected_in_report(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("QUOTEMUX_RUNTIME_ROOT", str(tmp_path))
    reset_config_runtime_cache()
    _publish_stock_quote_order(tmp_path, ("efinance-default", "tushare-default"))
    runtime = QuoteMux()
    fake_source_call = _source_call_stub(
        {
            ("efinance", "get_stock_quotes"): [_stock_quote_item()],
        }
    )

    with (
        patch("quotemux.stocks._source_package_call", side_effect=fake_source_call),
        patch("quotemux.stocks._expected_trade_dates", return_value=["2026-04-03"]),
    ):
        _, report = runtime.stocks.get_quotes_with_report(
            StockQuotesRequest(codes=["600000"], freq="1d", start_date="2026-04-03", end_date="2026-04-03")
        )

    assert report.source_instance_reports[0].source_instance_id == "efinance-default"
    assert report.source_instance_reports[0].package_id == "efinance"


def test_tushare_error_falls_back_to_efinance(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("QUOTEMUX_RUNTIME_ROOT", str(tmp_path))
    reset_config_runtime_cache()
    _publish_stock_quote_order(tmp_path, ("tushare-default", "efinance-default"))
    runtime = QuoteMux()
    fake_source_call = _source_call_stub(
        {
            ("tushare", "get_stock_quotes"): RuntimeError("tushare down"),
            ("efinance", "get_stock_quotes"): [_stock_quote_item()],
        }
    )

    with (
        patch("quotemux.stocks._source_package_call", side_effect=fake_source_call),
        patch("quotemux.stocks._expected_trade_dates", return_value=["2026-04-03"]),
    ):
        items, report = runtime.stocks.get_quotes_with_report(
            StockQuotesRequest(codes=["600000"], freq="1d", start_date="2026-04-03", end_date="2026-04-03")
        )

    assert items == [_stock_quote_item()]
    assert any(item.source_instance_id == "efinance-default" for item in report.source_instance_reports)
    assert any(item.source_instance_id == "tushare-default" and item.error_count == 1 for item in report.source_instance_reports)


def test_runtime_profile_publish_rollback_and_snapshot_isolation(tmp_path: Path) -> None:
    runtime = QuoteMuxConfigRuntime(tmp_path)
    runtime.ensure_initialized()
    store = RuntimeConfigStore(tmp_path)
    default_profile = runtime.get_active_profile()
    store.write_draft_policies(
        (
            ContractPolicyOverride(
                contract_name="unknown.contract",
                mode="auto",
                source_order=("missing-default",),
            ),
        )
    )

    assert runtime.validate_draft_profile()["valid"] is False
    with pytest.raises(ConfigValidationError):
        runtime.publish_profile("失败发布", "不应切换")
    assert runtime.get_active_profile().profile_id == default_profile.profile_id

    default_policies = list_default_contract_policies()
    store.write_draft_policies(default_policies)
    active_before = runtime.publish_profile("基线发布", "用于回滚")
    snapshot_before = runtime.get_active_snapshot()
    first_instance = next(item for item in store.read_instances() if item.enabled)
    store.write_instances(tuple(replace(item, enabled=False) if item.instance_id == first_instance.instance_id else item for item in store.read_instances()))
    diff = runtime.diff_draft_profile()
    assert first_instance.instance_id in diff["changed_instances"]

    published = runtime.publish_profile("测试发布", "验证发布")
    assert runtime.get_active_profile().profile_id == published.profile_id
    assert snapshot_before.profile_id == active_before.profile_id

    rolled_back = runtime.rollback_profile(active_before.profile_id)
    transitions = runtime.list_profile_transitions()
    assert rolled_back.profile_id == active_before.profile_id
    assert transitions[-1]["action"] == "rollback"
    assert transitions[-1]["from_profile_id"] == published.profile_id
    assert transitions[-1]["to_profile_id"] == active_before.profile_id


def test_daily_gap_fill_writes_suspended_placeholder(monkeypatch) -> None:
    request = StockQuotesRequest(codes=["600000"], freq="1d", start_date="2026-04-02", end_date="2026-04-03")
    base_items = [
        StockQuoteItem(code="600000", trade_time="2026-04-01", freq="1d", close=10.0, adjust="none", is_st=True),
        StockQuoteItem(code="600000", trade_time="2026-04-03", freq="1d", close=10.5, adjust="none", is_st=True),
    ]
    written_items: list[StockQuoteItem] = []

    monkeypatch.setattr("quotemux.stocks._today_text", lambda: "2026-04-10")
    monkeypatch.setattr("quotemux.stocks._expected_trade_dates", lambda start_date, end_date, settings: ["2026-04-02", "2026-04-03"])
    monkeypatch.setattr("quotemux.stocks.get_local_stock_daily_previous", lambda codes, before_date, adjust: [base_items[0]])
    monkeypatch.setattr("quotemux.stocks.get_fact_ref_writer", lambda capability_id: lambda items: written_items.extend(items) or True)

    filled_items = _fill_suspended_daily_gaps(request, base_items[1:], None, "none", QuoteMuxSettings())
    suspended_item = next(item for item in filled_items if item.trade_time == "2026-04-02")

    assert suspended_item.open == 10.0
    assert suspended_item.high == 10.0
    assert suspended_item.low == 10.0
    assert suspended_item.close == 10.0
    assert suspended_item.volume == 0.0
    assert suspended_item.amount == 0.0
    assert suspended_item.is_suspended is True
    assert suspended_item.is_st is True
    assert written_items == [suspended_item]


def test_daily_gap_fill_skips_without_previous_item(monkeypatch) -> None:
    request = StockQuotesRequest(codes=["600000"], freq="1d", start_date="2026-04-02", end_date="2026-04-02")
    written_items: list[StockQuoteItem] = []

    monkeypatch.setattr("quotemux.stocks._today_text", lambda: "2026-04-10")
    monkeypatch.setattr("quotemux.stocks._expected_trade_dates", lambda start_date, end_date, settings: ["2026-04-02"])
    monkeypatch.setattr("quotemux.stocks.get_local_stock_daily_previous", lambda codes, before_date, adjust: [])
    monkeypatch.setattr("quotemux.stocks.get_fact_ref_writer", lambda capability_id: lambda items: written_items.extend(items) or True)

    filled_items = _fill_suspended_daily_gaps(request, [], None, "none", QuoteMuxSettings())

    assert filled_items == []
    assert written_items == []


def test_fill_missing_controls_suspended_quote_return() -> None:
    suspended_item = StockQuoteItem(code="600000", trade_time="2026-04-02", freq="1d", close=10.0, adjust="none", is_suspended=True)
    active_item = StockQuoteItem(code="600000", trade_time="2026-04-03", freq="1d", close=10.5, adjust="none")

    default_result = _build_stock_quotes_query_result(["600000"], [active_item], [active_item], [suspended_item, active_item], "1d", None, ["2026-04-02", "2026-04-03"], None, set())
    fill_result = _build_stock_quotes_query_result(["600000"], [suspended_item, active_item], [suspended_item, active_item], [suspended_item, active_item], "1d", None, ["2026-04-02", "2026-04-03"], None, set())

    assert [item.trade_time for item in default_result.items] == ["2026-04-03"]
    assert default_result.meta.total_rows == 1
    assert default_result.meta.codes[0].missing_trade_dates == []
    assert [item.trade_time for item in fill_result.items] == ["2026-04-02", "2026-04-03"]
    assert fill_result.meta.total_rows == 2
