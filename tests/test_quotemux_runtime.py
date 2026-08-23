from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import date, datetime
from pathlib import Path
import time
from unittest.mock import patch

import pandas as pd
import pytest

from quotemux import IndexBar1dRequest, IndexMembersRequest, IndexQuotesRequest, NextTradingDaysRequest, PreviousTradingDaysRequest, QuoteMux, StockBar1mRequest, StockDailyOhlcvaRepairRequest, StockDailySnapshotRequest, StockQuotesRequest, TradingCalendarRequest, YearlyTradingCalendarRequest
from quotemux.config_runtime.models import ContractPolicyOverride, RuntimeProfile, RuntimeSnapshot, SourceInstanceConfig
from quotemux.config_runtime.runtime import QuoteMuxConfigRuntime, reset_config_runtime_cache
from quotemux.config_runtime.store import RuntimeConfigStore
from quotemux.config_runtime.validation import ConfigValidationError, validate_instance, validate_manifests, validate_profile
from quotemux.contracts.policies import get_contract_policy, list_default_contract_policies
from quotemux.contracts.registry import get_contract_allowed_merge_strategies, get_contract_result_shape, list_contract_names
from quotemux.common import intraday_quote_cache_needs_refresh
from quotemux.models import AuctionItem, BlockTradeItem, BoardCatalogItem, BoardCategoryItem, BoardMemberItem, BoardMoneyFlowItem, BoardQuoteItem, ConceptCatalogItem, ConceptCategoryItem, ConceptMemberItem, ConceptMoneyFlowItem, ConceptQuoteItem, ConnectCapitalFlowItem, DisclosureDateItem, DividendItem, DragonTigerInstitutionItem, DragonTigerItem, ExpressItem, ForecastItem, HKConnectHoldingItem, HotMoneyDetailItem, IndexMemberItem, IndexQuoteItem, LimitOrderAmountItem, MainBusinessItem, MarketCapitalFlowItem, PledgeDetailItem, PledgeStatItem, RepurchaseItem, ResearchReportItem, RightsIssueItem, ShareChangeItem, ShareholderCountItem, ShareholderTop10Item, StockBasicInfo, StockFinanceIndicatorItem, StockFinancialStatementItem, StockMoneyFlowItem, StockPremarketItem, StockProfileItem, StockQuoteItem, SurveyItem, TradingCalendarItem, UnlockScheduleItem
from quotemux.models import ConceptAliasGroupItem, ConceptAliasGroupMemberItem, HLSignalItem, ShareholderChangeItem, TechnicalFactorItem
from platform_models import IndexCatalogItem
from quotemux.infra.db.market_reads import _stock_daily_snapshot_query, load_concept_daily_frame, load_index_daily_frame, load_stock_daily_frame
from quotemux.provider_timeout.adaptive import resolve_provider_timeout
from quotemux.provider_timeout.policy import CapabilityTimeoutMetric, CapabilityTimeoutPolicy, ProviderTimeoutMetric, ProviderTimeoutPolicy, TIMEOUT_STATUS_EMPTY, TIMEOUT_STATUS_SUCCESS, TIMEOUT_STATUS_TIMEOUT, default_capability_timeout_policy, default_provider_timeout_policy
from quotemux.provider_timeout.runtime import run_provider_request
from quotemux.runtime_core.executor import ProviderStep, SourceInstanceExecutor, run_fallback_chain_with_report
from quotemux.reports import ContractReport
from quotemux.settings import QuoteMuxSettings
from quotemux.local_store import get_local_concept_members, get_local_index_quotes
from quotemux.stocks import _assert_daily_snapshot_coverage, _build_missing_quote_requests, _build_snapshot_requests, _build_stock_quotes_query_result, _expected_trade_dates, _filter_standard_intraday_items, _filter_suspended_quote_items, _limit_order_candidates
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


class _MemoryTimeoutStore:
    def __init__(self) -> None:
        self.provider_policy = ProviderTimeoutPolicy("stocks.quotes.daily", "efinance", 10.0, 3.0, 60.0, 200, 20)
        self.capability_policy = CapabilityTimeoutPolicy("stocks.quotes.daily", 30.0, 3.0, 60.0, 200, 20)
        self.provider_samples: tuple[float, ...] = ()
        self.capability_samples: tuple[float, ...] = ()
        self.provider_metrics: list[ProviderTimeoutMetric] = []
        self.capability_metrics: list[CapabilityTimeoutMetric] = []

    def get_provider_policy(self, capability_id: str, provider: str) -> ProviderTimeoutPolicy:
        return ProviderTimeoutPolicy(capability_id, provider, self.provider_policy.default_timeout_seconds, self.provider_policy.min_timeout_seconds, self.provider_policy.max_timeout_seconds, self.provider_policy.sample_window_size, self.provider_policy.min_sample_count)

    def get_capability_policy(self, capability_id: str) -> CapabilityTimeoutPolicy:
        return CapabilityTimeoutPolicy(capability_id, self.capability_policy.default_timeout_seconds, self.capability_policy.min_timeout_seconds, self.capability_policy.max_timeout_seconds, self.capability_policy.sample_window_size, self.capability_policy.min_sample_count)

    def list_provider_success_elapsed_ms(self, capability_id: str, provider: str, limit: int) -> tuple[float, ...]:
        return self.provider_samples[:limit]

    def list_capability_success_elapsed_ms(self, capability_id: str, limit: int) -> tuple[float, ...]:
        return self.capability_samples[:limit]

    def write_provider_metric(self, metric: ProviderTimeoutMetric) -> None:
        self.provider_metrics.append(metric)

    def write_capability_metric(self, metric: CapabilityTimeoutMetric) -> None:
        self.capability_metrics.append(metric)


@pytest.fixture(autouse=True)
def isolate_runtime_root(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("QUOTEMUX_RUNTIME_ROOT", str(tmp_path / "runtime"))
    monkeypatch.setattr("quotemux.query_engine.load_store_result", lambda capability_id, request_identity, model_type: ([], _FakeStoreRead()))
    monkeypatch.setattr("quotemux.query_engine.store_result", lambda capability_id, request_identity, items, report, quarantine_count=0: _FakeStoreWrite())
    monkeypatch.setattr("quotemux.concept_runtime.load_store_result", lambda capability_id, request_identity, model_type: ([], _FakeStoreRead()))
    monkeypatch.setattr("quotemux.concept_runtime.store_result", lambda capability_id, request_identity, items, report, quarantine_count=0: _FakeStoreWrite())
    monkeypatch.setattr("quotemux.stocks.get_fact_ref_writer", lambda capability_id: None)
    monkeypatch.setattr("quotemux.indexes.get_fact_ref_writer", lambda capability_id: None)
    monkeypatch.setattr("quotemux.concept_runtime.get_fact_ref_writer", lambda capability_id: None)
    monkeypatch.setattr("quotemux.markets.get_fact_ref_writer", lambda capability_id: None)
    monkeypatch.setattr("quotemux.stocks.get_local_stock_quotes", lambda codes, freq, trade_date, start_date, end_date, start_time, end_time, count, adjust, adjustment_base_date: [])
    monkeypatch.setattr("quotemux.stocks.get_local_stock_intraday_quotes", lambda codes, freq, trade_date, start_date, end_date, start_time, end_time, count: [])
    monkeypatch.setattr("quotemux.stocks.get_local_stock_daily_snapshot_full", lambda trade_date: [])
    monkeypatch.setattr("quotemux.stocks.load_stock_active_codes_frame", lambda trade_date: pd.DataFrame())
    monkeypatch.setattr("quotemux.indexes.get_local_index_quotes", lambda index_codes, freq, trade_date, start_date, end_date, count: [])
    monkeypatch.setattr("quotemux.indexes.get_local_index_catalog", lambda index_codes: [])
    monkeypatch.setattr("quotemux.indexes.get_local_index_profile", lambda index_code: [])
    monkeypatch.setattr("quotemux.concept_runtime.get_local_concept_daily_snapshot", lambda trade_date, limit, offset: [])
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


def test_limit_order_amount_timeout_defaults_are_elevated() -> None:
    capability_policy = default_capability_timeout_policy("stocks.signals.limit_order_amount")
    provider_policy = default_provider_timeout_policy("stocks.signals.limit_order_amount", "crawler_provider")
    other_provider_policy = default_provider_timeout_policy("stocks.signals.limit_order_amount", "efinance")

    assert capability_policy.default_timeout_seconds == 120.0
    assert capability_policy.min_timeout_seconds == 3.0
    assert capability_policy.max_timeout_seconds == 180.0
    assert provider_policy.default_timeout_seconds == 20.0
    assert provider_policy.min_timeout_seconds == 3.0
    assert provider_policy.max_timeout_seconds == 60.0
    assert other_provider_policy.default_timeout_seconds == 10.0


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
            StockQuotesRequest(codes=["600000"], freq="1d", start_date="2026-04-03", end_date="2026-04-03", fill_missing=True)
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


def test_stocks_quotes_explicit_window_uses_provider_by_default() -> None:
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
        result = runtime.stocks.get_quotes_query_result(
            StockQuotesRequest(codes=["600000"], freq="1d", start_date="2026-04-03", end_date="2026-04-03")
        )

    assert len(result.items) == 1
    assert result.meta.complete is True


def test_stocks_quotes_fill_missing_keeps_provider_fill_for_explicit_window() -> None:
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
        result = runtime.stocks.get_quotes_query_result(
            StockQuotesRequest(codes=["600000"], freq="1d", start_date="2026-04-03", end_date="2026-04-03", fill_missing=True)
        )

    assert len(result.items) == 1
    assert result.meta.complete is True


def test_daily_snapshot_with_report_fills_missing_codes_from_b3() -> None:
    runtime = QuoteMux()
    fact_ref_items: list[StockQuoteItem] = []
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
        patch("quotemux.stocks.get_fact_ref_writer", return_value=lambda items: fact_ref_items.extend(items) is None or True),
    ):
        items, report = runtime.stocks.get_daily_snapshot_with_report(StockDailySnapshotRequest(trade_date="2026-04-03"))

    assert len(items) == 1
    assert fact_ref_items == items
    assert report.source_hit_counts["efinance"] == 1


def test_daily_snapshot_partial_gap_prefers_market_snapshot(monkeypatch) -> None:
    runtime = QuoteMux()
    calls: list[tuple[str, str, tuple[object, ...]]] = []
    local_item = StockQuoteItem(code="600000", trade_time="2026-04-03", freq="1d", open=10.0, high=11.0, low=9.5, close=10.5, pre_close=10.0, pct_chg=5.0, volume=1000.0, amount=1000000.0, adjust="none")
    fetched_item = StockQuoteItem(code="000001", trade_time="2026-04-03", freq="1d", open=9.0, high=10.0, low=8.5, close=9.5, pre_close=9.0, pct_chg=5.56, volume=1000.0, amount=1000000.0, adjust="none")
    monkeypatch.setattr("quotemux.stocks.get_local_stock_daily_snapshot_full", lambda trade_date: [local_item])
    monkeypatch.setattr("quotemux.stocks.load_stock_active_codes_frame", lambda trade_date: pd.DataFrame([{"code": "600000"}, {"code": "000001"}]))
    monkeypatch.setattr(
        "quotemux.stocks._source_package_call",
        _source_call_stub({("efinance", "get_stock_daily_snapshot_full"): [fetched_item]}, calls),
    )

    items, _ = runtime.stocks.get_daily_snapshot_with_report(StockDailySnapshotRequest(trade_date="2026-04-03"))

    assert [item.code for item in items] == ["000001", "600000"]
    assert ("efinance", "get_stock_daily_snapshot_full", ("2026-04-03",)) in calls


def test_daily_snapshot_requests_partial_missing_codes(monkeypatch) -> None:
    active_frame = pd.DataFrame([{"code": "600000"}, {"code": "000001"}])
    local_items = [StockQuoteItem(code="600000", trade_time="2026-04-03", freq="1d", close=10.5, adjust="none")]

    monkeypatch.setattr("quotemux.stocks.load_stock_active_codes_frame", lambda trade_date: active_frame)

    assert _build_snapshot_requests("2026-04-03", local_items) == [(["600000", "000001"], "2026-04-03")]


def test_daily_snapshot_requests_large_gap_use_full_snapshot(monkeypatch) -> None:
    active_frame = pd.DataFrame([{"code": f"{index:06d}"} for index in range(1, 122)])
    local_items = [StockQuoteItem(code="000001", trade_time="2026-04-03", freq="1d", close=10.5, pre_close=10.0, pct_chg=5.0, amount=1000000.0, adjust="none")]

    monkeypatch.setattr("quotemux.stocks.load_stock_active_codes_frame", lambda trade_date: active_frame)

    assert _build_snapshot_requests("2026-04-03", local_items) == [([], "2026-04-03")]


def test_limit_order_candidates_use_close_and_limit_prices() -> None:
    quotes = [
        StockQuoteItem(code="600001", trade_time="2026-04-03", freq="1d", close=11.0),
        StockQuoteItem(code="000001", trade_time="2026-04-03", freq="1d", close=9.0),
        StockQuoteItem(code="300001", trade_time="2026-04-03", freq="1d", close=10.0),
    ]
    limits = [
        StockPremarketItem(code="600001", trade_date="2026-04-03", limit_up=11.0, limit_down=9.0),
        StockPremarketItem(code="000001", trade_date="2026-04-03", limit_up=11.0, limit_down=9.0),
        StockPremarketItem(code="300001", trade_date="2026-04-03", limit_up=11.0, limit_down=9.0),
    ]

    candidates = _limit_order_candidates(quotes, limits, "2026-04-03")

    assert [(item.code, item.limit_side, item.limit_price) for item in candidates] == [
        ("600001", "up", 11.0),
        ("000001", "down", 9.0),
    ]


def test_limit_order_candidates_require_limit_price() -> None:
    quotes = [
        StockQuoteItem(code="600001", trade_time="2026-04-03", freq="1d", close=11.0),
        StockQuoteItem(code="000001", trade_time="2026-04-03", freq="1d", close=9.0),
    ]
    limits = [
        StockPremarketItem(code="600001", trade_date="2026-04-03", limit_up=None, limit_down=9.0),
        StockPremarketItem(code="000001", trade_date="2026-04-03", limit_up=11.0, limit_down=None),
    ]

    assert _limit_order_candidates(quotes, limits, "2026-04-03") == []


def test_limit_order_amount_reads_akshare_when_store_is_empty(monkeypatch) -> None:
    runtime = QuoteMux(QuoteMuxSettings(enabled_sources=("akshare",)))
    expected_items = [LimitOrderAmountItem(code="600001", trade_date="2026-04-03", limit_side="up", market="sh", order_amount=1000000.0, captured_at="2026-04-03 15:00:00")]
    written: list[tuple[object, ...]] = []

    monkeypatch.setattr("quotemux.stocks.load_store_result", lambda *args, **kwargs: ([], object()))
    monkeypatch.setattr("quotemux.stocks._source_package_call", lambda package_id, handler_name, *args: expected_items)
    monkeypatch.setattr("quotemux.stocks.store_result", lambda *args, **kwargs: written.append(args) or type("WriteResult", (), {"status": "write"})())

    items = runtime.stocks.get_limit_order_amount("2026-04-03")

    assert items == expected_items
    assert written[0][0] == "stocks.signals.limit_order_amount"
    assert written[0][1] == {"trade_date": "2026-04-03"}


def test_limit_order_capture_candidates_use_crawler_provider(monkeypatch) -> None:
    runtime = QuoteMux()
    expected_items = [
        LimitOrderAmountItem(code="600001", trade_date="2026-04-03", limit_side="up", market="sh", captured_at=""),
        LimitOrderAmountItem(code="000001", trade_date="2026-04-03", limit_side="down", market="sz", captured_at=""),
    ]
    calls: list[tuple[str, str, tuple[object, ...]]] = []

    def fake_source_call(package_id: str, handler_name: str, *args: object) -> object:
        calls.append((package_id, handler_name, args))
        return expected_items

    monkeypatch.setattr("quotemux.stocks._source_package_call", fake_source_call)
    monkeypatch.setattr(runtime.stocks, "get_daily_snapshot", lambda request: pytest.fail("不应读取日线快照"))
    monkeypatch.setattr(runtime.stocks, "get_premarket", lambda code, trade_date, start_date, end_date: pytest.fail("不应读取涨跌停价"))

    candidates = runtime.stocks.build_limit_order_amount_candidates("2026-04-03")

    assert candidates == [expected_items[1], expected_items[0]]
    assert calls == [("crawler_provider", "get_limit_stock_candidates", ("2026-04-03",))]


def test_tushare_premarket_market_limit_fallback(monkeypatch) -> None:
    from quotemux_packages.tushare import stocks as tushare_stocks

    def fake_query_frame(api_name: str, **kwargs: object) -> pd.DataFrame:
        if api_name == "stk_premarket":
            return pd.DataFrame()
        if api_name == "stk_limit":
            assert kwargs == {"trade_date": "20260403"}
            return pd.DataFrame(
                [
                    {"ts_code": "600001.SH", "up_limit": 11.0, "down_limit": 9.0},
                    {"ts_code": "000001.SZ", "up_limit": 12.0, "down_limit": 10.0},
                ]
            )
        return pd.DataFrame()

    monkeypatch.setattr(tushare_stocks, "query_frame", fake_query_frame)

    items = tushare_stocks.get_premarket("", "2026-04-03", "", "")

    assert [(item.code, item.trade_date, item.limit_up, item.limit_down) for item in items] == [
        ("000001", "2026-04-03", 12.0, 10.0),
        ("600001", "2026-04-03", 11.0, 9.0),
    ]


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


def test_index_quotes_with_report_prefers_akshare_before_other_b3() -> None:
    runtime = QuoteMux(QuoteMuxSettings(enabled_sources=("akshare", "mootdx")))
    fake_source_call = _source_call_stub(
        {
            ("akshare", "get_index_quotes"): [
                IndexQuoteItem(index_code="SHSE.000001", trade_time="2026-04-03", freq="1d", open=3301.0, high=3312.0, low=3291.0, close=3306.0, amount=223000000.0)
            ],
            ("mootdx", "get_index_quotes"): [
                IndexQuoteItem(index_code="SHSE.000001", trade_time="2026-04-03", freq="1d", open=3200.0, high=3210.0, low=3190.0, close=3205.0, amount=123000000.0)
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
    assert items[0].close == 3306.0
    assert report.source_hit_counts["akshare"] == 1


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


def test_concepts_runtime_uses_akshare_source_package_capabilities(monkeypatch) -> None:
    runtime = QuoteMux(QuoteMuxSettings(enabled_sources=("akshare",)))
    monkeypatch.setattr(runtime.concepts, "_get_concept_money_flow_from_stock_flows", lambda *args: [])
    monkeypatch.setattr(runtime.concepts, "_get_money_flow_from_market_snapshot", lambda *args: ([], False))
    monkeypatch.setattr("quotemux.concept_runtime._expected_trade_dates", lambda *args: ["2026-04-03"])
    group = ConceptAliasGroupItem(
        concept_id="C1",
        canonical_name="????",
        members=[ConceptAliasGroupMemberItem(provider="akshare", provider_concept_type="ths", provider_concept_code="BK0815", provider_concept_name="????")],
    )
    monkeypatch.setattr("quotemux.concepts._read_alias_groups", lambda trade_date: [group])
    fake_source_call = _source_call_stub(
        {
            ("akshare", "get_concept_members"): [BoardMemberItem(board_code="BK0815", code="600000", name="????")],
            ("akshare", "get_concept_quotes"): [BoardQuoteItem(board_code="BK0815", trade_time="2026-04-03", freq="1d", close=1000.0)],
            ("akshare", "get_concept_money_flow"): [BoardMoneyFlowItem(board_code="BK0815", trade_date="2026-04-03", scope="concept", net_inflow=1000000.0)],
            ("akshare", "get_concept_daily_money_flow_snapshot"): [BoardMoneyFlowItem(board_code="BK0815", trade_date="2026-04-03", scope="concept", inflow=2000000.0, outflow=1000000.0, net_inflow=1000000.0)],
            ("akshare", "get_concept_categories"): [ConceptCategoryItem(category_code="concept", category_name="????", level=1)],
        }
    )
    timed_source_call = lambda settings, capability_id, package_id, handler_name, *args: fake_source_call(package_id, handler_name, *args)
    with (
        patch("quotemux.concept_runtime._source_package_call", side_effect=fake_source_call),
        patch("quotemux.concept_runtime._timed_source_package_call", side_effect=timed_source_call),
    ):
        catalog = runtime.concepts.get_catalog("concept", "a_share", "active", 10, 0)
        profile = runtime.concepts.get_profile("C1")
        members = runtime.concepts.get_members("C1", "2026-04-03")
        quotes = runtime.concepts.get_quotes(["C1"], "1d", "", "2026-04-03", "2026-04-03", "", "", None, 10)
        flow = runtime.concepts.get_money_flow("C1", "2026-04-03", "", "", "concept")
        snapshot = runtime.concepts.get_market_money_flow("2026-04-03", "concept", 10, 0)
        raw_members = runtime.concepts.get_members("BK0815", "2026-04-03")
        categories = runtime.concepts.get_categories("", 1)

    assert catalog[0].concept_id == "C1"
    assert profile is not None and profile.concept_name == "????"
    assert members[0].code == "600000"
    assert members[0].concept_id == "C1"
    assert raw_members == []
    assert quotes[0].close == 1000.0
    assert quotes[0].concept_id == "C1"
    assert flow[0].net_inflow == 1000000.0
    assert flow[0].concept_id == "C1"
    assert snapshot[0].concept_id == "C1"
    assert snapshot[0].inflow == 2000000.0
    assert categories[0].category_code == "concept"


def test_previous_trading_days_calls_derived_core_directly(monkeypatch) -> None:
    calls: list[tuple[str, str, tuple[object, ...]]] = []

    def fake_source_call(package_id: str, handler_name: str, *args: object):
        calls.append((package_id, handler_name, args))
        return [TradingCalendarItem(exchange="SSE", trade_date="2026-06-26", is_open=True)]

    monkeypatch.setattr("quotemux.markets._source_package_call", fake_source_call)

    items = QuoteMux().markets.get_previous_trading_days(PreviousTradingDaysRequest(exchange="SSE", trade_date="20260627", n=1))

    assert [item.trade_date for item in items] == ["2026-06-26"]
    assert calls == [("derived_core", "get_previous_trading_days", ("SSE", "20260627", 1))]


def test_hot_money_details_uses_akshare_when_store_path_is_empty(monkeypatch) -> None:
    runtime = QuoteMux(
        QuoteMuxSettings(
            enabled_sources=("akshare",),
            contract_source_orders={"markets.participants.hot_money.details": ("akshare",)},
        )
    )
    calls: list[tuple[str, str, tuple[object, ...]]] = []
    fake_source_call = _source_call_stub(
        {
            ("akshare", "get_hot_money_details"): [
                HotMoneyDetailItem(trade_date="2026-06-26", name="营业部A", code="600000", stock_name="浦发银行"),
            ],
        },
        calls,
    )

    with patch("quotemux.markets._source_package_call", side_effect=fake_source_call):
        items = runtime.markets.get_hot_money_details("2026-06-26", "", "", "", 20, 0)

    assert [(item.trade_date, item.name, item.code) for item in items] == [("2026-06-26", "营业部A", "600000")]
    assert calls == [("akshare", "get_hot_money_details", ("2026-06-26", "", "", "", 20))]


def test_open_auctions_empty_codes_use_dragon_tiger_candidates(monkeypatch) -> None:
    runtime = QuoteMux()

    def fake_store_list(capability_id, store_identity, *args, **kwargs):
        assert store_identity["code"] == ""
        return []

    def fake_load_store_result(capability_id, request_identity, model_type):
        assert capability_id == "markets.trading.open_auctions"
        if request_identity["code"] == "000004":
            return [AuctionItem(code="000004", trade_date="2026-06-26", auction_time="09:25:00", price=0.26, volume=2302.0, amount=59852.0, session="open")], object()
        if request_identity["code"] == "600584":
            return [AuctionItem(code="600584", trade_date="2026-06-26", auction_time="09:25:00", price=15.0, volume=1000.0, amount=15000.0, session="open")], object()
        return [], object()

    monkeypatch.setattr(runtime.markets, "_store_list", fake_store_list)
    monkeypatch.setattr("quotemux.markets.load_store_result", fake_load_store_result)
    monkeypatch.setattr(
        runtime.markets,
        "get_dragon_tiger",
        lambda trade_date, start_date, end_date, code, limit: [
            DragonTigerItem(trade_date="2026-06-26", code="000004", name="国华退", reason="退市整理期"),
            DragonTigerItem(trade_date="2026-06-26", code="000004", name="国华退", reason="重复上榜"),
            DragonTigerItem(trade_date="2026-06-26", code="600584", name="长电科技", reason="涨幅偏离"),
        ],
    )

    items = runtime.markets.get_open_auctions("", "2026-06-26")

    assert [(item.code, item.auction_time, item.amount) for item in items] == [("000004", "09:25:00", 59852.0), ("600584", "09:25:00", 15000.0)]


def test_concept_daily_snapshot_uses_derived_metrics_when_provider_is_partial(monkeypatch) -> None:
    runtime = QuoteMux(QuoteMuxSettings(enabled_sources=("akshare", "derived_core")))
    monkeypatch.setattr("quotemux.concept_runtime._expected_trade_dates", lambda *args: ["2026-04-03"])
    group = ConceptAliasGroupItem(
        concept_id="C1",
        canonical_name="概念A",
        members=[ConceptAliasGroupMemberItem(provider="akshare", provider_concept_type="ths", provider_concept_code="BK0815", provider_concept_name="概念A")],
    )
    monkeypatch.setattr("quotemux.concepts._read_alias_groups", lambda trade_date: [group])
    monkeypatch.setattr("quotemux.concept_runtime.get_local_concept_daily_snapshot", lambda trade_date, limit, offset: [])

    fake_source_call = _source_call_stub(
        {
            ("akshare", "get_concept_quotes"): [BoardQuoteItem(board_code="BK0815", trade_time="2026-04-03", freq="1d", close=1000.0)],
            ("derived_core", "get_concept_quotes"): [BoardQuoteItem(board_code="BK0815", trade_time="2026-04-03", freq="1d", pct_chg=2.5, amount=300000000.0)],
        }
    )

    timed_source_call = lambda settings, capability_id, package_id, handler_name, *args: fake_source_call(package_id, handler_name, *args)
    with (
        patch("quotemux.concept_runtime._source_package_call", side_effect=fake_source_call),
        patch("quotemux.concept_runtime._timed_source_package_call", side_effect=timed_source_call),
    ):
        items = runtime.concepts.get_market_daily_snapshot("2026-04-03", 10, 0)

    assert [(item.concept_id, item.pct_chg, item.amount) for item in items] == [("C1", 2.5, 300000000.0)]


def test_concept_daily_snapshot_returns_available_derived_rows(monkeypatch) -> None:
    runtime = QuoteMux(QuoteMuxSettings(enabled_sources=("derived_core",)))
    groups = [
        ConceptAliasGroupItem(
            concept_id="C1",
            canonical_name="概念A",
            members=[ConceptAliasGroupMemberItem(provider="akshare", provider_concept_type="ths", provider_concept_code="BK0001", provider_concept_name="概念A")],
        ),
        ConceptAliasGroupItem(
            concept_id="C2",
            canonical_name="概念B",
            members=[ConceptAliasGroupMemberItem(provider="akshare", provider_concept_type="ths", provider_concept_code="BK0002", provider_concept_name="概念B")],
        ),
    ]
    monkeypatch.setattr("quotemux.concepts._read_alias_groups", lambda trade_date: groups)
    monkeypatch.setattr("quotemux.concept_runtime.get_local_concept_daily_snapshot", lambda trade_date, limit, offset: [])
    monkeypatch.setattr(
        runtime.concepts,
        "get_catalog",
        lambda concept_type, name, status, limit, offset: [
            ConceptCatalogItem(concept_id="C1", concept_name="概念A", concept_type="concept", status="active"),
            ConceptCatalogItem(concept_id="C2", concept_name="概念B", concept_type="concept", status="active"),
        ],
    )
    calls: list[tuple[str, str, tuple[object, ...]]] = []
    fake_source_call = _source_call_stub(
        {
            ("derived_core", "get_concept_quotes"): [
                BoardQuoteItem(board_code="C1", trade_time="2026-04-03", freq="1d", pct_chg=1.2, amount=100000000.0),
            ],
        },
        calls,
    )

    timed_source_call = lambda settings, capability_id, package_id, handler_name, *args: fake_source_call(package_id, handler_name, *args)
    with (
        patch("quotemux.concept_runtime._source_package_call", side_effect=fake_source_call),
        patch("quotemux.concept_runtime._timed_source_package_call", side_effect=timed_source_call),
    ):
        items = runtime.concepts.get_market_daily_snapshot("2026-04-03", 10, 0)

    assert [(item.concept_id, item.pct_chg, item.amount) for item in items] == [("C1", 1.2, 100000000.0)]
    assert calls == [("derived_core", "get_concept_quotes", (["C1", "C2"], "1d", "2026-04-03", "", "", "", "", None))]


def test_concepts_aliases_follow_runtime_source_order(monkeypatch) -> None:
    runtime = QuoteMux(
        QuoteMuxSettings(
            enabled_sources=("akshare", "tushare"),
            contract_source_orders={
                "concepts.members": ("akshare", "tushare"),
                "concepts.quotes.daily": ("akshare", "tushare"),
                "concepts.indicators.money_flow": ("akshare", "tushare"),
            },
        )
    )
    monkeypatch.setattr(runtime.concepts, "_get_concept_money_flow_from_stock_flows", lambda *args: [])
    monkeypatch.setattr(runtime.concepts, "_get_money_flow_from_market_snapshot", lambda *args: ([], False))
    monkeypatch.setattr("quotemux.concept_runtime._expected_trade_dates", lambda *args: ["2026-04-03", "2026-04-04"])
    group = ConceptAliasGroupItem(
        concept_id="C231",
        canonical_name="????",
        members=[
            ConceptAliasGroupMemberItem(provider="tushare", provider_concept_type="ths", provider_concept_code="885806", provider_concept_name="????"),
            ConceptAliasGroupMemberItem(provider="akshare", provider_concept_type="ths", provider_concept_code="301459", provider_concept_name="????"),
        ],
    )
    monkeypatch.setattr("quotemux.concepts._read_alias_groups", lambda trade_date: [group])
    fake_source_call = _source_call_stub(
        {
            ("akshare", "get_concept_members"): [BoardMemberItem(board_code="301459", code="600000", name="AkName")],
            ("tushare", "get_concept_members"): [BoardMemberItem(board_code="885806", code="600000", name="TsName"), BoardMemberItem(board_code="885806", code="600001", name="TsOnly")],
            ("akshare", "get_concept_quotes"): [BoardQuoteItem(board_code="301459", trade_time="2026-04-03", freq="1d", close=1.0)],
            ("tushare", "get_concept_quotes"): [BoardQuoteItem(board_code="885806", trade_time="2026-04-03", freq="1d", close=2.0), BoardQuoteItem(board_code="885806", trade_time="2026-04-04", freq="1d", close=3.0)],
            ("akshare", "get_concept_money_flow"): [BoardMoneyFlowItem(board_code="301459", trade_date="2026-04-03", scope="concept", net_inflow=1.0)],
            ("tushare", "get_concept_money_flow"): [BoardMoneyFlowItem(board_code="885806", trade_date="2026-04-03", scope="concept", net_inflow=2.0), BoardMoneyFlowItem(board_code="885806", trade_date="2026-04-04", scope="concept", net_inflow=3.0)],
        }
    )
    timed_source_call = lambda settings, capability_id, package_id, handler_name, *args: fake_source_call(package_id, handler_name, *args)
    with (
        patch("quotemux.concept_runtime._source_package_call", side_effect=fake_source_call),
        patch("quotemux.concept_runtime._timed_source_package_call", side_effect=timed_source_call),
    ):
        members = runtime.concepts.get_members("C231", "2026-04-03")
        quotes = runtime.concepts.get_quotes(["C231"], "1d", "", "2026-04-03", "2026-04-04", "", "", None, 10)
        flows = runtime.concepts.get_money_flow("C231", "", "2026-04-03", "2026-04-04", "concept")

    assert [(item.code, item.name) for item in members] == [("600000", "AkName"), ("600001", "TsOnly")]
    assert [(item.trade_time, item.close) for item in quotes] == [("2026-04-03", 1.0), ("2026-04-04", 3.0)]
    assert [(item.trade_date, item.net_inflow) for item in flows] == [("2026-04-03", 1.0), ("2026-04-04", 3.0)]
    assert {item.concept_id for item in [*members, *quotes, *flows]} == {"C231"}

def test_default_concepts_members_order_includes_derived_core(tmp_path: Path) -> None:
    store = RuntimeConfigStore(tmp_path)
    store.ensure_initialized(load_builtin_manifests(), list_default_contract_policies())
    profile = store.read_profiles()[0]
    policy = next(item for item in profile.contract_policy_overrides if item.contract_name == "concepts.members")

    assert policy.source_order[:2] == ("crawler_provider-default", "derived_core-default")
    assert "tushare-default" in policy.source_order
    assert "akshare-default" in policy.source_order


def test_local_concept_members_falls_back_to_latest_valid_rows(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_query_dataframe(query: str, params: tuple[object, ...]) -> pd.DataFrame:
        captured["query"] = query
        captured["params"] = params
        return pd.DataFrame()

    monkeypatch.setattr("quotemux.infra.db.reference_reads.query_dataframe", fake_query_dataframe)
    from quotemux.infra.db.reference_reads import load_concept_members_frame

    load_concept_members_frame("C231", "2026-06-17")

    assert "from ref.concept_stock_membership m" in str(captured["query"])
    assert "from fact.stock_daily_1d" in str(captured["query"])
    assert "and m.valid_from <= %s" in str(captured["query"])
    assert captured["params"] == ("2026-06-17", "C231", "2026-06-17", "2026-06-17", "C231", "2026-06-17", "C231", "2026-06-17")


def test_local_concept_members_uses_today_when_trade_date_empty(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_load_concept_members_frame(concept_id: str, trade_date: str) -> pd.DataFrame:
        captured["concept_id"] = concept_id
        captured["trade_date"] = trade_date
        return pd.DataFrame()

    monkeypatch.setattr("quotemux.local_store.load_concept_members_frame", fake_load_concept_members_frame)

    get_local_concept_members("C231", "")

    assert captured["concept_id"] == "C231"
    assert captured["trade_date"] != ""


def test_concept_members_writer_upserts_stock_names(monkeypatch) -> None:
    from quotemux import fact_ref_writes

    calls: list[tuple[str, list[tuple[object, ...]]]] = []

    def fake_execute_many(query: str, params: list[tuple[object, ...]]) -> bool:
        calls.append((query, params))
        return True

    def fake_query_dataframe(query: str, params: tuple[object, ...]) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"concept_id": "C231", "stock_market": "SHSE", "stock_code": "688583", "valid_from": "1900-01-01", "weight": None},
                {"concept_id": "C231", "stock_market": "SHSE", "stock_code": "688600", "valid_from": "1900-01-01", "weight": None},
            ]
        )

    monkeypatch.setattr(fact_ref_writes, "_ensure_concept_membership_table", lambda: True)
    monkeypatch.setattr(fact_ref_writes, "execute_many", fake_execute_many)
    monkeypatch.setattr(fact_ref_writes, "execute_sql", lambda query, params: True)
    monkeypatch.setattr(fact_ref_writes, "query_dataframe", fake_query_dataframe)

    assert fact_ref_writes._upsert_concept_members(
        [
            ConceptMemberItem(concept_id="C231", code="688583", name="SampleName"),
            ConceptMemberItem(concept_id="C231", code="688600", name=""),
        ]
    )

    assert "insert into ref.stock" in calls[0][0]
    assert calls[0][1] == [("SHSE", "688583", "SampleName")]
    assert "insert into ref.concept_stock_membership" in calls[1][0]
    assert calls[1][1] == [
        ("C231", "SHSE", "688583", "1900-01-01", None),
        ("C231", "SHSE", "688600", "1900-01-01", None),
    ]


def test_concept_member_filter_requires_stock_daily_for_concrete_dates(monkeypatch) -> None:
    from quotemux import fact_ref_writes

    captured: dict[str, object] = {}

    def fake_query_dataframe(query: str, params: tuple[object, ...]) -> pd.DataFrame:
        captured["query"] = query
        captured["params"] = params
        return pd.DataFrame()

    monkeypatch.setattr(fact_ref_writes, "query_dataframe", fake_query_dataframe)

    result = fact_ref_writes._filter_concept_member_params([("C14", "SHSE", "900901", "2026-07-10", None)])

    assert result == []
    query_text = str(captured["query"])
    assert "from fact.stock_daily_1d daily_rows" in query_text
    assert "from fact.concept_daily_1d" not in query_text
    assert captured["params"] == (["C14"], ["SHSE"], ["900901"], ["2026-07-10"], [None])


def test_index_daily_writer_upserts_known_catalog_metadata(monkeypatch) -> None:
    from quotemux import fact_ref_writes

    calls: list[tuple[str, list[tuple[object, ...]]]] = []

    def fake_execute_many(query: str, params: list[tuple[object, ...]]) -> bool:
        calls.append((query, params))
        return True

    monkeypatch.setattr(fact_ref_writes, "_existing_columns", lambda table_schema, table_name: set())
    monkeypatch.setattr(fact_ref_writes, "execute_many", fake_execute_many)

    assert fact_ref_writes._upsert_index_daily([IndexQuoteItem(index_code="000300", trade_time="2026-07-10", freq="1d", close=4020.0)])

    assert "insert into fact.index_bar_1d" in calls[0][0]
    assert "insert into ref.index" in calls[1][0]
    assert calls[1][1] == [("000300", "沪深300", "broad_market", "A_SHARE", "CSI", "", "active")]


def test_index_catalog_writer_uses_stable_known_names(monkeypatch) -> None:
    from quotemux import fact_ref_writes

    captured: dict[str, list[tuple[object, ...]]] = {}

    def fake_execute_many(query: str, params: list[tuple[object, ...]]) -> bool:
        captured["params"] = params
        return True

    monkeypatch.setattr(fact_ref_writes, "execute_many", fake_execute_many)

    assert fact_ref_writes._upsert_index_catalog([IndexCatalogItem(index_code="000001", index_name="????", category="", market="", publisher="", status="active")])

    assert captured["params"] == [("000001", "上证指数", "broad_market", "SHSE", "SSE", "", "active")]


def test_stock_market_maps_shanghai_b_shares_to_shse() -> None:
    from quotemux import fact_ref_writes
    from quotemux.infra.db import market_reads

    assert fact_ref_writes._stock_market("900901") == "SHSE"
    assert market_reads._stock_market("900901") == "SHSE"
    assert fact_ref_writes._stock_market("920117") == "BJSE"
    assert market_reads._stock_market("920117") == "BJSE"


def test_fact_ref_board_member_history_rebuilds_multiple_intervals(monkeypatch) -> None:
    from pandas import DataFrame
    from platform_models import BoardMemberHistoryItem
    from quotemux import fact_ref_writes

    captured: dict[str, object] = {}

    def fake_query_dataframe(query: str, params: tuple[object, ...]):
        captured["query_params"] = params
        return DataFrame(
            [
                {"board_code": "801010", "stock_market": "SZSE", "stock_code": "000001", "valid_from": "2020-01-01", "valid_to": "2021-06-30"},
                {"board_code": "801010", "stock_market": "SZSE", "stock_code": "000001", "valid_from": "2022-01-01", "valid_to": ""},
            ]
        )

    def fake_execute_sql(query: str, params: tuple[object, ...]) -> bool:
        captured["write_query"] = query
        captured["write_params"] = params
        return True

    monkeypatch.setattr(fact_ref_writes, "query_dataframe", fake_query_dataframe)
    monkeypatch.setattr(fact_ref_writes, "execute_sql", fake_execute_sql)

    items = [
        BoardMemberHistoryItem(board_code="801010", code="000001", name="", effective_date="2020-01-01", action="add"),
        BoardMemberHistoryItem(board_code="801010", code="000001", name="", effective_date="2021-06-30", action="remove"),
        BoardMemberHistoryItem(board_code="801010", code="000001", name="", effective_date="2022-01-01", action="add"),
    ]

    assert fact_ref_writes._upsert_board_member_history(items) is True
    assert "delete from ref.board_stock_membership" in str(captured["write_query"])
    assert "insert into ref.board_stock_membership" in str(captured["write_query"])
    assert captured["write_params"] == (
        ["801010", "801010"],
        ["SZSE", "SZSE"],
        ["000001", "000001"],
        ["2020-01-01", "2022-01-01"],
        ["2021-06-30", ""],
    )


def test_local_stock_5m_reads_matching_fact_frequency(monkeypatch) -> None:
    from pandas import DataFrame
    from quotemux import local_store

    captured: dict[str, object] = {}

    def fake_load_stock_intraday_frame(codes, start_time, end_time, freq):
        captured["codes"] = codes
        captured["freq"] = freq
        return DataFrame(
            [
                {
                    "code": "000001",
                    "trade_time": "2024-01-02 09:35:00",
                    "open": 10.0,
                    "high": 10.2,
                    "low": 9.9,
                    "close": 10.1,
                    "volume": 1000,
                    "amount": 10000.0,
                }
            ]
        )

    monkeypatch.setattr(local_store, "load_stock_intraday_frame", fake_load_stock_intraday_frame)

    items = local_store.get_local_stock_intraday_quotes(["000001"], "5m", "2024-01-02", "", "", "", "", None)

    assert captured == {"codes": ["000001"], "freq": "5m"}
    assert len(items) == 1
    assert items[0].freq == "5m"
    assert items[0].trade_time == "2024-01-02 09:35:00"


def test_fact_ref_stock_intraday_writes_only_complete_standard_1m_days(monkeypatch) -> None:
    from quotemux import fact_ref_writes
    from quotemux.common import EXPECTED_INTRADAY_BAR_TIMES

    calls: list[list[tuple[object, ...]]] = []

    def fake_execute_many(query: str, params: list[tuple[object, ...]], **_kwargs: object) -> bool:
        calls.append(params)
        return True

    complete_items = [
        StockQuoteItem(
            code="600000",
            trade_time=f"2026-07-20 {bar_time}",
            freq="1m",
            open=10.0,
            high=10.1,
            low=9.9,
            close=10.0,
            amount=1000.0,
        )
        for bar_time in EXPECTED_INTRADAY_BAR_TIMES["1m"]
    ]
    partial_items = [
        StockQuoteItem(
            code="000001",
            trade_time=f"2026-07-20 {bar_time}",
            freq="1m",
            open=11.0,
            high=11.1,
            low=10.9,
            close=11.0,
        )
        for bar_time in EXPECTED_INTRADAY_BAR_TIMES["1m"][:-1]
    ]
    opening_item = StockQuoteItem(
        code="600000",
        trade_time="2026-07-20 09:30:00",
        freq="1m",
        open=10.0,
        high=10.1,
        low=9.9,
        close=10.0,
    )
    monkeypatch.setattr(fact_ref_writes, "execute_many", fake_execute_many)
    monkeypatch.setattr(fact_ref_writes, "execute_many_with_migration_journal", fake_execute_many)
    monkeypatch.setattr(fact_ref_writes, "execute_sql", lambda query, params: True)

    assert fact_ref_writes._upsert_stock_intraday(complete_items + partial_items + [opening_item])

    assert len(calls[0]) == 240
    assert {params[1] for params in calls[0]} == {"600000"}
    assert {str(params[2])[11:19] for params in calls[0]} == set(EXPECTED_INTRADAY_BAR_TIMES["1m"])
    assert calls[1] == []
    assert calls[2] == [("quotemux.fact_ref_writer.complete_standard_grid", "2026-07-20 09:31:00", "2026-07-20 15:00:00", 240)]


def test_fact_ref_stock_intraday_rejects_day_with_missing_ohlc(monkeypatch) -> None:
    from quotemux import fact_ref_writes
    from quotemux.common import EXPECTED_INTRADAY_BAR_TIMES

    calls: list[list[tuple[object, ...]]] = []

    def fake_execute_many(query: str, params: list[tuple[object, ...]], **_kwargs: object) -> bool:
        calls.append(params)
        return True

    items = [
        StockQuoteItem(
            code="600000",
            trade_time=f"2026-07-20 {bar_time}",
            freq="1m",
            open=10.0,
            high=10.1,
            low=9.9,
            close=None if index == 0 else 10.0,
        )
        for index, bar_time in enumerate(EXPECTED_INTRADAY_BAR_TIMES["1m"])
    ]
    monkeypatch.setattr(fact_ref_writes, "execute_many", fake_execute_many)
    monkeypatch.setattr(fact_ref_writes, "execute_many_with_migration_journal", fake_execute_many)
    monkeypatch.setattr(fact_ref_writes, "execute_sql", lambda query, params: True)

    assert fact_ref_writes._upsert_stock_intraday(items)

    assert calls == [[], []]


def test_stock_daily_writer_repairs_missing_listed_date(monkeypatch) -> None:
    from quotemux import fact_ref_writes

    execute_many_calls: list[tuple[str, list[tuple[object, ...]]]] = []
    execute_sql_calls: list[tuple[str, tuple[object, ...]]] = []

    def fake_execute_many(query: str, params: list[tuple[object, ...]]) -> bool:
        execute_many_calls.append((query, params))
        return True

    def fake_execute_sql(query: str, params: tuple[object, ...] = ()) -> bool:
        execute_sql_calls.append((query, params))
        return True

    monkeypatch.setattr(fact_ref_writes, "_existing_columns", lambda table_schema, table_name: set())
    monkeypatch.setattr(fact_ref_writes, "execute_many", fake_execute_many)
    monkeypatch.setattr(fact_ref_writes, "execute_sql", fake_execute_sql)

    assert fact_ref_writes._upsert_stock_daily(
        [
            StockQuoteItem(code="001248", trade_time="2026-07-02", freq="1d", close=10.0),
            StockQuoteItem(code="001248", trade_time="2026-07-02", freq="1d", close=10.0),
        ]
    )

    assert "insert into fact.stock_daily_1d" in execute_many_calls[0][0]
    assert "insert into ref.stock" in execute_sql_calls[0][0]
    assert "update ref.stock stock_ref" in execute_sql_calls[1][0]
    assert "update fact.stock_daily_1d target" in execute_sql_calls[2][0]
    assert [call[1] for call in execute_sql_calls] == [(["001248"],), (["001248"],), (["001248"],)]


def test_stock_daily_metrics_repair_uses_first_day_fallback(monkeypatch) -> None:
    from quotemux import fact_ref_writes

    captured: dict[str, object] = {}

    def fake_execute_sql(query: str, params: tuple[object, ...] = ()) -> bool:
        captured["query"] = query
        captured["params"] = params
        return True

    monkeypatch.setattr(fact_ref_writes, "execute_sql", fake_execute_sql)

    assert fact_ref_writes._repair_stock_daily_metrics(["900945"])

    query_text = str(captured["query"])
    assert "coalesce(target.pre_close, metric_rows.previous_close, metric_rows.close)" in query_text
    assert "metric_rows.close - coalesce(metric_rows.previous_close, metric_rows.close)" in query_text
    assert captured["params"] == (["900945"],)


def test_intraday_ohlc_normalization_expands_price_envelope() -> None:
    from quotemux import fact_ref_writes

    assert fact_ref_writes._normalized_ohlc(69.05, 69.81, 69.12, 69.13, 200, 1388606.6) == (69.05, 69.81, 69.05, 69.13)
    assert fact_ref_writes._normalized_ohlc(2.90, 2.86, 2.86, 2.86, 1503, 433935.2) == (2.90, 2.90, 2.86, 2.86)


def test_load_concept_daily_frame_qualifies_joined_columns(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_query_dataframe(query: str, params: tuple[object, ...]) -> pd.DataFrame:
        captured["query"] = query
        captured["params"] = params
        return pd.DataFrame()

    monkeypatch.setattr("quotemux.infra.db.market_reads.query_dataframe", fake_query_dataframe)
    monkeypatch.setattr("quotemux.infra.db.market_reads._table_exists", lambda table_schema, table_name: True)
    monkeypatch.setattr(
        "quotemux.infra.db.market_reads._existing_columns",
        lambda table_schema, table_name: {"concept_id", "trade_date", "open", "high", "low", "close", "volume", "amount"},
    )

    load_concept_daily_frame(["C231"], "2026-06-12", "2026-06-15")

    query_text = str(captured["query"])
    assert "day_rows.concept_id = any(%s)" in query_text
    assert "from fact.concept_daily_1d day_rows" in query_text
    assert "left join ref.concept concept_ref on concept_ref.concept_id = day_rows.concept_id" in query_text
    assert "lag(day_rows.close) over (partition by day_rows.concept_id order by day_rows.trade_date) as previous_close" in query_text


def test_concept_daily_writer_skips_when_fact_table_missing(monkeypatch) -> None:
    from quotemux import fact_ref_writes

    called = False

    def fake_execute_many(query: str, params: list[tuple[object, ...]]) -> bool:
        nonlocal called
        called = True
        return True

    monkeypatch.setattr(fact_ref_writes, "_table_exists", lambda table_schema, table_name: False)
    monkeypatch.setattr(fact_ref_writes, "execute_many", fake_execute_many)

    result = fact_ref_writes._upsert_concept_daily([ConceptQuoteItem(concept_id="C231", trade_time="2026-06-26", freq="1d", close=1000.0)])

    assert result is False
    assert called is False

def test_stock_catalog_writer_normalizes_provider_market_values(monkeypatch) -> None:
    from quotemux import fact_ref_writes

    captured: dict[str, list[tuple[object, ...]]] = {}

    def fake_execute_many(query: str, params: list[tuple[object, ...]]) -> bool:
        captured["params"] = params
        return True

    monkeypatch.setattr(fact_ref_writes, "execute_many", fake_execute_many)

    assert fact_ref_writes._upsert_stock_catalog(
        [
            StockBasicInfo(code="688669", name="聚石化学", exchange="", market="star_market", list_status="listed", list_date="2021-01-25", delist_date="", industry="化工原料", area="广东"),
            StockBasicInfo(code="920028", name="新恒泰", exchange="", market="beijing", list_status="listed", list_date="2026-03-20", delist_date="", industry="塑料", area="浙江"),
            StockBasicInfo(code="301001", name="样例创业板", exchange="", market="chi_next", list_status="listed", list_date="2021-01-01", delist_date="", industry="", area=""),
        ]
    )

    assert captured["params"] == [
        ("SHSE", "688669", "聚石化学", "化工原料", "star_market", "2021-01-25", "", "广东"),
        ("BJSE", "920028", "新恒泰", "塑料", "beijing", "2026-03-20", "", "浙江"),
        ("SZSE", "301001", "样例创业板", "", "chi_next", "2021-01-01", "", ""),
    ]


def test_stock_catalog_writer_keeps_board_type_compatible(monkeypatch) -> None:
    from quotemux import fact_ref_writes

    captured: dict[str, object] = {}

    def fake_execute_many(query: str, params: list[tuple[object, ...]]) -> bool:
        captured["query"] = query
        captured["params"] = params
        return True

    monkeypatch.setattr(fact_ref_writes, "_existing_columns", lambda table_schema, table_name: {"board_type"} if table_schema == "ref" and table_name == "stock" else set())
    monkeypatch.setattr(fact_ref_writes, "execute_many", fake_execute_many)

    assert fact_ref_writes._upsert_stock_catalog(
        [StockBasicInfo(code="920028", name="新恒泰", exchange="", market="beijing", list_status="listed", list_date="2026-03-20", delist_date="", industry="塑料", area="浙江")]
    )

    assert "board_type" in str(captured["query"])
    assert captured["params"] == [("BJSE", "920028", "新恒泰", "塑料", "beijing", "2026-03-20", "", "浙江", "beijing")]


def test_local_index_quotes_preserve_daily_pre_close(monkeypatch) -> None:
    monkeypatch.setattr(
        "quotemux.local_store.load_index_daily_frame",
        lambda index_codes, start_date, end_date: pd.DataFrame(
            [
                {
                    "index_code": "399006",
                    "trade_time": "2026-06-15",
                    "open": 3896.17,
                    "high": 4033.53,
                    "low": 3844.08,
                    "close": 4033.53,
                    "pre_close": 3844.08,
                    "change": 189.45,
                    "pct_chg": pd.NA,
                    "volume": 2286512.34,
                    "amount": 796207492760.91,
                }
            ]
        ),
    )

    items = get_local_index_quotes(["399006"], "1d", "2026-06-15", "", "", None)

    assert len(items) == 1
    assert items[0].pre_close == 3844.08
    assert items[0].change == pytest.approx(189.45, rel=1e-6)
    assert items[0].pct_chg is None


def test_local_stock_quotes_derive_missing_daily_change_fields(monkeypatch) -> None:
    def fake_load_stock_daily_frame(codes, start_date, end_date):
        assert codes == ["001270"]
        assert start_date == "2026-06-17"
        assert end_date == "2026-06-17"
        return pd.DataFrame(
            [
                {
                    "code": "001270",
                    "trade_time": "2026-06-17",
                    "open": 110.0,
                    "high": 128.0,
                    "low": 109.0,
                    "close": 127.78,
                    "pre_close": 100.0,
                    "change": 27.78,
                    "pct_chg": 27.78,
                    "volume": 2000.0,
                    "amount": 1537780.55493,
                },
            ]
        )

    monkeypatch.setattr("quotemux.local_daily.load_stock_daily_frame", fake_load_stock_daily_frame)
    from quotemux.local_daily import get_stock_quotes

    items = get_stock_quotes(["001270"], "1d", "2026-06-17", "", "", "", "", None, "none")

    assert len(items) == 1
    assert items[0].pre_close == 100.0
    assert items[0].change == pytest.approx(27.78)
    assert items[0].pct_chg == pytest.approx(27.78)


def test_local_stock_quotes_build_qfq_from_raw_prices_and_adjustment_factor(monkeypatch) -> None:
    def fake_load_stock_daily_frame(codes, start_date, end_date):
        assert codes == ["600000"]
        return pd.DataFrame(
            [
                {
                    "code": "600000",
                    "trade_time": "2024-01-02",
                    "open": 10.0,
                    "high": 11.0,
                    "low": 9.0,
                    "close": 10.0,
                    "adj_factor": 2.0,
                    "volume": 100.0,
                    "amount": 1000.0,
                    "is_suspended": False,
                    "is_st": False,
                },
                {
                    "code": "600000",
                    "trade_time": "2024-01-03",
                    "open": 12.0,
                    "high": 13.0,
                    "low": 11.0,
                    "close": 12.0,
                    "adj_factor": 3.0,
                    "volume": 100.0,
                    "amount": 1200.0,
                    "is_suspended": False,
                    "is_st": False,
                },
            ]
        )

    monkeypatch.setattr("quotemux.local_daily.load_stock_daily_frame", fake_load_stock_daily_frame)
    from quotemux.local_daily import get_stock_quotes

    items = get_stock_quotes(["600000"], "1d", "", "2024-01-02", "2024-01-03", "", "", None, "qfq")

    assert items[0].trade_time == "2024-01-02"
    assert items[0].close == pytest.approx(20.0 / 3.0)
    assert items[1].trade_time == "2024-01-03"
    assert items[1].close == pytest.approx(12.0)
    assert items[1].pre_close == pytest.approx(20.0 / 3.0)
    assert items[1].pct_chg == pytest.approx(80.0)


def test_local_stock_quotes_qfq_uses_frozen_adjustment_base_date(monkeypatch) -> None:
    def fake_load_stock_daily_frame(codes, start_date, end_date):
        assert codes == ["600000"]
        return pd.DataFrame(
            [
                {"code": "600000", "trade_time": "2024-01-02", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "adj_factor": 2.0, "volume": 1.0, "amount": 1.0},
                {"code": "600000", "trade_time": "2024-01-03", "open": 12.0, "high": 12.0, "low": 12.0, "close": 12.0, "adj_factor": 3.0, "volume": 1.0, "amount": 1.0},
            ]
        )

    def fake_load_base_frame(codes, base_date):
        assert codes == ["600000"]
        assert base_date == "2024-01-31"
        return pd.DataFrame([{"code": "600000", "adjustment_base_factor": 4.0}])

    monkeypatch.setattr("quotemux.local_daily.load_stock_daily_frame", fake_load_stock_daily_frame)
    monkeypatch.setattr("quotemux.local_daily.load_stock_adjustment_base_factor_frame", fake_load_base_frame)
    from quotemux.local_daily import get_stock_quotes

    short_window = get_stock_quotes(["600000"], "1d", "", "2024-01-02", "2024-01-02", "", "", None, "qfq", "2024-01-31")
    long_window = get_stock_quotes(["600000"], "1d", "", "2024-01-02", "2024-01-03", "", "", None, "qfq", "2024-01-31")

    assert short_window[0].close == pytest.approx(5.0)
    assert long_window[0].close == pytest.approx(5.0)


@pytest.mark.parametrize("adjust", ["qfq", "hfq"])
def test_adjusted_daily_quotes_do_not_fetch_missing_factors_in_request(monkeypatch, adjust: str) -> None:
    from quotemux.stocks import QuoteMuxStocks

    captured_base_dates: list[str] = []

    def fake_local_quotes(*args):
        captured_base_dates.append(args[-1])
        return []

    monkeypatch.setenv("QUOTEMUX_ADJUSTMENT_BASE_DATE", "2024-01-31")
    monkeypatch.setattr("quotemux.stocks.get_local_stock_quotes", fake_local_quotes)
    monkeypatch.setattr("quotemux.stocks._build_missing_quote_requests", lambda *args: [])
    monkeypatch.setattr("quotemux.stocks._build_local_daily_query_result", lambda *args: ("local-only", ContractReport.empty("stocks.quotes.daily")))
    monkeypatch.setattr("quotemux.stocks.execute_capability_query", lambda *args: (_ for _ in ()).throw(AssertionError("调整行情不得在请求内回源")))

    result, _ = QuoteMuxStocks(QuoteMuxSettings(enabled_sources=("tushare",))).get_quotes_query_result_with_report(
        StockQuotesRequest(codes=["600000"], freq="1d", start_date="2024-01-02", end_date="2024-01-03", adjust=adjust)
    )

    assert result == "local-only"
    assert captured_base_dates == ["2024-01-31"]


def test_adjusted_daily_quote_hydration_requests_each_real_factor_range_once(monkeypatch) -> None:
    from quotemux.stocks import QuoteMuxStocks

    calls: list[tuple[str, str, str, str]] = []
    stocks = QuoteMuxStocks(QuoteMuxSettings(enabled_sources=("tushare",)))

    def fake_get_adj_factors(code: str, start_date: str, end_date: str, base_date: str):
        calls.append((code, start_date, end_date, base_date))
        return []

    monkeypatch.setattr(stocks, "get_adj_factors", fake_get_adj_factors)

    stocks._hydrate_missing_daily_adjustment_factors(
        [(["600000", "600000.SH"], "2024-01-02", "2024-01-03"), (["000001.SZ"], "", "2024-01-03")],
        "2024-01-31",
    )

    assert calls == [("600000", "2024-01-02", "2024-01-03", "2024-01-31")]


def test_local_stock_quotes_carry_forward_latest_adjustment_factor(monkeypatch) -> None:
    def fake_load_stock_daily_frame(codes, start_date, end_date):
        assert codes == ["600000"]
        return pd.DataFrame(
            [
                {
                    "code": "600000",
                    "trade_time": "2024-01-02",
                    "open": 10.0,
                    "high": 11.0,
                    "low": 9.0,
                    "close": 10.0,
                    "adj_factor": 2.0,
                    "volume": 100.0,
                    "amount": 1000.0,
                    "is_suspended": False,
                    "is_st": False,
                },
                {
                    "code": "600000",
                    "trade_time": "2024-01-03",
                    "open": 12.0,
                    "high": 13.0,
                    "low": 11.0,
                    "close": 12.0,
                    "adj_factor": None,
                    "volume": 100.0,
                    "amount": 1200.0,
                    "is_suspended": False,
                    "is_st": False,
                },
            ]
        )

    monkeypatch.setattr("quotemux.local_daily.load_stock_daily_frame", fake_load_stock_daily_frame)
    from quotemux.local_daily import get_stock_quotes

    items = get_stock_quotes(["600000"], "1d", "", "2024-01-02", "2024-01-03", "", "", None, "qfq")

    assert [item.trade_time for item in items] == ["2024-01-02", "2024-01-03"]
    assert [item.close for item in items] == [pytest.approx(10.0), pytest.approx(12.0)]


def test_local_stock_quotes_reject_adjusted_window_without_factors(monkeypatch) -> None:
    monkeypatch.setattr(
        "quotemux.local_daily.load_stock_daily_frame",
        lambda *args: pd.DataFrame([{"code": "600000", "trade_time": "2024-01-02", "close": 10.0}]),
    )
    from quotemux.local_daily import get_stock_quotes

    assert get_stock_quotes(["600000"], "1d", "", "2024-01-02", "2024-01-02", "", "", None, "qfq") == []


def test_tushare_stock_daily_amount_uses_yuan_unit(monkeypatch) -> None:
    from quotemux_packages.tushare import source

    def fake_call_tushare_api(api_name, fetcher, **kwargs):
        del api_name, fetcher, kwargs
        return pd.DataFrame(
            [
                {
                    "ts_code": "600000.SH",
                    "trade_date": "20260403",
                    "open": 10.0,
                    "high": 11.0,
                    "low": 9.8,
                    "close": 10.5,
                    "pre_close": 10.0,
                    "change": 0.5,
                    "pct_chg": 5.0,
                    "vol": 1000.0,
                    "amount": 1234.5,
                }
            ]
        )

    class _Ts:
        def set_token(self, api_key):
            del api_key

        def pro_bar(self, **kwargs):
            return kwargs

    monkeypatch.setattr(source, "ts", _Ts())
    monkeypatch.setattr(source, "get_provider_api_key", lambda: "token")
    monkeypatch.setattr(source, "call_tushare_api", fake_call_tushare_api)

    frame = source._fetch_stock_quotes_frame("600000", "1d", pd.Timestamp("2026-04-03"), pd.Timestamp("2026-04-03"), "none")
    items = source._frame_to_stock_quotes(frame, "1d")

    assert len(items) == 1
    assert items[0].amount == pytest.approx(1234500.0)


def test_tushare_stock_name_history_supports_full_market_capture(monkeypatch) -> None:
    from quotemux_packages.tushare import source

    class _Pro:
        namechange = object()

    captured_kwargs: dict[str, object] = {}

    def fake_call_tushare_api(api_name, fetcher, **kwargs):
        del api_name, fetcher
        captured_kwargs.update(kwargs)
        return pd.DataFrame(
            [
                {
                    "ts_code": "600000.SH",
                    "name": "浦发银行",
                    "start_date": "19991110",
                    "end_date": "",
                    "ann_date": "19991110",
                },
                {
                    "ts_code": "000001.SZ",
                    "name": "平安银行",
                    "start_date": "19910403",
                    "end_date": "",
                    "ann_date": "19910403",
                },
            ]
        )

    monkeypatch.setattr(source, "get_ts_pro", lambda: _Pro())
    monkeypatch.setattr(source, "call_tushare_api", fake_call_tushare_api)

    items = source.get_stock_name_history("", "", "")

    assert captured_kwargs == {"limit": 5000, "offset": 0}
    assert [(item.code, item.name) for item in items] == [("000001", "平安银行"), ("600000", "浦发银行")]


def test_tushare_stock_name_history_full_market_capture_reads_all_pages(monkeypatch) -> None:
    from quotemux_packages.tushare import source

    class _Pro:
        namechange = object()

    calls: list[tuple[int, int]] = []

    def fake_call_tushare_api(api_name, fetcher, **kwargs):
        del api_name, fetcher
        limit = int(kwargs["limit"])
        offset = int(kwargs["offset"])
        calls.append((limit, offset))
        row_count = 5000 if offset == 0 else 1
        return pd.DataFrame(
            [
                {
                    "ts_code": f"{index % 1000000:06d}.SZ",
                    "name": f"名称{index}",
                    "start_date": "20000101",
                    "end_date": "",
                    "ann_date": "20000101",
                }
                for index in range(offset + 1, offset + row_count + 1)
            ]
        )

    monkeypatch.setattr(source, "get_ts_pro", lambda: _Pro())
    monkeypatch.setattr(source, "call_tushare_api", fake_call_tushare_api)

    items = source.get_stock_name_history("", "", "")

    assert calls == [(5000, 0), (5000, 5000)]
    assert len(items) == 5001


def test_tushare_stock_money_flow_uses_yuan_unit(monkeypatch) -> None:
    from quotemux_packages.tushare import source

    class _Pro:
        moneyflow = object()

    def fake_call_tushare_api(api_name, fetcher, **kwargs):
        del api_name, fetcher, kwargs
        return pd.DataFrame(
            [
                {
                    "trade_date": "20260403",
                    "buy_sm_amount": 1.0,
                    "buy_md_amount": 2.0,
                    "buy_lg_amount": 10.0,
                    "buy_elg_amount": 20.0,
                    "sell_lg_amount": 5.0,
                    "sell_elg_amount": 7.0,
                    "net_mf_amount": 18.0,
                }
            ]
        )

    monkeypatch.setattr(source, "get_ts_pro", lambda: _Pro())
    monkeypatch.setattr(source, "call_tushare_api", fake_call_tushare_api)

    frame = source._fetch_money_flow_frame("600000", "20260403", "20260403", "main")

    assert frame["main_inflow"].iloc[0] == pytest.approx(300000.0)
    assert frame["main_outflow"].iloc[0] == pytest.approx(120000.0)
    assert frame["net_inflow"].iloc[0] == pytest.approx(180000.0)
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

    items = stocks.get_stock_daily_basic("", "", "2026-04-03", "", "")

    assert items[0].free_share == 70000.0


def test_tushare_market_main_capital_flow_keeps_provider_unit(monkeypatch) -> None:
    from quotemux_packages.tushare import source

    class _Pro:
        moneyflow_mkt_dc = object()

    def fake_call_tushare_api(api_name, fetcher, **kwargs):
        del api_name, fetcher, kwargs
        return pd.DataFrame(
            [
                {
                    "trade_date": "20260403",
                    "buy_lg_amount": 100.0,
                    "buy_elg_amount": 200.0,
                    "sell_lg_amount": 50.0,
                    "sell_elg_amount": 70.0,
                    "net_amount": 180.0,
                }
            ]
        )

    monkeypatch.setattr(source, "get_ts_pro", lambda: _Pro())
    monkeypatch.setattr(source, "call_tushare_api", fake_call_tushare_api)

    frame = source._fetch_market_capital_flow_frame("20260403", "20260403")

    assert frame["main_inflow"].iloc[0] == pytest.approx(300.0)
    assert frame["main_outflow"].iloc[0] == pytest.approx(120.0)
    assert frame["net_inflow"].iloc[0] == pytest.approx(180.0)


def test_stock_money_flow_batch_fetches_missing_cached_codes(monkeypatch) -> None:
    cached_item = StockMoneyFlowItem(code="600000", trade_date="2026-04-03", view="main", net_inflow=100.0)
    fetched_items = [
        StockMoneyFlowItem(code="600000", trade_date="2026-04-03", view="main", net_inflow=100.0),
        StockMoneyFlowItem(code="600001", trade_date="2026-04-03", view="main", net_inflow=200.0),
    ]
    calls: list[tuple[str, str, tuple[object, ...]]] = []
    runtime = QuoteMux(QuoteMuxSettings(enabled_sources=("tushare",)))

    monkeypatch.setattr(
        "quotemux.query_engine.load_store_result",
        lambda capability_id, request_identity, model_type: ([cached_item], _FakeStoreRead(status="hit", hit=True)),
    )

    fake_source_call = _source_call_stub(
        {("tushare", "get_stock_money_flow_batch"): fetched_items},
        calls,
    )
    with patch("quotemux.stocks._source_package_call", side_effect=fake_source_call):
        items = runtime.stocks.get_money_flow_batch("600000,600001", "2026-04-03", "main")

    assert [item.code for item in items] == ["600000", "600001"]
    assert ("tushare", "get_stock_money_flow_batch", ("600001", "2026-04-03", "main")) in calls


def test_stock_money_flow_batch_splits_large_missing_requests(monkeypatch) -> None:
    requested_codes = [f"600{index:03d}" for index in range(25)]
    calls: list[tuple[str, str, tuple[object, ...]]] = []
    runtime = QuoteMux(QuoteMuxSettings(enabled_sources=("tushare",)))

    def fake_batch(codes: str, trade_date: str, view: str) -> list[StockMoneyFlowItem]:
        return [
            StockMoneyFlowItem(code=code, trade_date=trade_date, view=view, net_inflow=100.0)
            for code in codes.split(",")
        ]

    fake_source_call = _source_call_stub(
        {("tushare", "get_stock_money_flow_batch"): fake_batch},
        calls,
    )
    with patch("quotemux.stocks._source_package_call", side_effect=fake_source_call):
        items = runtime.stocks.get_money_flow_batch(",".join(requested_codes), "2026-04-03", "main")

    assert len(items) == 25
    assert [
        call[2][0]
        for call in calls
        if call[0] == "tushare" and call[1] == "get_stock_money_flow_batch"
    ] == [
        ",".join(requested_codes[:10]),
        ",".join(requested_codes[10:20]),
        ",".join(requested_codes[20:]),
    ]


def test_akshare_index_daily_frame_uses_non_em_history_and_filters_range() -> None:
    from quotemux_packages.akshare import source as akshare_source

    fake_frame = pd.DataFrame(
        [
            {"date": "2026-05-31", "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 10.0},
            {"date": "2026-06-01", "open": 2.0, "high": 3.0, "low": 1.5, "close": 2.5, "volume": 20.0},
            {"date": "2026-06-16", "open": 4.0, "high": 5.0, "low": 3.5, "close": 4.5, "volume": 40.0},
            {"date": "2026-06-17", "open": 6.0, "high": 6.0, "low": 5.5, "close": 5.8, "volume": 60.0},
        ]
    )
    setattr(akshare_source.ak, "stock_zh_index_daily", object())
    with patch.object(akshare_source, "_call_ak", return_value=fake_frame) as mocked_call:
        frame = akshare_source._fetch_index_daily_frame("000001", "1d", pd.Timestamp("2026-06-01"), pd.Timestamp("2026-06-16"))

    assert mocked_call.call_args.args[0] == "stock_zh_index_daily"
    assert frame["trade_time"].dt.strftime("%Y-%m-%d").tolist() == ["2026-06-01", "2026-06-16"]
    assert frame["amount"].isna().all()


def test_stock_quote_provider_batch_matches_capture_batch_size() -> None:
    from quotemux import stocks as stocks_module

    codes = [f"600{index:03d}" for index in range(45)]

    assert [len(batch) for batch in stocks_module._chunk_quote_codes(codes)] == [20, 20, 5]


def test_akshare_index_daily_frame_keeps_previous_day_for_single_trade_date() -> None:
    from quotemux_packages.akshare import source as akshare_source

    fake_frame = pd.DataFrame(
        [
            {"date": "2026-06-12", "open": 1.0, "high": 1.0, "low": 1.0, "close": 100.0, "volume": 10.0},
            {"date": "2026-06-15", "open": 2.0, "high": 2.0, "low": 2.0, "close": 110.0, "volume": 20.0},
        ]
    )
    setattr(akshare_source.ak, "stock_zh_index_daily", object())
    with patch.object(akshare_source, "_call_ak", return_value=fake_frame):
        frame = akshare_source._fetch_index_daily_frame("000001", "1d", pd.Timestamp("2026-06-15"), pd.Timestamp("2026-06-15"))

    assert frame["trade_time"].dt.strftime("%Y-%m-%d").tolist() == ["2026-06-12", "2026-06-15"]


def test_akshare_board_quote_frame_uses_catalog_symbol(monkeypatch) -> None:
    from quotemux_packages.akshare import source as akshare_source

    fake_frame = pd.DataFrame(
        [
            {"日期": "2026-06-15", "开盘": 1.0, "最高": 2.0, "最低": 0.5, "收盘": 1.5, "成交量": 100.0, "成交额": 200.0},
        ]
    )
    monkeypatch.setattr(akshare_source, "_board_symbol_and_category", lambda board_code: ("新能源", "concept"))
    setattr(akshare_source.ak, "stock_board_concept_hist_em", object())
    with patch.object(akshare_source, "_call_ak", return_value=fake_frame) as mocked_call:
        frame = akshare_source._fetch_board_quote_frame("BK0493", "1d", pd.Timestamp("2026-06-15"), pd.Timestamp("2026-06-15"))

    assert mocked_call.call_args.kwargs["symbol"] == "新能源"
    assert frame["board_code"].tolist() == ["BK0493"]


def test_tushare_board_quotes_map_bk_code_inside_provider(monkeypatch) -> None:
    from quotemux_packages.tushare import source as tushare_source

    captured: dict[str, object] = {}

    def fake_query_dataframe(query: str, params: tuple[object, ...]) -> pd.DataFrame:
        captured["query_params"] = params
        return pd.DataFrame([{"name": "半导体设备", "board_type": "industry"}])

    def fake_load_catalog(index_type: str) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"board_code": "884229", "name": "半导体设备", "category": "", "status": "active"},
            ]
        )

    def fake_fetch(provider_code: str, start_value: str, end_value: str, output_board_code: str = "") -> pd.DataFrame:
        captured["fetch_args"] = (provider_code, start_value, end_value, output_board_code)
        return pd.DataFrame(
            [
                {
                    "board_code": output_board_code,
                    "trade_time": pd.Timestamp("2026-06-18"),
                    "open": 269839.21,
                    "high": 280192.38,
                    "low": 269675.54,
                    "close": 278864.12,
                    "pre_close": 269713.32,
                    "change": 9150.80,
                    "pct_chg": 3.3928,
                    "volume": 3394859.2,
                    "amount": 61119095781.36,
                }
            ]
        )

    monkeypatch.setattr("quotemux.infra.db.client.query_dataframe", fake_query_dataframe)
    monkeypatch.setattr(tushare_source, "_load_board_catalog_frame", fake_load_catalog)
    monkeypatch.setattr(tushare_source, "read_cache_frame", lambda path: pd.DataFrame())
    monkeypatch.setattr(tushare_source, "write_cache_frame", lambda path, df: None)
    monkeypatch.setattr(tushare_source, "_fetch_board_quotes_frame", fake_fetch)

    items = tushare_source.get_board_quotes(["BK1326"], "1d", "2026-06-18", "", "", "", "", None)

    assert captured["query_params"] == ("BK1326",)
    assert captured["fetch_args"] == ("884229", "20260618", "20260618", "BK1326")
    assert len(items) == 1
    assert items[0].board_code == "BK1326"
    assert items[0].pre_close == 269713.32
    assert items[0].change == 9150.80
    assert items[0].pct_chg == 3.3928
    assert items[0].amount == 61119095781.36


def test_tushare_board_quote_frame_keeps_daily_metrics(monkeypatch) -> None:
    from quotemux_packages.tushare import source as tushare_source

    class FakePro:
        ths_daily = object()

    fake_frame = pd.DataFrame(
        [
            {
                "trade_date": "20260618",
                "open": 269839.21,
                "high": 280192.38,
                "low": 269675.54,
                "close": 278864.12,
                "pre_close": 269713.32,
                "change": 9150.80,
                "pct_change": 3.3928,
                "vol": 3394859.2,
                "avg_price": 179.9008,
            }
        ]
    )

    monkeypatch.setattr(tushare_source, "get_ts_pro", lambda: FakePro())
    monkeypatch.setattr(tushare_source, "call_tushare_api", lambda api_name, fetcher, **kwargs: fake_frame)

    frame = tushare_source._fetch_board_quotes_frame("884229", "20260618", "20260618", "BK1326")

    assert frame["board_code"].tolist() == ["BK1326"]
    assert frame["pre_close"].tolist() == [269713.32]
    assert frame["change"].tolist() == [9150.80]
    assert frame["pct_chg"].tolist() == [3.3928]
    assert frame["amount"].iloc[0] == pytest.approx(179.9008 * 3394859.2 * 100)


def test_akshare_index_quotes_keeps_previous_row_for_single_count(monkeypatch) -> None:
    from quotemux_packages.akshare import source as akshare_source

    frame = pd.DataFrame(
        [
            {"index_code": "000001", "trade_time": pd.Timestamp("2026-06-12"), "freq": "1d", "open": 1.0, "high": 1.0, "low": 1.0, "close": 100.0, "volume": 10.0, "amount": pd.NA},
            {"index_code": "000001", "trade_time": pd.Timestamp("2026-06-15"), "freq": "1d", "open": 2.0, "high": 2.0, "low": 2.0, "close": 110.0, "volume": 20.0, "amount": pd.NA},
        ]
    )
    monkeypatch.setattr(akshare_source, "read_cache_frame", lambda path: pd.DataFrame())
    monkeypatch.setattr(akshare_source, "write_cache_frame", lambda path, df: None)
    monkeypatch.setattr(akshare_source, "_fetch_index_daily_frame", lambda index_code, freq, start_dt, end_dt: frame)

    items = akshare_source.get_index_quotes(["000001"], "1d", "2026-06-15", "", "", 1)

    assert len(items) == 1
    assert items[0].trade_time == "2026-06-15"
    assert items[0].pre_close == 100.0
    assert items[0].pct_chg == pytest.approx(10.0)


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


def test_akshare_connect_flow_builds_northbound_from_connect_legs(monkeypatch) -> None:
    from quotemux_packages.akshare import source as akshare_source

    frames = {
        "沪股通": pd.DataFrame([{"日期": "2026-04-03", "买入成交额": 100.0, "卖出成交额": 80.0, "当日成交净买额": 20.0}]),
        "深股通": pd.DataFrame([{"日期": "2026-04-03", "买入成交额": 70.0, "卖出成交额": 60.0, "当日成交净买额": 10.0}]),
    }

    def fake_call_ak(api_name, func, **kwargs):
        del api_name
        del func
        return frames.get(kwargs["symbol"], pd.DataFrame())

    monkeypatch.setattr(akshare_source.ak, "stock_hsgt_hist_em", object(), raising=False)
    monkeypatch.setattr(akshare_source, "_call_ak", fake_call_ak)

    items = akshare_source.get_connect_capital_flow("2026-04-03", "", "")
    northbound = [item for item in items if item.market == "northbound"][0]

    assert northbound.buy_amount == 170.0
    assert northbound.sell_amount == 140.0
    assert northbound.net_amount == 30.0


def test_api_model_dump_converts_non_finite_float_to_none() -> None:
    item = IndexQuoteItem(index_code="899050", trade_time="2026-06-18", freq="1d", amount=float("nan"))

    payload = item.model_dump()

    assert payload["amount"] is None


def test_connect_flow_fills_provider_direct_amounts_after_partial_cache(monkeypatch) -> None:
    cached_item = ConnectCapitalFlowItem(trade_date="2026-04-03", market="northbound", net_amount=30.0)
    akshare_item = ConnectCapitalFlowItem(trade_date="2026-04-03", market="northbound", buy_amount=170.0, sell_amount=140.0, net_amount=30.0)

    monkeypatch.setattr("quotemux.query_engine.load_store_result", lambda capability_id, request_identity, model_type: ([cached_item], _FakeStoreRead(status="hit", hit=True)))
    monkeypatch.setattr("quotemux.query_engine.store_result", lambda capability_id, request_identity, items, report, quarantine_count=0: _FakeStoreWrite(status="write"))

    runtime = QuoteMux(QuoteMuxSettings(enabled_sources=("tushare", "akshare")))
    fake_source_call = _source_call_stub(
        {
            ("tushare", "get_connect_capital_flow"): [],
            ("akshare", "get_connect_capital_flow"): [akshare_item],
        }
    )

    with patch("quotemux.markets._source_package_call", side_effect=fake_source_call):
        items = runtime.markets.get_connect_capital_flow("2026-04-03", "", "")

    northbound = [item for item in items if item.market == "northbound"][0]
    assert northbound.buy_amount == 170.0
    assert northbound.sell_amount == 140.0
    assert northbound.net_amount == 30.0


def test_connect_flow_uses_runtime_instance_order_for_provider_direct_amounts(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("QUOTEMUX_RUNTIME_ROOT", str(tmp_path))
    reset_config_runtime_cache()
    config_runtime = QuoteMuxConfigRuntime()
    config_runtime.ensure_initialized()
    RuntimeConfigStore(tmp_path).write_draft_policies(
        (
            ContractPolicyOverride(
                contract_name="markets.connect.capital_flow",
                mode="auto",
                source_order=("akshare-default", "tushare-default"),
                merge_strategy="append_dedupe",
            ),
        )
    )
    config_runtime.publish_profile("connect flow 测试", "")
    cached_item = ConnectCapitalFlowItem(trade_date="2026-04-03", market="northbound", net_amount=30.0)
    akshare_item = ConnectCapitalFlowItem(trade_date="2026-04-03", market="northbound", buy_amount=170.0, sell_amount=140.0, net_amount=30.0)
    source_calls: list[tuple[str, str, tuple[object, ...]]] = []

    monkeypatch.setattr("quotemux.query_engine.load_store_result", lambda capability_id, request_identity, model_type: ([cached_item], _FakeStoreRead(status="hit", hit=True)))
    monkeypatch.setattr("quotemux.query_engine.store_result", lambda capability_id, request_identity, items, report, quarantine_count=0: _FakeStoreWrite(status="write"))
    fake_source_call = _source_call_stub(
        {
            ("akshare", "get_connect_capital_flow"): [akshare_item],
            ("tushare", "get_connect_capital_flow"): [],
        },
        source_calls,
    )

    with patch("quotemux.markets._source_package_call", side_effect=fake_source_call):
        items = QuoteMux().markets.get_connect_capital_flow("2026-04-03", "", "")

    northbound = [item for item in items if item.market == "northbound"][0]
    assert northbound.buy_amount == 170.0
    assert northbound.sell_amount == 140.0
    assert [package_id for package_id, _, _ in source_calls] == ["akshare"]


def test_shareholder_top10_range_fetches_missing_periods_after_partial_cache(monkeypatch) -> None:
    cached_items = [
        ShareholderTop10Item(code="600000", report_period="2024-12-31", rank=rank, shareholder_name=f"缓存股东{rank}")
        for rank in range(1, 11)
    ]

    def fake_provider(code: str, report_period: str, start_period: str, end_period: str, float_only: bool):
        del code, start_period, end_period, float_only
        return [
            ShareholderTop10Item(code="600000", report_period=report_period, rank=rank, shareholder_name=f"{report_period}股东{rank}")
            for rank in range(1, 11)
        ]

    source_calls: list[tuple[str, str, tuple[object, ...]]] = []
    monkeypatch.setattr("quotemux.query_engine.load_store_result", lambda capability_id, request_identity, model_type: (cached_items, _FakeStoreRead(status="hit", hit=True)))
    monkeypatch.setattr("quotemux.query_engine.store_result", lambda capability_id, request_identity, items, report, quarantine_count=0: _FakeStoreWrite(status="write"))
    fake_source_call = _source_call_stub({("akshare", "get_shareholder_top10"): fake_provider}, source_calls)

    with patch("quotemux.stocks._source_package_call", side_effect=fake_source_call):
        items = QuoteMux(QuoteMuxSettings(enabled_sources=("akshare",))).stocks.get_shareholder_top10("600000", "", "20240101", "20241231")

    assert len(items) == 40
    assert {item.report_period for item in items} == {"2024-03-31", "2024-06-30", "2024-09-30", "2024-12-31"}
    assert [args[1] for _, _, args in source_calls] == ["2024-03-31", "2024-06-30", "2024-09-30"]


def test_tushare_connect_flow_builds_northbound_from_connect_legs(monkeypatch) -> None:
    from quotemux_packages.tushare import market_topics

    frame = pd.DataFrame(
        [
            {
                "trade_date": "20260403",
                "north_money": pd.NA,
                "south_money": 40.0,
                "hgt": 20.0,
                "sgt": 10.0,
                "ggt_ss": 5.0,
                "ggt_sz": 7.0,
            }
        ]
    )
    monkeypatch.setattr(market_topics, "read_cached_ranges", lambda namespace, identity, column, start_value, end_value, unit, fetcher, extra_key_columns=(): fetcher(start_value, end_value))
    monkeypatch.setattr(market_topics, "query_frame", lambda api_name, **kwargs: frame)

    items = market_topics.get_connect_capital_flow("2026-04-03", "", "")
    by_market = {item.market: item for item in items}

    assert by_market["northbound"].net_amount == 300000.0
    assert by_market["sh_connect"].net_amount == 200000.0
    assert by_market["sz_connect"].net_amount == 100000.0
    assert by_market["sh_hk"].net_amount == 50000.0
    assert by_market["sz_hk"].net_amount == 70000.0


def test_tushare_main_capital_flow_refetches_incomplete_provider_cache(monkeypatch) -> None:
    from quotemux_packages.tushare import source as tushare_source

    cached = pd.DataFrame([{"trade_date": "20260403", "market": "all", "main_inflow": None, "main_outflow": None, "net_inflow": 30.0}])
    fetched = pd.DataFrame([{"trade_date": "20260403", "market": "all", "main_inflow": 100.0, "main_outflow": 70.0, "net_inflow": 30.0}])
    writes: list[pd.DataFrame] = []

    monkeypatch.setattr(tushare_source, "build_cache_path", lambda provider, namespace, identity: "cache.parquet")
    monkeypatch.setattr(tushare_source, "read_cache_frame", lambda cache_path: cached)
    monkeypatch.setattr(tushare_source, "plan_missing_ranges", lambda cache_df, column, start_value, end_value, unit: [])
    monkeypatch.setattr(tushare_source, "_fetch_market_capital_flow_frame", lambda start_value, end_value: fetched)
    monkeypatch.setattr(tushare_source, "write_cache_frame", lambda cache_path, frame: writes.append(frame))

    items = tushare_source.get_market_capital_flow("2026-04-03", "", "")

    assert items[0].main_inflow == 100.0
    assert items[0].main_outflow == 70.0
    assert items[0].net_inflow == 30.0
    assert len(writes) == 1


def test_tushare_cached_ranges_keeps_extra_key_columns(monkeypatch, tmp_path) -> None:
    from quotemux_packages.tushare import helpers

    cache_path = tmp_path / "connect.parquet"
    fetched = pd.DataFrame(
        [
            {"trade_date": "20260617", "market": "northbound", "net_amount": 1.0},
            {"trade_date": "20260617", "market": "sz_hk", "net_amount": 2.0},
        ]
    )
    monkeypatch.setattr(helpers, "build_cache_path", lambda provider, namespace, identity, file_name="data": cache_path)
    monkeypatch.setattr(helpers, "read_cache_frame", lambda path: pd.DataFrame())
    monkeypatch.setattr(helpers, "write_cache_frame", lambda path, df: None)
    monkeypatch.setattr(helpers, "plan_missing_ranges", lambda cache_df, column, start_value, end_value, unit: [("20260617", "20260617")])

    calls: list[tuple[str, str]] = []

    def fake_fetcher(start_value: str, end_value: str) -> pd.DataFrame:
        calls.append((start_value, end_value))
        return fetched

    frame = helpers.read_cached_ranges(["markets", "connect", "capital-flow"], {"scope": "all"}, "trade_date", "20260617", "20260617", "day", fake_fetcher, extra_key_columns=("market",))

    assert len(frame) == 2
    assert sorted(frame["market"].tolist()) == ["northbound", "sz_hk"]


def test_market_flow_contract_dedupes_same_trade_date_market() -> None:
    from quotemux.markets import _dedupe_market_flow_items

    items = _dedupe_market_flow_items(
        [
            MarketCapitalFlowItem(trade_date="2026-06-17", market="all", net_inflow=-1.0),
            MarketCapitalFlowItem(trade_date="20260617", market="all", net_inflow=-2.0),
        ]
    )

    assert len(items) == 1
    assert items[0].trade_date == "2026-06-17"
    assert items[0].net_inflow == -2.0


def test_connect_flow_contract_dedupes_normalized_trade_date_market() -> None:
    from quotemux.markets import _dedupe_connect_flow_items

    items = _dedupe_connect_flow_items(
        [
            ConnectCapitalFlowItem(trade_date="2026-06-17", market="northbound", net_amount=20.0),
            ConnectCapitalFlowItem(trade_date="20260617", market="northbound", net_amount=30.0),
        ]
    )

    assert len(items) == 1
    assert items[0].trade_date == "2026-06-17"
    assert items[0].net_amount == 30.0


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


def test_intraday_missing_requests_require_full_1m_session(monkeypatch) -> None:
    partial_item = StockQuoteItem(
        code="600000",
        trade_time="2026-04-03 09:31:00",
        freq="1m",
        open=10.0,
        high=10.0,
        low=10.0,
        close=10.0,
        volume=100.0,
        amount=1000.0,
    )

    monkeypatch.setattr("quotemux.stocks._expected_trade_dates", lambda start_date, end_date, settings: ["2026-04-03"])

    requests = _build_missing_quote_requests(["600000"], [partial_item], "1m", "", "20260403", "20260403", "", "", None, QuoteMuxSettings(enabled_sources=("tushare",)))

    assert requests == [(["600000"], "2026-04-03", "2026-04-03")]


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
    assert any("markets.events.news" in manifest.contract_names for manifest in manifests)
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
            ("derived_core", "get_previous_trading_days"): [TradingCalendarItem(exchange="SSE", trade_date="2026-04-02", is_open=True)],
            ("derived_core", "get_next_trading_days"): [TradingCalendarItem(exchange="SSE", trade_date="2026-04-07", is_open=True)],
            ("derived_core", "get_yearly_trading_calendar"): [TradingCalendarItem(exchange="SSE", trade_date="2026-04-03", is_open=True)],
        },
        source_calls,
    )

    with patch("quotemux.stocks._source_package_call", side_effect=fake_source_call):
        assert runtime.stocks.get_hl_signal("600000", "2026-04-03", "", "") == [HLSignalItem(code="600000", trade_date="2026-04-03", first_extreme="high", signal="high_first")]
        assert runtime.stocks.get_technical_factors("600000", "2026-04-03", "", "", "none") == [TechnicalFactorItem(code="600000", trade_date="2026-04-03", adjust="none")]
        assert runtime.stocks.get_shareholder_changes("600000", "", "2026-01-01", "2026-03-31") == [ShareholderChangeItem(code="600000", trade_date="2026-03-31", holder_count=100000)]
    with patch("quotemux.markets._source_package_call", side_effect=fake_source_call):
        assert runtime.markets.get_previous_trading_days(PreviousTradingDaysRequest(exchange="SSE", trade_date="2026-04-03", n=1)) == [TradingCalendarItem(exchange="SSE", trade_date="2026-04-02", is_open=True)]
        assert runtime.markets.get_next_trading_days(NextTradingDaysRequest(exchange="SSE", trade_date="2026-04-03", n=1)) == [TradingCalendarItem(exchange="SSE", trade_date="2026-04-07", is_open=True)]
        assert runtime.markets.get_yearly_trading_calendar(YearlyTradingCalendarRequest(exchange="SSE", start_year=2026, end_year=2026)) == [TradingCalendarItem(exchange="SSE", trade_date="2026-04-03", is_open=True)]

    assert ("derived_core", "get_hl_signal", ("600000", "", "2026-04-03", "2026-04-03")) in source_calls
    assert ("derived_core", "get_technical_factors", ("600000", "2026-04-03", "", "", "none")) in source_calls
    assert ("derived_core", "get_shareholder_changes", ("600000", "", "2026-01-01", "2026-03-31")) in source_calls
    assert ("derived_core", "get_previous_trading_days", ("SSE", "2026-04-03", 1)) in source_calls
    assert ("derived_core", "get_next_trading_days", ("SSE", "2026-04-03", 1)) in source_calls
    assert ("derived_core", "get_yearly_trading_calendar", ("SSE", 2026, 2026)) in source_calls


def test_capability_registry_has_policy_shape_and_merge_strategy_for_every_contract() -> None:
    from quotemux.capabilities import get_capability_definition, is_independently_configurable_capability_id, list_capability_ids

    contract_names = list_contract_names()
    independently_configurable_ids = tuple(capability_id for capability_id in list_capability_ids() if is_independently_configurable_capability_id(capability_id))

    assert contract_names == independently_configurable_ids
    assert "markets.events.news" in contract_names
    news_definition = get_capability_definition("markets.events.news")
    assert news_definition.allowed_packages == ("akshare",)
    assert news_definition.default_source_order == ("akshare",)
    for capability_id in ("markets.calendar.trading.next", "markets.calendar.trading.previous", "markets.calendar.trading.yearly"):
        definition = get_capability_definition(capability_id)
        assert definition.allowed_packages == ("derived_core",)
        assert definition.default_source_order == ("derived_core",)
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
            StockQuotesRequest(codes=["600000"], freq="1d", start_date="2026-04-03", end_date="2026-04-03", fill_missing=True)
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


def test_manifest_validation_allows_derived_core_capability_declarations() -> None:
    manifest = _manifest_with_capabilities(
        "derived_core",
        ("markets.calendar.trading.next",),
        (("get_next_trading_days", "quotemux_packages.derived_core.source:get_next_trading_days"),),
    )

    validate_manifests((manifest,))


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


def test_provider_timeout_uses_adaptive_p95(monkeypatch) -> None:
    store = _MemoryTimeoutStore()
    store.provider_samples = tuple(float(value) for value in range(1000, 21000, 1000))
    monkeypatch.setattr("quotemux.provider_timeout.adaptive._get_timeout_store", lambda: store)

    resolved = resolve_provider_timeout("stocks.quotes.daily", "efinance", None)

    assert resolved.source == "adaptive"
    assert resolved.sample_count == 20
    assert resolved.timeout_seconds == pytest.approx(28.575)


def test_source_instance_timeout_overrides_adaptive_value(monkeypatch) -> None:
    store = _MemoryTimeoutStore()
    store.provider_samples = tuple(float(value) for value in range(1000, 21000, 1000))
    monkeypatch.setattr("quotemux.provider_timeout.adaptive._get_timeout_store", lambda: store)

    resolved = resolve_provider_timeout("stocks.quotes.daily", "efinance", 7)

    assert resolved.source == "source_instance"
    assert resolved.timeout_seconds == 7.0


def test_provider_request_records_empty_and_success(monkeypatch) -> None:
    store = _MemoryTimeoutStore()
    monkeypatch.setattr("quotemux.provider_timeout.adaptive._get_timeout_store", lambda: store)
    monkeypatch.setattr("quotemux.provider_timeout.metrics._get_timeout_store", lambda: store)

    empty_items = run_provider_request("stocks.quotes.daily", "efinance", "efinance-default", "get_stock_quotes", None, lambda: [], None)
    success_items = run_provider_request("stocks.quotes.daily", "efinance", "efinance-default", "get_stock_quotes", None, lambda: [_stock_quote_item()], None)

    assert empty_items == []
    assert success_items == [_stock_quote_item()]
    assert [metric.status for metric in store.provider_metrics] == [TIMEOUT_STATUS_EMPTY, TIMEOUT_STATUS_SUCCESS]


def test_fallback_chain_continues_after_provider_timeout(monkeypatch) -> None:
    store = _MemoryTimeoutStore()
    store.provider_policy = ProviderTimeoutPolicy("stocks.quotes.daily", "efinance", 0.01, 0.01, 0.01, 200, 20)
    monkeypatch.setattr("quotemux.provider_timeout.adaptive._get_timeout_store", lambda: store)
    monkeypatch.setattr("quotemux.provider_timeout.metrics._get_timeout_store", lambda: store)
    monkeypatch.setattr("quotemux.runtime_core.executor.record_provider_event", lambda *args, **kwargs: None)

    def slow_fetcher() -> list[StockQuoteItem]:
        time.sleep(0.05)
        return [_stock_quote_item()]

    steps = (
        ProviderStep(name="efinance", fetcher=slow_fetcher, source_instance_id="efinance-primary", handler="get_stock_quotes"),
        ProviderStep(name="tushare", fetcher=lambda: [_stock_quote_item()], source_instance_id="tushare-default", handler="get_stock_quotes"),
    )

    items, report = run_fallback_chain_with_report(
        "stocks.quotes.daily",
        [],
        ("code", "trade_time", "freq"),
        lambda current_items: [()] if current_items == [] else [],
        steps,
        ("efinance-primary", "tushare-default"),
    )

    assert items == [_stock_quote_item()]
    assert report.steps[0].error_count == 1
    assert report.steps[1].request_count == 1
    assert any(metric.status == TIMEOUT_STATUS_TIMEOUT for metric in store.provider_metrics)
    assert store.capability_metrics[-1].status == TIMEOUT_STATUS_SUCCESS


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
            StockQuotesRequest(codes=["600000"], freq="1d", start_date="2026-04-03", end_date="2026-04-03", fill_missing=True)
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
            StockQuotesRequest(codes=["600000"], freq="1d", start_date="2026-04-03", end_date="2026-04-03", fill_missing=True)
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
            StockQuotesRequest(codes=["600000"], freq="1d", start_date="2026-04-03", end_date="2026-04-03", fill_missing=True)
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


def test_core_does_not_expose_suspended_gap_fill() -> None:
    import quotemux.stocks as stocks_module

    assert not hasattr(stocks_module, "_fill_suspended_daily_gaps")


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


def test_stock_quote_meta_normalizes_compact_daily_dates() -> None:
    compact_item = StockQuoteItem(code="600000", trade_time="20260403", freq="1d", close=10.5, adjust="none")

    result = _build_stock_quotes_query_result(["600000"], [compact_item], [compact_item], [compact_item], "1d", None, ["2026-04-03"], None, set())

    assert result.meta.complete is True


def _minute_quote_items(missing_time: str = "", time_separator: str = " ") -> list[StockQuoteItem]:
    morning = pd.date_range("2026-07-15 09:31", "2026-07-15 11:30", freq="1min")
    afternoon = pd.date_range("2026-07-15 13:01", "2026-07-15 15:00", freq="1min")
    items: list[StockQuoteItem] = []
    for trade_time in [*morning, *afternoon]:
        text = trade_time.strftime(f"%Y-%m-%d{time_separator}%H:%M")
        if text.replace("T", " ") == missing_time:
            continue
        items.append(StockQuoteItem(code="600000", trade_time=text, freq="1m", close=10.0))
    return items


def test_intraday_quote_meta_marks_complete_240_bar_day() -> None:
    items = _minute_quote_items(time_separator="T")

    result = _build_stock_quotes_query_result(
        ["600000"], items, items, items, "1m", None, ["2026-07-15"], None, set(), "summary"
    )

    summary = result.meta.codes[0]
    assert summary.expected_bar_count == 240
    assert summary.actual_bar_count == 240
    assert summary.missing_count == 0
    assert summary.missing_trade_dates == []
    assert summary.missing_trade_times == []
    assert summary.complete is True
    assert result.meta.complete is True


def test_intraday_quote_meta_marks_complete_48_bar_5m_day() -> None:
    from quotemux.common import EXPECTED_INTRADAY_BAR_TIMES

    items = [
        StockQuoteItem(code="600000", trade_time=f"2024-01-02 {bar_time}", freq="5m", close=10.0)
        for bar_time in EXPECTED_INTRADAY_BAR_TIMES["5m"]
    ]

    result = _build_stock_quotes_query_result(
        ["600000"], items, items, items, "5m", None, ["2024-01-02"], None, set(), "summary"
    )

    summary = result.meta.codes[0]
    assert summary.expected_bar_count == 48
    assert summary.actual_bar_count == 48
    assert summary.missing_count == 0
    assert summary.complete is True
    assert result.meta.complete is True


def test_intraday_quote_result_filters_non_standard_opening_minute() -> None:
    items = [
        StockQuoteItem(code="920000", trade_time="2026-07-13 09:30", freq="1m", close=10.0),
        *_minute_quote_items(),
    ]
    items = [item.model_copy(update={"code": "920000", "trade_time": item.trade_time.replace("2026-07-15", "2026-07-13")}) for item in items]

    filtered_items = _filter_standard_intraday_items(items, "1m", ["2026-07-13"])
    result = _build_stock_quotes_query_result(
        ["920000"], filtered_items, filtered_items, filtered_items, "1m", None, ["2026-07-13"], None, set(), "summary"
    )

    assert len(result.items) == 240
    assert result.items[0].trade_time.endswith("09:31")
    assert result.meta.total_rows == 240
    assert result.meta.codes[0].actual_bar_count == 240
    assert result.meta.complete is True


def test_intraday_quote_meta_reports_only_one_missing_minute_in_full_mode() -> None:
    items = _minute_quote_items("2026-07-15 10:15")

    result = _build_stock_quotes_query_result(
        ["600000"], items, items, items, "1m", None, ["2026-07-15"], None, set(), "full"
    )

    summary = result.meta.codes[0]
    assert summary.actual_bar_count == 239
    assert summary.missing_count == 1
    assert summary.missing_trade_dates == []
    assert summary.missing_trade_times == ["2026-07-15 10:15:00"]
    assert summary.complete is False


def test_intraday_quote_meta_summarizes_empty_day_without_expanding_times() -> None:
    result = _build_stock_quotes_query_result(
        ["600000"], [], [], [], "1m", None, ["2026-07-15"], None, set(), "summary"
    )

    summary = result.meta.codes[0]
    assert summary.actual_bar_count == 0
    assert summary.missing_count == 240
    assert summary.missing_trade_dates == ["2026-07-15"]
    assert summary.missing_trade_times == []
    assert summary.complete is False


def test_stock_quote_limit_is_an_explicit_hard_truncation() -> None:
    items = _minute_quote_items()[:2]

    complete_result = _build_stock_quotes_query_result(
        ["600000"], items, items, items, "1m", None, ["2026-07-15"], None, set(), "summary"
    )
    truncated_result = _build_stock_quotes_query_result(
        ["600000"], items, items, items, "1m", 1, ["2026-07-15"], None, set(), "summary"
    )

    assert complete_result.meta.total_rows == 2
    assert complete_result.meta.returned_rows == 2
    assert complete_result.meta.truncated is False
    assert truncated_result.meta.total_rows == 2
    assert truncated_result.meta.returned_rows == 1
    assert truncated_result.meta.truncated is True
    assert truncated_result.meta.codes[0].truncated is True


def test_stock_quote_missing_requests_normalize_compact_daily_dates(monkeypatch) -> None:
    monkeypatch.setattr("quotemux.stocks._expected_trade_dates", lambda *args, **kwargs: ["2026-04-03"])
    compact_item = StockQuoteItem(code="600000", trade_time="20260403", freq="1d", close=10.5, adjust="none")

    requests = _build_missing_quote_requests(
        ["600000"],
        [compact_item],
        "1d",
        "",
        "20260403",
        "20260403",
        "",
        "",
        None,
        QuoteMuxSettings(enabled_sources=("tushare",)),
    )

    assert requests == []


def test_stock_quote_expected_dates_normalize_calendar_dates(monkeypatch) -> None:
    from quotemux.markets import QuoteMuxMarkets

    monkeypatch.setattr(
        QuoteMuxMarkets,
        "get_trading_calendar",
        lambda self, request: [
            TradingCalendarItem(exchange="SSE", trade_date="20260403", is_open=True),
            TradingCalendarItem(exchange="SSE", trade_date="2026-04-03", is_open=True),
        ],
    )

    assert _expected_trade_dates("20260403", "20260403", QuoteMuxSettings()) == ["2026-04-03"]


def test_skip_suspended_false_keeps_suspended_quotes() -> None:
    suspended_item = StockQuoteItem(code="600000", trade_time="2026-04-02", freq="1d", close=10.0, adjust="none", is_suspended=True)
    active_item = StockQuoteItem(code="600000", trade_time="2026-04-03", freq="1d", close=10.5, adjust="none")

    filtered_items = _filter_suspended_quote_items([suspended_item, active_item], False, False, "1d")

    assert [item.trade_time for item in filtered_items] == ["2026-04-02", "2026-04-03"]


def test_load_index_daily_frame_qualifies_day_rows_columns(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_query_dataframe(query: str, params: tuple[object, ...]) -> pd.DataFrame:
        captured.setdefault("queries", []).append(query)
        captured["params"] = params
        return pd.DataFrame()

    monkeypatch.setattr("quotemux.infra.db.market_reads.query_dataframe", fake_query_dataframe)
    monkeypatch.setattr(
        "quotemux.infra.db.market_reads._existing_columns",
        lambda table_schema, table_name: {"index_code", "trade_date", "open", "high", "low", "close", "volume", "amount"},
    )

    load_index_daily_frame(["399006"], "2026-06-12", "2026-06-15")

    query_text = str(captured["queries"][0])
    assert "day_rows.index_code = any(%s)" in query_text
    assert "day_rows.trade_date >= %s" in query_text
    assert "day_rows.trade_date <= %s" in query_text
    assert "null as pre_close" in query_text
    assert "null as pct_chg" in query_text


def test_stock_daily_snapshot_query_uses_table_alias_for_optional_columns(monkeypatch) -> None:
    monkeypatch.setattr(
        "quotemux.infra.db.market_reads._existing_columns",
        lambda table_schema, table_name: {"code", "trade_date", "open", "high", "low", "close", "volume", "amount"},
    )

    query_text = _stock_daily_snapshot_query()

    assert "from fact.stock_daily_1d day_rows" in query_text
    assert "null as pre_close" in query_text
    assert "left(day_rows.code, 3) = '900'" in query_text
    assert "left(day_rows.code, 3) = '920'" in query_text


def test_load_stock_daily_frame_filters_canonical_market(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_query_dataframe(query: str, params: tuple[object, ...]) -> pd.DataFrame:
        captured["query"] = query
        captured["params"] = params
        return pd.DataFrame()

    monkeypatch.setattr("quotemux.infra.db.market_reads.query_dataframe", fake_query_dataframe)
    monkeypatch.setattr("quotemux.infra.db.market_reads._table_exists", lambda table_schema, table_name: True)
    monkeypatch.setattr(
        "quotemux.infra.db.market_reads._existing_columns",
        lambda table_schema, table_name: {"code", "trade_date", "open", "high", "low", "close", "volume", "amount"},
    )

    load_stock_daily_frame(["920000"], "2026-05-18", "2026-06-09")

    query_text = str(captured["query"])
    assert "day_rows.code = any(%s)" in query_text
    assert "left(day_rows.code, 3) = '900'" in query_text
    assert "left(day_rows.code, 3) = '920'" in query_text
    assert captured["params"] == (["920000"], "2026-06-09", "2026-05-18", "2026-06-09")


def test_daily_snapshot_coverage_rejects_partial_snapshot(monkeypatch) -> None:
    active_frame = pd.DataFrame.from_records([{"code": "000001"}, {"code": "000002"}, {"code": "920000"}])
    monkeypatch.setattr("quotemux.stocks.load_stock_active_codes_frame", lambda trade_date: active_frame)
    items = [
        StockQuoteItem(
            code="000001",
            trade_time="2026-07-02",
            freq="1d",
            close=1.0,
            pre_close=1.0,
            pct_chg=0.0,
            amount=1.0,
        )
    ]

    with pytest.raises(RuntimeError, match="股票日线快照不完整"):
        _assert_daily_snapshot_coverage("2026-07-02", items, 10000, 0)


def test_daily_snapshot_coverage_allows_small_upstream_gap(monkeypatch) -> None:
    active_frame = pd.DataFrame.from_records([{"code": f"{index:06d}"} for index in range(100)])
    monkeypatch.setattr("quotemux.stocks.load_stock_active_codes_frame", lambda trade_date: active_frame)
    items = [
        StockQuoteItem(
            code=f"{index:06d}",
            trade_time="2026-07-02",
            freq="1d",
            close=1.0,
            pre_close=1.0,
            pct_chg=0.0,
            amount=1.0,
        )
        for index in range(99)
    ]

    _assert_daily_snapshot_coverage("2026-07-02", items, 10000, 0)


def test_daily_snapshot_coverage_does_not_accept_synthetic_suspension_rows(monkeypatch) -> None:
    active_frame = pd.DataFrame.from_records([{"code": "000001"}, {"code": "000002"}])
    monkeypatch.setattr("quotemux.stocks.load_stock_active_codes_frame", lambda trade_date: active_frame)
    items = [
        StockQuoteItem(code="000001", trade_time="2026-07-02", freq="1d", close=1.0, pre_close=0.9, pct_chg=11.11, amount=100.0),
        StockQuoteItem(code="000002", trade_time="2026-07-02", freq="1d", close=8.5, pre_close=8.5, pct_chg=0.0, amount=0.0, is_suspended=True),
    ]

    with pytest.raises(RuntimeError, match="快照不完整"):
        _assert_daily_snapshot_coverage("2026-07-02", items, 10000, 0)


def test_daily_snapshot_rejects_market_wide_placeholders_before_fact_write(monkeypatch) -> None:
    active_frame = pd.DataFrame.from_records([{"code": f"{index:06d}"} for index in range(1, 11)])
    fetched_items = [StockQuoteItem(code="000001", trade_time="2026-07-02", freq="1d", close=1.0, amount=1.0)]
    previous_items = [
        StockQuoteItem(code=f"{index:06d}", trade_time="2026-07-01", freq="1d", close=1.0)
        for index in range(2, 11)
    ]
    fact_ref_items: list[StockQuoteItem] = []

    monkeypatch.setattr("quotemux.stocks.get_local_stock_daily_snapshot_full", lambda trade_date: [])
    monkeypatch.setattr("quotemux.stocks.execute_capability_query", lambda *args, **kwargs: (fetched_items, ContractReport(contract_name="stocks.quotes.daily_snapshot")))
    monkeypatch.setattr("quotemux.stocks.load_stock_active_codes_frame", lambda trade_date: active_frame)
    monkeypatch.setattr(
        "quotemux.stocks.get_local_stock_quotes",
        lambda codes, freq, trade_date, start_date, end_date, start_time, end_time, count, adjust: previous_items,
    )
    monkeypatch.setattr("quotemux.stocks.get_fact_ref_writer", lambda capability_id: lambda items: fact_ref_items.extend(items) is None or True)

    with pytest.raises(RuntimeError):
        QuoteMux().stocks.get_daily_snapshot_with_report(StockDailySnapshotRequest(trade_date="2026-07-02"))

    assert fact_ref_items == []


def test_missing_concept_daily_table_returns_empty_without_querying_fact(monkeypatch) -> None:
    from quotemux.infra.db import market_reads

    calls: list[str] = []

    def fake_query_dataframe(query: str, params: tuple[object, ...]):
        calls.append(query)
        return pd.DataFrame()

    monkeypatch.setattr(market_reads, "query_dataframe", fake_query_dataframe)

    assert market_reads.load_concept_daily_frame(["C1"], "2026-06-26", "2026-06-26").empty
    assert market_reads.load_concept_daily_snapshot_frame("2026-06-26", 20, 0).empty
    assert market_reads.load_latest_complete_concept_daily_snapshot_ids("2026-06-26", 20, 0) == []
    assert market_reads.load_latest_complete_concept_daily_snapshot_frame("2026-06-26", 20, 0).empty
    assert all("from fact.concept_daily_1d" not in query for query in calls)


def test_intraday_quote_cache_requires_all_standard_minutes_for_full_day() -> None:
    morning = pd.date_range("2026-07-15 09:31:00", "2026-07-15 11:30:00", freq="1min")
    afternoon = pd.date_range("2026-07-15 13:01:00", "2026-07-15 15:00:00", freq="1min")
    complete_frame = pd.DataFrame({"trade_time": morning.append(afternoon)})
    partial_frame = complete_frame.iloc[:-1].copy()
    start_dt = datetime(2026, 7, 15)
    end_dt = datetime(2026, 7, 15, 23, 59, 59, 999999)

    assert intraday_quote_cache_needs_refresh(complete_frame, "1m", start_dt, end_dt, None) is False
    assert intraday_quote_cache_needs_refresh(partial_frame, "1m", start_dt, end_dt, None) is True
    assert intraday_quote_cache_needs_refresh(partial_frame, "1m", start_dt, end_dt, 100) is False
