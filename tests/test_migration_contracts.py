from __future__ import annotations

import importlib
from types import SimpleNamespace

from pydantic import TypeAdapter
import pytest

from platform_models.migration_contracts import (
    FinancialEventData,
    ForecastEventsRequest,
    MigrationPage,
    MigrationRecord,
)
from platform_models.provider_contracts import canonical_json_sha256
from quotemux.config_runtime.models import SourceInstanceConfig
from quotemux.p0_fundamentals.errors import P0QueryError


query_module = importlib.import_module("quotemux.migration_contracts.query")


@pytest.fixture(autouse=True)
def clear_cache() -> None:
    query_module._CACHE.clear()


def _request(instance_id: str = "eastmoney_official-default") -> ForecastEventsRequest:
    return ForecastEventsRequest(
        capability_id="stocks.finance.forecasts",
        provider="eastmoney_official",
        provider_instance_id=instance_id,
        code="600000",
        market="SH",
        range_start="2025-01-01",
        range_end="2025-12-31",
        cursor="",
        data_version="quotemux.stocks.finance.forecasts.v2",
        source_version="eastmoney.datacenter.financial_forecast.v1",
    )


def _page() -> MigrationPage[FinancialEventData]:
    projection = {
        "SECURITY_CODE": "600000",
        "REPORT_DATE": "2025-12-31",
        "PREDICT_FINANCE_CODE": "004",
    }
    data = FinancialEventData(
        code="600000",
        market="SH",
        security_code="SH.600000",
        report_period="2025-12-31",
        notice_date="2026-01-20",
        notice_time="2026-01-20 18:00:00",
        event_type="forecast",
        event_subtype="预增",
        is_revision=False,
        notice_title="2025年度业绩预告",
        notice_url="",
        notice_summary="预计增长",
        forecast_metric_code="004",
        forecast_metric_name="归属于母公司股东的净利润",
        forecast_summary="预计增长",
        forecast_direction="increase",
        forecast_amount_lower="100",
        forecast_amount_upper="120",
        forecast_yoy_lower="10",
        forecast_yoy_upper="20",
        net_profit_lower="100",
        net_profit_upper="120",
        net_profit_yoy_lower="10",
        net_profit_yoy_upper="20",
        net_profit_excl_nonrecurring_lower=None,
        net_profit_excl_nonrecurring_upper=None,
        net_profit_excl_nonrecurring_yoy_lower=None,
        net_profit_excl_nonrecurring_yoy_upper=None,
        operating_revenue_lower=None,
        operating_revenue_upper=None,
        operating_revenue_yoy_lower=None,
        operating_revenue_yoy_upper=None,
        forecast_amount_unit="CNY",
        operating_revenue=None,
        operating_revenue_yoy=None,
        net_profit=None,
        net_profit_parent=None,
        net_profit_yoy=None,
        basic_eps=None,
        bps=None,
        roe=None,
        data_quality_flags=[],
    )
    return MigrationPage[FinancialEventData](
        capability_id="stocks.finance.forecasts",
        data_version="quotemux.stocks.finance.forecasts.v2",
        provider="eastmoney_official",
        source="eastmoney_datacenter_financial_events",
        source_version="eastmoney.datacenter.financial_forecast.v1",
        fetched_at="2026-08-12T10:00:00Z",
        confirmed_empty=False,
        next_cursor="",
        records=[
            MigrationRecord[FinancialEventData](
                source_event_id="SH.600000:forecast:1",
                raw_hash=canonical_json_sha256(projection),
                raw_projection=projection,
                data=data,
            )
        ],
    )


def test_cache_and_live_are_isomorphic(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0

    def fake_live(request: object, adapter: TypeAdapter[object]):
        nonlocal calls
        calls += 1
        return _page()

    monkeypatch.setattr(query_module, "_load_live_page", fake_live)
    live = query_module.query(_request())
    cached = query_module.query(_request())
    assert calls == 1
    assert live.model_dump(mode="json") == cached.model_dump(mode="json")


def test_cache_corruption_does_not_fallback_live(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(query_module._CACHE, "get", lambda request: b'{"broken":true}')
    monkeypatch.setattr(
        query_module,
        "_load_live_page",
        lambda *args: pytest.fail("损坏 cache 不得进入 live fallback"),
    )
    with pytest.raises(P0QueryError) as error:
        query_module.query(_request())
    assert error.value.kind == "cache_error"


def test_live_uses_only_explicit_enabled_instance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    selected = SourceInstanceConfig(
        instance_id="eastmoney_official-default",
        package_id="eastmoney_official",
        display_name="Eastmoney",
        enabled=True,
        priority=1,
        timeout_seconds=None,
        config_values={},
        secret_values={},
        tags=(),
    )
    other = SourceInstanceConfig(
        instance_id="tushare-default",
        package_id="tushare",
        display_name="Tushare",
        enabled=True,
        priority=2,
        timeout_seconds=None,
        config_values={},
        secret_values={},
        tags=(),
    )
    registry = SimpleNamespace(
        get_manifest=lambda provider: SimpleNamespace(supports_multi_instance=False),
        get_handler=lambda provider, name: lambda payload: _page().model_dump(
            mode="json"
        ),
    )
    snapshot = SimpleNamespace(
        list_enabled_source_instances=lambda: (selected, other),
        get_contract_source_order=lambda *args: pytest.fail("不得读取 fallback 顺序"),
        get_contract_source_instances=lambda *args: pytest.fail(
            "不得选择备用 Provider"
        ),
    )
    monkeypatch.setattr(
        query_module, "get_default_source_package_registry", lambda: registry
    )
    monkeypatch.setattr(
        query_module,
        "get_config_runtime",
        lambda: SimpleNamespace(get_active_snapshot=lambda: snapshot),
    )
    monkeypatch.setattr(
        query_module,
        "call_provider_api",
        lambda provider, capability, handler, payload: handler(payload),
    )
    assert query_module.query(_request()).provider == "eastmoney_official"


def test_missing_explicit_instance_is_contract_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = SimpleNamespace(
        get_manifest=lambda provider: SimpleNamespace(supports_multi_instance=False),
        get_handler=lambda provider, name: lambda payload: _page(),
    )
    snapshot = SimpleNamespace(list_enabled_source_instances=lambda: ())
    monkeypatch.setattr(
        query_module, "get_default_source_package_registry", lambda: registry
    )
    monkeypatch.setattr(
        query_module,
        "get_config_runtime",
        lambda: SimpleNamespace(get_active_snapshot=lambda: snapshot),
    )
    with pytest.raises(P0QueryError) as error:
        query_module.query(_request("not-enabled"))
    assert error.value.kind == "contract_error"


def test_page_rejects_raw_hash_and_identity_drift() -> None:
    broken_hash = _page().model_copy(deep=True)
    broken_hash.records[0].raw_hash = "0" * 64
    with pytest.raises(P0QueryError, match="raw_hash"):
        query_module._validate_page(_request(), broken_hash)

    broken_identity = _page().model_copy(deep=True)
    broken_identity.records[0].data.code = "600001"
    with pytest.raises(P0QueryError, match="证券身份"):
        query_module._validate_page(_request(), broken_identity)
