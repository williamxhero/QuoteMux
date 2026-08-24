from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest

from platform_models import FutureBar1mItem, FutureContractCatalogItem, FutureContractRealtimeQuoteItem, FutureMainContractMappingItem, FutureRealtimeQuoteItem, FutureSeriesCoverageItem
from quotemux.config_runtime.models import SourceInstanceConfig
from quotemux import futures
from quotemux.capabilities import get_capability_definition, get_public_api_binding
from quotemux.contracts import get_contract_definition
from quotemux.source_packages.instance_context import current_source_instance
from quotemux.store.capture import CapturePolicy, CaptureRequest, DEFAULT_CAPTURE_POLICY_SPECS, build_capture_requests
from quotemux.store.default_update_policy import get_capability_update_policy_default


def _published_catalog_rows(*, include_expired: bool = False) -> list[dict[str, object]]:
    snapshot_id = "snapshot-1"
    captured_at = "2026-08-24 10:12:00"
    source = {"package_id": "shinny_tqsdk", "source_instance_id": "test", "provider_version": {"availability": "unavailable"}}
    items = [
        futures._normalize_catalog_item(
            FutureContractCatalogItem(
                provider_symbol=f"{exchange}.{product}2609", contract_symbol=f"{exchange}.{product}2609",
                product_code=product, exchange=exchange, ins_class="FUTURE",
                price_tick=1.0, price_decs=0, volume_multiple=10.0,
            ), snapshot_id=snapshot_id, captured_at=captured_at, source=source,
        )
        for product, exchange in futures.PRODUCT_EXCHANGE.items()
    ]
    checksum = futures._catalog_content_checksum(items)
    items = [item.model_copy(update={"content_checksum": checksum}) for item in items]
    header = {
        "snapshot_id": snapshot_id, "scope_include_expired": include_expired,
        "schema_version": futures.FUTURE_CONTRACT_CATALOG_SCHEMA_VERSION, "captured_at": captured_at,
        "source_package_id": "shinny_tqsdk", "source_instance_id": "test", "content_checksum": checksum,
        "row_count": len(items), "product_count": len(futures.PRODUCT_EXCHANGE), "complete": True,
    }
    return [{**header, "item_provider_symbol": item.provider_symbol, "payload": item.model_dump(mode="json")} for item in items]


def test_future_product_map_covers_apex_l0_universe() -> None:
    assert len(futures.PRODUCT_EXCHANGE) == 84
    assert futures.PRODUCT_EXCHANGE["IF"] == "CFFEX"
    assert futures.PRODUCT_EXCHANGE["au"] == "SHFE"
    assert futures.PRODUCT_EXCHANGE["MA"] == "CZCE"


def test_main_continuous_default_capture_is_daily_at_0030_with_two_day_overlap() -> None:
    policy = next(item for item in DEFAULT_CAPTURE_POLICY_SPECS if item.capability_id == futures.MAIN_CONTINUOUS_CAPABILITY_ID)

    assert policy.enabled is True
    assert policy.cadence == "daily"
    assert policy.run_time.isoformat() == "00:30:00"
    assert policy.timezone == "Asia/Shanghai"
    capture_policy = CapturePolicy(
        capability_id=policy.capability_id,
        enabled=policy.enabled,
        cadence=policy.cadence,
        run_time=policy.run_time,
        timezone=policy.timezone,
        weekday=None,
        month=None,
        month_day=None,
        scope_profile=policy.scope_profile,
        window_count=policy.window_count,
        batch_size=policy.batch_size,
        notes="",
    )
    assert build_capture_requests(capture_policy, datetime(2026, 8, 12, 20, 30))[0].request_identity == {"overlap_days": 2}


def test_realtime_main_continuous_default_is_request_only_without_scheduled_capture() -> None:
    policy = next(
        item
        for item in DEFAULT_CAPTURE_POLICY_SPECS
        if item.capability_id == futures.REALTIME_MAIN_CONTINUOUS_CAPABILITY_ID
    )

    assert policy.enabled is False
    assert policy.cadence == "daily"
    assert get_capability_update_policy_default(policy.capability_id).cache_ttl_days == 0
    definition = get_capability_definition(futures.REALTIME_MAIN_CONTINUOUS_CAPABILITY_ID)
    assert definition.api_paths == ("/api/futures/quotes/realtime",)
    assert definition.allowed_packages == ("shinny_tqsdk",)
    assert get_public_api_binding("/api/futures/quotes/realtime").capability_ids == (
        futures.REALTIME_MAIN_CONTINUOUS_CAPABILITY_ID,
    )


def test_realtime_main_continuous_dispatches_configured_instance(monkeypatch) -> None:
    instance = SourceInstanceConfig(
        instance_id="shinny-tqsdk-default",
        package_id="shinny_tqsdk",
        display_name="Shinny TqSdk",
        enabled=True,
        priority=1,
        timeout_seconds=None,
        config_values={"account": "test"},
        secret_values={},
        tags=(),
    )
    calls: list[list[tuple[str, str]]] = []

    class Settings:
        def get_contract_source_instances(self, capability_id: str, fallback: tuple[str, ...]):
            assert capability_id == futures.REALTIME_MAIN_CONTINUOUS_CAPABILITY_ID
            assert fallback == ("shinny_tqsdk",)
            return (instance,)

    def handler(products: list[tuple[str, str]]) -> list[FutureRealtimeQuoteItem]:
        assert current_source_instance() == instance
        calls.append(products)
        return [
            FutureRealtimeQuoteItem(
                product_code="IF",
                exchange="CFFEX",
                provider_symbol="KQ.m@CFFEX.IF",
                contract_symbol="CFFEX.IF2609",
                quote_time="2026-08-20 09:31:00",
                last_price=4600.0,
                trading_status="TRADING",
            )
        ]

    class Registry:
        def get_handler(self, package_id: str, handler_name: str):
            assert (package_id, handler_name) == ("shinny_tqsdk", "get_future_main_continuous_realtime")
            return handler

    monkeypatch.setattr(futures, "QuoteMuxSettings", Settings)
    monkeypatch.setattr(futures, "get_default_source_package_registry", lambda: Registry())

    items = futures.QuoteMuxFutures().get_main_continuous_realtime("if,au")

    assert calls == [[("IF", "CFFEX"), ("au", "SHFE")]]
    assert items[0].provider_symbol == "KQ.m@CFFEX.IF"


def test_realtime_main_continuous_requires_enabled_shinny_instance(monkeypatch) -> None:
    class Settings:
        def get_contract_source_instances(self, *_args):
            return ()

    monkeypatch.setattr(futures, "QuoteMuxSettings", Settings)

    with pytest.raises(RuntimeError, match="shinny_tqsdk"):
        futures.QuoteMuxFutures().get_main_continuous_realtime("IF")


def test_realtime_main_continuous_rejects_unknown_product_without_provider_call(monkeypatch) -> None:
    monkeypatch.setattr(futures, "get_default_source_package_registry", lambda: pytest.fail("不应调用 provider"))

    with pytest.raises(ValueError, match="未知期货品种代码"):
        futures.QuoteMuxFutures().get_main_continuous_realtime("UNKNOWN")


def test_tqsdk_p0_contract_inventory_and_update_policies() -> None:
    expected = {
        futures.FUTURE_CONTRACT_CATALOG_CAPABILITY_ID: (
            "/api/futures/contracts",
            ("shinny_tqsdk",),
            "reference_table",
            ("provider_symbol",),
            True,
            "weekly",
            30,
        ),
        futures.FUTURE_MAIN_CONTRACT_MAPPING_CAPABILITY_ID: (
            "/api/futures/contracts/main-mapping",
            ("shinny_tqsdk",),
            "keyed_records",
            ("product_code", "exchange"),
            True,
            "daily",
            1,
        ),
        futures.FUTURE_CONTRACT_REALTIME_CAPABILITY_ID: (
            "/api/futures/contracts/realtime",
            ("shinny_tqsdk",),
            "keyed_records",
            ("provider_symbol",),
            False,
            "daily",
            0,
        ),
    }

    for capability_id, (path, packages, shape, keys, capture_enabled, cadence, ttl_days) in expected.items():
        definition = get_capability_definition(capability_id)
        capture_policy = next(item for item in DEFAULT_CAPTURE_POLICY_SPECS if item.capability_id == capability_id)
        assert definition.api_paths == (path,)
        assert definition.allowed_packages == packages
        assert definition.result_shape == shape
        assert definition.key_fields == keys
        assert get_public_api_binding(path).capability_ids == (capability_id,)
        assert capture_policy.enabled is capture_enabled
        assert capture_policy.cadence == cadence
        assert get_capability_update_policy_default(capability_id).cache_ttl_days == ttl_days

    for capability_id, expected_identity in {
        futures.FUTURE_CONTRACT_CATALOG_CAPABILITY_ID: {"codes": [], "include_expired": False},
        futures.FUTURE_MAIN_CONTRACT_MAPPING_CAPABILITY_ID: {"codes": []},
    }.items():
        spec = next(item for item in DEFAULT_CAPTURE_POLICY_SPECS if item.capability_id == capability_id)
        policy = CapturePolicy(
            capability_id=spec.capability_id,
            enabled=spec.enabled,
            cadence=spec.cadence,
            run_time=spec.run_time,
            timezone=spec.timezone,
            weekday=None,
            month=None,
            month_day=None,
            scope_profile=spec.scope_profile,
            window_count=spec.window_count,
            batch_size=spec.batch_size,
            notes="",
        )
        assert build_capture_requests(policy, datetime(2026, 8, 20, 18, 30)) == (
            CaptureRequest(capability_id, expected_identity),
        )


def test_tqsdk_p0_contract_capture_dispatches_in_enabled_instance_context(monkeypatch) -> None:
    instance = SourceInstanceConfig(
        instance_id="shinny-tqsdk-default",
        package_id="shinny_tqsdk",
        display_name="Shinny TqSdk",
        enabled=True,
        priority=1,
        timeout_seconds=None,
        config_values={},
        secret_values={},
        tags=(),
    )
    calls: list[tuple[str, object]] = []

    class Settings:
        def get_contract_source_instances(self, _capability_id: str, _fallback: tuple[str, ...]):
            return (instance,)

    def catalog_handler(products: list[tuple[str, str]], include_expired: bool):
        assert current_source_instance() == instance
        calls.append(("catalog", (products, include_expired)))
        return [
            FutureContractCatalogItem(
                provider_symbol=f"{exchange}.{product}2609", contract_symbol=f"{exchange}.{product}2609",
                product_code=product, exchange=exchange, ins_class="FUTURE",
                price_tick=1.0, price_decs=0, volume_multiple=10.0,
            )
            for product, exchange in products
        ]

    def mapping_handler(products: list[tuple[str, str]]):
        assert current_source_instance() == instance
        calls.append(("mapping", products))
        return [FutureMainContractMappingItem(product_code="IF", exchange="CFFEX", provider_symbol="KQ.m@CFFEX.IF", contract_symbol="CFFEX.IF2609", updated_time="2026-08-20 09:31:00")]

    def realtime_handler(symbols: list[str]):
        assert current_source_instance() == instance
        calls.append(("realtime", symbols))
        return [FutureContractRealtimeQuoteItem(provider_symbol="SHFE.rb2610", contract_symbol="SHFE.rb2610", quote_time="2026-08-20 09:31:00", bid_price5=3400.0, ask_volume5=2.0)]

    class Registry:
        def get_handler(self, package_id: str, handler_name: str):
            assert package_id == "shinny_tqsdk"
            return {
                "get_future_contract_catalog": catalog_handler,
                "get_future_main_contract_mapping": mapping_handler,
                "get_future_contract_realtime_quotes": realtime_handler,
            }[handler_name]

    monkeypatch.setattr(futures, "get_default_source_package_registry", lambda: Registry())
    monkeypatch.setattr(futures, "ensure_future_schema", lambda: None)
    runtime = futures.QuoteMuxFutures(Settings())
    published: list[object] = []
    monkeypatch.setattr(runtime, "_publish_contract_catalog_snapshot", lambda *args: published.append(args))

    captured_catalog = runtime.capture_contract_catalog()
    assert captured_catalog[0].catalog_schema_version == futures.FUTURE_CONTRACT_CATALOG_SCHEMA_VERSION
    assert captured_catalog[0].snapshot_complete is True
    assert len(captured_catalog[0].content_checksum) == 64
    assert captured_catalog[0].source["source_instance_id"] == "shinny-tqsdk-default"
    assert {"ag", "al", "AP", "CF", "cu", "hc", "i", "j", "m", "MA", "ni", "p", "ru", "sc", "T", "TA", "TF", "v", "y", "lh", "SA", "ao", "si"} <= {item.product_code for item in captured_catalog}
    assert runtime.get_main_contract_mappings()[0].contract_symbol == "CFFEX.IF2609"
    assert runtime.get_contract_realtime("SHFE.rb2610,SHFE.rb2610")[0].bid_price5 == 3400.0
    assert calls[0][0] == "catalog"
    assert len(calls[0][1][0]) == 84
    assert calls[0][1][1] is False
    assert len(published) == 1
    assert calls[1][0] == "mapping"
    assert len(calls[1][1]) == 84
    assert calls[2] == ("realtime", ["SHFE.rb2610"])


def test_contract_catalog_public_read_is_local_only_and_exposes_normalized_contract(monkeypatch) -> None:
    monkeypatch.setattr(futures, "get_default_source_package_registry", lambda: pytest.fail("GET 不得调用 provider"))
    monkeypatch.setattr(futures, "ensure_future_schema", lambda: pytest.fail("GET 不得运行 DDL"))
    monkeypatch.setattr(futures, "execute_sql", lambda: pytest.fail("GET 不得写库"))
    monkeypatch.setattr(futures, "query_dataframe", lambda *_args, **_kwargs: pd.DataFrame(_published_catalog_rows()))

    item = futures.QuoteMuxFutures().get_contract_catalog("rb")[0]

    assert item.tick_size == item.price_tick == 1.0
    assert item.price_precision == item.price_decs == 0
    assert item.multiplier == item.volume_multiple == 10.0
    assert item.currency == "CNY"
    assert item.lot_size is None and item.asset_class is None
    assert item.commission_open is None and item.initial_margin is None
    assert item.snapshot_complete is True and len(item.content_checksum) == 64
    assert item.provenance["currency"] == {"kind": "market_rule", "rule_id": "cn_futures_currency_v1"}
    assert item.availability["execution_profile_required"] is True


def test_contract_catalog_public_read_fails_closed_when_snapshot_or_product_is_missing(monkeypatch) -> None:
    monkeypatch.setattr(futures, "query_dataframe", lambda *_args, **_kwargs: pd.DataFrame())
    with pytest.raises(futures.FutureContractCatalogIncompleteError) as absent:
        futures.QuoteMuxFutures().get_contract_catalog("rb")
    assert absent.value.details["dataset_id"] == "future_contract_reference"
    assert absent.value.details["repair_endpoint"] == "/api/admin/data-repairs"
    assert absent.value.details["repair_template"] == {
        "dataset_id": "future_contract_reference",
        "scope": {"codes": [], "include_expired": False},
    }

    with pytest.raises(futures.FutureContractCatalogIncompleteError) as expired:
        futures.QuoteMuxFutures().get_contract_catalog("rb", include_expired=True)
    assert expired.value.details["reason"] == "complete_published_expired_scope_unavailable"


@pytest.mark.parametrize("mutation, reason", [
    (lambda rows: rows.__setitem__(0, {**rows[0], "content_checksum": "bad"}), "published_snapshot_header_inconsistent"),
    (lambda rows: rows[0].__setitem__("row_count", 1), "published_snapshot_header_inconsistent"),
    (lambda rows: rows.pop(), "published_snapshot_count_mismatch"),
])
def test_contract_catalog_public_read_rejects_corrupt_complete_snapshot(monkeypatch, mutation, reason) -> None:
    rows = _published_catalog_rows()
    mutation(rows)
    monkeypatch.setattr(futures, "query_dataframe", lambda *_args, **_kwargs: pd.DataFrame(rows))
    with pytest.raises(futures.FutureContractCatalogIncompleteError) as error:
        futures.QuoteMuxFutures().get_contract_catalog("rb")
    assert error.value.details["reason"] == reason


def test_contract_catalog_expired_scope_is_independent_and_repairable(monkeypatch) -> None:
    rows = _published_catalog_rows(include_expired=True)
    seen_params: list[tuple[object, ...]] = []

    def query(_sql: str, params: tuple[object, ...]):
        seen_params.append(params)
        return pd.DataFrame(rows)

    monkeypatch.setattr(futures, "query_dataframe", query)
    assert futures.QuoteMuxFutures().get_contract_catalog("rb", include_expired=True)[0].product_code == "rb"
    assert seen_params == [(True,)]


def test_contract_catalog_expired_capture_combines_active_and_expired_rows(monkeypatch) -> None:
    runtime = futures.QuoteMuxFutures()
    calls: list[bool] = []

    def handler(products, expired):
        calls.append(expired)
        return [
            FutureContractCatalogItem(
                provider_symbol=f"{exchange}.{product}2609", contract_symbol=f"{exchange}.{product}2609",
                product_code=product, exchange=exchange, ins_class="FUTURE",
                price_tick=1.0, price_decs=0, volume_multiple=10.0,
            )
            for product, exchange in products
        ]

    monkeypatch.setattr(runtime, "_tqsdk_handler", lambda *_args: (object(), handler))
    monkeypatch.setattr(futures, "ensure_future_schema", lambda: None)
    published: list[tuple[object, ...]] = []
    monkeypatch.setattr(runtime, "_publish_contract_catalog_snapshot", lambda *args: published.append(args))

    captured = runtime.capture_contract_catalog(include_expired=True)

    assert calls == [False, True]
    assert len(captured) == len(futures.PRODUCT_EXCHANGE)
    assert published[0][-1] is True


def test_contract_catalog_capture_rejects_partial_universe_without_publishing(monkeypatch) -> None:
    runtime = futures.QuoteMuxFutures()
    monkeypatch.setattr(runtime, "_tqsdk_handler", lambda *_args: (object(), lambda *_args: []))
    monkeypatch.setattr(runtime, "_publish_contract_catalog_snapshot", lambda *_args: pytest.fail("partial capture must not publish"))

    with pytest.raises(ValueError, match="未返回任何数据"):
        runtime.capture_contract_catalog()


def test_tqsdk_p0_contract_realtime_requires_symbols() -> None:
    with pytest.raises(ValueError, match="symbols 不能为空"):
        futures.QuoteMuxFutures().get_contract_realtime(" , ")


def test_tqsdk_p0_models_expose_documented_openapi_fields() -> None:
    for model in (FutureContractCatalogItem, FutureMainContractMappingItem, FutureContractRealtimeQuoteItem):
        properties = model.model_json_schema()["properties"]
        assert properties
        assert all(field.get("description", "") != "" for field in properties.values())
        assert all(field.get("examples", []) != [] for field in properties.values())


def test_tqsdk_p0_contract_registry_exposes_platform_result_models() -> None:
    assert get_contract_definition(futures.FUTURE_CONTRACT_CATALOG_CAPABILITY_ID).result_type is FutureContractCatalogItem
    assert get_contract_definition(futures.FUTURE_MAIN_CONTRACT_MAPPING_CAPABILITY_ID).result_type is FutureMainContractMappingItem
    assert get_contract_definition(futures.FUTURE_CONTRACT_REALTIME_CAPABILITY_ID).result_type is FutureContractRealtimeQuoteItem


def test_future_query_requires_explicit_supported_series(monkeypatch) -> None:
    monkeypatch.setattr(futures, "ensure_future_schema", lambda: None)

    with pytest.raises(ValueError, match="series_type"):
        futures.QuoteMuxFutures().get_quotes_1m("IF", "stitched")


def test_future_query_reads_local_fact_without_provider_details(monkeypatch) -> None:
    monkeypatch.setattr(futures, "ensure_future_schema", lambda: None)
    monkeypatch.setattr(
        futures,
        "query_dataframe",
        lambda *_args, **_kwargs: pd.DataFrame(
            [
                {
                    "product_code": "IF",
                    "exchange": "CFFEX",
                    "series_type": "main_continuous",
                    "bar_time": datetime(2026, 8, 11, 9, 31),
                    "open": 4642.0,
                    "high": 4647.8,
                    "low": 4640.0,
                    "close": 4647.4,
                    "volume": 1351.0,
                    "open_interest": 142736.0,
                    "adjustment_offset": None,
                }
            ]
        ),
    )

    items = futures.QuoteMuxFutures().get_quotes_1m("if", "main_continuous")

    assert items == [
        FutureBar1mItem(
            product_code="IF",
            exchange="CFFEX",
            series_type="main_continuous",
            bar_time="2026-08-11 09:31:00",
            open=4642.0,
            high=4647.8,
            low=4640.0,
            close=4647.4,
            volume=1351.0,
            open_interest=142736.0,
            adjustment_offset=None,
        )
    ]
    assert "source_key" not in items[0].model_dump()


def test_future_query_uses_null_for_missing_time_bounds(monkeypatch) -> None:
    monkeypatch.setattr(futures, "ensure_future_schema", lambda: None)
    captured: dict[str, object] = {}

    def fake_query(_query: str, params: tuple[object, ...]) -> pd.DataFrame:
        captured["params"] = params
        return pd.DataFrame()

    monkeypatch.setattr(futures, "query_dataframe", fake_query)

    futures.QuoteMuxFutures().get_quotes_1m("IF", "back_adjusted_continuous", start_time="2026-01-20 14:59:00")

    assert captured["params"] == (["IF"], "apex_l0_adjusted", "2026-01-20 14:59:00", "2026-01-20 14:59:00", None, None, 10000)


def test_main_continuous_update_starts_from_local_l0_and_upserts(monkeypatch) -> None:
    monkeypatch.setattr(futures, "ensure_future_schema", lambda: None)
    monkeypatch.setattr(futures, "PRODUCT_EXCHANGE", {"IF": "CFFEX"})
    local_coverage = FutureSeriesCoverageItem(
        product_code="IF",
        exchange="CFFEX",
        series_type="back_adjusted_continuous",
        row_count=1,
        first_bar_time="2026-08-10 15:00:00",
        last_bar_time="2026-08-10 15:00:00",
    )
    handler_calls: list[tuple[str, str, str, str]] = []

    def handler(product_code: str, exchange: str, start_time: str, end_time: str) -> list[FutureBar1mItem]:
        handler_calls.append((product_code, exchange, start_time, end_time))
        if len(handler_calls) == 1:
            raise TimeoutError("transient")
        return [
            FutureBar1mItem(
                product_code="IF",
                exchange="CFFEX",
                series_type="main_continuous",
                bar_time="2026-08-11 09:31:00",
                open=1,
                high=1,
                low=1,
                close=1,
                volume=1,
                open_interest=1,
                adjustment_offset=None,
            )
        ]

    class Registry:
        def get_handler(self, package_id: str, handler_name: str):
            assert (package_id, handler_name) == ("shinny_edb", "get_future_main_continuous_1m")
            return handler

    runtime = futures.QuoteMuxFutures()
    monkeypatch.setattr(futures, "_china_market_now", lambda: datetime(2026, 8, 12, 20, 35))
    monkeypatch.setattr(runtime, "list_coverage", lambda: [local_coverage])
    monkeypatch.setattr(runtime, "_upsert_main_continuous", lambda items: len(items))
    monkeypatch.setattr(futures, "get_default_source_package_registry", lambda: Registry())
    monkeypatch.setattr(futures.time_module, "sleep", lambda _seconds: None)

    result = runtime.update_main_continuous(overlap_days=2)

    assert result["fetched_rows"] == 1
    assert result["written_rows"] == 1
    assert result["updated_products"] == 1
    assert len(handler_calls) == 2
    assert handler_calls[-1][:2] == ("IF", "CFFEX")
    assert datetime.fromisoformat(handler_calls[-1][2]) <= datetime(2026, 8, 8, 15, 0)
    assert handler_calls[-1][3] == "2026-08-12 20:35:00"
