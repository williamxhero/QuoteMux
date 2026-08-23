from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
import sys
import threading

import pandas as pd
import pytest

from quotemux.config_runtime.models import SourceInstanceConfig
from quotemux.config_runtime.runtime import QuoteMuxConfigRuntime, reset_config_runtime_cache
from quotemux.capabilities import DERIVED_CAPABILITY_BASE_IDS, get_capability_definition, list_capability_ids, list_public_api_bindings
from quotemux.source_packages.loader import load_builtin_manifests
from quotemux.settings import QuoteMuxSettings
from quotemux.source_packages.registry import build_source_package_registry
from quotemux.source_packages.instance_context import use_source_instance
from quotemux.source_packages.isolated import IsolatedPackageHandler, WORKER_RESPONSE_PREFIX, _decode_worker_response


@pytest.fixture(autouse=True)
def isolate_runtime_root(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("QUOTEMUX_RUNTIME_ROOT", str(tmp_path / "runtime"))
    reset_config_runtime_cache()
    package_project_root = Path(__file__).resolve().parents[2] / "QuoteMux_Packages"
    QuoteMuxConfigRuntime().add_import_root(str(package_project_root))
    yield
    reset_config_runtime_cache()


def test_builtin_manifests_cover_current_package_inventory() -> None:
    manifests = load_builtin_manifests()
    package_ids = {manifest.package_id for manifest in manifests}
    tushare_manifest = next(manifest for manifest in manifests if manifest.package_id == "tushare")

    assert package_ids == {"tushare", "opentdx", "efinance", "mootdx", "akshare", "derived_core", "crawler_provider", "eastmoney_official", "cninfo_evidence", "shinny_edb", "shinny_tqsdk"}
    assert tushare_manifest.get_handler_target("get_stock_basic") == "quotemux_packages.tushare.source:get_stock_basic"
    assert tushare_manifest.get_handler_target("get_market_sessions") == "quotemux_packages.tushare.source:get_market_sessions"
    derived_manifest = next(manifest for manifest in manifests if manifest.package_id == "derived_core")
    assert derived_manifest.get_handler_target("get_technical_factors") == "quotemux_packages.derived_core.source:get_technical_factors"
    assert derived_manifest.get_handler_target("get_strategy_factor_window") == "quotemux_packages.derived_core.source:get_strategy_factor_window"
    assert derived_manifest.get_handler_target("get_shareholder_changes") == "quotemux_packages.derived_core.source:get_shareholder_changes"
    assert derived_manifest.get_handler_target("get_hl_signal") == "quotemux_packages.derived_core.source:get_hl_signal"
    assert derived_manifest.get_handler_target("get_concept_quotes") == "quotemux_packages.derived_core.source:get_concept_quotes"
    assert derived_manifest.get_handler_target("get_previous_trading_days") == "quotemux_packages.derived_core.source:get_previous_trading_days"
    assert derived_manifest.get_handler_target("get_next_trading_days") == "quotemux_packages.derived_core.source:get_next_trading_days"
    assert derived_manifest.get_handler_target("get_yearly_trading_calendar") == "quotemux_packages.derived_core.source:get_yearly_trading_calendar"


def test_builtin_manifest_handlers_and_capability_coverage() -> None:
    package_project_root = Path(__file__).resolve().parents[2] / "QuoteMux_Packages"
    registry = build_source_package_registry((str(package_project_root),))
    manifests = registry.list_packages()
    manifest_capability_ids = {capability.capability_id for manifest in manifests for capability in manifest.capabilities}
    known_capability_ids = set(list_capability_ids())
    derived_capability_ids = set(DERIVED_CAPABILITY_BASE_IDS)
    manifest_derived_capability_ids = {capability.capability_id for manifest in manifests if manifest.package_id == "derived_core" for capability in manifest.capabilities}

    assert "markets.events.news" in known_capability_ids
    assert known_capability_ids - manifest_capability_ids == {
        "concepts.alias.groups",
        "concepts.alias.resolve",
        "futures.quotes.back_adjusted_continuous.1m",
    } | (derived_capability_ids - manifest_derived_capability_ids)
    assert manifest_capability_ids.intersection(derived_capability_ids) == derived_capability_ids
    assert manifest_capability_ids.intersection(derived_capability_ids).issubset(manifest_derived_capability_ids)
    assert all(registry.check_package_health(manifest.package_id).status == "ok" for manifest in manifests)

    for binding in list_public_api_bindings():
        assert set(binding.capability_ids).issubset(known_capability_ids)

    for manifest in manifests:
        for capability in manifest.capabilities:
            assert capability.capability_id in known_capability_ids
            assert capability.handler_name in manifest.list_handler_names()
            assert registry.has_handler(manifest.package_id, capability.handler_name)


def test_namespace_import_root_does_not_shadow_provider_dependency(monkeypatch, tmp_path: Path) -> None:
    real_site = tmp_path / "real_site"
    real_akshare = real_site / "akshare"
    real_akshare.mkdir(parents=True)
    (real_akshare / "__init__.py").write_text("VALUE = 'real'\n", encoding="utf-8")

    source_root = tmp_path / "source_packages"
    shadow_akshare = source_root / "akshare"
    shadow_akshare.mkdir(parents=True)
    (shadow_akshare / "__init__.py").write_text("VALUE = 'shadow'\n", encoding="utf-8")

    package_root = source_root / "quotemux_packages" / "demo"
    package_root.mkdir(parents=True)
    (source_root / "quotemux_packages" / "__init__.py").write_text("", encoding="utf-8")
    (package_root / "__init__.py").write_text("", encoding="utf-8")
    (package_root / "source.py").write_text("import akshare\n\ndef get_value():\n    return akshare.VALUE\n", encoding="utf-8")
    (package_root / "quotemux_package.json").write_text(
        json.dumps(
            {
                "package_id": "demo",
                "version": "1.0.0",
                "source_name": "demo",
                "display_name": "Demo",
                "description": "",
                "contract_names": ["stocks.quotes.daily"],
                "capability_tags": ["test"],
                "config_schema": [],
                "secret_fields": [],
                "supports_multi_instance": True,
                "handler_targets": {"get_value": "quotemux_packages.demo.source:get_value"},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    monkeypatch.syspath_prepend(str(real_site))
    for module_name in ("akshare", "quotemux_packages", "quotemux_packages.demo", "quotemux_packages.demo.source"):
        sys.modules.pop(module_name, None)

    registry = build_source_package_registry((str(source_root / "quotemux_packages"),))

    assert registry.get_handler("demo", "get_value")() == "real"


def test_package_requirements_use_isolated_handler_process(tmp_path: Path) -> None:
    source_root = tmp_path / "isolated_packages"
    package_root = source_root / "demo_isolated"
    package_root.mkdir(parents=True)
    (package_root / "__init__.py").write_text("", encoding="utf-8")
    (package_root / "source.py").write_text("def get_value():\n    return 'isolated'\n", encoding="utf-8")
    (package_root / "requirements.txt").write_text("", encoding="utf-8")
    (package_root / "quotemux_package.json").write_text(
        json.dumps(
            {
                "package_id": "demo_isolated",
                "version": "1.0.0",
                "source_name": "demo_isolated",
                "display_name": "Demo Isolated",
                "description": "",
                "contract_names": ["stocks.quotes.daily"],
                "capability_tags": ["test"],
                "config_schema": [],
                "secret_fields": [],
                "supports_multi_instance": True,
                "handler_targets": {"get_value": "demo_isolated.source:get_value"},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    registry = build_source_package_registry((str(source_root),))

    assert isinstance(registry.get_handler("demo_isolated", "get_value"), IsolatedPackageHandler)


def test_isolated_worker_response_ignores_stdout_noise() -> None:
    import base64
    import pickle

    response = {"status": "ok", "result": "isolated"}
    stdout = b"pip install log\n" + WORKER_RESPONSE_PREFIX + base64.b64encode(pickle.dumps(response)) + b"\n"

    assert _decode_worker_response(stdout, b"", "demo_isolated") == response


def test_isolated_handler_uses_source_instance_timeout(tmp_path: Path, monkeypatch) -> None:
    from types import SimpleNamespace
    from quotemux.source_packages import isolated

    source_root = tmp_path / "isolated_packages"
    package_root = source_root / "demo_timeout"
    package_root.mkdir(parents=True)
    (package_root / "__init__.py").write_text("", encoding="utf-8")
    (package_root / "source.py").write_text("import time\n\ndef get_value():\n    time.sleep(5)\n    return 'late'\n", encoding="utf-8")
    (package_root / "requirements.txt").write_text("", encoding="utf-8")
    (package_root / "quotemux_package.json").write_text(
        json.dumps(
            {
                "package_id": "demo_timeout",
                "version": "1.0.0",
                "source_name": "demo_timeout",
                "display_name": "Demo Timeout",
                "description": "",
                "contract_names": ["stocks.quotes.daily"],
                "capability_tags": ["test"],
                "config_schema": [],
                "secret_fields": [],
                "supports_multi_instance": True,
                "handler_targets": {"get_value": "demo_timeout.source:get_value"},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    registry = build_source_package_registry((str(source_root),))
    manifest = registry.get_manifest("demo_timeout")
    handler = IsolatedPackageHandler(manifest, manifest.get_handler_target("get_value"), (str(source_root),))
    instance = SourceInstanceConfig.from_dict(
        {
            "instance_id": "demo-timeout-default",
            "package_id": "demo_timeout",
            "display_name": "Demo Timeout",
            "enabled": True,
            "priority": 1,
            "config_values": {"timeout_seconds": "1"},
            "secret_values": {},
            "tags": [],
        }
    )

    captured: dict[str, object] = {}

    def timeout_run(*args, **kwargs):
        captured["timeout"] = kwargs["timeout"]
        raise isolated.subprocess.TimeoutExpired(args[0], kwargs["timeout"])

    monkeypatch.setattr(isolated, "ensure_package_environment", lambda manifest: SimpleNamespace(python_executable=sys.executable))
    monkeypatch.setattr(isolated.subprocess, "run", timeout_run)

    with use_source_instance(instance):
        with pytest.raises(TimeoutError, match="执行超时"):
            handler()
    assert captured["timeout"] == 1


def test_derived_and_static_capabilities_do_not_claim_native_support() -> None:
    expected_levels = {
        "concepts.reference.categories": {"static", "derived"},
        "markets.connect.quotas": {"derived"},
        "markets.trading.sessions": {"static"},
        "stocks.factors.technical": {"derived"},
        "stocks.ownership.shareholders.changes": {"derived"},
        "stocks.signals.hl": {"derived"},
    }
    manifests = load_builtin_manifests()

    for manifest in manifests:
        for capability in manifest.capabilities:
            allowed_levels = expected_levels.get(capability.capability_id)
            if allowed_levels is not None:
                assert capability.support_level in allowed_levels


def test_default_source_order_matches_allowed_packages() -> None:
    for capability_id in list_capability_ids():
        definition = get_capability_definition(capability_id)
        assert set(definition.default_source_order).issubset(set(definition.allowed_packages))
        if capability_id == "markets.events.news":
            assert definition.allowed_packages == ("akshare",)
            assert definition.default_source_order == ("akshare",)


def test_intraday_quotes_prioritize_historical_minute_sources() -> None:
    definition = get_capability_definition("stocks.quotes.intraday")

    assert definition.default_source_order == ("opentdx", "mootdx", "efinance", "akshare", "tushare")


def test_tushare_catalog_adds_replaced_bse_code_as_delisted() -> None:
    from platform_models import BSECodeMappingItem
    from quotemux_packages.tushare import source

    catalog = pd.DataFrame(
        [
            {"code": "920489", "list_status2": "listed", "delist_date": ""},
        ]
    )
    mappings = [
        BSECodeMappingItem(
            old_code="430489",
            new_code="920489",
            effective_date="20200727",
            status="active",
        )
    ]

    result = source._apply_bse_code_mappings(catalog, mappings)

    old_code = result[result["code"] == "430489"].iloc[0]
    new_code = result[result["code"] == "920489"].iloc[0]
    assert old_code["list_status2"] == "delisted"
    assert old_code["delist_date"] == "20200727"
    assert new_code["list_status2"] == "listed"
    assert new_code["delist_date"] == ""


def test_tushare_pro_client_uses_configured_request_timeout(monkeypatch) -> None:
    from quotemux_packages.tushare import source

    class FakeTushare:
        def __init__(self) -> None:
            self.calls: list[tuple[str, float]] = []

        def pro_api(self, api_key: str, timeout: float):
            self.calls.append((api_key, timeout))
            return object()

    fake_tushare = FakeTushare()
    source._build_ts_pro.cache_clear()
    monkeypatch.setattr(source, "ts", fake_tushare)
    monkeypatch.setattr(source, "get_provider_api_key", lambda: "test-token")
    instance = SourceInstanceConfig.from_dict(
        {
            "instance_id": "tushare-timeout-test",
            "package_id": "tushare",
            "display_name": "Tushare",
            "enabled": True,
            "priority": 1,
            "config_values": {"timeout_seconds": "7"},
            "secret_values": {},
            "tags": [],
        }
    )

    with use_source_instance(instance):
        assert source.get_ts_pro() is not None
    assert fake_tushare.calls == [("test-token", 7.0)]

    source._build_ts_pro.cache_clear()


def test_tushare_adj_factors_fill_confirmed_internal_trading_day_gap(monkeypatch) -> None:
    from quotemux_packages.tushare import source

    class _Pro:
        adj_factor = object()
        daily = object()

    def fake_call_tushare_api(api_name, fetcher, **kwargs):
        del fetcher, kwargs
        if api_name == "adj_factor":
            return pd.DataFrame(
                [
                    {"trade_date": "20260202", "adj_factor": 1.861},
                    {"trade_date": "20260204", "adj_factor": 1.861},
                ]
            )
        assert api_name == "daily"
        return pd.DataFrame([{"trade_date": "20260203"}])

    monkeypatch.setattr(source, "get_ts_pro", lambda: _Pro())
    monkeypatch.setattr(source, "call_tushare_api", fake_call_tushare_api)

    items = source.get_adj_factors("002462", "2026-02-03", "2026-02-03", "")

    assert [(item.trade_date, item.adj_factor) for item in items] == [("20260203", 1.861)]


def test_tushare_adj_factors_do_not_cross_an_adjustment_event(monkeypatch) -> None:
    from quotemux_packages.tushare import source

    class _Pro:
        adj_factor = object()
        daily = object()

    def fake_call_tushare_api(api_name, fetcher, **kwargs):
        del fetcher, kwargs
        if api_name == "adj_factor":
            return pd.DataFrame(
                [
                    {"trade_date": "20260202", "adj_factor": 1.0},
                    {"trade_date": "20260204", "adj_factor": 2.0},
                ]
            )
        return pd.DataFrame([{"trade_date": "20260203"}])

    monkeypatch.setattr(source, "get_ts_pro", lambda: _Pro())
    monkeypatch.setattr(source, "call_tushare_api", fake_call_tushare_api)

    assert source.get_adj_factors("002462", "2026-02-03", "2026-02-03", "") == []


def test_tushare_adj_factors_fill_bak_daily_confirmed_gap(monkeypatch) -> None:
    from quotemux_packages.tushare import source

    class _Pro:
        adj_factor = object()
        daily = object()
        bak_daily = object()

    def fake_call_tushare_api(api_name, fetcher, **kwargs):
        del fetcher, kwargs
        if api_name == "adj_factor":
            return pd.DataFrame(
                [
                    {"trade_date": "20260202", "adj_factor": 1.861},
                    {"trade_date": "20260204", "adj_factor": 1.861},
                ]
            )
        if api_name == "daily":
            return pd.DataFrame()
        assert api_name == "bak_daily"
        return pd.DataFrame([{"trade_date": "20260203"}])

    monkeypatch.setattr(source, "get_ts_pro", lambda: _Pro())
    monkeypatch.setattr(source, "call_tushare_api", fake_call_tushare_api)

    items = source.get_adj_factors("002462", "2026-02-03", "2026-02-03", "")

    assert [(item.trade_date, item.adj_factor) for item in items] == [("20260203", 1.861)]


def test_tushare_adj_factors_do_not_fill_bak_daily_gap_across_adjustment_event(monkeypatch) -> None:
    from quotemux_packages.tushare import source

    class _Pro:
        adj_factor = object()
        daily = object()
        bak_daily = object()

    def fake_call_tushare_api(api_name, fetcher, **kwargs):
        del fetcher, kwargs
        if api_name == "adj_factor":
            return pd.DataFrame(
                [
                    {"trade_date": "20260202", "adj_factor": 1.0},
                    {"trade_date": "20260204", "adj_factor": 2.0},
                ]
            )
        if api_name == "daily":
            return pd.DataFrame()
        assert api_name == "bak_daily"
        return pd.DataFrame([{"trade_date": "20260203"}])

    monkeypatch.setattr(source, "get_ts_pro", lambda: _Pro())
    monkeypatch.setattr(source, "call_tushare_api", fake_call_tushare_api)

    assert source.get_adj_factors("002462", "2026-02-03", "2026-02-03", "") == []


def test_tushare_adj_factors_do_not_fill_unconfirmed_gap(monkeypatch) -> None:
    from quotemux_packages.tushare import source

    class _Pro:
        adj_factor = object()
        daily = object()
        bak_daily = object()

    def fake_call_tushare_api(api_name, fetcher, **kwargs):
        del fetcher, kwargs
        if api_name == "adj_factor":
            return pd.DataFrame(
                [
                    {"trade_date": "20260202", "adj_factor": 1.861},
                    {"trade_date": "20260204", "adj_factor": 1.861},
                ]
            )
        return pd.DataFrame()

    monkeypatch.setattr(source, "get_ts_pro", lambda: _Pro())
    monkeypatch.setattr(source, "call_tushare_api", fake_call_tushare_api)

    assert source.get_adj_factors("002462", "2026-02-03", "2026-02-03", "") == []


def test_akshare_adj_factors_only_handle_b_shares() -> None:
    from quotemux_packages.akshare import source

    assert source.get_adj_factors("002462", "2026-02-03", "2026-02-03", "") == []


def test_opentdx_historical_bar_count_covers_latest_to_target_window(monkeypatch) -> None:
    from quotemux_packages.opentdx import source

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 7, 22, 14, 0, tzinfo=tz)

    monkeypatch.setattr(source, "datetime", FixedDateTime)

    assert source._estimate_bar_count(datetime(2026, 7, 20), datetime(2026, 7, 20, 23, 59)) == 726


def test_opentdx_maps_new_beijing_exchange_prefix(monkeypatch) -> None:
    from quotemux_packages.opentdx import source

    class FakeMarket:
        BJ = "BJ"
        SH = "SH"
        SZ = "SZ"

    monkeypatch.setattr(source, "MARKET", FakeMarket)

    assert source._market_from_code("920117") == "BJ"
    assert source._market_from_code("900901") == "SH"


def test_opentdx_reuses_client_inside_provider_worker_thread(monkeypatch) -> None:
    from quotemux_packages.opentdx import source

    created_clients: list[object] = []

    class FakeClient:
        def __exit__(self, exc_type, exc_value, traceback) -> None:
            return None

    def create_client() -> FakeClient:
        client = FakeClient()
        created_clients.append(client)
        return client

    monkeypatch.setattr(source, "_CLIENT_STATE", threading.local())
    monkeypatch.setattr(source, "_client_factory", lambda: create_client)
    monkeypatch.setattr(source, "call_provider_api", lambda provider, api_name, invoke: invoke())

    first_client = source._call_tdx("stock_kline", lambda client: client)
    second_client = source._call_tdx("stock_kline", lambda client: client)

    assert first_client is second_client
    assert created_clients == [first_client]


def test_opentdx_intraday_codes_use_bounded_concurrency(monkeypatch) -> None:
    from quotemux_packages.opentdx import source

    barrier = threading.Barrier(4)
    thread_ids: set[int] = set()

    def fake_fetch(code: str, start_dt: datetime, end_dt: datetime, adjust: str) -> pd.DataFrame:
        del code, start_dt, end_dt, adjust
        thread_ids.add(threading.get_ident())
        barrier.wait(timeout=2)
        return pd.DataFrame()

    monkeypatch.setattr(source, "read_cache_frame", lambda path: pd.DataFrame())
    monkeypatch.setattr(source, "_fetch_stock_intraday_frame", fake_fetch)

    items = source.get_stock_quotes(
        ["000001", "000002", "000003", "000004"],
        "1m",
        "2026-07-20",
        "",
        "",
        "",
        "",
        None,
        "none",
    )

    assert items == []
    assert len(thread_ids) == 4


def test_opentdx_intraday_isolates_single_code_failure(monkeypatch) -> None:
    from quotemux_packages.opentdx import source

    def fake_fetch(code: str, start_dt: datetime, end_dt: datetime, adjust: str) -> pd.DataFrame:
        del start_dt, end_dt, adjust
        if code == "000001":
            raise ConnectionError("连接失败")
        return pd.DataFrame()

    monkeypatch.setattr(source, "read_cache_frame", lambda path: pd.DataFrame())
    monkeypatch.setattr(source, "_fetch_stock_intraday_frame", fake_fetch)

    items = source.get_stock_quotes(
        ["000001", "000002"],
        "1m",
        "2026-07-20",
        "",
        "",
        "",
        "",
        None,
        "none",
    )

    assert items == []


def test_opentdx_intraday_retries_partial_batch_failure(monkeypatch) -> None:
    from quotemux_packages.opentdx import source

    calls = {"000001": 0, "000002": 0}

    def fake_fetch(code: str, start_dt: datetime, end_dt: datetime, adjust: str) -> pd.DataFrame:
        del start_dt, end_dt
        calls[code] += 1
        if code == "000001" and calls[code] == 1:
            raise ConnectionError("连接失败")
        return pd.DataFrame(
            [
                {
                    "code": code,
                    "trade_time": pd.Timestamp("2026-07-20 09:31:00"),
                    "freq": "1m",
                    "open": 1.0,
                    "high": 1.0,
                    "low": 1.0,
                    "close": 1.0,
                    "volume": 1.0,
                    "amount": 1.0,
                    "adjust": adjust,
                }
            ]
        )

    monkeypatch.setattr(source, "read_cache_frame", lambda path: pd.DataFrame())
    monkeypatch.setattr(source, "write_cache_frame", lambda path, frame: None)
    monkeypatch.setattr(source, "_fetch_stock_intraday_frame", fake_fetch)

    items = source.get_stock_quotes(
        ["000001", "000002"],
        "1m",
        "2026-07-20",
        "",
        "",
        "",
        "",
        None,
        "none",
    )

    assert [item.code for item in items] == ["000001", "000002"]
    assert calls == {"000001": 2, "000002": 1}


def test_concepts_quotes_daily_uses_derived_core_as_fallback_by_default() -> None:
    definition = get_capability_definition("concepts.quotes.daily")

    assert definition.allowed_packages == ("crawler_provider", "tushare", "efinance", "akshare", "derived_core")
    assert definition.default_source_order == ("crawler_provider", "tushare", "efinance", "akshare", "derived_core")


def test_concept_money_flow_snapshot_uses_derived_core_as_fallback_by_default() -> None:
    definition = get_capability_definition("concepts.indicators.money_flow.snapshot")

    assert definition.allowed_packages == ("tushare", "akshare", "derived_core")
    assert definition.default_source_order == ("tushare", "akshare", "derived_core")


def test_crawler_provider_reads_limit_pool_when_order_book_missing(tmp_path: Path) -> None:
    from quotemux_packages.crawler_provider import source

    warehouse_root = tmp_path / "warehouse"
    limit_pool_dir = warehouse_root / "limit_pool" / "month=2026-06"
    limit_pool_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {"trading_date": "2026-06-18", "limit_type": "limit_up", "stock_code": "600000", "stock_name": "A"},
            {"trading_date": "2026-06-18", "limit_type": "limit_down", "stock_code": "000001", "stock_name": "B"},
        ]
    ).to_parquet(limit_pool_dir / "limit_pool_2026-06-18.parquet")
    instance = SourceInstanceConfig.from_dict(
        {
            "instance_id": "crawler-provider-test",
            "package_id": "crawler_provider",
            "display_name": "Crawler Provider",
            "enabled": True,
            "priority": 1,
            "config_values": {"warehouse_root": str(warehouse_root)},
            "secret_values": {},
            "tags": [],
        }
    )

    with use_source_instance(instance):
        items = source.get_limit_order_amount("2026-06-18")

    assert [(item.code, item.limit_side, item.order_amount) for item in items] == [("000001", "down", None), ("600000", "up", None)]


def test_crawler_provider_reads_concept_warehouse(tmp_path: Path) -> None:
    from quotemux_packages.crawler_provider import source

    warehouse_root = tmp_path / "warehouse"
    (warehouse_root / "concept_catalog" / "provider=eastmoney" / "month=2026-06").mkdir(parents=True)
    (warehouse_root / "concept_trend" / "provider=eastmoney" / "month=2026-06").mkdir(parents=True)
    (warehouse_root / "concept_members" / "provider=eastmoney" / "month=2026-06").mkdir(parents=True)
    pd.DataFrame(
        [
            {"provider": "eastmoney", "trading_date": "2026-06-18", "concept_code": "BK1184", "concept_name": "测试题材", "constituent_count": 1},
        ]
    ).to_parquet(warehouse_root / "concept_catalog" / "provider=eastmoney" / "month=2026-06" / "concept_catalog_2026-06-18.parquet")
    pd.DataFrame(
        [
            {
                "provider": "eastmoney",
                "trade_date": "2026-06-18",
                "concept_code": "BK1184",
                "open_price": 1.0,
                "low_price": 0.9,
                "high_price": 1.2,
                "close_price": 1.1,
                "net_inflow_amount": 100.0,
                "volume_hand": 200.0,
                "turnover_amount": 300.0,
            },
        ]
    ).to_parquet(warehouse_root / "concept_trend" / "provider=eastmoney" / "month=2026-06" / "concept_trend_2026-06-18.parquet")
    pd.DataFrame(
        [
            {"provider": "eastmoney", "trading_date": "2026-06-18", "concept_code": "BK1184", "stock_code": "600000"},
        ]
    ).to_parquet(warehouse_root / "concept_members" / "provider=eastmoney" / "month=2026-06" / "concept_members_BK1184_2026-06-18.parquet")
    instance = SourceInstanceConfig.from_dict(
        {
            "instance_id": "crawler-provider-test",
            "package_id": "crawler_provider",
            "display_name": "Crawler Provider",
            "enabled": True,
            "priority": 1,
            "config_values": {"warehouse_root": str(warehouse_root)},
            "secret_values": {},
            "tags": [],
        }
    )

    with use_source_instance(instance):
        catalog_items = source.get_concept_catalog("concept", "em", "active", 10, 0)
        quote_items = source.get_concept_quotes(["BK1184"], "1d", "2026-06-18", "", "", "", "", None)
        member_items = source.get_concept_members("BK1184", "2026-06-18")

    assert [(item.board_code, item.board_name, item.market) for item in catalog_items] == [("BK1184", "测试题材", "em")]
    assert [(item.board_code, item.trade_time, item.close, item.amount) for item in quote_items] == [("BK1184", "2026-06-18", 1.1, 300.0)]
    assert [(item.board_code, item.code, item.join_date) for item in member_items] == [("BK1184", "600000", "2026-06-18")]


def test_derived_core_board_quotes_uses_weighted_snapshot_metrics(monkeypatch) -> None:
    from quotemux_packages.derived_core import source

    captured: dict[str, object] = {}

    def fake_board_quote_snapshot_frame(board_codes: list[str], trade_date: str):
        import pandas as pd

        captured["board_codes"] = board_codes
        captured["trade_date"] = trade_date
        return pd.DataFrame(
            [
                {
                    "board_code": "BK1001",
                    "board_name": "测试板块",
                    "trade_time": "2026-06-18",
                    "volume": 1200.0,
                    "amount": 13200.0,
                    "pct_chg": 3.5,
                },
            ]
        )

    monkeypatch.setattr(source, "_board_quote_snapshot_frame", fake_board_quote_snapshot_frame)

    items = source.get_board_quotes(["bk1001"], "1d", "2026-06-18", "", "", "", "", None)

    assert captured["board_codes"] == ["BK1001"]
    assert captured["trade_date"] == "2026-06-18"
    assert len(items) == 1
    assert items[0].trade_time == "2026-06-18"
    assert items[0].close is None
    assert items[0].pre_close is None
    assert items[0].change is None
    assert items[0].pct_chg == 3.5
    assert items[0].amount == 13200.0


def test_derived_core_board_quotes_empty_codes_returns_empty(monkeypatch) -> None:
    from quotemux_packages.derived_core import source

    def fake_board_quote_frame(board_codes: list[str], start_date: str, end_date: str):
        raise AssertionError("empty board_codes should not query derived board quotes")

    monkeypatch.setattr(source, "_board_quote_frame", fake_board_quote_frame)

    items = source.get_board_quotes([], "1d", "2026-06-18", "", "", "", "", None)

    assert items == []


def test_derived_core_concept_quotes_use_audit_lag_pre_close(monkeypatch) -> None:
    from quotemux_packages.derived_core import source

    captured: dict[str, object] = {}

    def fake_query_dataframe(query: str, params: tuple[object, ...]):
        import pandas as pd

        captured["query"] = query
        captured["params"] = params
        return pd.DataFrame()

    monkeypatch.setattr("quotemux.infra.db.client.query_dataframe", fake_query_dataframe)

    source._canonical_concept_quote_frame(["C1"], "2026-07-01", "2026-07-10")

    query_text = str(captured["query"])
    assert "lag(stock_rows.close)" in query_text
    assert captured["params"] == ("2026-07-10", ["C1"], "2026-07-10", "2026-07-01", "2026-07-10")


def test_derived_core_concept_snapshot_uses_previous_close(monkeypatch) -> None:
    from quotemux_packages.derived_core import source

    captured: dict[str, object] = {}

    def fake_query_dataframe(query: str, params: tuple[object, ...]):
        import pandas as pd

        captured["query"] = query
        captured["params"] = params
        return pd.DataFrame()

    monkeypatch.setattr("quotemux.infra.db.client.query_dataframe", fake_query_dataframe)

    source._canonical_concept_quote_snapshot_frame(["C1"], "2026-07-10")

    query_text = str(captured["query"])
    assert "previous_rows.close as pre_close" in query_text
    assert captured["params"] == ("2026-07-10", ["C1"], "2026-07-10", "2026-07-10", "2026-07-10")


def test_derived_core_industry_board_quotes_returns_complete_daily_prices(monkeypatch) -> None:
    from quotemux_packages.derived_core import source

    def fake_industry_frame(start_date: str, end_date: str, board_codes: list[str]):
        import pandas as pd

        return pd.DataFrame(
            [
                {
                    "board_code": "INDUSTRY:家用电器",
                    "board_name": "家用电器",
                    "trade_time": "2026-07-10",
                    "open": 10.0,
                    "high": 11.0,
                    "low": 9.5,
                    "close": 10.5,
                    "pre_close": 9.8,
                    "change": 0.7,
                    "pct_chg": 7.142857,
                    "volume": 1200.0,
                    "amount": 13200.0,
                }
            ]
        )

    monkeypatch.setattr(source, "_industry_board_quote_frame", fake_industry_frame)

    items = source.get_industry_board_quotes([], "1d", "2026-07-10", "", "", "", "", None)

    assert len(items) == 1
    assert items[0].open == 10.0
    assert items[0].high == 11.0
    assert items[0].low == 9.5
    assert items[0].close == 10.5
    assert items[0].pre_close == 9.8
    assert items[0].change == 0.7
    assert items[0].pct_chg == 7.142857


def test_derived_core_board_money_flow_sums_member_stock_flow(monkeypatch) -> None:
    from platform_models import BoardMemberItem, StockMoneyFlowItem
    from quotemux_packages.derived_core import source

    class _Concepts:
        def get_members(self, concept_id: str, trade_date: str):
            assert concept_id == "BK1001"
            assert trade_date == "2026-06-18"
            return [
                BoardMemberItem(board_code="BK1001", code="600000", name="A"),
                BoardMemberItem(board_code="BK1001", code="600001", name="B"),
            ]

    class _Stocks:
        def get_money_flow_batch(self, codes: str, trade_date: str, view: str):
            assert codes == "600000,600001"
            assert trade_date == "2026-06-18"
            assert view == "main"
            return [
                StockMoneyFlowItem(code="600000", trade_date="2026-06-18", view="main", main_inflow=100.0, main_outflow=40.0, net_inflow=60.0),
                StockMoneyFlowItem(code="600001", trade_date="2026-06-18", view="main", main_inflow=200.0, main_outflow=70.0, net_inflow=130.0),
            ]

    class _QuoteMux:
        concepts = _Concepts()
        stocks = _Stocks()

    monkeypatch.setattr(source, "_quote_mux", lambda: _QuoteMux())

    items = source.get_board_money_flow("bk1001", "2026-06-18", "", "", "board")

    assert len(items) == 1
    assert items[0].board_code == "BK1001"
    assert items[0].inflow == 0.000003
    assert items[0].outflow == 0.0000011
    assert items[0].net_inflow == 0.0000019


def test_derived_core_previous_trading_days_uses_explicit_calendar_window(monkeypatch) -> None:
    from platform_models import TradingCalendarItem
    from quotemux_packages.derived_core import source

    captured: dict[str, object] = {}

    class _Markets:
        def get_trading_calendar(self, request):
            captured["start_date"] = request.start_date
            captured["end_date"] = request.end_date
            return [
                TradingCalendarItem(exchange="SSE", trade_date="2026-04-01", is_open=True),
                TradingCalendarItem(exchange="SSE", trade_date="2026-04-02", is_open=True),
                TradingCalendarItem(exchange="SSE", trade_date="2026-04-03", is_open=True),
            ]

    class _QuoteMux:
        markets = _Markets()

    monkeypatch.setattr(source, "_quote_mux", lambda: _QuoteMux())

    items = source.get_previous_trading_days("SSE", "2026-04-03", 1)

    assert captured["start_date"] != ""
    assert captured["end_date"] == "2026-04-03"
    assert [item.trade_date for item in items] == ["2026-04-02"]


def test_tushare_stock_market_from_row_treats_920_as_beijing() -> None:
    from quotemux_packages.tushare import source

    assert source._stock_market_from_row("", "", "920028") == "beijing"
    assert source._stock_market_from_row("科创板", "", "688669") == "star_market"


def test_enabled_sources_ignore_runtime_snapshot_source_order(monkeypatch) -> None:
    class _Snapshot:
        def get_contract_source_order(self, contract_name: str, fallback: tuple[str, ...]) -> tuple[str, ...]:
            del contract_name
            del fallback
            return ("tushare",)

        def get_contract_mode(self, contract_name: str, fallback: str) -> str:
            del contract_name
            return "degraded"

        def get_contract_merge_strategy(self, contract_name: str, fallback: str) -> str:
            del contract_name
            return "first_success"

    class _Runtime:
        def get_active_snapshot(self) -> _Snapshot:
            return _Snapshot()

    monkeypatch.setattr("quotemux.settings.get_config_runtime", lambda: _Runtime())

    settings = QuoteMuxSettings(enabled_sources=("efinance",))

    assert settings.get_contract_source_order("stocks.quotes.daily", ("efinance", "tushare")) == ("efinance", "tushare")
    assert settings.get_contract_mode("stocks.quotes.daily", "auto") == "auto"
    assert settings.get_contract_merge_strategy("stocks.quotes.daily", "priority_fallback") == "priority_fallback"
