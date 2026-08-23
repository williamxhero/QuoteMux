from __future__ import annotations

from hashlib import sha256
import importlib
import json
from pathlib import Path
from types import SimpleNamespace

from pydantic import TypeAdapter, ValidationError
import pytest

from platform_models.p0_fundamentals import (
    CompanyP0Data,
    CompanyP0Request,
    P0Page,
    P0Record,
    P0Request,
    ReportDisclosureP0Data,
    ReportDisclosuresP0Request,
    canonical_json_sha256,
)
from quotemux.capabilities import get_capability_definition, get_public_api_binding
from quotemux.config_runtime.models import SourceInstanceConfig
from quotemux.config_runtime.runtime import QuoteMuxConfigRuntime
from quotemux.config_runtime.store import RuntimeConfigStore
from quotemux.infra.provider_runtime.core import PROVIDER_POLICIES
from quotemux.p0_fundamentals.cache import BoundedP0Cache
from quotemux.p0_fundamentals.errors import P0CacheWriteError, P0QueryError
from quotemux.p0_fundamentals.policy import (
    P0_CACHE_BYTES_BY_CAPABILITY,
    P0_CACHE_TOTAL_BYTES,
    P0_CACHE_TTL_SECONDS_BY_CAPABILITY,
    P0_REQUIRED_PROVIDER_BY_CAPABILITY,
)


query_module = importlib.import_module("quotemux.p0_fundamentals.query")


@pytest.fixture(autouse=True)
def clear_p0_cache() -> None:
    query_module._CACHE.clear()


def _request() -> CompanyP0Request:
    return CompanyP0Request(
        capability_id="stocks.profile.company",
        provider="eastmoney_official",
        code="600000",
        market="SH",
        range_start="",
        range_end="",
        cursor="",
        data_version="quotemux.stocks.profile.company.v1",
        source_version="eastmoney.hsf10.company_survey.v1",
    )


def _page() -> P0Page[CompanyP0Data]:
    projection = {
        "SECURITY_CODE": "600000",
        "SECUCODE": "600000.SH",
        "ORG_NAME": "上海浦东发展银行股份有限公司",
    }
    return P0Page[CompanyP0Data](
        capability_id="stocks.profile.company",
        data_version="quotemux.stocks.profile.company.v1",
        provider="eastmoney_official",
        source="eastmoney_hsf10_company_survey",
        source_version="eastmoney.hsf10.company_survey.v1",
        fetched_at="2026-08-11T10:00:00Z",
        confirmed_empty=False,
        next_cursor="",
        records=[
            P0Record[CompanyP0Data](
                source_event_id="SH.600000:company_survey",
                raw_hash=canonical_json_sha256(projection),
                raw_projection=projection,
                data=CompanyP0Data(
                    code="600000",
                    market="SH",
                    security_code="SH.600000",
                    company_name="浦发银行",
                    company_full_name="上海浦东发展银行股份有限公司",
                    security_type="上交所主板A股",
                    trade_market="上海证券交易所",
                    industry_system="eastmoney_em2016",
                    industry_code="",
                    industry_name="银行",
                    industry_path="金融-银行",
                    industry_csrc_path="金融业-货币金融服务",
                    listing_date="1999-11-10",
                    found_date="1992-10-19",
                ),
            )
        ],
    )


def _disclosure_request() -> ReportDisclosuresP0Request:
    return ReportDisclosuresP0Request(
        capability_id="stocks.finance.report_disclosures",
        provider="cninfo_evidence",
        code="600000",
        market="SH",
        report_period="2025-12-31",
        document_kind="annual",
        range_start="2025-12-31",
        range_end="2025-12-31",
        cursor="",
        data_version="quotemux.stocks.finance.report_disclosures.v2",
        source_version="cninfo_disclosure/v1",
    )


def _disclosure_page() -> P0Page[ReportDisclosureP0Data]:
    projection = {
        "evidence_id": "cninfo:1200000001",
        "code": "600000",
        "report_period": "2025-12-31",
        "document_kind": "annual",
        "published_at": "2026-03-20 18:03:00+08:00",
        "title": "2025年年度报告",
        "source_url": "https://static.cninfo.com.cn/report.pdf",
        "content_hash": "a" * 64,
        "source_version": "cninfo_disclosure/v1",
    }
    return P0Page[ReportDisclosureP0Data](
        capability_id="stocks.finance.report_disclosures",
        data_version="quotemux.stocks.finance.report_disclosures.v2",
        provider="cninfo_evidence",
        source="news_crawler_cninfo_formal_report_evidence",
        source_version="cninfo_disclosure/v1",
        fetched_at="2026-08-12T10:00:00Z",
        confirmed_empty=False,
        next_cursor="",
        records=[
            P0Record[ReportDisclosureP0Data](
                source_event_id="cninfo:1200000001",
                raw_hash=canonical_json_sha256(projection),
                raw_projection=projection,
                data=ReportDisclosureP0Data(
                    code="600000",
                    market="SH",
                    security_code="SH.600000",
                    report_period="2025-12-31",
                    report_kind="annual",
                    notice_date="2026-03-20",
                    notice_title="2025年年度报告",
                    article_code="cninfo:1200000001",
                    evidence_id="cninfo:1200000001",
                    published_at="2026-03-20 18:03:00+08:00",
                    source_url="https://static.cninfo.com.cn/report.pdf",
                    content_hash="a" * 64,
                ),
            )
        ],
    )


def test_request_is_discriminated_and_accepts_only_one_provider() -> None:
    payload = _request().model_dump(mode="json")
    assert TypeAdapter(P0Request).validate_python(payload) == _request()
    payload["provider"] = ["eastmoney_official", "tushare"]
    with pytest.raises(ValidationError):
        TypeAdapter(P0Request).validate_python(payload)
    assert (
        TypeAdapter(P0Request).validate_python(
            _disclosure_request().model_dump(mode="json")
        )
        == _disclosure_request()
    )


def test_canonical_sha256_is_utf8_sorted_json_and_rejects_float() -> None:
    projection = {"名称": "浦发银行", "value": 1}
    canonical = json.dumps(
        projection, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    assert canonical_json_sha256(projection) == sha256(canonical).hexdigest()
    with pytest.raises(TypeError, match="禁止 float"):
        canonical_json_sha256({"value": 1.5})


def test_cache_and_live_pages_are_deeply_equal(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0

    def fake_live(
        request: P0Request, adapter: TypeAdapter[object]
    ) -> P0Page[CompanyP0Data]:
        nonlocal calls
        calls += 1
        return _page()

    monkeypatch.setattr(query_module, "_load_live_page", fake_live)
    live = query_module.query(_request())
    cached = query_module.query(_request())
    assert calls == 1
    assert live.model_dump(mode="json") == cached.model_dump(mode="json")


def test_cache_corruption_stops_without_live_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(query_module._CACHE, "get", lambda request: b'{"broken":true}')
    called = False

    def forbidden_live(*args: object) -> object:
        nonlocal called
        called = True
        raise AssertionError("cache corruption 不得 fallback")

    monkeypatch.setattr(query_module, "_load_live_page", forbidden_live)
    with pytest.raises(P0QueryError) as error:
        query_module.query(_request())
    assert error.value.kind == "cache_error"
    assert called is False


def test_source_version_change_is_contract_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    changed = _page().model_copy(
        update={"source_version": "eastmoney.hsf10.company_survey.v2"}
    )
    monkeypatch.setattr(
        query_module, "_load_live_page", lambda request, adapter: changed
    )
    with pytest.raises(P0QueryError) as error:
        query_module.query(_request())
    assert error.value.kind == "contract_error"


def test_live_validation_source_version_change_is_contract_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    instance = SourceInstanceConfig(
        instance_id="eastmoney_official-default",
        package_id="eastmoney_official",
        display_name="东方财富官方数据默认实例",
        enabled=True,
        priority=1,
        timeout_seconds=None,
        config_values={},
        secret_values={},
        tags=(),
    )
    manifest = SimpleNamespace(supports_multi_instance=False)
    payload = _page().model_dump(mode="json")
    payload["source_version"] = "eastmoney.hsf10.company_survey.v2"
    registry = SimpleNamespace(
        get_manifest=lambda provider: manifest,
        get_handler=lambda provider, handler: lambda request: payload,
    )
    snapshot = SimpleNamespace(list_enabled_source_instances=lambda: (instance,))
    runtime = SimpleNamespace(get_active_snapshot=lambda: snapshot)
    monkeypatch.setattr(
        query_module, "get_default_source_package_registry", lambda: registry
    )
    monkeypatch.setattr(query_module, "get_config_runtime", lambda: runtime)
    monkeypatch.setattr(
        query_module,
        "call_provider_api",
        lambda provider, api_name, handler, request: handler(request),
    )
    with pytest.raises(P0QueryError) as error:
        query_module.query(_request())
    assert error.value.kind == "contract_error"


def test_live_path_uses_exactly_one_explicit_instance_without_generic_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    instance = SourceInstanceConfig(
        instance_id="eastmoney_official-default",
        package_id="eastmoney_official",
        display_name="东方财富官方数据默认实例",
        enabled=True,
        priority=1,
        timeout_seconds=None,
        config_values={},
        secret_values={},
        tags=(),
    )
    manifest = SimpleNamespace(supports_multi_instance=False)
    registry = SimpleNamespace(
        get_manifest=lambda provider: manifest,
        get_handler=lambda provider, handler: lambda payload: _page().model_dump(
            mode="json"
        ),
    )
    snapshot = SimpleNamespace(
        list_enabled_source_instances=lambda: (instance,),
        get_contract_source_instances=lambda *args: pytest.fail(
            "P0 query 不得读取 generic source order"
        ),
        get_contract_source_order=lambda *args: pytest.fail(
            "P0 query 不得进入 generic fallback"
        ),
    )
    runtime = SimpleNamespace(get_active_snapshot=lambda: snapshot)
    captured: list[tuple[str, str]] = []

    def fake_provider_call(
        provider: str, api_name: str, handler: object, payload: object
    ) -> object:
        captured.append((provider, api_name))
        return handler(payload)

    monkeypatch.setattr(
        query_module, "get_default_source_package_registry", lambda: registry
    )
    monkeypatch.setattr(query_module, "get_config_runtime", lambda: runtime)
    monkeypatch.setattr(query_module, "call_provider_api", fake_provider_call)
    result = query_module.query(_request())
    assert result.records[0].data.code == "600000"
    assert captured == [("eastmoney_official", "stocks.profile.company")]


def test_p0_registry_policy_cache_capacity_and_ttl_are_frozen() -> None:
    assert P0_REQUIRED_PROVIDER_BY_CAPABILITY == {
        "stocks.profile.company": "eastmoney_official",
        "stocks.corporate_actions.share_changes": "eastmoney_official",
        "stocks.finance.report_disclosures": "cninfo_evidence",
        "stocks.finance.statements": "eastmoney_official",
    }
    with pytest.raises(KeyError):
        get_public_api_binding("/api/quotemux/p0/query")
    provider_policy = PROVIDER_POLICIES["eastmoney_official"]
    assert provider_policy.concurrency == 1
    assert provider_policy.calls_per_second == 2.0
    assert provider_policy.max_retries == 0
    evidence_policy = PROVIDER_POLICIES["cninfo_evidence"]
    assert evidence_policy.concurrency == 1
    assert evidence_policy.calls_per_second == 2.0
    assert evidence_policy.max_retries == 0
    assert P0_CACHE_TOTAL_BYTES == 64 * 1024 * 1024
    assert P0_CACHE_BYTES_BY_CAPABILITY["stocks.finance.statements"] == 32 * 1024 * 1024
    assert (
        P0_CACHE_TTL_SECONDS_BY_CAPABILITY["stocks.finance.report_disclosures"]
        == 30 * 86400
    )


def test_clean_runtime_default_profile_contains_one_enabled_p0_instance_per_provider(
    tmp_path: Path,
) -> None:
    runtime_root = tmp_path / "runtime"
    runtime = QuoteMuxConfigRuntime(runtime_root)
    runtime.ensure_initialized()

    store = RuntimeConfigStore(runtime_root)
    instances = tuple(
        item
        for item in store.read_instances()
        if item.package_id == "eastmoney_official"
    )
    assert len(instances) == 1
    assert instances[0].instance_id == "eastmoney_official-default"
    assert instances[0].enabled is True
    assert store.read_state().active_profile_id == "profile-default"

    snapshot = runtime.get_active_snapshot()
    active_instances = tuple(
        item
        for item in snapshot.list_enabled_source_instances()
        if item.package_id == "eastmoney_official"
    )
    assert snapshot.profile_id == "profile-default"
    assert active_instances == instances
    evidence_instances = tuple(
        item
        for item in snapshot.list_enabled_source_instances()
        if item.package_id == "cninfo_evidence"
    )
    assert len(evidence_instances) == 1
    assert evidence_instances[0].instance_id == "cninfo_evidence-default"
    assert evidence_instances[0].config_values == {"base_url": ""}


def test_legacy_capability_metadata_is_unchanged_by_p0_route() -> None:
    expected = {
        "stocks.profile.company": (
            ("/api/stocks/{code}/profile",),
            "priority_fallback",
            ("code",),
        ),
        "stocks.corporate_actions.share_changes": (
            ("/api/stocks/{code}/corporate-actions/share-changes",),
            "append_dedupe",
            ("code", "trade_date"),
        ),
        "stocks.finance.statements": (
            ("/api/stocks/finance/statements",),
            "append_dedupe",
            ("code", "report_period", "report_type"),
        ),
    }
    for capability_id, (api_paths, merge_strategy, key_fields) in expected.items():
        definition = get_capability_definition(capability_id)
        assert definition.api_paths == api_paths
        assert definition.allowed_packages == ("tushare", "akshare")
        assert definition.default_source_order == ("tushare", "akshare")
        assert definition.default_merge_strategy == merge_strategy
        assert definition.key_fields == key_fields


def test_report_disclosures_inventory_uses_evidence_identity() -> None:
    definition = get_capability_definition("stocks.finance.report_disclosures")
    assert definition.allowed_packages == ("cninfo_evidence",)
    assert definition.default_source_order == ("cninfo_evidence",)
    assert definition.key_fields == ("code", "evidence_id")
    assert "trade_date" not in definition.key_fields


def test_disclosure_page_validates_evidence_hash_and_source() -> None:
    query_module._validate_page(_disclosure_request(), _disclosure_page())
    broken = _disclosure_page().model_copy(update={"source": "eastmoney_notice_security_ann"})
    with pytest.raises(P0QueryError) as error:
        query_module._validate_page(_disclosure_request(), broken)
    assert error.value.kind == "contract_error"


def test_cache_capacity_failure_is_not_treated_as_miss() -> None:
    cache = BoundedP0Cache()
    oversized = b"x" * (P0_CACHE_BYTES_BY_CAPABILITY["stocks.profile.company"] + 1)
    with pytest.raises(P0CacheWriteError):
        cache.put(_request(), oversized)


def test_query_maps_cache_write_failure_to_database_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        query_module, "_load_live_page", lambda request, adapter: _page()
    )
    monkeypatch.setattr(
        query_module._CACHE,
        "put",
        lambda request, payload: (_ for _ in ()).throw(P0CacheWriteError("容量不足")),
    )
    with pytest.raises(P0QueryError) as error:
        query_module.query(_request())
    assert error.value.kind == "database_error"
