from __future__ import annotations

from pydantic import TypeAdapter, ValidationError

from platform_models.p0_fundamentals import (
    CapitalP0Data,
    CompanyP0Data,
    P0Data,
    P0Page,
    P0Request,
    ReportDisclosureP0Data,
    StatementP0Data,
    canonical_json_sha256,
)
from quotemux.config_runtime.runtime import get_config_runtime
from quotemux.infra.provider_runtime import call_provider_api
from quotemux.p0_fundamentals.cache import BoundedP0Cache
from quotemux.p0_fundamentals.errors import (
    P0CacheWriteError,
    P0QueryError,
    P0_ERROR_KINDS,
)
from quotemux.p0_fundamentals.policy import (
    P0_REQUIRED_PROVIDER_BY_CAPABILITY,
    P0_SOURCE_BY_CAPABILITY,
)
from quotemux.source_packages.instance_context import use_source_instance
from quotemux.source_packages.registry import get_default_source_package_registry


_CACHE = BoundedP0Cache()
_PAGE_ADAPTERS = {
    "stocks.profile.company": TypeAdapter(P0Page[CompanyP0Data]),
    "stocks.corporate_actions.share_changes": TypeAdapter(P0Page[CapitalP0Data]),
    "stocks.finance.report_disclosures": TypeAdapter(P0Page[ReportDisclosureP0Data]),
    "stocks.finance.statements": TypeAdapter(P0Page[StatementP0Data]),
}


def query(request: P0Request) -> P0Page[P0Data]:
    adapter = _PAGE_ADAPTERS[request.capability_id]
    cached = _read_cache(request, adapter)
    if cached is not None:
        return cached
    page = _load_live_page(request, adapter)
    _validate_page(request, page)
    try:
        _CACHE.put(request, page.model_dump_json().encode("utf-8"))
    except P0CacheWriteError as exc:
        raise P0QueryError("database_error", str(exc)) from exc
    return page


def _read_cache(
    request: P0Request, adapter: TypeAdapter[object]
) -> P0Page[P0Data] | None:
    try:
        payload = _CACHE.get(request)
    except Exception as exc:
        raise P0QueryError("cache_error", f"P0 cache 读取失败: {exc}") from exc
    if payload is None:
        return None
    try:
        page = adapter.validate_json(payload)
        _validate_page(request, page)
    except (ValidationError, ValueError, TypeError, P0QueryError) as exc:
        raise P0QueryError("cache_error", f"P0 cache 损坏: {exc}") from exc
    return page


def _load_live_page(request: P0Request, adapter: TypeAdapter[object]) -> P0Page[P0Data]:
    provider = P0_REQUIRED_PROVIDER_BY_CAPABILITY[request.capability_id]
    if request.provider != provider:
        raise P0QueryError("contract_error", "P0 request provider 不匹配")
    registry = get_default_source_package_registry()
    try:
        manifest = registry.get_manifest(provider)
        handler = registry.get_handler(provider, "query")
    except KeyError as exc:
        raise P0QueryError("contract_error", f"P0 Provider 未注册: {provider}") from exc
    if manifest.supports_multi_instance:
        raise P0QueryError("contract_error", f"{provider} 必须是单实例 package")
    instances = tuple(
        instance
        for instance in get_config_runtime()
        .get_active_snapshot()
        .list_enabled_source_instances()
        if instance.package_id == provider
    )
    if len(instances) != 1:
        raise P0QueryError(
            "contract_error", f"{provider} 必须且只能启用一个实例"
        )
    try:
        with use_source_instance(instances[0]):
            payload = call_provider_api(
                provider,
                request.capability_id,
                handler,
                request.model_dump(mode="json"),
            )
        return adapter.validate_python(payload)
    except P0QueryError:
        raise
    except ValidationError as exc:
        raise P0QueryError(
            _validation_error_kind(exc), f"Provider 响应不符合 P0Page: {exc}"
        ) from exc
    except TimeoutError as exc:
        raise P0QueryError("timeout_error", str(exc)) from exc
    except Exception as exc:
        kind = getattr(exc, "kind", "request_error")
        if kind not in P0_ERROR_KINDS:
            kind = "request_error"
        raise P0QueryError(kind, str(exc)) from exc


def _validate_page(request: P0Request, page: P0Page[P0Data]) -> None:
    expected_provider = P0_REQUIRED_PROVIDER_BY_CAPABILITY[request.capability_id]
    expected_source = P0_SOURCE_BY_CAPABILITY[request.capability_id]
    if (
        page.capability_id != request.capability_id
        or page.data_version != request.data_version
    ):
        raise P0QueryError("contract_error", "P0Page capability/data_version 不匹配")
    if page.provider != expected_provider or page.source != expected_source:
        raise P0QueryError("contract_error", "P0Page provider/source 不匹配")
    if page.source_version != request.source_version:
        raise P0QueryError("contract_error", "P0Page source_version 不匹配")
    event_ids: set[str] = set()
    for record in page.records:
        if record.source_event_id in event_ids:
            raise P0QueryError("contract_error", "P0Page 出现重复 source_event_id")
        event_ids.add(record.source_event_id)
        if canonical_json_sha256(record.raw_projection) != record.raw_hash:
            raise P0QueryError("contract_error", "P0Record raw_hash 不匹配")
        data = record.data
        if data.code != request.code or data.market != request.market:
            raise P0QueryError("contract_error", "P0Record 证券身份与请求不一致")
        if data.security_code != f"{request.market}.{request.code}":
            raise P0QueryError("contract_error", "P0Record security_code 不匹配")


def _validation_error_kind(exc: ValidationError) -> str:
    contract_fields = {"capability_id", "data_version", "provider", "source_version"}
    if any(
        contract_fields.intersection(str(part) for part in error["loc"])
        for error in exc.errors()
    ):
        return "contract_error"
    return "schema_error"
