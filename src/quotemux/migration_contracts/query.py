from __future__ import annotations

from pydantic import TypeAdapter, ValidationError

from platform_models.migration_contracts import (
    EtfProfileData,
    FinancialEventData,
    IndexMemberAuditData,
    MigrationData,
    MigrationPage,
    MigrationRequest,
)
from platform_models.provider_contracts import canonical_json_sha256
from quotemux.config_runtime.runtime import get_config_runtime
from quotemux.infra.provider_runtime import call_provider_api
from quotemux.migration_contracts.cache import BoundedMigrationCache
from quotemux.migration_contracts.policy import MIGRATION_SOURCE_BY_CAPABILITY
from quotemux.p0_fundamentals.errors import (
    P0CacheWriteError,
    P0QueryError,
    P0_ERROR_KINDS,
)
from quotemux.source_packages.instance_context import use_source_instance
from quotemux.source_packages.registry import get_default_source_package_registry


_CACHE = BoundedMigrationCache()
_PAGE_ADAPTERS = {
    "stocks.finance.forecasts": TypeAdapter(MigrationPage[FinancialEventData]),
    "stocks.finance.express": TypeAdapter(MigrationPage[FinancialEventData]),
    "funds.etf.profile": TypeAdapter(MigrationPage[EtfProfileData]),
    "indexes.members": TypeAdapter(MigrationPage[IndexMemberAuditData]),
}


def query(request: MigrationRequest) -> MigrationPage[MigrationData]:
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


def _read_cache(request: MigrationRequest, adapter: TypeAdapter[object]):
    try:
        payload = _CACHE.get(request)
    except Exception as exc:
        raise P0QueryError("cache_error", f"migration cache 读取失败: {exc}") from exc
    if payload is None:
        return None
    try:
        page = adapter.validate_json(payload)
        _validate_page(request, page)
    except (ValidationError, ValueError, TypeError, P0QueryError) as exc:
        raise P0QueryError("cache_error", f"migration cache 损坏: {exc}") from exc
    return page


def _load_live_page(request: MigrationRequest, adapter: TypeAdapter[object]):
    registry = get_default_source_package_registry()
    try:
        manifest = registry.get_manifest(request.provider)
        handler = registry.get_handler(request.provider, "query_migration")
    except KeyError as exc:
        raise P0QueryError(
            "contract_error", f"migration Provider 未注册: {request.provider}"
        ) from exc
    if request.provider == "eastmoney_official" and manifest.supports_multi_instance:
        raise P0QueryError("contract_error", "eastmoney_official 必须是单实例 package")
    instances = tuple(
        instance
        for instance in get_config_runtime()
        .get_active_snapshot()
        .list_enabled_source_instances()
        if instance.package_id == request.provider
        and instance.instance_id == request.provider_instance_id
    )
    if len(instances) != 1:
        raise P0QueryError(
            "contract_error", "请求指定的 Provider instance 必须且只能启用一个"
        )
    try:
        with use_source_instance(instances[0]):
            payload = call_provider_api(
                request.provider,
                request.capability_id,
                handler,
                request.model_dump(mode="json"),
            )
        return adapter.validate_python(payload)
    except P0QueryError:
        raise
    except ValidationError as exc:
        raise P0QueryError(
            _validation_error_kind(exc), f"Provider 响应不符合 audited page: {exc}"
        ) from exc
    except TimeoutError as exc:
        raise P0QueryError("timeout_error", str(exc)) from exc
    except Exception as exc:
        kind = getattr(exc, "kind", "request_error")
        if kind not in P0_ERROR_KINDS:
            kind = "request_error"
        raise P0QueryError(kind, str(exc)) from exc


def _validate_page(
    request: MigrationRequest, page: MigrationPage[MigrationData]
) -> None:
    if (
        page.capability_id != request.capability_id
        or page.data_version != request.data_version
    ):
        raise P0QueryError(
            "contract_error", "audited page capability/data_version 不匹配"
        )
    if page.provider != request.provider:
        raise P0QueryError("contract_error", "audited page provider 不匹配")
    if page.source != MIGRATION_SOURCE_BY_CAPABILITY[request.capability_id]:
        raise P0QueryError("contract_error", "audited page source 不匹配")
    if page.source_version != request.source_version:
        raise P0QueryError("contract_error", "audited page source_version 不匹配")
    event_ids: set[str] = set()
    for record in page.records:
        if record.source_event_id in event_ids:
            raise P0QueryError(
                "contract_error", "audited page 出现重复 source_event_id"
            )
        event_ids.add(record.source_event_id)
        if canonical_json_sha256(record.raw_projection) != record.raw_hash:
            raise P0QueryError("contract_error", "audited record raw_hash 不匹配")
        data = record.data
        if request.capability_id == "indexes.members":
            if data.index_code != request.index_code:
                raise P0QueryError("contract_error", "指数身份与请求不一致")
            if not request.range_start <= data.as_of_date <= request.range_end:
                raise P0QueryError("contract_error", "指数权重日期超出请求范围")
            continue
        if data.code != request.code or data.market != request.market:
            raise P0QueryError("contract_error", "证券身份与请求不一致")
        if data.security_code != f"{request.market}.{request.code}":
            raise P0QueryError("contract_error", "security_code 不匹配")


def _validation_error_kind(exc: ValidationError) -> str:
    contract_fields = {"capability_id", "data_version", "provider", "source_version"}
    if any(
        contract_fields.intersection(str(part) for part in error["loc"])
        for error in exc.errors()
    ):
        return "contract_error"
    return "schema_error"
