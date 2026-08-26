from __future__ import annotations

from datetime import datetime, timedelta
import hashlib
import json
from threading import Lock
import time as time_module
from typing import Iterable
from uuid import uuid4
from zoneinfo import ZoneInfo

from platform_models import FutureBar1mItem, FutureContractCatalogItem, FutureContractRealtimeQuoteItem, FutureMainContractMappingItem, FutureRealtimeQuoteItem, FutureSeriesCoverageItem
from quotemux.infra.db.client import (
    _acquire_connection,
    _release_connection,
    append_migration_range_journals,
    discover_migration_range_journals,
    enable_explicit_range_journaling,
    execute_sql,
    query_dataframe,
)
from quotemux.settings import QuoteMuxSettings
from quotemux.source_packages.instance_context import use_source_instance
from quotemux.source_packages.registry import get_default_source_package_registry


SERIES_BACK_ADJUSTED_CONTINUOUS = "back_adjusted_continuous"
SERIES_MAIN_CONTINUOUS = "main_continuous"
VALID_SERIES_TYPES = (SERIES_BACK_ADJUSTED_CONTINUOUS, SERIES_MAIN_CONTINUOUS)
_STORAGE_SERIES_TYPE = {
    SERIES_BACK_ADJUSTED_CONTINUOUS: "apex_l0_adjusted",
    SERIES_MAIN_CONTINUOUS: SERIES_MAIN_CONTINUOUS,
}
MAIN_CONTINUOUS_CAPABILITY_ID = "futures.quotes.main_continuous.1m"
REALTIME_MAIN_CONTINUOUS_CAPABILITY_ID = "futures.quotes.main_continuous.realtime"
FUTURE_CONTRACT_CATALOG_CAPABILITY_ID = "futures.contracts.catalog"
FUTURE_MAIN_CONTRACT_MAPPING_CAPABILITY_ID = "futures.contracts.main_mapping"
FUTURE_CONTRACT_REALTIME_CAPABILITY_ID = "futures.quotes.contract.realtime"
REALTIME_MAIN_CONTINUOUS_PROVIDER_ID = "shinny_tqsdk"
CHINA_TIMEZONE = ZoneInfo("Asia/Shanghai")

FUTURE_EXCHANGE_PRODUCTS: dict[str, tuple[str, ...]] = {
    "CFFEX": ("IF", "IH", "IC", "IM", "T", "TF", "TS", "TL"),
    "SHFE": ("ad", "ag", "al", "ao", "au", "br", "bu", "cu", "fu", "hc", "ni", "op", "pb", "rb", "ru", "sn", "sp", "ss", "wr", "zn"),
    "DCE": ("a", "b", "bz", "c", "cs", "eb", "eg", "fb", "i", "j", "jd", "jm", "l", "lg", "lh", "m", "p", "pg", "pp", "rr", "v", "y"),
    "CZCE": ("AP", "CF", "CJ", "CY", "FG", "JR", "MA", "OI", "PF", "PK", "PL", "PR", "PX", "RM", "RS", "SA", "SF", "SH", "SM", "SR", "TA", "UR", "WH", "ZC"),
    "INE": ("bc", "ec", "lu", "nr", "sc"),
    "GFEX": ("lc", "pd", "ps", "pt", "si"),
}
PRODUCT_EXCHANGE = {product: exchange for exchange, products in FUTURE_EXCHANGE_PRODUCTS.items() for product in products}
FUTURE_CONTRACT_CATALOG_SCHEMA_VERSION = "future_contract_catalog_v2"


class FutureContractCatalogIncompleteError(RuntimeError):
    """A public catalog read cannot be answered from a complete local snapshot."""

    def __init__(
        self,
        reason: str,
        *,
        requested_codes: tuple[str, ...] = (),
        include_expired: bool = False,
        missing_products: tuple[str, ...] = (),
        missing_fields: tuple[str, ...] = (),
    ) -> None:
        self.details: dict[str, object] = {
            "dataset_id": "future_contract_reference",
            "reason": reason,
            "requested_codes": list(requested_codes),
            "include_expired": include_expired,
            "missing_products": list(missing_products),
            "missing_fields": list(missing_fields),
            "repair_endpoint": "/api/admin/data-repairs",
            "repair_template": {
                "dataset_id": "future_contract_reference",
                "scope": {"codes": [], "include_expired": include_expired},
            },
        }
        super().__init__(f"期货合约目录本地数据不完整: {reason}")

FUTURE_SCHEMA_SQL = (
    """
    create table if not exists ref.future_series (
        product_code text not null,
        exchange text not null,
        series_type text not null,
        display_name text not null default '',
        loaded_at timestamp with time zone not null default now(),
        primary key (product_code, exchange, series_type),
        check (series_type in ('apex_l0_adjusted', 'main_continuous'))
    )
    """,
    """
    create table if not exists fact.future_bar_1m (
        product_code text not null,
        exchange text not null,
        series_type text not null,
        bar_time timestamp without time zone not null,
        open double precision,
        high double precision,
        low double precision,
        close double precision,
        volume double precision,
        open_interest double precision,
        adjustment_offset double precision,
        source_key text not null,
        loaded_at timestamp with time zone not null default now(),
        primary key (product_code, exchange, series_type, bar_time),
        foreign key (product_code, exchange, series_type)
            references ref.future_series (product_code, exchange, series_type)
    )
    """,
    "create index if not exists future_bar_1m_time_idx on fact.future_bar_1m (bar_time, product_code, series_type)",
    """
    create table if not exists fact.future_bar_1m_coverage (
        product_code text not null,
        exchange text not null,
        series_type text not null,
        row_count bigint not null,
        first_bar_time timestamp without time zone not null,
        last_bar_time timestamp without time zone not null,
        updated_at timestamp with time zone not null default now(),
        primary key (product_code, exchange, series_type),
        check (row_count > 0)
    )
    """,
    """
    insert into fact.future_bar_1m_coverage (
        product_code, exchange, series_type, row_count, first_bar_time, last_bar_time, updated_at
    )
    select bars.product_code, bars.exchange, bars.series_type, count(*),
           min(bars.bar_time), max(bars.bar_time), now()
    from fact.future_bar_1m bars
    where not exists (select 1 from fact.future_bar_1m_coverage)
    group by bars.product_code, bars.exchange, bars.series_type
    on conflict (product_code, exchange, series_type) do update set
        row_count = excluded.row_count,
        first_bar_time = excluded.first_bar_time,
        last_bar_time = excluded.last_bar_time,
        updated_at = now()
    """,
    """
    create or replace function fact.refresh_future_bar_1m_coverage_group(
        target_product_code text,
        target_exchange text,
        target_series_type text
    ) returns void language plpgsql as $$
    begin
        delete from fact.future_bar_1m_coverage
        where product_code = target_product_code
          and exchange = target_exchange
          and series_type = target_series_type;

        insert into fact.future_bar_1m_coverage (
            product_code, exchange, series_type, row_count, first_bar_time, last_bar_time, updated_at
        )
        select bars.product_code, bars.exchange, bars.series_type, count(*),
               min(bars.bar_time), max(bars.bar_time), now()
        from fact.future_bar_1m bars
        where bars.product_code = target_product_code
          and bars.exchange = target_exchange
          and bars.series_type = target_series_type
        group by bars.product_code, bars.exchange, bars.series_type;
    end
    $$
    """,
    """
    create or replace function fact.maintain_future_bar_1m_coverage_after_insert()
    returns trigger language plpgsql as $$
    begin
        insert into fact.future_bar_1m_coverage (
            product_code, exchange, series_type, row_count, first_bar_time, last_bar_time, updated_at
        )
        select inserted.product_code, inserted.exchange, inserted.series_type, count(*),
               min(inserted.bar_time), max(inserted.bar_time), now()
        from inserted_rows inserted
        group by inserted.product_code, inserted.exchange, inserted.series_type
        on conflict (product_code, exchange, series_type) do update set
            row_count = fact.future_bar_1m_coverage.row_count + excluded.row_count,
            first_bar_time = least(fact.future_bar_1m_coverage.first_bar_time, excluded.first_bar_time),
            last_bar_time = greatest(fact.future_bar_1m_coverage.last_bar_time, excluded.last_bar_time),
            updated_at = now();
        return null;
    end
    $$
    """,
    """
    create or replace function fact.maintain_future_bar_1m_coverage_after_delete()
    returns trigger language plpgsql as $$
    declare
        affected record;
    begin
        for affected in
            select distinct deleted.product_code, deleted.exchange, deleted.series_type
            from deleted_rows deleted
        loop
            perform fact.refresh_future_bar_1m_coverage_group(
                affected.product_code, affected.exchange, affected.series_type
            );
        end loop;
        return null;
    end
    $$
    """,
    """
    create or replace function fact.maintain_future_bar_1m_coverage_after_update()
    returns trigger language plpgsql as $$
    declare
        affected record;
    begin
        if not exists (
            (select old_rows.product_code, old_rows.exchange, old_rows.series_type, old_rows.bar_time
             from updated_old_rows old_rows
             except
             select new_rows.product_code, new_rows.exchange, new_rows.series_type, new_rows.bar_time
             from updated_new_rows new_rows)
            union all
            (select new_rows.product_code, new_rows.exchange, new_rows.series_type, new_rows.bar_time
             from updated_new_rows new_rows
             except
             select old_rows.product_code, old_rows.exchange, old_rows.series_type, old_rows.bar_time
             from updated_old_rows old_rows)
        ) then
            return null;
        end if;

        for affected in
            select old_rows.product_code, old_rows.exchange, old_rows.series_type
            from updated_old_rows old_rows
            union
            select new_rows.product_code, new_rows.exchange, new_rows.series_type
            from updated_new_rows new_rows
        loop
            perform fact.refresh_future_bar_1m_coverage_group(
                affected.product_code, affected.exchange, affected.series_type
            );
        end loop;
        return null;
    end
    $$
    """,
    """
    do $$
    begin
        if not exists (
            select 1 from pg_trigger
            where tgrelid = 'fact.future_bar_1m'::regclass
              and tgname = 'future_bar_1m_coverage_after_insert'
              and not tgisinternal
        ) then
            create trigger future_bar_1m_coverage_after_insert
            after insert on fact.future_bar_1m
            referencing new table as inserted_rows
            for each statement execute function fact.maintain_future_bar_1m_coverage_after_insert();
        end if;
    end
    $$
    """,
    """
    do $$
    begin
        if not exists (
            select 1 from pg_trigger
            where tgrelid = 'fact.future_bar_1m'::regclass
              and tgname = 'future_bar_1m_coverage_after_delete'
              and not tgisinternal
        ) then
            create trigger future_bar_1m_coverage_after_delete
            after delete on fact.future_bar_1m
            referencing old table as deleted_rows
            for each statement execute function fact.maintain_future_bar_1m_coverage_after_delete();
        end if;
    end
    $$
    """,
    """
    do $$
    begin
        if exists (
            select 1 from pg_trigger
            where tgrelid = 'fact.future_bar_1m'::regclass
              and tgname = 'future_bar_1m_coverage_after_key_update'
              and not tgisinternal
        ) then
            drop trigger future_bar_1m_coverage_after_key_update on fact.future_bar_1m;
        end if;

        if not exists (
            select 1 from pg_trigger
            where tgrelid = 'fact.future_bar_1m'::regclass
              and tgname = 'future_bar_1m_coverage_after_update'
              and not tgisinternal
        ) then
            create trigger future_bar_1m_coverage_after_update
            after update on fact.future_bar_1m
            referencing old table as updated_old_rows new table as updated_new_rows
            for each statement execute function fact.maintain_future_bar_1m_coverage_after_update();
        end if;
    end
    $$
    """,
    """
    create table if not exists audit.future_bar_1m_series_generation (
        series_type text not null,
        generation bigint not null,
        row_count bigint not null,
        first_bar_time timestamp without time zone,
        last_bar_time timestamp without time zone,
        transaction_id bigint not null,
        operation text not null,
        delta_fingerprint text not null,
        recorded_at timestamp with time zone not null default now(),
        primary key (series_type, generation),
        check (series_type in ('apex_l0_adjusted', 'main_continuous')),
        check (row_count >= 0)
    )
    """,
    """
    insert into audit.future_bar_1m_series_generation (
        series_type, generation, row_count, first_bar_time, last_bar_time,
        transaction_id, operation, delta_fingerprint
    )
    select bars.series_type, 1, count(*), min(bars.bar_time), max(bars.bar_time),
           txid_current(), 'bootstrap', md5(bars.series_type || '|' || count(*)::text || '|' || min(bars.bar_time)::text || '|' || max(bars.bar_time)::text)
    from fact.future_bar_1m bars
    where not exists (select 1 from audit.future_bar_1m_series_generation)
    group by bars.series_type
    """,
    """
    create or replace function audit.record_future_bar_1m_series_generation(
        target_series_type text, target_operation text, target_delta_fingerprint text
    ) returns void language plpgsql as $$
    declare next_generation bigint;
    begin
        perform pg_advisory_xact_lock(hashtext('future_bar_1m_series_generation:' || target_series_type));
        select coalesce(max(generation), 0) + 1 into next_generation
        from audit.future_bar_1m_series_generation where series_type = target_series_type;
        insert into audit.future_bar_1m_series_generation (
            series_type, generation, row_count, first_bar_time, last_bar_time,
            transaction_id, operation, delta_fingerprint
        )
        select target_series_type, next_generation, count(*), min(bar_time), max(bar_time),
               txid_current(), target_operation, target_delta_fingerprint
        from fact.future_bar_1m where series_type = target_series_type;
    end
    $$
    """,
    """
    create or replace function audit.maintain_future_bar_1m_series_generation_after_insert()
    returns trigger language plpgsql security definer set search_path = pg_catalog, fact, audit as $$
    declare changed record;
    begin
        -- Compact aggregate: never materialize a multi-million-row string in a
        -- transition trigger. The receipt's rowset SHA remains authoritative.
        for changed in select series_type, md5(series_type || '|' || count(*)::text || '|' || min(bar_time)::text || '|' || max(bar_time)::text) as delta_fingerprint from inserted_rows group by series_type loop
            perform audit.record_future_bar_1m_series_generation(changed.series_type, 'insert', changed.delta_fingerprint);
        end loop;
        return null;
    end
    $$
    """,
    """
    do $$ begin
        if not exists (select 1 from pg_trigger where tgrelid = 'fact.future_bar_1m'::regclass and tgname = 'future_bar_1m_series_generation_after_insert' and not tgisinternal) then
            create trigger future_bar_1m_series_generation_after_insert after insert on fact.future_bar_1m referencing new table as inserted_rows for each statement execute function audit.maintain_future_bar_1m_series_generation_after_insert();
        end if;
    end $$
    """,
    """
    create or replace function fact.maintain_future_bar_1m_after_truncate()
    returns trigger language plpgsql security definer set search_path = pg_catalog, fact, audit as $$
    begin
        delete from fact.future_bar_1m_coverage;
        perform audit.record_future_bar_1m_series_generation('apex_l0_adjusted', 'truncate', md5('apex_l0_adjusted|truncate|' || txid_current()::text));
        perform audit.record_future_bar_1m_series_generation('main_continuous', 'truncate', md5('main_continuous|truncate|' || txid_current()::text));
        return null;
    end $$
    """,
    """
    do $$ begin
        if not exists (select 1 from pg_trigger where tgrelid='fact.future_bar_1m'::regclass and tgname='future_bar_1m_after_truncate' and not tgisinternal) then
            create trigger future_bar_1m_after_truncate after truncate on fact.future_bar_1m for each statement execute function fact.maintain_future_bar_1m_after_truncate();
        end if;
    end $$
    """,
    """
    create or replace function audit.maintain_future_bar_1m_series_generation_after_update()
    returns trigger language plpgsql security definer set search_path = pg_catalog, fact, audit as $$
    declare changed record;
    begin
        for changed in select series_type, md5(series_type || '|' || count(*)::text || '|' || min(bar_time)::text || '|' || max(bar_time)::text) as delta_fingerprint
            from (select series_type, bar_time from updated_old_rows union select series_type, bar_time from updated_new_rows) changed_rows group by series_type loop
            perform audit.record_future_bar_1m_series_generation(changed.series_type, 'update', changed.delta_fingerprint);
        end loop;
        return null;
    end
    $$
    """,
    """
    do $$ begin
        if not exists (select 1 from pg_trigger where tgrelid = 'fact.future_bar_1m'::regclass and tgname = 'future_bar_1m_series_generation_after_update' and not tgisinternal) then
            create trigger future_bar_1m_series_generation_after_update after update on fact.future_bar_1m referencing new table as updated_new_rows for each statement execute function audit.maintain_future_bar_1m_series_generation_after_update();
        end if;
    end $$
    """,
    """
    create or replace function audit.maintain_future_bar_1m_series_generation_after_delete()
    returns trigger language plpgsql security definer set search_path = pg_catalog, fact, audit as $$
    declare changed record;
    begin
        for changed in select series_type, md5(series_type || '|' || count(*)::text || '|' || min(bar_time)::text || '|' || max(bar_time)::text) as delta_fingerprint from deleted_rows group by series_type loop
            perform audit.record_future_bar_1m_series_generation(changed.series_type, 'delete', changed.delta_fingerprint);
        end loop;
        return null;
    end $$
    """,
    """
    do $$ begin
        if not exists (select 1 from pg_trigger where tgrelid='fact.future_bar_1m'::regclass and tgname='future_bar_1m_series_generation_after_delete' and not tgisinternal) then
            create trigger future_bar_1m_series_generation_after_delete after delete on fact.future_bar_1m referencing old table as deleted_rows for each statement execute function audit.maintain_future_bar_1m_series_generation_after_delete();
        end if;
    end $$
    """,
    """
    create table if not exists ref.future_contract_catalog_snapshot (
        snapshot_id text primary key,
        scope_include_expired boolean not null,
        schema_version text not null,
        captured_at timestamp without time zone not null,
        source_package_id text not null,
        source_instance_id text not null default '',
        content_checksum text not null,
        row_count integer not null check (row_count > 0),
        product_count integer not null check (product_count > 0),
        complete boolean not null,
        created_at timestamp with time zone not null default now(),
        check (complete)
    )
    """,
    """
    create table if not exists ref.future_contract_catalog_snapshot_item (
        snapshot_id text not null references ref.future_contract_catalog_snapshot(snapshot_id),
        provider_symbol text not null,
        product_code text not null,
        exchange text not null,
        payload jsonb not null,
        primary key (snapshot_id, provider_symbol)
    )
    """,
    "create index if not exists future_contract_catalog_snapshot_item_product_idx on ref.future_contract_catalog_snapshot_item (snapshot_id, product_code, exchange)",
    """
    create table if not exists ref.future_contract_catalog_publication (
        scope_include_expired boolean primary key,
        snapshot_id text not null references ref.future_contract_catalog_snapshot(snapshot_id),
        published_at timestamp with time zone not null default now()
    )
    """,
)

_FUTURE_SCHEMA_READY = False
_FUTURE_SCHEMA_LOCK = Lock()


def ensure_future_schema() -> None:
    global _FUTURE_SCHEMA_READY
    if _FUTURE_SCHEMA_READY:
        return
    with _FUTURE_SCHEMA_LOCK:
        if _FUTURE_SCHEMA_READY:
            return
        for statement in FUTURE_SCHEMA_SQL:
            if not execute_sql(statement):
                raise RuntimeError("无法创建期货 1m 事实表")
        _FUTURE_SCHEMA_READY = True


def normalize_product_codes(codes: str | Iterable[str]) -> tuple[str, ...]:
    values = codes.split(",") if isinstance(codes, str) else list(codes)
    result: list[str] = []
    canonical = {code.lower(): code for code in PRODUCT_EXCHANGE}
    for value in values:
        text = str(value).strip()
        if text == "":
            continue
        code = canonical.get(text.lower())
        if code is None:
            raise ValueError(f"未知期货品种代码: {text}")
        if code not in result:
            result.append(code)
    return tuple(result)


def normalize_contract_symbols(symbols: str | Iterable[str]) -> tuple[str, ...]:
    values = symbols.split(",") if isinstance(symbols, str) else list(symbols)
    result: list[str] = []
    for value in values:
        symbol = str(value).strip()
        if symbol != "" and symbol not in result:
            result.append(symbol)
    return tuple(result)


def require_series_type(series_type: str) -> str:
    if series_type not in VALID_SERIES_TYPES:
        raise ValueError(f"series_type 仅支持: {', '.join(VALID_SERIES_TYPES)}")
    return series_type


def _optional_float(value: object) -> float | None:
    return None if value is None else float(value)


def _format_time(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def _china_market_now() -> datetime:
    return datetime.now(CHINA_TIMEZONE).replace(tzinfo=None)


def _call_provider_with_retry(handler, *args: object):
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            return handler(*args)
        except Exception as exc:
            last_error = exc
            if attempt < 2:
                # 免费 HTTP 源偶发连接超时，按品种短退避重试，不重复已完成品种。
                time_module.sleep(2**attempt)
    assert last_error is not None
    raise last_error


def _catalog_availability() -> dict[str, object]:
    return {
        "native_contract_spec": True,
        "execution_profile_required": True,
        "asset_class": {"available": False, "reason": "execution_profile_required"},
        "lot_size": {"available": False, "reason": "execution_profile_required"},
        "commission_open": {"available": False, "reason": "execution_profile_required"},
        "commission_close": {"available": False, "reason": "execution_profile_required"},
        "commission_close_today": {"available": False, "reason": "execution_profile_required"},
        "initial_margin": {"available": False, "reason": "execution_profile_required"},
        "maintenance_margin": {"available": False, "reason": "execution_profile_required"},
    }


def _catalog_provenance() -> dict[str, object]:
    return {
        "tick_size": {"kind": "provider_field", "field": "price_tick"},
        "price_precision": {"kind": "provider_field", "field": "price_decs"},
        "multiplier": {"kind": "provider_field", "field": "volume_multiple"},
        "currency": {"kind": "market_rule", "rule_id": "cn_futures_currency_v1"},
        "asset_class": {"kind": "unavailable", "reason": "execution_profile_required"},
        "lot_size": {"kind": "unavailable", "reason": "execution_profile_required"},
        "commission_open": {"kind": "unavailable", "reason": "execution_profile_required"},
        "commission_close": {"kind": "unavailable", "reason": "execution_profile_required"},
        "commission_close_today": {"kind": "unavailable", "reason": "execution_profile_required"},
        "initial_margin": {"kind": "unavailable", "reason": "execution_profile_required"},
        "maintenance_margin": {"kind": "unavailable", "reason": "execution_profile_required"},
    }


_CATALOG_CHECKSUM_EXCLUDED_FIELDS = frozenset({
    "snapshot_id", "snapshot_complete", "captured_at", "catalog_dataset_version", "content_checksum",
})


def _catalog_content_checksum(items: list[FutureContractCatalogItem]) -> str:
    """Hash stable provider/normalization content, not per-capture publication metadata."""
    canonical_items = []
    for item in sorted(items, key=lambda candidate: candidate.provider_symbol):
        payload = item.model_dump(mode="json")
        canonical_items.append({key: value for key, value in payload.items() if key not in _CATALOG_CHECKSUM_EXCLUDED_FIELDS})
    encoded = json.dumps(canonical_items, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _normalize_catalog_item(
    item: FutureContractCatalogItem,
    *,
    snapshot_id: str,
    captured_at: str,
    source: dict[str, object],
) -> FutureContractCatalogItem:
    return item.model_copy(
        update={
            "tick_size": item.price_tick,
            "price_precision": item.price_decs,
            "multiplier": item.volume_multiple,
            "currency": "CNY",
            "lot_size": None,
            "asset_class": None,
            "commission_open": None,
            "commission_close": None,
            "commission_close_today": None,
            "initial_margin": None,
            "maintenance_margin": None,
            "catalog_schema_version": FUTURE_CONTRACT_CATALOG_SCHEMA_VERSION,
            "catalog_dataset_version": "",
            "snapshot_id": snapshot_id,
            "snapshot_complete": True,
            "captured_at": captured_at,
            "source": source,
            "availability": _catalog_availability(),
            "provenance": _catalog_provenance(),
        }
    )


def _validate_catalog_capture(items: list[FutureContractCatalogItem], products: tuple[str, ...]) -> None:
    if items == []:
        raise ValueError("期货合约目录采集未返回任何数据")
    seen_symbols: set[str] = set()
    covered_products: set[str] = set()
    for item in items:
        if item.provider_symbol == "" or item.provider_symbol in seen_symbols:
            raise ValueError(f"期货合约目录 provider_symbol 缺失或重复: {item.provider_symbol}")
        seen_symbols.add(item.provider_symbol)
        if item.ins_class != "FUTURE":
            raise ValueError(f"期货合约目录包含非 FUTURE 项: {item.provider_symbol}")
        if item.product_code not in PRODUCT_EXCHANGE or PRODUCT_EXCHANGE[item.product_code] != item.exchange:
            raise ValueError(f"期货合约目录包含未知国内品种或交易所: {item.provider_symbol}")
        if item.product_code not in products:
            raise ValueError(f"期货合约目录超出请求 scope: {item.provider_symbol}")
        if item.price_tick is None or item.price_decs is None or item.volume_multiple is None:
            raise ValueError(f"期货合约目录缺少 native 规格: {item.provider_symbol}")
        covered_products.add(item.product_code)
    missing = tuple(product for product in products if product not in covered_products)
    if missing:
        raise ValueError(f"期货合约目录覆盖不完整: missing={','.join(missing)}")


def _validate_published_catalog_rows(
    rows: list[dict[str, object]], *, include_expired: bool
) -> list[FutureContractCatalogItem]:
    """Verify the complete immutable publication before serving any subset of it."""
    if rows == []:
        raise FutureContractCatalogIncompleteError("no_complete_published_snapshot", include_expired=include_expired)
    first = rows[0]
    required_header = (
        "snapshot_id", "scope_include_expired", "schema_version", "captured_at", "source_package_id",
        "content_checksum", "row_count", "product_count", "complete",
    )
    if any(first.get(field) in (None, "") for field in required_header):
        raise FutureContractCatalogIncompleteError("published_snapshot_header_incomplete", include_expired=include_expired)
    snapshot_id = str(first["snapshot_id"])
    checksum = str(first["content_checksum"])
    if bool(first.get("scope_include_expired")) != include_expired:
        raise FutureContractCatalogIncompleteError("published_snapshot_scope_mismatch", include_expired=include_expired)
    if str(first["schema_version"]) != FUTURE_CONTRACT_CATALOG_SCHEMA_VERSION:
        raise FutureContractCatalogIncompleteError("published_snapshot_schema_unsupported", include_expired=include_expired)
    if bool(first.get("complete")) is not True:
        raise FutureContractCatalogIncompleteError("published_snapshot_not_complete", include_expired=include_expired)
    for row in rows:
        if any(row.get(field) != first.get(field) for field in required_header):
            raise FutureContractCatalogIncompleteError("published_snapshot_header_inconsistent", include_expired=include_expired)
    try:
        items = []
        for row in rows:
            payload = row.get("payload")
            if isinstance(payload, str):
                payload = json.loads(payload)
            if not isinstance(payload, dict):
                raise ValueError("invalid payload")
            item = FutureContractCatalogItem.model_validate(payload)
            if (
                item.snapshot_id != snapshot_id or not item.snapshot_complete
                or item.catalog_schema_version != FUTURE_CONTRACT_CATALOG_SCHEMA_VERSION
                or item.content_checksum != checksum
                or item.captured_at != _format_time(first["captured_at"])
                or item.provider_symbol != row.get("item_provider_symbol")
            ):
                raise ValueError("row snapshot evidence mismatch")
            if (
                item.source.get("package_id") != first["source_package_id"]
                or item.source.get("source_instance_id") != first["source_instance_id"]
                or not isinstance(item.source.get("provider_version"), dict)
            ):
                raise ValueError("row source evidence mismatch")
            items.append(item)
        if int(first["row_count"]) != len(items) or int(first["product_count"]) != len(PRODUCT_EXCHANGE):
            raise FutureContractCatalogIncompleteError("published_snapshot_count_mismatch", include_expired=include_expired)
        _validate_catalog_capture(items, tuple(PRODUCT_EXCHANGE))
    except (TypeError, ValueError) as exc:
        raise FutureContractCatalogIncompleteError("published_snapshot_item_invalid", include_expired=include_expired) from exc
    if _catalog_content_checksum(items) != checksum:
        raise FutureContractCatalogIncompleteError("published_snapshot_checksum_mismatch", include_expired=include_expired)
    return items


class QuoteMuxFutures:
    def __init__(self, settings: QuoteMuxSettings | None = None) -> None:
        self._settings = settings or QuoteMuxSettings()

    def _tqsdk_handler(self, capability_id: str, handler_name: str):
        source_instance = next(
            (
                instance
                for instance in self._settings.get_contract_source_instances(
                    capability_id,
                    (REALTIME_MAIN_CONTINUOUS_PROVIDER_ID,),
                )
                if instance.package_id == REALTIME_MAIN_CONTINUOUS_PROVIDER_ID and instance.enabled
            ),
            None,
        )
        if source_instance is None:
            raise RuntimeError("未配置或未启用 shinny_tqsdk 的期货 source instance")
        handler = get_default_source_package_registry().get_handler(REALTIME_MAIN_CONTINUOUS_PROVIDER_ID, handler_name)
        return source_instance, handler

    def get_contract_catalog(
        self,
        codes: str | Iterable[str] = (),
        include_expired: bool = False,
    ) -> list[FutureContractCatalogItem]:
        products = normalize_product_codes(codes)
        # Public reads intentionally do not call ensure_future_schema: a missing table
        # is data-incomplete, not permission to run DDL or contact the provider.
        frame = query_dataframe(
            """
            select snapshot.snapshot_id, snapshot.scope_include_expired, snapshot.schema_version,
                   snapshot.captured_at, snapshot.source_package_id, snapshot.source_instance_id,
                   snapshot.content_checksum, snapshot.row_count, snapshot.product_count,
                   snapshot.complete, item.provider_symbol as item_provider_symbol, item.payload
            from ref.future_contract_catalog_publication publication
            join ref.future_contract_catalog_snapshot snapshot
              on snapshot.snapshot_id = publication.snapshot_id
            join ref.future_contract_catalog_snapshot_item item
              on item.snapshot_id = snapshot.snapshot_id
            where publication.scope_include_expired = %s
              and snapshot.complete = true
            order by item.product_code, item.provider_symbol
            """,
            (include_expired,),
        )
        if frame.empty:
            raise FutureContractCatalogIncompleteError(
                "complete_published_expired_scope_unavailable" if include_expired else "no_complete_published_snapshot",
                requested_codes=products, include_expired=include_expired,
            )
        try:
            items = _validate_published_catalog_rows(frame.to_dict("records"), include_expired=include_expired)
        except FutureContractCatalogIncompleteError:
            raise
        except Exception as exc:
            raise FutureContractCatalogIncompleteError(
                "invalid_published_snapshot", requested_codes=products, include_expired=include_expired,
            ) from exc
        requested_products = products or tuple(PRODUCT_EXCHANGE)
        available = {item.product_code for item in items}
        missing = tuple(product for product in requested_products if product not in available)
        if missing:
            raise FutureContractCatalogIncompleteError(
                "requested_product_absent_from_published_snapshot",
                requested_codes=products, include_expired=include_expired,
                missing_products=missing,
            )
        return [item for item in items if item.product_code in requested_products]

    def capture_contract_catalog(
        self,
        codes: str | Iterable[str] = (),
        include_expired: bool = False,
    ) -> list[FutureContractCatalogItem]:
        """Fetch and atomically publish the only catalog scope currently supported.

        This method is capture/admin-only.  Its empty scope deliberately means the
        configured domestic universe rather than the provider's unbounded universe.
        """
        products = normalize_product_codes(codes) or tuple(PRODUCT_EXCHANGE)
        if products != tuple(PRODUCT_EXCHANGE):
            raise ValueError("catalog capture 仅支持配置的完整国内 84 品种 scope")
        source_instance, handler = self._tqsdk_handler(FUTURE_CONTRACT_CATALOG_CAPABILITY_ID, "get_future_contract_catalog")
        with use_source_instance(source_instance):
            active_items = list(handler([(product_code, PRODUCT_EXCHANGE[product_code]) for product_code in products], False))
            expired_items = list(handler([(product_code, PRODUCT_EXCHANGE[product_code]) for product_code in products], True)) if include_expired else []
        by_provider_symbol = {item.provider_symbol: item for item in active_items}
        for item in expired_items:
            by_provider_symbol.setdefault(item.provider_symbol, item)
        raw_items = list(by_provider_symbol.values())
        _validate_catalog_capture(raw_items, products)
        captured_at = _format_time(_china_market_now())
        snapshot_id = str(uuid4())
        source = {
            "package_id": REALTIME_MAIN_CONTINUOUS_PROVIDER_ID,
            "source_instance_id": str(getattr(source_instance, "instance_id", "")),
            "provider_version": {"availability": "unavailable", "reason": "provider_package_does_not_expose_version"},
            "kind": "provider_capture",
        }
        items = [_normalize_catalog_item(item, snapshot_id=snapshot_id, captured_at=captured_at, source=source) for item in raw_items]
        checksum = _catalog_content_checksum(items)
        items = [item.model_copy(update={"content_checksum": checksum}) for item in items]
        encoded_payloads = [item.model_dump(mode="json") for item in items]
        ensure_future_schema()
        self._publish_contract_catalog_snapshot(
            snapshot_id, captured_at, source, checksum, products, items, encoded_payloads, include_expired
        )
        return items

    def _publish_contract_catalog_snapshot(
        self,
        snapshot_id: str,
        captured_at: str,
        source: dict[str, object],
        checksum: str,
        products: tuple[str, ...],
        items: list[FutureContractCatalogItem],
        encoded_payloads: list[dict[str, object]],
        include_expired: bool,
    ) -> None:
        connection = _acquire_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    insert into ref.future_contract_catalog_snapshot (
                        snapshot_id, scope_include_expired, schema_version, captured_at,
                        source_package_id, source_instance_id, content_checksum,
                        row_count, product_count, complete
                    ) values (%s, %s, %s, %s::timestamp, %s, %s, %s, %s, %s, true)
                    """,
                    (snapshot_id, include_expired, FUTURE_CONTRACT_CATALOG_SCHEMA_VERSION, captured_at,
                     REALTIME_MAIN_CONTINUOUS_PROVIDER_ID, source["source_instance_id"], checksum,
                     len(items), len(products)),
                )
                with cursor.copy(
                    """
                    copy ref.future_contract_catalog_snapshot_item
                        (snapshot_id, provider_symbol, product_code, exchange, payload)
                    from stdin
                    """
                ) as copy:
                    for item, payload in zip(items, encoded_payloads, strict=True):
                        copy.write_row((snapshot_id, item.provider_symbol, item.product_code, item.exchange, json.dumps(payload, ensure_ascii=False)))
                cursor.execute(
                    """
                    insert into ref.future_contract_catalog_publication (scope_include_expired, snapshot_id)
                    values (%s, %s)
                    on conflict (scope_include_expired) do update set
                        snapshot_id = excluded.snapshot_id, published_at = now()
                    """,
                    (include_expired, snapshot_id),
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            _release_connection(connection)

    def get_main_contract_mappings(
        self,
        codes: str | Iterable[str] = (),
    ) -> list[FutureMainContractMappingItem]:
        products = normalize_product_codes(codes) or tuple(PRODUCT_EXCHANGE)
        source_instance, handler = self._tqsdk_handler(FUTURE_MAIN_CONTRACT_MAPPING_CAPABILITY_ID, "get_future_main_contract_mapping")
        with use_source_instance(source_instance):
            return list(handler([(product_code, PRODUCT_EXCHANGE[product_code]) for product_code in products]))

    def get_contract_realtime(
        self,
        symbols: str | Iterable[str],
    ) -> list[FutureContractRealtimeQuoteItem]:
        contract_symbols = normalize_contract_symbols(symbols)
        if contract_symbols == ():
            raise ValueError("symbols 不能为空")
        source_instance, handler = self._tqsdk_handler(FUTURE_CONTRACT_REALTIME_CAPABILITY_ID, "get_future_contract_realtime_quotes")
        with use_source_instance(source_instance):
            return list(handler(list(contract_symbols)))

    def get_main_continuous_realtime(self, codes: str | Iterable[str]) -> list[FutureRealtimeQuoteItem]:
        products = normalize_product_codes(codes)
        if products == ():
            raise ValueError("codes 不能为空")
        source_instance, handler = self._tqsdk_handler(
            REALTIME_MAIN_CONTINUOUS_CAPABILITY_ID,
            "get_future_main_continuous_realtime",
        )
        with use_source_instance(source_instance):
            return list(handler([(product_code, PRODUCT_EXCHANGE[product_code]) for product_code in products]))

    def get_quotes_1m(
        self,
        codes: str | Iterable[str],
        series_type: str,
        start_time: str = "",
        end_time: str = "",
        limit: int = 10000,
    ) -> list[FutureBar1mItem]:
        products = normalize_product_codes(codes)
        if products == ():
            raise ValueError("codes 不能为空")
        actual_series_type = require_series_type(series_type)
        ensure_future_schema()
        storage_series_type = _STORAGE_SERIES_TYPE[actual_series_type]
        start_value = start_time or None
        end_value = end_time or None
        frame = query_dataframe(
            """
            select product_code, exchange, series_type, bar_time, open, high, low, close,
                   volume, open_interest, adjustment_offset
            from fact.future_bar_1m
            where product_code = any(%s)
              and series_type = %s
              and (%s::timestamp is null or bar_time >= %s::timestamp)
              and (%s::timestamp is null or bar_time <= %s::timestamp)
            order by bar_time asc, product_code asc
            limit %s
            """,
            (list(products), storage_series_type, start_value, start_value, end_value, end_value, max(1, min(int(limit), 500000))),
        )
        return [
            FutureBar1mItem(
                product_code=str(row["product_code"]),
                exchange=str(row["exchange"]),
                series_type=actual_series_type,
                bar_time=_format_time(row["bar_time"]),
                open=_optional_float(row.get("open")),
                high=_optional_float(row.get("high")),
                low=_optional_float(row.get("low")),
                close=_optional_float(row.get("close")),
                volume=_optional_float(row.get("volume")),
                open_interest=_optional_float(row.get("open_interest")),
                adjustment_offset=_optional_float(row.get("adjustment_offset")),
            )
            for row in frame.to_dict("records")
        ]

    def list_coverage(self, series_type: str = "") -> list[FutureSeriesCoverageItem]:
        if series_type != "":
            require_series_type(series_type)
        ensure_future_schema()
        storage_series_type = _STORAGE_SERIES_TYPE.get(series_type, "")
        frame = query_dataframe(
            """
            select product_code, exchange, series_type, row_count,
                   first_bar_time, last_bar_time
            from fact.future_bar_1m_coverage
            where (%s = '' or series_type = %s)
            order by series_type, exchange, product_code
            """,
            (storage_series_type, storage_series_type),
        )
        return [
            FutureSeriesCoverageItem(
                product_code=str(row["product_code"]),
                exchange=str(row["exchange"]),
                series_type=SERIES_BACK_ADJUSTED_CONTINUOUS if str(row["series_type"]) == "apex_l0_adjusted" else str(row["series_type"]),
                row_count=int(row["row_count"]),
                first_bar_time=_format_time(row.get("first_bar_time")),
                last_bar_time=_format_time(row.get("last_bar_time")),
            )
            for row in frame.to_dict("records")
        ]

    def update_main_continuous(self, overlap_days: int = 2) -> dict[str, object]:
        ensure_future_schema()
        coverage = {(item.product_code, item.series_type): item for item in self.list_coverage()}
        handler = get_default_source_package_registry().get_handler("shinny_edb", "get_future_main_continuous_1m")
        # EDB 的 start_time/end_time 都按中国市场本地时间解释，不能依赖服务器系统时区。
        now = _china_market_now()
        free_window_start = now - timedelta(days=364)
        fetched_rows = 0
        written_rows = 0
        updated_products = 0
        skipped_products: list[dict[str, str]] = []
        errors: list[dict[str, str]] = []
        for product_code, exchange in PRODUCT_EXCHANGE.items():
            main_coverage = coverage.get((product_code, SERIES_MAIN_CONTINUOUS))
            l0_coverage = coverage.get((product_code, SERIES_BACK_ADJUSTED_CONTINUOUS))
            last_text = main_coverage.last_bar_time if main_coverage is not None else (l0_coverage.last_bar_time if l0_coverage is not None else "")
            if last_text == "":
                skipped_products.append({"product_code": product_code, "reason": "no_local_coverage"})
                continue
            last_time = datetime.fromisoformat(last_text)
            if last_time < free_window_start:
                skipped_products.append({"product_code": product_code, "reason": "outside_free_window"})
                continue
            start_time = max(free_window_start, last_time - timedelta(days=max(1, overlap_days)))
            try:
                items = list(
                    _call_provider_with_retry(
                        handler,
                        product_code,
                        exchange,
                        start_time.strftime("%Y-%m-%d %H:%M:%S"),
                        now.strftime("%Y-%m-%d %H:%M:%S"),
                    )
                )
                fetched_rows += len(items)
                if items == []:
                    skipped_products.append({"product_code": product_code, "reason": "provider_empty"})
                    continue
                written_rows += self._upsert_main_continuous(items)
                updated_products += 1
            except Exception as exc:
                errors.append({"product_code": product_code, "error": f"{type(exc).__name__}: {exc}"})
        return {
            "capability_id": MAIN_CONTINUOUS_CAPABILITY_ID,
            "fetched_rows": fetched_rows,
            "written_rows": written_rows,
            "updated_products": updated_products,
            "skipped_products": skipped_products,
            "errors": errors,
        }

    def _upsert_main_continuous(self, items: list[FutureBar1mItem]) -> int:
        if items == []:
            return 0
        first = items[0]
        connection = _acquire_connection()
        try:
            with connection.cursor() as cursor:
                journal_state = discover_migration_range_journals(cursor, "future_bar_1m")
                if journal_state.has_active_journal:
                    enable_explicit_range_journaling(cursor)
                cursor.execute(
                    """
                    insert into ref.future_series (product_code, exchange, series_type, display_name)
                    values (%s, %s, %s, %s)
                    on conflict (product_code, exchange, series_type) do update
                    set loaded_at = now()
                    """,
                    (first.product_code, first.exchange, SERIES_MAIN_CONTINUOUS, f"{first.product_code} 主力连续"),
                )
                cursor.execute("create temporary table future_bar_1m_stage (like fact.future_bar_1m including defaults) on commit drop")
                with cursor.copy(
                    """
                    copy future_bar_1m_stage
                        (product_code, exchange, series_type, bar_time, open, high, low, close,
                         volume, open_interest, adjustment_offset, source_key)
                    from stdin
                    """
                ) as copy:
                    for item in items:
                        copy.write_row(
                            (
                                item.product_code,
                                item.exchange,
                                SERIES_MAIN_CONTINUOUS,
                                item.bar_time,
                                item.open,
                                item.high,
                                item.low,
                                item.close,
                                item.volume,
                                item.open_interest,
                                None,
                                "shinny_edb",
                            )
                        )
                cursor.execute(
                    """
                    insert into fact.future_bar_1m
                        (product_code, exchange, series_type, bar_time, open, high, low, close,
                         volume, open_interest, adjustment_offset, source_key)
                    select product_code, exchange, series_type, bar_time, open, high, low, close,
                           volume, open_interest, adjustment_offset, source_key
                    from future_bar_1m_stage
                    on conflict (product_code, exchange, series_type, bar_time) do update set
                        open = excluded.open,
                        high = excluded.high,
                        low = excluded.low,
                        close = excluded.close,
                        volume = excluded.volume,
                        open_interest = excluded.open_interest,
                        adjustment_offset = excluded.adjustment_offset,
                        source_key = excluded.source_key,
                        loaded_at = now()
                    """
                )
                append_migration_range_journals(cursor, journal_state, [item.bar_time for item in items])
            connection.commit()
            return len(items)
        except Exception:
            connection.rollback()
            raise
        finally:
            _release_connection(connection)
