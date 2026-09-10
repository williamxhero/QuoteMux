from __future__ import annotations

import json
import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from hashlib import sha256
from typing import Any
from zoneinfo import ZoneInfo

from psycopg.rows import tuple_row
from psycopg.types.json import Jsonb

from quotemux.infra.common import format_date_value, stock_market_name
from quotemux.infra.db.client import _acquire_connection, _release_connection
from quotemux.strict_read import reject_in_strict_public_read

IDENTITY_PROVISIONAL = "provisional"
IDENTITY_AUTHORITATIVE = "authoritative"
AUTHORITY_PROVIDER = "tushare"
REQUIRED_AUTHORITY_SHARDS = ("listed", "pending", "delisted")
SHANGHAI_TIMEZONE = ZoneInfo("Asia/Shanghai")
MARKET_CLOSE = time(15, 0)

_LEGACY_STOCK_COLUMNS = {
    "market",
    "code",
    "name",
    "industry",
    "listing_board",
    "listed_date",
    "delisted_date",
    "area",
}
_AUTHORITY_STOCK_COLUMNS = {
    "identity_status",
    "identity_source",
    "authority_provider",
    "authority_input_id",
    "authority_verified_at",
}
_MIGRATION_LOCK = threading.Lock()
_MIGRATION_READY = False


STOCK_REFERENCE_AUTHORITY_SCHEMA_SQL = (
    "create schema if not exists audit",
    """
    alter table ref.stock
    add column if not exists identity_status text not null default 'provisional'
    """,
    "alter table ref.stock add column if not exists identity_source text not null default 'legacy'",
    "alter table ref.stock add column if not exists authority_provider text",
    "alter table ref.stock add column if not exists authority_input_id text",
    "alter table ref.stock add column if not exists authority_verified_at timestamp with time zone",
    """
    do $$ begin
      if not exists (
        select 1 from pg_constraint
        where conname = 'stock_identity_source_check'
          and conrelid = 'ref.stock'::regclass
      ) then
        alter table ref.stock add constraint stock_identity_source_check check (
          identity_source in ('legacy', 'stock_daily_1d', 'catalog_partial', 'tushare_catalog')
        );
      end if;
    end $$
    """,
    """
    do $$ begin
      if not exists (
        select 1 from pg_constraint
        where conname = 'stock_identity_status_check'
          and conrelid = 'ref.stock'::regclass
      ) then
        alter table ref.stock add constraint stock_identity_status_check check (
          (
            identity_status = 'provisional'
            and authority_provider is null
            and authority_input_id is null
            and authority_verified_at is null
          )
          or (
            identity_status = 'authoritative'
            and identity_source = 'tushare_catalog'
            and authority_provider = 'tushare'
            and authority_input_id ~ '^[0-9a-f]{64}$'
            and authority_verified_at is not null
            and btrim(name) <> ''
          )
        );
      end if;
    end $$
    """,
    """
    create or replace function ref.enforce_stock_identity_transition()
    returns trigger language plpgsql as $$
    begin
      if old.identity_status = 'authoritative'
         and new.identity_status = 'provisional' then
        raise exception 'authoritative stock identity cannot be downgraded';
      end if;
      return new;
    end $$
    """,
    "drop trigger if exists stock_identity_transition_guard on ref.stock",
    """
    create trigger stock_identity_transition_guard
    before update on ref.stock
    for each row execute function ref.enforce_stock_identity_transition()
    """,
    """
    create table if not exists audit.stock_authority_input (
        input_id text primary key,
        provider text not null,
        source_refreshed_at timestamp with time zone not null,
        fresh_through date,
        content_sha256 text not null,
        request_status text not null,
        shard_counts jsonb not null,
        candidate_count integer not null,
        rejection_reason text not null default '',
        loaded_at timestamp with time zone not null default now(),
        constraint stock_authority_input_provider_check check (provider = 'tushare'),
        constraint stock_authority_input_sha256_check check (
            input_id ~ '^[0-9a-f]{64}$' and content_sha256 ~ '^[0-9a-f]{64}$'
        ),
        constraint stock_authority_input_status_check check (
            request_status in ('accepted', 'rejected')
            and (request_status = 'rejected' or fresh_through is not null)
        ),
        constraint stock_authority_input_count_check check (candidate_count >= 0)
    )
    """,
    """
    create table if not exists audit.stock_authority_input_item (
        input_id text not null references audit.stock_authority_input(input_id),
        market text not null,
        code text not null,
        name text not null,
        industry text not null,
        listing_board text not null,
        listed_date date,
        delisted_date date,
        area text not null,
        list_status text not null,
        primary key (input_id, market, code),
        constraint stock_authority_item_code_check check (code ~ '^[0-9]{6}$'),
        constraint stock_authority_item_name_check check (btrim(name) <> ''),
        constraint stock_authority_item_status_check check (list_status in ('L', 'P', 'D'))
    )
    """,
    """
    create table if not exists audit.stock_reference_reconciliation (
        run_key text primary key,
        input_id text not null references audit.stock_authority_input(input_id),
        provider text not null,
        input_sha256 text not null,
        fresh_through date not null,
        candidate_count integer not null,
        existing_count integer not null,
        inserted_count integer not null,
        promoted_count integer not null,
        renamed_count integer not null,
        missing_count integer not null,
        conflict_count integer not null,
        transaction_result text not null,
        normalized_output_sha256 text not null,
        audit_content_sha256 text not null,
        committed_at timestamp with time zone not null default now(),
        constraint stock_reference_reconciliation_provider_check check (provider = 'tushare'),
        constraint stock_reference_reconciliation_hash_check check (
            input_sha256 ~ '^[0-9a-f]{64}$'
            and normalized_output_sha256 ~ '^[0-9a-f]{64}$'
            and audit_content_sha256 ~ '^[0-9a-f]{64}$'
        ),
        constraint stock_reference_reconciliation_result_check check (
            transaction_result = 'committed'
        ),
        constraint stock_reference_reconciliation_count_check check (
            candidate_count >= 0 and existing_count >= 0 and inserted_count >= 0
            and promoted_count >= 0 and renamed_count >= 0 and missing_count >= 0
            and conflict_count >= 0
        )
    )
    """,
    """
    create or replace function audit.reject_stock_authority_audit_mutation()
    returns trigger language plpgsql as $$
    begin
      raise exception 'stock authority audit records are immutable';
    end $$
    """,
    "drop trigger if exists stock_authority_input_immutable on audit.stock_authority_input",
    """
    create trigger stock_authority_input_immutable
    before update or delete on audit.stock_authority_input
    for each row execute function audit.reject_stock_authority_audit_mutation()
    """,
    """
    drop trigger if exists stock_authority_input_item_immutable
    on audit.stock_authority_input_item
    """,
    """
    create trigger stock_authority_input_item_immutable
    before update or delete on audit.stock_authority_input_item
    for each row execute function audit.reject_stock_authority_audit_mutation()
    """,
    """
    drop trigger if exists stock_reference_reconciliation_immutable
    on audit.stock_reference_reconciliation
    """,
    """
    create trigger stock_reference_reconciliation_immutable
    before update or delete on audit.stock_reference_reconciliation
    for each row execute function audit.reject_stock_authority_audit_mutation()
    """,
    "revoke insert, update, delete, truncate on audit.stock_authority_input from public",
    "revoke insert, update, delete, truncate on audit.stock_authority_input_item from public",
    "revoke insert, update, delete, truncate on audit.stock_reference_reconciliation from public",
    """
    create index if not exists stock_identity_status_idx
    on ref.stock (identity_status, market, code)
    """,
)


class StockReferenceMigrationError(RuntimeError):
    pass


class StockAuthorityInputError(RuntimeError):
    def __init__(self, reason: str, *, shard_counts: Mapping[str, int] | None = None) -> None:
        super().__init__(reason)
        self.reason = reason
        self.shard_counts = dict(shard_counts or {})


@dataclass(frozen=True)
class NormalizedStockAuthorityItem:
    market: str
    code: str
    name: str
    industry: str
    listing_board: str
    listed_date: str
    delisted_date: str
    area: str
    list_status: str

    def to_payload(self) -> dict[str, str]:
        return {
            "market": self.market,
            "code": self.code,
            "name": self.name,
            "industry": self.industry,
            "listing_board": self.listing_board,
            "listed_date": self.listed_date,
            "delisted_date": self.delisted_date,
            "area": self.area,
            "list_status": self.list_status,
        }


@dataclass(frozen=True)
class StockAuthorityInput:
    input_id: str
    provider: str
    source_refreshed_at_utc: datetime
    fresh_through: date
    content_sha256: str
    shard_counts: Mapping[str, int]
    items: tuple[NormalizedStockAuthorityItem, ...]


def _canonical_hash(payload: object) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def _normalized_list_status(value: object) -> str:
    text = str(value).strip().lower()
    return {
        "l": "L",
        "listed": "L",
        "p": "P",
        "pending": "P",
        "d": "D",
        "delisted": "D",
    }.get(text, "")


def _normalized_market(value: object, code: str) -> str:
    text = str(value).strip().lower()
    mapped = {
        "sse": "SHSE",
        "sh": "SHSE",
        "shse": "SHSE",
        "star_market": "SHSE",
        "sz": "SZSE",
        "szse": "SZSE",
        "chi_next": "SZSE",
        "bse": "BJSE",
        "bj": "BJSE",
        "bjse": "BJSE",
        "beijing": "BJSE",
    }.get(text, "")
    return mapped or stock_market_name(code)


def _normalize_authority_item(item: object, expected_shard: str) -> NormalizedStockAuthorityItem:
    raw_code = getattr(item, "code", "")
    if not isinstance(raw_code, str):
        raise StockAuthorityInputError("stock authority code must be text")
    code = raw_code.strip()
    if len(code) != 6 or not code.isascii() or not code.isdigit():
        raise StockAuthorityInputError(f"invalid stock authority code: {raw_code!r}")

    raw_name = getattr(item, "name", "")
    if not isinstance(raw_name, str):
        raise StockAuthorityInputError(f"stock authority name must be text: {code}")
    name = raw_name.strip()
    if name == "":
        raise StockAuthorityInputError(f"stock authority name is blank: {code}")

    expected_status = _normalized_list_status(expected_shard)
    actual_status = _normalized_list_status(getattr(item, "list_status", ""))
    if actual_status != expected_status:
        raise StockAuthorityInputError(
            "stock authority status mismatch: "
            f"{code} expected={expected_status} actual={actual_status}"
        )

    market = _normalized_market(
        getattr(item, "exchange", "") or getattr(item, "market", ""),
        code,
    )
    expected_market = stock_market_name(code)
    if market not in {"SHSE", "SZSE", "BJSE"} or (
        expected_market in {"SHSE", "SZSE", "BJSE"} and market != expected_market
    ):
        raise StockAuthorityInputError(
            f"stock authority market mismatch: {code} market={market} expected={expected_market}"
        )

    return NormalizedStockAuthorityItem(
        market=market,
        code=code,
        name=name,
        industry=str(getattr(item, "industry", "") or ""),
        listing_board=str(getattr(item, "listing_board", "") or getattr(item, "market", "") or ""),
        listed_date=format_date_value(getattr(item, "list_date", "")),
        delisted_date=format_date_value(getattr(item, "delist_date", "")),
        area=str(getattr(item, "area", "") or ""),
        list_status=actual_status,
    )


def latest_completed_trading_day(
    as_of_utc: datetime,
    open_trade_dates: Sequence[date],
) -> date:
    if as_of_utc.tzinfo is None or as_of_utc.utcoffset() is None:
        raise StockAuthorityInputError("authority refresh timestamp must be timezone-aware")
    local_time = as_of_utc.astimezone(SHANGHAI_TIMEZONE)
    cutoff = local_time.date()
    if local_time.time().replace(tzinfo=None) < MARKET_CLOSE:
        cutoff -= timedelta(days=1)
    eligible = [trade_date for trade_date in open_trade_dates if trade_date <= cutoff]
    if eligible == []:
        raise StockAuthorityInputError("authoritative trading calendar has no completed date")
    return max(eligible)


def load_latest_completed_trading_day(
    as_of_utc: datetime,
    *,
    connection_factory: Callable[[], Any] | None = None,
) -> date:
    if as_of_utc.tzinfo is None or as_of_utc.utcoffset() is None:
        raise StockAuthorityInputError("authority refresh timestamp must be timezone-aware")
    local_time = as_of_utc.astimezone(SHANGHAI_TIMEZONE)
    cutoff = local_time.date()
    if local_time.time().replace(tzinfo=None) < MARKET_CLOSE:
        cutoff -= timedelta(days=1)
    factory = connection_factory or _acquire_connection
    connection = factory()
    owns_connection = connection_factory is None
    try:
        with connection.cursor(row_factory=tuple_row) as cursor:
            cursor.execute(
                """
                select distinct trade_date
                from ref.trade_calendar
                where exchange in ('SSE', 'SHSE', 'SZSE', 'BSE', 'BJSE')
                  and is_open
                  and trade_date <= %s
                order by trade_date desc
                limit 10
                """,
                (cutoff,),
            )
            rows = cursor.fetchall()
        connection.rollback()
    except Exception:
        connection.rollback()
        raise
    finally:
        if owns_connection:
            _release_connection(connection)
    dates = [value for row in rows if (value := _row_value(row, "trade_date", 0)) is not None]
    normalized_dates = [
        value if isinstance(value, date) else date.fromisoformat(str(value)) for value in dates
    ]
    return latest_completed_trading_day(as_of_utc, normalized_dates)


def prepare_stock_authority_input(
    shards: Mapping[str, Sequence[object]],
    *,
    source_refreshed_at_utc: datetime,
    fresh_through: date,
) -> StockAuthorityInput:
    shard_counts = {shard: len(shards.get(shard, ())) for shard in REQUIRED_AUTHORITY_SHARDS}
    missing_shards = [shard for shard, count in shard_counts.items() if count == 0]
    if missing_shards:
        raise StockAuthorityInputError(
            "stock authority input has empty required shards: " + ", ".join(missing_shards),
            shard_counts=shard_counts,
        )
    if source_refreshed_at_utc.tzinfo is None or source_refreshed_at_utc.utcoffset() is None:
        raise StockAuthorityInputError(
            "authority refresh timestamp must be timezone-aware",
            shard_counts=shard_counts,
        )
    refreshed_local_date = source_refreshed_at_utc.astimezone(SHANGHAI_TIMEZONE).date()
    if refreshed_local_date < fresh_through:
        raise StockAuthorityInputError(
            "stock authority input is stale: "
            f"refreshed={refreshed_local_date.isoformat()} "
            f"required={fresh_through.isoformat()}",
            shard_counts=shard_counts,
        )

    items_by_code: dict[str, NormalizedStockAuthorityItem] = {}
    for shard in REQUIRED_AUTHORITY_SHARDS:
        for raw_item in shards[shard]:
            try:
                normalized = _normalize_authority_item(raw_item, shard)
            except StockAuthorityInputError as exc:
                raise StockAuthorityInputError(exc.reason, shard_counts=shard_counts) from exc
            existing = items_by_code.get(normalized.code)
            if existing is None:
                items_by_code[normalized.code] = normalized
                continue
            if existing != normalized:
                raise StockAuthorityInputError(
                    f"conflicting stock authority identity: {normalized.code}",
                    shard_counts=shard_counts,
                )

    normalized_items = tuple(items_by_code[code] for code in sorted(items_by_code))
    content_sha256 = _canonical_hash([item.to_payload() for item in normalized_items])
    input_id = _canonical_hash(
        {
            "provider": AUTHORITY_PROVIDER,
            "content_sha256": content_sha256,
            "fresh_through": fresh_through.isoformat(),
        }
    )
    return StockAuthorityInput(
        input_id=input_id,
        provider=AUTHORITY_PROVIDER,
        source_refreshed_at_utc=source_refreshed_at_utc.astimezone(UTC),
        fresh_through=fresh_through,
        content_sha256=content_sha256,
        shard_counts=shard_counts,
        items=normalized_items,
    )


def _persist_stock_authority_input(
    authority_input: StockAuthorityInput,
    *,
    connection_factory: Callable[[], Any] | None = None,
) -> str:
    ensure_stock_reference_authority_schema()
    factory = connection_factory or _acquire_connection
    connection = factory()
    owns_connection = connection_factory is None
    try:
        with connection.cursor(row_factory=tuple_row) as cursor:
            cursor.execute(
                """
                select provider, content_sha256, fresh_through::text
                from audit.stock_authority_input
                where input_id = %s
                """,
                (authority_input.input_id,),
            )
            existing = cursor.fetchone()
            if existing is not None:
                actual = (
                    str(_row_value(existing, "provider", 0)),
                    str(_row_value(existing, "content_sha256", 1)),
                    str(_row_value(existing, "fresh_through", 2)),
                )
                expected = (
                    authority_input.provider,
                    authority_input.content_sha256,
                    authority_input.fresh_through.isoformat(),
                )
                if actual != expected:
                    raise StockAuthorityInputError(
                        f"authority input identity collision: {authority_input.input_id}"
                    )
                connection.rollback()
                return "idempotent"
            cursor.execute(
                """
                insert into audit.stock_authority_input (
                    input_id, provider, source_refreshed_at, fresh_through,
                    content_sha256, request_status, shard_counts, candidate_count
                )
                values (%s, %s, %s, %s, %s, 'accepted', %s, %s)
                """,
                (
                    authority_input.input_id,
                    authority_input.provider,
                    authority_input.source_refreshed_at_utc,
                    authority_input.fresh_through,
                    authority_input.content_sha256,
                    Jsonb(dict(authority_input.shard_counts)),
                    len(authority_input.items),
                ),
            )
            cursor.executemany(
                """
                insert into audit.stock_authority_input_item (
                    input_id, market, code, name, industry, listing_board,
                    listed_date, delisted_date, area, list_status
                )
                values (%s, %s, %s, %s, %s, %s, nullif(%s, '')::date,
                        nullif(%s, '')::date, %s, %s)
                """,
                [
                    (
                        authority_input.input_id,
                        item.market,
                        item.code,
                        item.name,
                        item.industry,
                        item.listing_board,
                        item.listed_date,
                        item.delisted_date,
                        item.area,
                        item.list_status,
                    )
                    for item in authority_input.items
                ],
            )
        connection.commit()
        return "accepted"
    except Exception:
        connection.rollback()
        raise
    finally:
        if owns_connection:
            _release_connection(connection)


def record_rejected_stock_authority_input(
    error: StockAuthorityInputError,
    *,
    source_refreshed_at_utc: datetime,
    fresh_through: date | None,
    connection_factory: Callable[[], Any] | None = None,
) -> str:
    ensure_stock_reference_authority_schema()
    rejection_payload = {
        "provider": AUTHORITY_PROVIDER,
        "request_status": "rejected",
        "reason": error.reason,
        "shard_counts": dict(sorted(error.shard_counts.items())),
        "fresh_through": fresh_through.isoformat() if fresh_through is not None else "",
    }
    content_sha256 = _canonical_hash(rejection_payload)
    input_id = _canonical_hash(
        {
            **rejection_payload,
            "source_refreshed_at_utc": source_refreshed_at_utc.astimezone(UTC).isoformat(),
        }
    )
    factory = connection_factory or _acquire_connection
    connection = factory()
    owns_connection = connection_factory is None
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                insert into audit.stock_authority_input (
                    input_id, provider, source_refreshed_at, fresh_through,
                    content_sha256, request_status, shard_counts,
                    candidate_count, rejection_reason
                )
                values (%s, 'tushare', %s, %s, %s, 'rejected', %s, %s, %s)
                on conflict (input_id) do nothing
                """,
                (
                    input_id,
                    source_refreshed_at_utc.astimezone(UTC),
                    fresh_through,
                    content_sha256,
                    Jsonb(dict(error.shard_counts)),
                    sum(error.shard_counts.values()),
                    error.reason,
                ),
            )
        connection.commit()
        return input_id
    except Exception:
        connection.rollback()
        raise
    finally:
        if owns_connection:
            _release_connection(connection)


def freeze_stock_authority_input(
    shards: Mapping[str, Sequence[object]],
    *,
    source_refreshed_at_utc: datetime,
    fresh_through: date,
    connection_factory: Callable[[], Any] | None = None,
) -> StockAuthorityInput:
    try:
        authority_input = prepare_stock_authority_input(
            shards,
            source_refreshed_at_utc=source_refreshed_at_utc,
            fresh_through=fresh_through,
        )
    except StockAuthorityInputError as exc:
        record_rejected_stock_authority_input(
            exc,
            source_refreshed_at_utc=source_refreshed_at_utc,
            fresh_through=fresh_through,
            connection_factory=connection_factory,
        )
        raise
    _persist_stock_authority_input(
        authority_input,
        connection_factory=connection_factory,
    )
    return authority_input


def _row_value(row: object, key: str, index: int) -> object:
    if isinstance(row, dict):
        return row.get(key)
    if isinstance(row, (tuple, list)) and len(row) > index:
        return row[index]
    return None


def _load_stock_columns(cursor: Any) -> set[str]:
    cursor.execute(
        """
        select column_name
        from information_schema.columns
        where table_schema = 'ref' and table_name = 'stock'
        """
    )
    return {
        str(_row_value(row, "column_name", 0))
        for row in cursor.fetchall()
        if _row_value(row, "column_name", 0) is not None
    }


def _preflight_stock_reference_table(cursor: Any) -> None:
    cursor.execute("select to_regclass('ref.stock')")
    row = cursor.fetchone()
    if row is None or _row_value(row, "to_regclass", 0) is None:
        raise StockReferenceMigrationError(
            "ref.stock must exist before the authority expand migration"
        )
    missing = sorted(_LEGACY_STOCK_COLUMNS - _load_stock_columns(cursor))
    if missing:
        raise StockReferenceMigrationError(
            "ref.stock is missing legacy compatibility columns: " + ", ".join(missing)
        )


def _verify_stock_reference_authority_schema(cursor: Any) -> None:
    missing = sorted(_AUTHORITY_STOCK_COLUMNS - _load_stock_columns(cursor))
    if missing:
        raise StockReferenceMigrationError(
            "stock authority migration did not create columns: " + ", ".join(missing)
        )
    cursor.execute(
        """
        select to_regclass('audit.stock_authority_input'),
               to_regclass('audit.stock_authority_input_item'),
               to_regclass('audit.stock_reference_reconciliation')
        """
    )
    row = cursor.fetchone()
    if row is None or any(_row_value(row, "unused", index) is None for index in range(3)):
        raise StockReferenceMigrationError("stock authority audit tables are incomplete")


def apply_stock_reference_authority_migration(
    connection_factory: Callable[[], Any] = _acquire_connection,
) -> None:
    """Apply the rollback-compatible authority lifecycle expansion atomically."""

    reject_in_strict_public_read("stock_reference_authority:migrate")
    connection = connection_factory()
    owns_connection = connection_factory is _acquire_connection
    try:
        with connection.cursor(row_factory=tuple_row) as cursor:
            _preflight_stock_reference_table(cursor)
            for statement in STOCK_REFERENCE_AUTHORITY_SCHEMA_SQL:
                cursor.execute(statement)
            _verify_stock_reference_authority_schema(cursor)
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        if owns_connection:
            _release_connection(connection)


def ensure_stock_reference_authority_schema() -> None:
    """Apply the expand migration once per process before an authority-aware write."""

    global _MIGRATION_READY
    if _MIGRATION_READY:
        return
    with _MIGRATION_LOCK:
        if _MIGRATION_READY:
            return
        apply_stock_reference_authority_migration()
        _MIGRATION_READY = True
