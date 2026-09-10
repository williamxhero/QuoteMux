from __future__ import annotations

from collections.abc import Callable
import threading
from typing import Any

from psycopg.rows import tuple_row

from quotemux.infra.db.client import _acquire_connection, _release_connection
from quotemux.strict_read import reject_in_strict_public_read


IDENTITY_PROVISIONAL = "provisional"
IDENTITY_AUTHORITATIVE = "authoritative"
AUTHORITY_PROVIDER = "tushare"

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
    "alter table ref.stock add column if not exists identity_status text not null default 'provisional'",
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
        fresh_through date not null,
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
    "drop trigger if exists stock_authority_input_item_immutable on audit.stock_authority_input_item",
    """
    create trigger stock_authority_input_item_immutable
    before update or delete on audit.stock_authority_input_item
    for each row execute function audit.reject_stock_authority_audit_mutation()
    """,
    "drop trigger if exists stock_reference_reconciliation_immutable on audit.stock_reference_reconciliation",
    """
    create trigger stock_reference_reconciliation_immutable
    before update or delete on audit.stock_reference_reconciliation
    for each row execute function audit.reject_stock_authority_audit_mutation()
    """,
    "revoke insert, update, delete, truncate on audit.stock_authority_input from public",
    "revoke insert, update, delete, truncate on audit.stock_authority_input_item from public",
    "revoke insert, update, delete, truncate on audit.stock_reference_reconciliation from public",
    "create index if not exists stock_identity_status_idx on ref.stock (identity_status, market, code)",
)


class StockReferenceMigrationError(RuntimeError):
    pass


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
        raise StockReferenceMigrationError("ref.stock must exist before the authority expand migration")
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
