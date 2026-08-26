"""Privileged versioned migration for QuoteMux futures partial metadata/ACLs.

This is intentionally outside ``futures.FUTURE_SCHEMA_SQL``.  A normal
application bootstrap must never replace SECURITY DEFINER functions owned by a
NOLOGIN database owner.
"""
from __future__ import annotations

from collections.abc import Callable
from typing import Any

from psycopg import sql
from psycopg.rows import tuple_row
from quotemux.infra.db.client import _acquire_connection, _release_connection


METADATA_DDL = (
    """create table if not exists audit.future_bar_1m_import_publication (qmi_id text primary key check(qmi_id ~ '^qmi-v1-[0-9a-f]{64}$'),source_normalized_rowset_sha256 text not null,fact_transform_version text not null,canonical_fact_rowset_sha256 text not null,payload_sha256 text not null,manifest_json jsonb not null,inserted_count bigint not null check(inserted_count>=0),equivalent_count bigint not null check(equivalent_count>=0),conflict_count bigint not null check(conflict_count>=0),published_at timestamptz not null default now(),created_txid bigint not null default txid_current(),unique(source_normalized_rowset_sha256,fact_transform_version,canonical_fact_rowset_sha256,payload_sha256))""",
    """create table if not exists audit.future_bar_1m_import_disposition (qmi_id text not null references audit.future_bar_1m_import_publication(qmi_id),product_code text not null,exchange text not null,series_type text not null,bar_time timestamp not null,candidate_sha256 text not null,disposition text not null check(disposition in ('missing_valid','already_present_equivalent','existing_conflict')),existing_source_key text,existing_fact_sha256 text,primary key(qmi_id,product_code,exchange,series_type,bar_time,candidate_sha256))""",
    """create table if not exists audit.future_bar_1m_import_admission (qmi_id text not null references audit.future_bar_1m_import_publication(qmi_id),product_code text not null,exchange text not null,series_type text not null,bar_time timestamp not null,candidate_sha256 text not null,disposition text not null check(disposition in ('inserted','already_present_equivalent')),primary key(qmi_id,product_code,exchange,series_type,bar_time,candidate_sha256))""",
    """create table if not exists audit.future_bar_1m_partial_publication (qmp_id text primary key check(qmp_id ~ '^qmp-v1-[0-9a-f]{64}$'),dataset_id text not null,payload_json jsonb not null,payload_sha256 text not null,published_at timestamptz not null default now(),created_txid bigint not null default txid_current())""",
    """create table if not exists audit.future_bar_1m_partial_source_boundary (qmp_id text not null references audit.future_bar_1m_partial_publication(qmp_id),boundary_id text not null,product_code text not null,exchange text not null,series_type text not null,source_key text not null,start_time timestamp not null,end_time timestamp not null,evidence_json jsonb not null,primary key(qmp_id,boundary_id),check(start_time<=end_time))""",
    """create table if not exists audit.future_bar_1m_partial_revision (qmc_id text primary key check(qmc_id ~ '^qmc-v1-[0-9a-f]{64}$'),qmp_id text not null references audit.future_bar_1m_partial_publication(qmp_id),payload_json jsonb not null,payload_sha256 text not null,published_at timestamptz not null default now(),created_txid bigint not null default txid_current())""",
    """create table if not exists audit.future_bar_1m_partial_revision_interval (qmc_id text not null references audit.future_bar_1m_partial_revision(qmc_id),interval_id text not null,product_code text not null,exchange text not null,start_time timestamp not null,end_time timestamp not null,status text not null check(status='accepted'),observed_count bigint not null check(observed_count>0),residual_json jsonb not null,primary key(qmc_id,interval_id),check(start_time<=end_time))""",
    "create index if not exists future_partial_boundary_lookup_idx on audit.future_bar_1m_partial_source_boundary(qmp_id,product_code,exchange,series_type,source_key,start_time,end_time)",
    "create index if not exists future_partial_interval_lookup_idx on audit.future_bar_1m_partial_revision_interval(qmc_id,product_code,start_time,end_time,status)",
    "alter table audit.future_bar_1m_import_publication add column if not exists created_txid bigint not null default txid_current()",
    "alter table audit.future_bar_1m_partial_publication add column if not exists created_txid bigint not null default txid_current()",
    "alter table audit.future_bar_1m_partial_revision add column if not exists created_txid bigint not null default txid_current()",
    "alter table audit.future_bar_1m_import_disposition add column if not exists existing_source_key text",
    "alter table audit.future_bar_1m_import_disposition add column if not exists existing_fact_sha256 text",
    "alter table audit.future_bar_1m_partial_revision_interval add column if not exists exchange text",
    "alter table audit.future_bar_1m_partial_revision_interval alter column exchange set not null",
)


_FUNCTIONS = (
    "fact.refresh_future_bar_1m_coverage_group(text,text,text)",
    "fact.maintain_future_bar_1m_coverage_after_insert()",
    "fact.maintain_future_bar_1m_coverage_after_delete()",
    "fact.maintain_future_bar_1m_coverage_after_update()",
    "fact.maintain_future_bar_1m_after_truncate()",
    "audit.record_future_bar_1m_series_generation(text,text,text)",
    "audit.maintain_future_bar_1m_series_generation_after_insert()",
    "audit.maintain_future_bar_1m_series_generation_after_update()",
    "audit.maintain_future_bar_1m_series_generation_after_delete()",
    "audit.prevent_future_partial_child_append()",
)


HARDENED_FUNCTION_DDL = (
"""create or replace function fact.refresh_future_bar_1m_coverage_group(target_product_code text,target_exchange text,target_series_type text) returns void language plpgsql security definer set search_path=pg_catalog as $$ begin delete from fact.future_bar_1m_coverage where product_code=target_product_code and exchange=target_exchange and series_type=target_series_type; insert into fact.future_bar_1m_coverage(product_code,exchange,series_type,row_count,first_bar_time,last_bar_time,updated_at) select product_code,exchange,series_type,count(*),min(bar_time),max(bar_time),clock_timestamp() from fact.future_bar_1m where product_code=target_product_code and exchange=target_exchange and series_type=target_series_type group by product_code,exchange,series_type; end $$""",
"""create or replace function fact.maintain_future_bar_1m_coverage_after_insert() returns trigger language plpgsql security definer set search_path=pg_catalog as $$ begin insert into fact.future_bar_1m_coverage(product_code,exchange,series_type,row_count,first_bar_time,last_bar_time,updated_at) select product_code,exchange,series_type,count(*),min(bar_time),max(bar_time),clock_timestamp() from inserted_rows group by product_code,exchange,series_type on conflict(product_code,exchange,series_type) do update set row_count=fact.future_bar_1m_coverage.row_count+excluded.row_count,first_bar_time=least(fact.future_bar_1m_coverage.first_bar_time,excluded.first_bar_time),last_bar_time=greatest(fact.future_bar_1m_coverage.last_bar_time,excluded.last_bar_time),updated_at=clock_timestamp(); return null; end $$""",
"""create or replace function fact.maintain_future_bar_1m_coverage_after_delete() returns trigger language plpgsql security definer set search_path=pg_catalog as $$ declare item record; begin for item in select distinct product_code,exchange,series_type from deleted_rows loop perform fact.refresh_future_bar_1m_coverage_group(item.product_code,item.exchange,item.series_type); end loop; return null; end $$""",
"""create or replace function fact.maintain_future_bar_1m_coverage_after_update() returns trigger language plpgsql security definer set search_path=pg_catalog as $$ declare item record; begin for item in select product_code,exchange,series_type from updated_old_rows union select product_code,exchange,series_type from updated_new_rows loop perform fact.refresh_future_bar_1m_coverage_group(item.product_code,item.exchange,item.series_type); end loop; return null; end $$""",
"""create or replace function audit.record_future_bar_1m_series_generation(target_series_type text,target_operation text,target_delta_fingerprint text) returns void language plpgsql security definer set search_path=pg_catalog as $$ declare next_generation bigint; begin perform pg_advisory_xact_lock(hashtext('future_bar_1m_series_generation:'||target_series_type)); select coalesce(max(generation),0)+1 into next_generation from audit.future_bar_1m_series_generation where series_type=target_series_type; insert into audit.future_bar_1m_series_generation(series_type,generation,row_count,first_bar_time,last_bar_time,transaction_id,operation,delta_fingerprint) select target_series_type,next_generation,count(*),min(bar_time),max(bar_time),txid_current(),target_operation,target_delta_fingerprint from fact.future_bar_1m where series_type=target_series_type; end $$""",
"""create or replace function audit.maintain_future_bar_1m_series_generation_after_insert() returns trigger language plpgsql security definer set search_path=pg_catalog as $$ declare item record; begin for item in select series_type,count(*) n,min(bar_time) lo,max(bar_time) hi from inserted_rows group by series_type loop perform audit.record_future_bar_1m_series_generation(item.series_type,'insert',md5(item.series_type||'|'||item.n||'|'||item.lo||'|'||item.hi)); end loop; return null; end $$""",
"""create or replace function audit.maintain_future_bar_1m_series_generation_after_update() returns trigger language plpgsql security definer set search_path=pg_catalog as $$ declare item record; begin for item in select series_type,count(*) n,min(bar_time) lo,max(bar_time) hi from (select series_type,bar_time from updated_old_rows union select series_type,bar_time from updated_new_rows) changed group by series_type loop perform audit.record_future_bar_1m_series_generation(item.series_type,'update',md5(item.series_type||'|'||item.n||'|'||item.lo||'|'||item.hi)); end loop; return null; end $$""",
"""create or replace function audit.maintain_future_bar_1m_series_generation_after_delete() returns trigger language plpgsql security definer set search_path=pg_catalog as $$ declare item record; begin for item in select series_type,count(*) n,min(bar_time) lo,max(bar_time) hi from deleted_rows group by series_type loop perform audit.record_future_bar_1m_series_generation(item.series_type,'delete',md5(item.series_type||'|'||item.n||'|'||item.lo||'|'||item.hi)); end loop; return null; end $$""",
"""create or replace function fact.maintain_future_bar_1m_after_truncate() returns trigger language plpgsql security definer set search_path=pg_catalog as $$ begin delete from fact.future_bar_1m_coverage; perform audit.record_future_bar_1m_series_generation('apex_l0_adjusted','truncate',md5('apex_l0_adjusted|truncate|'||txid_current())); perform audit.record_future_bar_1m_series_generation('main_continuous','truncate',md5('main_continuous|truncate|'||txid_current())); return null; end $$""",
"""create or replace function audit.prevent_future_partial_child_append() returns trigger language plpgsql security definer set search_path=pg_catalog as $$ declare parent_txid bigint; begin if tg_table_name in ('future_bar_1m_import_disposition','future_bar_1m_import_admission') then select created_txid into parent_txid from audit.future_bar_1m_import_publication where qmi_id=new.qmi_id; elsif tg_table_name='future_bar_1m_partial_source_boundary' then select created_txid into parent_txid from audit.future_bar_1m_partial_publication where qmp_id=new.qmp_id; else select created_txid into parent_txid from audit.future_bar_1m_partial_revision where qmc_id=new.qmc_id; end if; if parent_txid is null or parent_txid<>txid_current() then raise exception 'immutable future partial parent is sealed'; end if; return new; end $$""",
)

TRIGGER_DDL = (
    "drop trigger if exists future_bar_1m_coverage_after_insert on fact.future_bar_1m", "create trigger future_bar_1m_coverage_after_insert after insert on fact.future_bar_1m referencing new table as inserted_rows for each statement execute function fact.maintain_future_bar_1m_coverage_after_insert()",
    "drop trigger if exists future_bar_1m_coverage_after_delete on fact.future_bar_1m", "create trigger future_bar_1m_coverage_after_delete after delete on fact.future_bar_1m referencing old table as deleted_rows for each statement execute function fact.maintain_future_bar_1m_coverage_after_delete()",
    "drop trigger if exists future_bar_1m_coverage_after_update on fact.future_bar_1m", "create trigger future_bar_1m_coverage_after_update after update on fact.future_bar_1m referencing old table as updated_old_rows new table as updated_new_rows for each statement execute function fact.maintain_future_bar_1m_coverage_after_update()",
    "drop trigger if exists future_bar_1m_series_generation_after_insert on fact.future_bar_1m", "create trigger future_bar_1m_series_generation_after_insert after insert on fact.future_bar_1m referencing new table as inserted_rows for each statement execute function audit.maintain_future_bar_1m_series_generation_after_insert()",
    "drop trigger if exists future_bar_1m_series_generation_after_update on fact.future_bar_1m", "create trigger future_bar_1m_series_generation_after_update after update on fact.future_bar_1m referencing old table as updated_old_rows new table as updated_new_rows for each statement execute function audit.maintain_future_bar_1m_series_generation_after_update()",
    "drop trigger if exists future_bar_1m_series_generation_after_delete on fact.future_bar_1m", "create trigger future_bar_1m_series_generation_after_delete after delete on fact.future_bar_1m referencing old table as deleted_rows for each statement execute function audit.maintain_future_bar_1m_series_generation_after_delete()",
    "drop trigger if exists future_bar_1m_after_truncate on fact.future_bar_1m", "create trigger future_bar_1m_after_truncate after truncate on fact.future_bar_1m for each statement execute function fact.maintain_future_bar_1m_after_truncate()",
    "drop trigger if exists future_pyramid_disposition_sealed on audit.future_bar_1m_import_disposition", "create trigger future_pyramid_disposition_sealed before insert on audit.future_bar_1m_import_disposition for each row execute function audit.prevent_future_partial_child_append()",
    "drop trigger if exists future_pyramid_admission_sealed on audit.future_bar_1m_import_admission", "create trigger future_pyramid_admission_sealed before insert on audit.future_bar_1m_import_admission for each row execute function audit.prevent_future_partial_child_append()",
    "drop trigger if exists future_partial_boundary_sealed on audit.future_bar_1m_partial_source_boundary", "create trigger future_partial_boundary_sealed before insert on audit.future_bar_1m_partial_source_boundary for each row execute function audit.prevent_future_partial_child_append()",
    "drop trigger if exists future_partial_interval_sealed on audit.future_bar_1m_partial_revision_interval", "create trigger future_partial_interval_sealed before insert on audit.future_bar_1m_partial_revision_interval for each row execute function audit.prevent_future_partial_child_append()",
)


# These grants are applied before the SECURITY DEFINER functions are handed to
# their NOLOGIN owner and before their triggers become live.  The owner can
# maintain derived coverage/generation state, but can never mutate raw facts.
OWNER_RUNTIME_GRANTS = (
    "grant usage on schema fact,audit to quotemux_futures_owner",
    "revoke insert,update,delete,truncate on fact.future_bar_1m from quotemux_futures_owner",
    "grant select on fact.future_bar_1m to quotemux_futures_owner",
    "grant select,insert,update,delete on fact.future_bar_1m_coverage to quotemux_futures_owner",
    "grant select,insert on audit.future_bar_1m_series_generation to quotemux_futures_owner",
    "grant select on audit.future_bar_1m_import_publication,audit.future_bar_1m_partial_publication,audit.future_bar_1m_partial_revision to quotemux_futures_owner",
)


def apply_futures_partial_migration(connection_factory: Callable[[], Any] = _acquire_connection) -> None:
    """Apply the locked privileged migration; it is safe to repeat."""
    connection = connection_factory(); owns = connection_factory is _acquire_connection
    try:
        with connection.cursor(row_factory=tuple_row) as cursor:
            cursor.execute("set local lock_timeout='3s'"); cursor.execute("select pg_advisory_xact_lock(hashtext('quotemux_futures_partial_migration_v2'))")
            cursor.execute("do $$ begin if not exists(select 1 from pg_roles where rolname='quotemux_futures_owner') then create role quotemux_futures_owner nologin; end if; end $$")
            for statement in METADATA_DDL: cursor.execute(statement)
            for statement in OWNER_RUNTIME_GRANTS: cursor.execute(statement)
            for statement in HARDENED_FUNCTION_DDL: cursor.execute(statement)
            for function in _FUNCTIONS:
                cursor.execute(f"alter function {function} owner to quotemux_futures_owner"); cursor.execute(f"revoke all on function {function} from public")
            for statement in TRIGGER_DDL: cursor.execute(statement)
        connection.commit()
    except Exception: connection.rollback(); raise
    finally:
        if owns: _release_connection(connection)


def provision_futures_partial_roles(publisher_password: str, reader_password: str, connection_factory: Callable[[], Any] = _acquire_connection) -> None:
    """Create separate least-privilege publisher and public-reader logins."""
    if not publisher_password or not reader_password: raise ValueError("publisher and reader secrets are required")
    apply_futures_partial_migration(connection_factory); connection=connection_factory(); owns=connection_factory is _acquire_connection
    try:
        # The pooled application connection defaults to ``dict_row``.  This
        # privileged migration deliberately uses positional rows, so force a
        # tuple cursor instead of depending on the caller's connection setup.
        with connection.cursor(row_factory=tuple_row) as cursor:
            cursor.execute("do $$ begin if not exists(select 1 from pg_roles where rolname='quotemux_futures_partial_publisher') then create role quotemux_futures_partial_publisher login; end if; if not exists(select 1 from pg_roles where rolname='quotemux_public_reader') then create role quotemux_public_reader login; end if; end $$")
            cursor.execute(sql.SQL("alter role quotemux_futures_partial_publisher password {}").format(sql.Literal(publisher_password))); cursor.execute(sql.SQL("alter role quotemux_public_reader password {}").format(sql.Literal(reader_password)))
            cursor.execute("select current_database()"); database_name = str(cursor.fetchone()[0])
            cursor.execute(sql.SQL("grant connect,temp on database {} to quotemux_futures_partial_publisher").format(sql.Identifier(database_name)))
            cursor.execute("revoke all privileges on fact.future_bar_1m,ref.future_series,audit.future_bar_1m_series_generation,audit.future_bar_1m_import_publication,audit.future_bar_1m_import_disposition,audit.future_bar_1m_import_admission,audit.future_bar_1m_partial_publication,audit.future_bar_1m_partial_source_boundary,audit.future_bar_1m_partial_revision,audit.future_bar_1m_partial_revision_interval from quotemux_futures_partial_publisher")
            cursor.execute("grant usage on schema fact,audit,ref to quotemux_futures_partial_publisher; grant select on fact.future_bar_1m,ref.future_series,audit.future_bar_1m_series_generation,audit.future_bar_1m_import_publication,audit.future_bar_1m_import_disposition,audit.future_bar_1m_import_admission,audit.future_bar_1m_partial_publication,audit.future_bar_1m_partial_source_boundary,audit.future_bar_1m_partial_revision,audit.future_bar_1m_partial_revision_interval to quotemux_futures_partial_publisher; grant insert on fact.future_bar_1m,audit.future_bar_1m_import_publication,audit.future_bar_1m_import_disposition,audit.future_bar_1m_import_admission,audit.future_bar_1m_partial_publication,audit.future_bar_1m_partial_source_boundary,audit.future_bar_1m_partial_revision,audit.future_bar_1m_partial_revision_interval to quotemux_futures_partial_publisher; revoke update,delete,truncate on fact.future_bar_1m from quotemux_futures_partial_publisher")
            cursor.execute(sql.SQL("grant connect on database {} to quotemux_public_reader").format(sql.Identifier(database_name)))
            cursor.execute("revoke all privileges on fact.stock_daily_1d,fact.stock_bar_1m,fact.future_bar_1m,fact.future_bar_1m_coverage,readmodel.stock_bar_1m_daily_coverage,audit.future_bar_1m_series_generation,audit.future_bar_1m_import_disposition,audit.future_bar_1m_import_admission,audit.future_bar_1m_partial_publication,audit.future_bar_1m_partial_source_boundary,audit.future_bar_1m_partial_revision,audit.future_bar_1m_partial_revision_interval,ref.future_series from quotemux_public_reader")
            cursor.execute("grant usage on schema fact,readmodel,audit,ref to quotemux_public_reader; grant select on fact.stock_daily_1d,fact.stock_bar_1m,fact.future_bar_1m,fact.future_bar_1m_coverage,readmodel.stock_bar_1m_daily_coverage,audit.future_bar_1m_series_generation,audit.future_bar_1m_import_disposition,audit.future_bar_1m_import_admission,audit.future_bar_1m_partial_publication,audit.future_bar_1m_partial_source_boundary,audit.future_bar_1m_partial_revision,audit.future_bar_1m_partial_revision_interval,ref.future_series to quotemux_public_reader")
            cursor.execute("""do $$ declare relation text; begin foreach relation in array array['ref.trade_calendar','readmodel.dataset_build_state','readmodel.future_1m_completeness_active_revision','readmodel.future_1m_completeness_revision','readmodel.future_1m_completeness_revision_interval','readmodel.future_1m_completeness_interval'] loop if to_regclass(relation) is not null then execute format('grant select on %s to quotemux_public_reader',relation); end if; end loop; end $$""")
            for statement in OWNER_RUNTIME_GRANTS: cursor.execute(statement)
        connection.commit()
    except Exception: connection.rollback(); raise
    finally:
        if owns: _release_connection(connection)
