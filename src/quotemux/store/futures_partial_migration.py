"""Privileged, idempotent DDL for the metadata-only futures partial contract."""
from __future__ import annotations
from collections.abc import Callable
from typing import Any
from psycopg import sql
from quotemux.infra.db.client import _acquire_connection, _release_connection

DDL = (
"""create table if not exists audit.future_bar_1m_import_publication (qmi_id text primary key, source_normalized_rowset_sha256 text not null, fact_transform_version text not null, canonical_fact_rowset_sha256 text not null, payload_sha256 text not null, manifest_json jsonb not null, inserted_count bigint not null, equivalent_count bigint not null, conflict_count bigint not null, published_at timestamptz not null default now(), unique(source_normalized_rowset_sha256,fact_transform_version,canonical_fact_rowset_sha256))""",
"""create table if not exists audit.future_bar_1m_import_admission (qmi_id text not null references audit.future_bar_1m_import_publication(qmi_id), product_code text not null, exchange text not null, series_type text not null, bar_time timestamp not null, candidate_sha256 text not null, disposition text not null check(disposition in ('inserted','already_present_equivalent')), primary key(qmi_id,product_code,exchange,series_type,bar_time,candidate_sha256))""",
"""create table if not exists audit.future_bar_1m_partial_publication (qmp_id text primary key, dataset_id text not null, payload_json jsonb not null, payload_sha256 text not null, published_at timestamptz not null default now())""",
"""create table if not exists audit.future_bar_1m_partial_source_boundary (qmp_id text not null, boundary_id text not null, product_code text not null, exchange text not null, series_type text not null, source_key text not null, start_time timestamp not null, end_time timestamp not null, evidence_json jsonb not null, primary key(qmp_id,boundary_id))""",
"""create table if not exists audit.future_bar_1m_partial_revision (qmc_id text primary key, qmp_id text not null, payload_json jsonb not null, payload_sha256 text not null, published_at timestamptz not null default now())""",
"""create table if not exists audit.future_bar_1m_partial_revision_interval (qmc_id text not null, interval_id text not null, product_code text not null, start_time timestamp not null, end_time timestamp not null, status text not null, observed_count bigint not null, residual_json jsonb not null default '{}'::jsonb, primary key(qmc_id,interval_id))""",
)

def apply_futures_partial_migration(connection_factory: Callable[[], Any] = _acquire_connection) -> None:
    connection = connection_factory()
    owns_connection = connection_factory is _acquire_connection
    try:
        with connection.cursor() as cursor:
            cursor.execute("set local lock_timeout = '3s'")
            for statement in DDL: cursor.execute(statement)
        connection.commit()
    except Exception:
        connection.rollback(); raise
    finally:
        if owns_connection:
            _release_connection(connection)


def provision_futures_partial_roles(
    publisher_password: str, reader_password: str, connection_factory: Callable[[], Any] = _acquire_connection,
) -> None:
    """Privileged one-time role setup; callers generate secrets and never log them.

    The importer has only INSERT/SELECT on its exact intake path.  The reader
    has only SELECT and is intentionally separate from the legacy API writer.
    """
    if not publisher_password or not reader_password:
        raise ValueError("publisher and reader secrets are required")
    # Roles reference the metadata tables, so DDL is deliberately applied
    # first. A caller-supplied privileged connection is never returned to the
    # QuoteMux read/write pool.
    apply_futures_partial_migration(connection_factory)
    connection = connection_factory()
    owns_connection = connection_factory is _acquire_connection
    try:
        with connection.cursor() as cursor:
            cursor.execute("do $$ begin if not exists (select 1 from pg_roles where rolname='quotemux_futures_owner') then create role quotemux_futures_owner nologin; end if; if not exists (select 1 from pg_roles where rolname='quotemux_futures_partial_publisher') then create role quotemux_futures_partial_publisher login; end if; if not exists (select 1 from pg_roles where rolname='quotemux_public_reader') then create role quotemux_public_reader login; end if; end $$")
            cursor.execute(sql.SQL("alter role quotemux_futures_partial_publisher password {}").format(sql.Literal(publisher_password)))
            cursor.execute(sql.SQL("alter role quotemux_public_reader password {}").format(sql.Literal(reader_password)))
            cursor.execute("revoke all on fact.future_bar_1m from public, quotemux_futures_partial_publisher, quotemux_public_reader")
            cursor.execute("grant usage on schema fact, audit, ref to quotemux_futures_partial_publisher, quotemux_public_reader")
            cursor.execute("grant select, insert on fact.future_bar_1m to quotemux_futures_partial_publisher")
            cursor.execute("grant select, insert on audit.future_bar_1m_import_publication, audit.future_bar_1m_import_admission, audit.future_bar_1m_partial_publication, audit.future_bar_1m_partial_source_boundary, audit.future_bar_1m_partial_revision, audit.future_bar_1m_partial_revision_interval to quotemux_futures_partial_publisher")
            cursor.execute("grant select on audit.future_bar_1m_series_generation to quotemux_futures_partial_publisher")
            cursor.execute("grant select on fact.future_bar_1m, fact.future_bar_1m_coverage, audit.future_bar_1m_series_generation, audit.future_bar_1m_partial_publication, audit.future_bar_1m_partial_source_boundary, audit.future_bar_1m_partial_revision, audit.future_bar_1m_partial_revision_interval, ref.future_series to quotemux_public_reader")
            # Trigger functions execute as the dedicated non-login owner; no
            # caller gets direct EXECUTE or destructive fact privileges.
            for function in ("fact.refresh_future_bar_1m_coverage_group(text,text,text)", "fact.maintain_future_bar_1m_coverage_after_insert()", "fact.maintain_future_bar_1m_coverage_after_delete()", "fact.maintain_future_bar_1m_coverage_after_update()", "audit.record_future_bar_1m_series_generation(text,text,text)", "audit.maintain_future_bar_1m_series_generation_after_insert()", "audit.maintain_future_bar_1m_series_generation_after_update()", "audit.maintain_future_bar_1m_series_generation_after_delete()"):
                cursor.execute(f"alter function {function} owner to quotemux_futures_owner")
                cursor.execute(f"revoke all on function {function} from public")
            cursor.execute("grant usage on schema fact, audit to quotemux_futures_owner")
            cursor.execute("grant select, insert, update, delete on fact.future_bar_1m, fact.future_bar_1m_coverage, audit.future_bar_1m_series_generation to quotemux_futures_owner")
            cursor.execute("revoke truncate, update, delete on fact.future_bar_1m from quotemux_futures_partial_publisher, quotemux_public_reader")
        connection.commit()
    except Exception:
        connection.rollback(); raise
    finally:
        if owns_connection:
            _release_connection(connection)
