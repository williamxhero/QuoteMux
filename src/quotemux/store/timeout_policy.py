from __future__ import annotations

from datetime import datetime

import pandas as pd

from quotemux.capabilities import is_independently_configurable_capability_id, list_capability_definitions
from quotemux.provider_timeout.policy import CapabilityTimeoutMetric, CapabilityTimeoutPolicy, ProviderTimeoutMetric, ProviderTimeoutPolicy, TIMEOUT_STATUS_SUCCESS, default_capability_timeout_policy, default_provider_timeout_policy
from quotemux.store.cache_db import execute_many, execute_sql, query_dataframe


SCHEMA_SQL = (
    """
    create table if not exists capability_timeout_policy (
        capability_id text primary key,
        default_timeout_seconds double precision not null,
        min_timeout_seconds double precision not null,
        max_timeout_seconds double precision not null,
        sample_window_size integer not null,
        min_sample_count integer not null,
        created_at timestamp without time zone not null default now(),
        updated_at timestamp without time zone not null default now()
    )
    """,
    """
    create table if not exists provider_timeout_policy (
        capability_id text not null,
        provider text not null,
        default_timeout_seconds double precision not null,
        min_timeout_seconds double precision not null,
        max_timeout_seconds double precision not null,
        sample_window_size integer not null,
        min_sample_count integer not null,
        created_at timestamp without time zone not null default now(),
        updated_at timestamp without time zone not null default now(),
        primary key (capability_id, provider)
    )
    """,
    """
    create table if not exists provider_timeout_metrics (
        id bigserial primary key,
        capability_id text not null,
        provider text not null,
        source_instance_id text not null default '',
        handler text not null default '',
        status text not null,
        elapsed_ms double precision not null,
        effective_timeout_seconds double precision not null,
        row_count integer not null default 0,
        error_text text not null default '',
        created_at timestamp without time zone not null default now()
    )
    """,
    """
    create table if not exists capability_timeout_metrics (
        id bigserial primary key,
        capability_id text not null,
        status text not null,
        elapsed_ms double precision not null,
        effective_timeout_seconds double precision not null,
        provider_request_count integer not null default 0,
        row_count integer not null default 0,
        error_count integer not null default 0,
        created_at timestamp without time zone not null default now()
    )
    """,
    "create index if not exists idx_provider_timeout_metrics_lookup on provider_timeout_metrics (capability_id, provider, status, created_at desc)",
    "create index if not exists idx_capability_timeout_metrics_lookup on capability_timeout_metrics (capability_id, status, created_at desc)",
)

_SCHEMA_READY = False
_SCHEMA_FAILED = False


class ProviderTimeoutStore:
    def get_capability_policy(self, capability_id: str) -> CapabilityTimeoutPolicy:
        if not _ensure_schema():
            return default_capability_timeout_policy(capability_id)
        frame = query_dataframe(
            """
            select capability_id, default_timeout_seconds, min_timeout_seconds,
                   max_timeout_seconds, sample_window_size, min_sample_count
            from capability_timeout_policy
            where capability_id = %s
            """,
            (capability_id,),
        )
        if _is_empty_dataframe(frame):
            return default_capability_timeout_policy(capability_id)
        return _capability_policy_from_row(frame.iloc[0].to_dict())

    def get_provider_policy(self, capability_id: str, provider: str) -> ProviderTimeoutPolicy:
        if not _ensure_schema():
            return default_provider_timeout_policy(capability_id, provider)
        frame = query_dataframe(
            """
            select capability_id, provider, default_timeout_seconds, min_timeout_seconds,
                   max_timeout_seconds, sample_window_size, min_sample_count
            from provider_timeout_policy
            where capability_id = %s and provider = %s
            """,
            (capability_id, provider),
        )
        if _is_empty_dataframe(frame):
            return default_provider_timeout_policy(capability_id, provider)
        return _provider_policy_from_row(frame.iloc[0].to_dict())

    def list_capability_policies(self) -> tuple[CapabilityTimeoutPolicy, ...]:
        if not _ensure_schema():
            return ()
        frame = query_dataframe(
            """
            select capability_id, default_timeout_seconds, min_timeout_seconds,
                   max_timeout_seconds, sample_window_size, min_sample_count
            from capability_timeout_policy
            order by capability_id asc
            """,
            (),
        )
        if _is_empty_dataframe(frame):
            return ()
        return tuple(_capability_policy_from_row(row) for row in frame.to_dict("records"))

    def list_provider_policies(self) -> tuple[ProviderTimeoutPolicy, ...]:
        if not _ensure_schema():
            return ()
        frame = query_dataframe(
            """
            select capability_id, provider, default_timeout_seconds, min_timeout_seconds,
                   max_timeout_seconds, sample_window_size, min_sample_count
            from provider_timeout_policy
            order by capability_id asc, provider asc
            """,
            (),
        )
        if _is_empty_dataframe(frame):
            return ()
        return tuple(_provider_policy_from_row(row) for row in frame.to_dict("records"))

    def update_capability_policy(self, policy: CapabilityTimeoutPolicy) -> bool:
        if not _ensure_schema():
            return False
        return execute_sql(
            """
            update capability_timeout_policy
            set default_timeout_seconds = %s,
                min_timeout_seconds = %s,
                max_timeout_seconds = %s,
                sample_window_size = %s,
                min_sample_count = %s,
                updated_at = now()
            where capability_id = %s
            """,
            (
                policy.default_timeout_seconds,
                policy.min_timeout_seconds,
                policy.max_timeout_seconds,
                policy.sample_window_size,
                policy.min_sample_count,
                policy.capability_id,
            ),
        )

    def update_provider_policy(self, policy: ProviderTimeoutPolicy) -> bool:
        if not _ensure_schema():
            return False
        return execute_sql(
            """
            update provider_timeout_policy
            set default_timeout_seconds = %s,
                min_timeout_seconds = %s,
                max_timeout_seconds = %s,
                sample_window_size = %s,
                min_sample_count = %s,
                updated_at = now()
            where capability_id = %s and provider = %s
            """,
            (
                policy.default_timeout_seconds,
                policy.min_timeout_seconds,
                policy.max_timeout_seconds,
                policy.sample_window_size,
                policy.min_sample_count,
                policy.capability_id,
                policy.provider,
            ),
        )

    def write_provider_metric(self, metric: ProviderTimeoutMetric) -> None:
        if not _ensure_schema():
            return
        execute_sql(
            """
            insert into provider_timeout_metrics (
                capability_id, provider, source_instance_id, handler, status,
                elapsed_ms, effective_timeout_seconds, row_count, error_text, created_at
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                metric.capability_id,
                metric.provider,
                metric.source_instance_id,
                metric.handler,
                metric.status,
                metric.elapsed_ms,
                metric.effective_timeout_seconds,
                metric.row_count,
                metric.error_text,
                metric.created_at,
            ),
        )

    def write_capability_metric(self, metric: CapabilityTimeoutMetric) -> None:
        if not _ensure_schema():
            return
        execute_sql(
            """
            insert into capability_timeout_metrics (
                capability_id, status, elapsed_ms, effective_timeout_seconds,
                provider_request_count, row_count, error_count, created_at
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                metric.capability_id,
                metric.status,
                metric.elapsed_ms,
                metric.effective_timeout_seconds,
                metric.provider_request_count,
                metric.row_count,
                metric.error_count,
                metric.created_at,
            ),
        )

    def list_provider_success_elapsed_ms(self, capability_id: str, provider: str, limit: int) -> tuple[float, ...]:
        if not _ensure_schema():
            return ()
        frame = query_dataframe(
            """
            select elapsed_ms
            from provider_timeout_metrics
            where capability_id = %s and provider = %s and status = %s
            order by created_at desc
            limit %s
            """,
            (capability_id, provider, TIMEOUT_STATUS_SUCCESS, max(1, limit)),
        )
        return _elapsed_values(frame)

    def list_capability_success_elapsed_ms(self, capability_id: str, limit: int) -> tuple[float, ...]:
        if not _ensure_schema():
            return ()
        frame = query_dataframe(
            """
            select elapsed_ms
            from capability_timeout_metrics
            where capability_id = %s and status = %s
            order by created_at desc
            limit %s
            """,
            (capability_id, TIMEOUT_STATUS_SUCCESS, max(1, limit)),
        )
        return _elapsed_values(frame)

    def list_provider_metrics(self, capability_id: str = "", provider: str = "", limit: int = 100) -> tuple[dict[str, object], ...]:
        if not _ensure_schema():
            return ()
        clauses: list[str] = []
        params: list[object] = []
        if capability_id != "":
            clauses.append("capability_id = %s")
            params.append(capability_id)
        if provider != "":
            clauses.append("provider = %s")
            params.append(provider)
        where_sql = " where " + " and ".join(clauses) if clauses else ""
        params.append(max(1, min(limit, 1000)))
        frame = query_dataframe(
            f"""
            select capability_id, provider, source_instance_id, handler, status,
                   elapsed_ms, effective_timeout_seconds, row_count, error_text, created_at
            from provider_timeout_metrics
            {where_sql}
            order by created_at desc
            limit %s
            """,
            tuple(params),
        )
        if _is_empty_dataframe(frame):
            return ()
        return tuple(_serialize_row(row) for row in frame.to_dict("records"))

    def list_capability_metrics(self, capability_id: str = "", limit: int = 100) -> tuple[dict[str, object], ...]:
        if not _ensure_schema():
            return ()
        clauses: list[str] = []
        params: list[object] = []
        if capability_id != "":
            clauses.append("capability_id = %s")
            params.append(capability_id)
        where_sql = " where " + " and ".join(clauses) if clauses else ""
        params.append(max(1, min(limit, 1000)))
        frame = query_dataframe(
            f"""
            select capability_id, status, elapsed_ms, effective_timeout_seconds,
                   provider_request_count, row_count, error_count, created_at
            from capability_timeout_metrics
            {where_sql}
            order by created_at desc
            limit %s
            """,
            tuple(params),
        )
        if _is_empty_dataframe(frame):
            return ()
        return tuple(_serialize_row(row) for row in frame.to_dict("records"))


def sync_default_timeout_policies() -> bool:
    if not _ensure_schema():
        return False
    return _insert_default_policies()


def get_timeout_store() -> ProviderTimeoutStore:
    return _STORE


def _ensure_schema() -> bool:
    global _SCHEMA_FAILED, _SCHEMA_READY
    if _SCHEMA_READY:
        return True
    if _SCHEMA_FAILED:
        return False
    for statement in SCHEMA_SQL:
        if not execute_sql(statement):
            _SCHEMA_FAILED = True
            return False
    ok = _insert_default_policies()
    _SCHEMA_READY = ok
    _SCHEMA_FAILED = not ok
    return ok


def _insert_default_policies() -> bool:
    capability_params: list[tuple[object, ...]] = []
    provider_params: list[tuple[object, ...]] = []
    for definition in list_capability_definitions():
        capability_id = definition.capability_id
        if not is_independently_configurable_capability_id(capability_id):
            continue
        capability_policy = default_capability_timeout_policy(capability_id)
        capability_params.append(_capability_policy_params(capability_policy))
        for provider in definition.allowed_packages:
            provider_policy = default_provider_timeout_policy(capability_id, provider)
            provider_params.append(_provider_policy_params(provider_policy))
    capability_ok = execute_many(
        """
        insert into capability_timeout_policy (
            capability_id, default_timeout_seconds, min_timeout_seconds,
            max_timeout_seconds, sample_window_size, min_sample_count
        )
        values (%s, %s, %s, %s, %s, %s)
        on conflict (capability_id) do nothing
        """,
        capability_params,
    )
    provider_ok = execute_many(
        """
        insert into provider_timeout_policy (
            capability_id, provider, default_timeout_seconds, min_timeout_seconds,
            max_timeout_seconds, sample_window_size, min_sample_count
        )
        values (%s, %s, %s, %s, %s, %s, %s)
        on conflict (capability_id, provider) do nothing
        """,
        provider_params,
    )
    return capability_ok and provider_ok


def _capability_policy_params(policy: CapabilityTimeoutPolicy) -> tuple[object, ...]:
    return (
        policy.capability_id,
        policy.default_timeout_seconds,
        policy.min_timeout_seconds,
        policy.max_timeout_seconds,
        policy.sample_window_size,
        policy.min_sample_count,
    )


def _provider_policy_params(policy: ProviderTimeoutPolicy) -> tuple[object, ...]:
    return (
        policy.capability_id,
        policy.provider,
        policy.default_timeout_seconds,
        policy.min_timeout_seconds,
        policy.max_timeout_seconds,
        policy.sample_window_size,
        policy.min_sample_count,
    )


def _capability_policy_from_row(row: dict[str, object]) -> CapabilityTimeoutPolicy:
    return CapabilityTimeoutPolicy(
        capability_id=str(row["capability_id"]),
        default_timeout_seconds=float(row["default_timeout_seconds"]),
        min_timeout_seconds=float(row["min_timeout_seconds"]),
        max_timeout_seconds=float(row["max_timeout_seconds"]),
        sample_window_size=int(row["sample_window_size"]),
        min_sample_count=int(row["min_sample_count"]),
    )


def _provider_policy_from_row(row: dict[str, object]) -> ProviderTimeoutPolicy:
    return ProviderTimeoutPolicy(
        capability_id=str(row["capability_id"]),
        provider=str(row["provider"]),
        default_timeout_seconds=float(row["default_timeout_seconds"]),
        min_timeout_seconds=float(row["min_timeout_seconds"]),
        max_timeout_seconds=float(row["max_timeout_seconds"]),
        sample_window_size=int(row["sample_window_size"]),
        min_sample_count=int(row["min_sample_count"]),
    )


def _is_empty_dataframe(frame: pd.DataFrame) -> bool:
    return frame.empty


def _elapsed_values(frame: pd.DataFrame) -> tuple[float, ...]:
    if _is_empty_dataframe(frame):
        return ()
    return tuple(float(row["elapsed_ms"]) for row in frame.to_dict("records"))


def _serialize_row(row: dict[str, object]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in row.items():
        if isinstance(value, datetime):
            result[key] = value.strftime("%Y-%m-%d %H:%M:%S")
        else:
            result[key] = value
    return result


_STORE = ProviderTimeoutStore()
