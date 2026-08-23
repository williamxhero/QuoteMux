from __future__ import annotations

from collections.abc import Generator, Sequence
from dataclasses import dataclass
import os
from queue import Empty, Queue
import sys
import threading
from typing import Any
from uuid import uuid4

import pandas as pd
import psycopg
from psycopg.rows import dict_row

from quotemux.infra.db.availability_gate import DbAvailabilityGate
from quotemux.infra.db.config import DB_CONNECT_TIMEOUT, DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER
from quotemux.infra.provider_runtime.core import call_provider_api
from quotemux.strict_read import reject_in_strict_public_read


def _int_env(name: str, default: int) -> int:
    text = os.getenv(name, "")
    try:
        return int(text)
    except ValueError:
        return default


DB_POOL_SIZE = _int_env("MHK_DB_POOL_SIZE", 8)
DB_FAILURE_COOLDOWN_SECONDS = 60.0
_POOL: Queue[psycopg.Connection] = Queue(maxsize=max(1, DB_POOL_SIZE))
_POOL_LOCK = threading.Lock()
_POOL_CREATED = 0
_POOL_ACTIVE = 0
_POOL_REUSED = 0
_POOL_DROPPED = 0
_DB_AVAILABILITY = DbAvailabilityGate(DB_FAILURE_COOLDOWN_SECONDS)
_MIGRATION_JOURNAL_TABLES: dict[str, tuple[str, str]] = {
    "stock_bar_1m": ("audit.stock_bar_1m_ts_forward_delta", "audit.stock_bar_1m_ts_reverse_delta"),
    "stock_bar_30m": ("audit.stock_bar_30m_ts_forward_delta", "audit.stock_bar_30m_ts_reverse_delta"),
    "future_bar_1m": ("audit.future_bar_1m_ts_forward_delta", "audit.future_bar_1m_ts_reverse_delta"),
}


@dataclass(frozen=True)
class MigrationRangeJournalState:
    fact_table: str
    forward_active: bool
    reverse_active: bool

    @property
    def has_active_journal(self) -> bool:
        return self.forward_active or self.reverse_active


def _db_available_for_attempt() -> bool:
    return _DB_AVAILABILITY.probe_port(DB_HOST, DB_PORT)


def _mark_db_unavailable() -> None:
    _DB_AVAILABILITY.mark_unavailable()


def is_db_available() -> bool:
    return _db_available_for_attempt()


def _connect() -> psycopg.Connection:
    connection = psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        connect_timeout=DB_CONNECT_TIMEOUT,
        row_factory=dict_row,
    )
    _DB_AVAILABILITY.mark_available()
    return connection


def _acquire_connection() -> psycopg.Connection:
    global _POOL_ACTIVE, _POOL_CREATED, _POOL_DROPPED, _POOL_REUSED
    while True:
        try:
            connection = _POOL.get_nowait()
            with _POOL_LOCK:
                _POOL_REUSED += 1
            if not connection.closed:
                with _POOL_LOCK:
                    _POOL_ACTIVE += 1
                return connection
            with _POOL_LOCK:
                _POOL_CREATED -= 1
                _POOL_DROPPED += 1
        except Empty:
            with _POOL_LOCK:
                if _POOL_CREATED < DB_POOL_SIZE:
                    _POOL_CREATED += 1
                    _POOL_ACTIVE += 1
                    should_create = True
                else:
                    should_create = False
            if should_create:
                try:
                    return _connect()
                except Exception:
                    with _POOL_LOCK:
                        _POOL_CREATED -= 1
                        _POOL_ACTIVE -= 1
                    raise
            connection = _POOL.get()
            with _POOL_LOCK:
                _POOL_REUSED += 1
            if not connection.closed:
                with _POOL_LOCK:
                    _POOL_ACTIVE += 1
                return connection
            with _POOL_LOCK:
                _POOL_CREATED -= 1
                _POOL_DROPPED += 1


def _release_connection(connection: psycopg.Connection) -> None:
    global _POOL_ACTIVE, _POOL_CREATED, _POOL_DROPPED
    with _POOL_LOCK:
        _POOL_ACTIVE -= 1
    if connection.closed:
        with _POOL_LOCK:
            _POOL_CREATED -= 1
            _POOL_DROPPED += 1
        return
    try:
        _POOL.put_nowait(connection)
    except Exception:
        connection.close()
        with _POOL_LOCK:
            _POOL_CREATED -= 1
            _POOL_DROPPED += 1


def _drop_connection(connection: psycopg.Connection) -> None:
    global _POOL_ACTIVE, _POOL_CREATED, _POOL_DROPPED
    try:
        connection.close()
    except Exception:
        pass
    finally:
        with _POOL_LOCK:
            _POOL_ACTIVE -= 1
            _POOL_CREATED -= 1
            _POOL_DROPPED += 1


def _cleanup_stream(
    connection: psycopg.Connection,
    cursor: Any,
    *,
    cancel: bool,
) -> Exception | None:
    cleanup_error: Exception | None = None

    if cancel:
        cancel_connection = getattr(connection, "cancel", None)
        if callable(cancel_connection):
            try:
                cancel_connection()
            except Exception as exc:
                cleanup_error = exc

    if cursor is not None:
        try:
            cursor.close()
        except Exception as exc:
            cleanup_error = cleanup_error or exc

    try:
        connection.rollback()
    except Exception as exc:
        cleanup_error = cleanup_error or exc

    if cleanup_error is None:
        _release_connection(connection)
    else:
        _drop_connection(connection)
    return cleanup_error


def _stream_query_batches(
    query: str,
    params: tuple[object, ...],
    batch_size: int,
) -> Generator[list[dict[str, object]], None, None]:
    connection = _acquire_connection()
    cursor = None
    exhausted = False
    try:
        cursor = connection.cursor(name=f"quotemux_stream_{uuid4().hex}")
        cursor.execute(query, params)
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                exhausted = True
                return
            yield rows
    finally:
        cleanup_error = _cleanup_stream(connection, cursor, cancel=not exhausted)
        if cleanup_error is not None and sys.exception() is None:
            raise cleanup_error


def stream_query_batches(
    query: str,
    params: tuple[object, ...] = (),
    *,
    batch_size: int = 1_000,
) -> Generator[list[dict[str, object]], None, None]:
    """Stream a read-only query in bounded batches from a server-side cursor.

    The caller should exhaust or explicitly close the returned iterator. Closing
    it early cancels the query before the transaction is rolled back and the
    pooled connection is returned.
    """
    if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")
    return _stream_query_batches(query, params, batch_size)


def _query_dataframe_once(query: str, params: tuple[object, ...]) -> pd.DataFrame:
    connection = _acquire_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
        connection.rollback()
    except Exception:
        connection.rollback()
        raise
    finally:
        _release_connection(connection)
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame.from_records(rows)


def query_dataframe(query: str, params: tuple[object, ...] = ()) -> pd.DataFrame:
    if not _db_available_for_attempt():
        return pd.DataFrame()
    try:
        return call_provider_api("store_db", "query_dataframe", _query_dataframe_once, query, params)
    except Exception as exc:
        _mark_db_unavailable()
        print(f"store db query failed: {exc}")
        return pd.DataFrame()


def _execute_sql_once(query: str, params: tuple[object, ...]) -> bool:
    connection = _acquire_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
        connection.commit()
        return True
    except Exception:
        connection.rollback()
        raise
    finally:
        _release_connection(connection)


def execute_sql(query: str, params: tuple[object, ...] = ()) -> bool:
    reject_in_strict_public_read("sql_write:execute_sql")
    if not _db_available_for_attempt():
        return False
    try:
        return call_provider_api("store_db", "execute_sql", _execute_sql_once, query, params)
    except Exception as exc:
        _mark_db_unavailable()
        print(f"store db execute failed: {exc}")
        return False


def _execute_many_once(query: str, params_list: list[tuple[object, ...]]) -> bool:
    connection = _acquire_connection()
    try:
        with connection.cursor() as cursor:
            cursor.executemany(query, params_list)
        connection.commit()
        return True
    except Exception:
        connection.rollback()
        raise
    finally:
        _release_connection(connection)


def execute_many(query: str, params_list: list[tuple[object, ...]]) -> bool:
    reject_in_strict_public_read("sql_write:execute_many")
    if not params_list:
        return True
    if not _db_available_for_attempt():
        return False
    try:
        return call_provider_api("store_db", "execute_many", _execute_many_once, query, params_list)
    except Exception as exc:
        _mark_db_unavailable()
        print(f"store db batch execute failed: {exc}")
        return False


def _migration_journal_tables(fact_table: str) -> tuple[str, str]:
    try:
        return _MIGRATION_JOURNAL_TABLES[fact_table]
    except KeyError as exc:
        raise ValueError(f"unsupported migration journal fact table: {fact_table}") from exc


def discover_migration_range_journals(
    cursor: Any,
    fact_table: str,
) -> MigrationRangeJournalState:
    forward_journal, reverse_journal = _migration_journal_tables(fact_table)
    cursor.execute(
        """
        with journals as (
            select to_regclass(%s) as forward_journal,
                   to_regclass(%s) as reverse_journal
        )
        select
            forward_journal is not null and (
                select count(*) = 2
                from pg_catalog.pg_attribute
                where attrelid = forward_journal
                  and attname in ('range_start', 'range_end')
                  and attnum > 0
                  and not attisdropped
            ) as forward_active,
            reverse_journal is not null and (
                select count(*) = 2
                from pg_catalog.pg_attribute
                where attrelid = reverse_journal
                  and attname in ('range_start', 'range_end')
                  and attnum > 0
                  and not attisdropped
            ) as reverse_active
        from journals
        """,
        (forward_journal, reverse_journal),
    )
    row = cursor.fetchone()
    return MigrationRangeJournalState(
        fact_table=fact_table,
        forward_active=bool(row and row["forward_active"]),
        reverse_active=bool(row and row["reverse_active"]),
    )


def append_migration_range_journals(
    cursor: Any,
    state: MigrationRangeJournalState,
    bar_times: Sequence[object],
) -> None:
    """Append one transaction range to the journals found by transaction preflight."""
    forward_journal, reverse_journal = _migration_journal_tables(state.fact_table)
    if not state.has_active_journal or not bar_times:
        return

    normalized_bar_times = [str(bar_time) for bar_time in bar_times]
    range_params = (min(normalized_bar_times), max(normalized_bar_times))
    if state.forward_active:
        cursor.execute(
            f"insert into {forward_journal} (range_start, range_end) values (%s::timestamp, %s::timestamp)",
            range_params,
        )
    if state.reverse_active:
        cursor.execute(
            f"insert into {reverse_journal} (range_start, range_end) values (%s::timestamp, %s::timestamp)",
            range_params,
        )


def enable_explicit_range_journaling(cursor: Any) -> None:
    cursor.execute("set local markethub.explicit_range_journal = 'on'")


def _execute_many_with_migration_journal_once(
    query: str,
    params_list: list[tuple[object, ...]],
    fact_table: str,
    bar_time_index: int,
) -> bool:
    connection = _acquire_connection()
    try:
        with connection.cursor() as cursor:
            journal_state = discover_migration_range_journals(cursor, fact_table)
            if journal_state.has_active_journal:
                enable_explicit_range_journaling(cursor)
            cursor.executemany(query, params_list)
            append_migration_range_journals(
                cursor,
                journal_state,
                [params[bar_time_index] for params in params_list],
            )
        connection.commit()
        return True
    except Exception:
        connection.rollback()
        raise
    finally:
        _release_connection(connection)


def execute_many_with_migration_journal(
    query: str,
    params_list: list[tuple[object, ...]],
    *,
    fact_table: str,
    bar_time_index: int,
) -> bool:
    reject_in_strict_public_read("sql_write:execute_many_with_migration_journal")
    _migration_journal_tables(fact_table)
    if bar_time_index < 0:
        raise ValueError("bar_time_index must be non-negative")
    if not params_list:
        return True
    if any(bar_time_index >= len(params) for params in params_list):
        raise ValueError("bar_time_index is outside the batch parameter rows")
    if not _db_available_for_attempt():
        return False
    try:
        return call_provider_api(
            "store_db",
            "execute_many_with_migration_journal",
            _execute_many_with_migration_journal_once,
            query,
            params_list,
            fact_table,
            bar_time_index,
        )
    except Exception as exc:
        _mark_db_unavailable()
        print(f"store db journaled batch execute failed: {exc}")
        return False


def get_pool_metrics() -> dict[str, int]:
    with _POOL_LOCK:
        return {
            "pool_size": DB_POOL_SIZE,
            "created": _POOL_CREATED,
            "active": _POOL_ACTIVE,
            "idle": _POOL.qsize(),
            "reused": _POOL_REUSED,
            "dropped": _POOL_DROPPED,
        }


def close_pool() -> None:
    global _POOL_CREATED
    while True:
        try:
            connection = _POOL.get_nowait()
        except Empty:
            break
        connection.close()
    with _POOL_LOCK:
        _POOL_CREATED = 0


