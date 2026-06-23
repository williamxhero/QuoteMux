from __future__ import annotations

import os
from queue import Empty, Queue
import threading

import pandas as pd
import psycopg
from psycopg.rows import dict_row

from quotemux.infra.db.availability_gate import DbAvailabilityGate
from quotemux.infra.db.config import DB_CONNECT_TIMEOUT, DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER
from quotemux.infra.provider_runtime.core import call_provider_api


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


