from __future__ import annotations

from queue import Empty, Queue
import os
import threading

import pandas as pd
import psycopg
from psycopg.rows import dict_row

from quotemux.infra.db.config import DB_CONNECT_TIMEOUT, DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER
from quotemux.infra.provider_runtime.core import call_provider_api


def _int_env(name: str, default: int) -> int:
    text = os.getenv(name, "")
    try:
        return int(text)
    except ValueError:
        return default


CACHE_DB_HOST = os.getenv("QUOTEMUX_CACHE_DB_HOST", DB_HOST)
CACHE_DB_PORT = _int_env("QUOTEMUX_CACHE_DB_PORT", DB_PORT)
CACHE_DB_NAME = os.getenv("QUOTEMUX_CACHE_DB_NAME", os.getenv("MARKETHUB_DB_NAME", DB_NAME))
CACHE_DB_USER = os.getenv("QUOTEMUX_CACHE_DB_USER", DB_USER)
CACHE_DB_PASSWORD = os.getenv("QUOTEMUX_CACHE_DB_PASSWORD", DB_PASSWORD)
CACHE_DB_POOL_SIZE = _int_env("QUOTEMUX_CACHE_DB_POOL_SIZE", 8)

_POOL: Queue[psycopg.Connection] = Queue(maxsize=max(1, CACHE_DB_POOL_SIZE))
_POOL_LOCK = threading.Lock()
_POOL_CREATED = 0
_POOL_ACTIVE = 0
_POOL_REUSED = 0
_POOL_DROPPED = 0


def _connect() -> psycopg.Connection:
    return psycopg.connect(
        host=CACHE_DB_HOST,
        port=CACHE_DB_PORT,
        dbname=CACHE_DB_NAME,
        user=CACHE_DB_USER,
        password=CACHE_DB_PASSWORD,
        connect_timeout=DB_CONNECT_TIMEOUT,
        row_factory=dict_row,
    )


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
                if _POOL_CREATED < CACHE_DB_POOL_SIZE:
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
    try:
        return call_provider_api("quotemux_cache_db", "query_dataframe", _query_dataframe_once, query, params)
    except Exception as exc:
        print(f"quotemux cache db query failed: {exc}")
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
    try:
        return call_provider_api("quotemux_cache_db", "execute_sql", _execute_sql_once, query, params)
    except Exception as exc:
        print(f"quotemux cache db execute failed: {exc}")
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
    try:
        return call_provider_api("quotemux_cache_db", "execute_many", _execute_many_once, query, params_list)
    except Exception as exc:
        print(f"quotemux cache db batch execute failed: {exc}")
        return False


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
