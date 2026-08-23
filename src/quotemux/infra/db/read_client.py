from __future__ import annotations

from collections.abc import Callable, Generator
from dataclasses import dataclass
import os
from queue import Empty, Queue
import sys
import threading
import time
from typing import Any
from uuid import uuid4

import psycopg
from psycopg.rows import tuple_row

from quotemux.infra.db.config import DB_CONNECT_TIMEOUT, DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER


StageCallback = Callable[[str, float], None]


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


READ_POOL_SIZE = max(1, _int_env("MHK_DB_READ_POOL_SIZE", 8))
_POOL: Queue[psycopg.Connection] = Queue(maxsize=READ_POOL_SIZE)
_POOL_LOCK = threading.Lock()
_POOL_CREATED = 0
_POOL_ACTIVE = 0
_POOL_REUSED = 0
_POOL_DROPPED = 0


@dataclass(frozen=True)
class QueryBatch:
    """A DB-native batch that can feed row or column-oriented encoders."""

    columns: tuple[str, ...]
    rows: tuple[tuple[object, ...], ...]

    def as_columns(self) -> dict[str, tuple[object, ...]]:
        return {
            column: tuple(row[index] for row in self.rows)
            for index, column in enumerate(self.columns)
        }

    def as_dicts(self) -> tuple[dict[str, object], ...]:
        return tuple(dict(zip(self.columns, row, strict=True)) for row in self.rows)

    def __len__(self) -> int:
        return len(self.rows)


def _emit(callback: StageCallback | None, stage: str, started_at: float) -> None:
    if callback is not None:
        try:
            callback(stage, max(0.0, time.monotonic() - started_at))
        except Exception:
            # Observability must never make a database read fail or leak its lease.
            pass


def _connect() -> psycopg.Connection:
    return psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        connect_timeout=DB_CONNECT_TIMEOUT,
        row_factory=tuple_row,
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
                if _POOL_CREATED < READ_POOL_SIZE:
                    _POOL_CREATED += 1
                    _POOL_ACTIVE += 1
                    should_create = True
                else:
                    should_create = False
            if should_create:
                try:
                    return _connect()
                except BaseException:
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
    with _POOL_LOCK:
        _POOL_ACTIVE -= 1
        _POOL_CREATED -= 1
        _POOL_DROPPED += 1


def _column_names(cursor: Any) -> tuple[str, ...]:
    return tuple(str(column.name) for column in (cursor.description or ()))


class ReadOnlySnapshot:
    """One consistent, rollback-only PostgreSQL snapshot."""

    def __init__(self, stage_callback: StageCallback | None = None) -> None:
        self._stage_callback = stage_callback
        started_at = time.monotonic()
        self._connection = _acquire_connection()
        _emit(stage_callback, "pool_wait", started_at)
        self._closed = False
        self._cancelled = False
        try:
            cursor = self._connection.cursor(row_factory=tuple_row)
            try:
                cursor.execute("begin isolation level repeatable read read only")
            finally:
                cursor.close()
        except BaseException:
            self.close(cancel=True)
            raise

    def __enter__(self) -> ReadOnlySnapshot:
        return self

    def __exit__(self, exc_type, exc, traceback) -> bool:
        del exc, traceback
        self.close(cancel=exc_type is not None)
        return False

    def _cancel(self) -> None:
        if self._cancelled:
            return
        cancel = getattr(self._connection, "cancel", None)
        if callable(cancel):
            cancel()
        self._cancelled = True

    def query_batch(
        self,
        query: str,
        params: tuple[object, ...] = (),
        *,
        stage: str = "sql",
    ) -> QueryBatch:
        del stage
        if self._closed:
            raise RuntimeError("read-only snapshot is closed")
        cursor = self._connection.cursor(row_factory=tuple_row)
        try:
            started_at = time.monotonic()
            cursor.execute(query, params)
            _emit(self._stage_callback, "sql_execute", started_at)
            columns = _column_names(cursor)
            started_at = time.monotonic()
            rows = tuple(tuple(row) for row in cursor.fetchall())
            _emit(self._stage_callback, "sql_fetch", started_at)
            return QueryBatch(columns, rows)
        finally:
            cursor.close()

    def stream_batches(
        self,
        query: str,
        params: tuple[object, ...] = (),
        *,
        batch_size: int = 1_000,
        stage: str = "sql",
    ) -> Generator[QueryBatch, None, None]:
        del stage
        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
            raise ValueError("batch_size must be a positive integer")
        if self._closed:
            raise RuntimeError("read-only snapshot is closed")
        cursor = self._connection.cursor(name=f"quotemux_read_{uuid4().hex}", row_factory=tuple_row)
        exhausted = False
        try:
            started_at = time.monotonic()
            cursor.execute(query, params)
            _emit(self._stage_callback, "sql_execute", started_at)
            columns = _column_names(cursor)
            while True:
                started_at = time.monotonic()
                rows = cursor.fetchmany(batch_size)
                _emit(self._stage_callback, "sql_fetch", started_at)
                if not rows:
                    exhausted = True
                    return
                yield QueryBatch(columns, tuple(tuple(row) for row in rows))
        finally:
            cleanup_error: BaseException | None = None
            if not exhausted:
                try:
                    self._cancel()
                except BaseException as exc:
                    cleanup_error = exc
            try:
                cursor.close()
            except BaseException as exc:
                cleanup_error = cleanup_error or exc
            if cleanup_error is not None and sys.exc_info()[0] is None:
                raise cleanup_error

    def close(self, *, cancel: bool = False) -> None:
        if self._closed:
            return
        cleanup_error: BaseException | None = None
        if cancel:
            try:
                self._cancel()
            except BaseException as exc:
                cleanup_error = exc
        try:
            self._connection.rollback()
        except BaseException as exc:
            cleanup_error = cleanup_error or exc
        self._closed = True
        if cleanup_error is None:
            _release_connection(self._connection)
        else:
            _drop_connection(self._connection)
            if sys.exc_info()[0] is None:
                raise cleanup_error


class ReadOnlyClient:
    def __init__(self, stage_callback: StageCallback | None = None) -> None:
        self._stage_callback = stage_callback

    def snapshot(self) -> ReadOnlySnapshot:
        return ReadOnlySnapshot(self._stage_callback)

    def query_batch(
        self,
        query: str,
        params: tuple[object, ...] = (),
        *,
        stage: str = "sql",
    ) -> QueryBatch:
        with self.snapshot() as snapshot:
            return snapshot.query_batch(query, params, stage=stage)

    def stream_batches(
        self,
        query: str,
        params: tuple[object, ...] = (),
        *,
        batch_size: int = 1_000,
        stage: str = "sql",
    ) -> Generator[QueryBatch, None, None]:
        with self.snapshot() as snapshot:
            yield from snapshot.stream_batches(query, params, batch_size=batch_size, stage=stage)


def get_read_pool_metrics() -> dict[str, int]:
    with _POOL_LOCK:
        return {
            "pool_size": READ_POOL_SIZE,
            "created": _POOL_CREATED,
            "active": _POOL_ACTIVE,
            "idle": _POOL.qsize(),
            "reused": _POOL_REUSED,
            "dropped": _POOL_DROPPED,
        }


def close_read_pool() -> None:
    global _POOL_CREATED
    while True:
        try:
            connection = _POOL.get_nowait()
        except Empty:
            break
        connection.close()
    with _POOL_LOCK:
        _POOL_CREATED = 0
