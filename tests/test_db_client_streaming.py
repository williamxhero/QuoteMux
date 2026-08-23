from __future__ import annotations

from collections.abc import Callable

import pytest

from quotemux.infra.db import client


class ConsumerError(RuntimeError):
    pass


def failing_cleanup(message: str) -> Callable[[], None]:
    def fail() -> None:
        raise RuntimeError(message)

    return fail


class FakeCursor:
    def __init__(self, results: list[list[dict[str, object]] | BaseException]) -> None:
        self._results = iter(results)
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetch_sizes: list[int] = []
        self.close_calls = 0

    def execute(self, query: str, params: tuple[object, ...]) -> None:
        self.execute_calls.append((query, params))

    def fetchmany(self, size: int) -> list[dict[str, object]]:
        self.fetch_sizes.append(size)
        result = next(self._results)
        if isinstance(result, BaseException):
            raise result
        return result

    def close(self) -> None:
        self.close_calls += 1


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self._cursor = cursor
        self.closed = False
        self.cursor_names: list[str] = []
        self.cancel_calls = 0
        self.rollback_calls = 0
        self.close_calls = 0

    def cursor(self, name: str = "") -> FakeCursor:
        self.cursor_names.append(name)
        return self._cursor

    def cancel(self) -> None:
        self.cancel_calls += 1

    def rollback(self) -> None:
        self.rollback_calls += 1

    def close(self) -> None:
        self.close_calls += 1
        self.closed = True


class FakeConnectionWithoutCancel:
    def __init__(self, cursor: FakeCursor) -> None:
        self._cursor = cursor
        self.closed = False
        self.rollback_calls = 0

    def cursor(self, name: str = "") -> FakeCursor:
        return self._cursor

    def rollback(self) -> None:
        self.rollback_calls += 1

    def close(self) -> None:
        self.closed = True


def install_fake_pool(
    monkeypatch: pytest.MonkeyPatch,
    connection: FakeConnection | FakeConnectionWithoutCancel,
) -> tuple[list[object], list[object]]:
    acquired: list[object] = []
    released: list[object] = []

    def acquire() -> object:
        acquired.append(connection)
        return connection

    monkeypatch.setattr(client, "_acquire_connection", acquire)
    monkeypatch.setattr(client, "_release_connection", released.append)
    return acquired, released


def test_stream_query_batches_uses_named_cursor_and_bounded_fetchmany(monkeypatch: pytest.MonkeyPatch) -> None:
    cursor = FakeCursor(
        [
            [{"code": "000001"}, {"code": "000002"}],
            [{"code": "000003"}],
            [],
        ]
    )
    connection = FakeConnection(cursor)
    acquired, released = install_fake_pool(monkeypatch, connection)

    batches = list(client.stream_query_batches("select code from fact.stock_daily", ("2026-08-21",), batch_size=2))

    assert batches == [
        [{"code": "000001"}, {"code": "000002"}],
        [{"code": "000003"}],
    ]
    assert len(acquired) == 1
    assert len(connection.cursor_names) == 1
    assert connection.cursor_names[0].startswith("quotemux_stream_")
    assert cursor.execute_calls == [("select code from fact.stock_daily", ("2026-08-21",))]
    assert cursor.fetch_sizes == [2, 2, 2]
    assert cursor.close_calls == 1
    assert connection.cancel_calls == 0
    assert connection.rollback_calls == 1
    assert released == [connection]


@pytest.mark.parametrize("batch_size", [0, -1, True])
def test_stream_query_batches_rejects_invalid_batch_size_before_acquiring_connection(
    monkeypatch: pytest.MonkeyPatch,
    batch_size: int,
) -> None:
    acquire_calls = 0

    def acquire() -> object:
        nonlocal acquire_calls
        acquire_calls += 1
        raise AssertionError("connection must not be acquired")

    monkeypatch.setattr(client, "_acquire_connection", acquire)

    with pytest.raises(ValueError, match="batch_size must be a positive integer"):
        client.stream_query_batches("select 1", batch_size=batch_size)

    assert acquire_calls == 0


def test_stream_query_batches_explicit_close_cancels_and_cleans_once(monkeypatch: pytest.MonkeyPatch) -> None:
    cursor = FakeCursor([[{"code": "000001"}], []])
    connection = FakeConnection(cursor)
    _, released = install_fake_pool(monkeypatch, connection)
    stream = client.stream_query_batches("select code from fact.stock_daily", batch_size=1)

    assert next(stream) == [{"code": "000001"}]
    stream.close()
    stream.close()

    assert connection.cancel_calls == 1
    assert cursor.close_calls == 1
    assert connection.rollback_calls == 1
    assert released == [connection]


def test_stream_query_batches_consumer_exception_cancels_and_cleans(monkeypatch: pytest.MonkeyPatch) -> None:
    cursor = FakeCursor([[{"code": "000001"}], []])
    connection = FakeConnection(cursor)
    _, released = install_fake_pool(monkeypatch, connection)
    stream = client.stream_query_batches("select code from fact.stock_daily", batch_size=1)

    assert next(stream) == [{"code": "000001"}]
    with pytest.raises(ConsumerError, match="consumer stopped"):
        stream.throw(ConsumerError("consumer stopped"))

    assert connection.cancel_calls == 1
    assert cursor.close_calls == 1
    assert connection.rollback_calls == 1
    assert released == [connection]


def test_stream_query_batches_fetch_exception_cancels_and_cleans(monkeypatch: pytest.MonkeyPatch) -> None:
    cursor = FakeCursor([RuntimeError("fetch failed")])
    connection = FakeConnection(cursor)
    _, released = install_fake_pool(monkeypatch, connection)

    with pytest.raises(RuntimeError, match="fetch failed"):
        next(client.stream_query_batches("select code from fact.stock_daily", batch_size=10))

    assert connection.cancel_calls == 1
    assert cursor.close_calls == 1
    assert connection.rollback_calls == 1
    assert released == [connection]


def test_stream_query_batches_does_not_require_connection_cancel(monkeypatch: pytest.MonkeyPatch) -> None:
    cursor = FakeCursor([[{"code": "000001"}], []])
    connection = FakeConnectionWithoutCancel(cursor)
    _, released = install_fake_pool(monkeypatch, connection)
    stream = client.stream_query_batches("select code from fact.stock_daily", batch_size=1)

    next(stream)
    stream.close()

    assert cursor.close_calls == 1
    assert connection.rollback_calls == 1
    assert released == [connection]


@pytest.mark.parametrize(
    "failure_target",
    [
        "cursor",
        "rollback",
    ],
)
def test_stream_query_batches_drops_connection_when_cleanup_fails(
    monkeypatch: pytest.MonkeyPatch,
    failure_target: str,
) -> None:
    cursor = FakeCursor([[]])
    connection = FakeConnection(cursor)
    if failure_target == "cursor":
        cursor.close = failing_cleanup("cursor close failed")  # type: ignore[method-assign]
    else:
        connection.rollback = failing_cleanup("rollback failed")  # type: ignore[method-assign]
    _, released = install_fake_pool(monkeypatch, connection)
    dropped: list[object] = []
    monkeypatch.setattr(client, "_drop_connection", dropped.append)

    with pytest.raises(RuntimeError, match=f"{failure_target}.*failed"):
        list(client.stream_query_batches("select code from fact.stock_daily", batch_size=10))

    assert released == []
    assert dropped == [connection]


def test_drop_connection_closes_and_updates_pool_metrics_once(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = FakeConnection(FakeCursor([]))
    monkeypatch.setattr(client, "_POOL_ACTIVE", 1)
    monkeypatch.setattr(client, "_POOL_CREATED", 1)
    monkeypatch.setattr(client, "_POOL_DROPPED", 0)

    client._drop_connection(connection)

    assert connection.close_calls == 1
    metrics = client.get_pool_metrics()
    assert metrics["active"] == 0
    assert metrics["created"] == 0
    assert metrics["dropped"] == 1
