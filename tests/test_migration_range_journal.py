from __future__ import annotations

from collections.abc import Sequence

import pytest

from platform_models import FutureBar1mItem, StockQuoteItem
from quotemux import fact_ref_writes, futures
from quotemux.infra.db import client, market_reads


class FakeJournalCursor:
    def __init__(
        self,
        *,
        forward_present: bool = False,
        reverse_present: bool = False,
        forward_has_range: bool = True,
        reverse_has_range: bool = True,
        fail_journal: str = "",
    ) -> None:
        self.forward_present = forward_present
        self.reverse_present = reverse_present
        self.forward_has_range = forward_has_range
        self.reverse_has_range = reverse_has_range
        self.fail_journal = fail_journal
        self.calls: list[tuple[str, object]] = []

    def __enter__(self) -> FakeJournalCursor:
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        return None

    def execute(self, query: str, params: object = ()) -> None:
        normalized = " ".join(query.split())
        self.calls.append((normalized, params))
        if self.fail_journal and f"insert into audit.{self.fail_journal}" in normalized:
            raise RuntimeError("journal insert failed")

    def executemany(self, query: str, params: list[tuple[object, ...]]) -> None:
        self.calls.append((" ".join(query.split()), params))

    def fetchone(self) -> dict[str, bool]:
        return {
            "forward_active": self.forward_present and self.forward_has_range,
            "reverse_active": self.reverse_present and self.reverse_has_range,
        }


class FakeJournalConnection:
    def __init__(self, cursor: FakeJournalCursor) -> None:
        self.cursor_instance = cursor
        self.events: list[str] = []
        self.commit_calls = 0
        self.rollback_calls = 0

    def cursor(self) -> FakeJournalCursor:
        self.events.append("cursor")
        return self.cursor_instance

    def commit(self) -> None:
        self.events.append("commit")
        self.cursor_instance.calls.append(("<commit>", ()))
        self.commit_calls += 1

    def rollback(self) -> None:
        self.events.append("rollback")
        self.cursor_instance.calls.append(("<rollback>", ()))
        self.rollback_calls += 1


@pytest.mark.parametrize(
    ("forward_present", "reverse_present", "expected_journals"),
    [
        (False, False, []),
        (True, False, ["stock_bar_1m_ts_forward_delta"]),
        (False, True, ["stock_bar_1m_ts_reverse_delta"]),
        (True, True, ["stock_bar_1m_ts_forward_delta", "stock_bar_1m_ts_reverse_delta"]),
    ],
)
def test_discover_and_append_migration_range_journals_handles_absent_and_present_journals(
    forward_present: bool,
    reverse_present: bool,
    expected_journals: list[str],
) -> None:
    cursor = FakeJournalCursor(forward_present=forward_present, reverse_present=reverse_present)

    state = client.discover_migration_range_journals(cursor, "stock_bar_1m")
    client.append_migration_range_journals(
        cursor,
        state,
        ["2026-08-22 10:01:00", "2026-08-22 09:31:00", "2026-08-22 09:45:00"],
    )

    assert "to_regclass" in cursor.calls[0][0]
    assert "pg_catalog.pg_attribute" in cursor.calls[0][0]
    assert cursor.calls[0][1] == (
        "audit.stock_bar_1m_ts_forward_delta",
        "audit.stock_bar_1m_ts_reverse_delta",
    )
    inserts = [call for call in cursor.calls if call[0].startswith("insert into audit.")]
    assert [next(name for name in expected_journals if name in query) for query, _ in inserts] == expected_journals
    assert all(params == ("2026-08-22 09:31:00", "2026-08-22 10:01:00") for _, params in inserts)


def test_append_migration_range_journals_rejects_non_allowlisted_table() -> None:
    cursor = FakeJournalCursor()

    with pytest.raises(ValueError, match="unsupported migration journal fact table"):
        client.discover_migration_range_journals(cursor, "stock_bar_5m")

    assert cursor.calls == []


def test_execute_many_with_migration_journal_commits_fact_and_journal_together(monkeypatch: pytest.MonkeyPatch) -> None:
    cursor = FakeJournalCursor(forward_present=True, reverse_present=True)
    connection = FakeJournalConnection(cursor)
    released: list[object] = []
    monkeypatch.setattr(client, "_acquire_connection", lambda: connection)
    monkeypatch.setattr(client, "_release_connection", released.append)

    result = client._execute_many_with_migration_journal_once(
        "insert into fact.stock_bar_30m values (%s, %s, %s)",
        [("SHSE", "600000", "2026-08-22 10:00:00")],
        "stock_bar_30m",
        2,
    )

    assert result is True
    queries = [query for query, _ in cursor.calls]
    assert "pg_catalog.pg_attribute" in queries[0]
    assert queries[1:] == [
        "set local markethub.explicit_range_journal = 'on'",
        "insert into fact.stock_bar_30m values (%s, %s, %s)",
        "insert into audit.stock_bar_30m_ts_forward_delta (range_start, range_end) values (%s::timestamp, %s::timestamp)",
        "insert into audit.stock_bar_30m_ts_reverse_delta (range_start, range_end) values (%s::timestamp, %s::timestamp)",
        "<commit>",
    ]
    assert connection.commit_calls == 1
    assert connection.rollback_calls == 0
    assert released == [connection]


def test_execute_many_with_migration_journal_rolls_back_fact_when_journal_insert_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = FakeJournalCursor(forward_present=True, fail_journal="stock_bar_30m_ts_forward_delta")
    connection = FakeJournalConnection(cursor)
    released: list[object] = []
    monkeypatch.setattr(client, "_acquire_connection", lambda: connection)
    monkeypatch.setattr(client, "_release_connection", released.append)

    with pytest.raises(RuntimeError, match="journal insert failed"):
        client._execute_many_with_migration_journal_once(
            "insert into fact.stock_bar_30m values (%s, %s, %s)",
            [("SHSE", "600000", "2026-08-22 10:00:00")],
            "stock_bar_30m",
            2,
        )

    assert "pg_catalog.pg_attribute" in cursor.calls[0][0]
    assert cursor.calls[1][0] == "set local markethub.explicit_range_journal = 'on'"
    assert cursor.calls[2][0].startswith("insert into fact.stock_bar_30m")
    assert connection.commit_calls == 0
    assert connection.rollback_calls == 1
    assert released == [connection]


def test_scalar_key_legacy_journal_keeps_trigger_authoritative(monkeypatch: pytest.MonkeyPatch) -> None:
    cursor = FakeJournalCursor(forward_present=True, forward_has_range=False)
    connection = FakeJournalConnection(cursor)
    monkeypatch.setattr(client, "_acquire_connection", lambda: connection)
    monkeypatch.setattr(client, "_release_connection", lambda _connection: None)

    assert client._execute_many_with_migration_journal_once(
        "insert into fact.stock_bar_1m values (%s, %s, %s)",
        [("SHSE", "600000", "2026-08-22 09:31:00")],
        "stock_bar_1m",
        2,
    )

    queries = [query for query, _ in cursor.calls]
    assert "pg_catalog.pg_attribute" in queries[0]
    assert queries[1:] == ["insert into fact.stock_bar_1m values (%s, %s, %s)", "<commit>"]
    assert all("set local markethub.explicit_range_journal" not in query for query in queries)
    assert all("insert into audit." not in query for query in queries)


def test_mixed_scalar_and_range_journals_only_append_to_range_journal() -> None:
    cursor = FakeJournalCursor(
        forward_present=True,
        forward_has_range=False,
        reverse_present=True,
        reverse_has_range=True,
    )

    state = client.discover_migration_range_journals(cursor, "stock_bar_1m")
    client.append_migration_range_journals(cursor, state, ["2026-08-22 09:31:00"])

    assert state.forward_active is False
    assert state.reverse_active is True
    inserts = [query for query, _ in cursor.calls if query.startswith("insert into audit.")]
    assert inserts == [
        "insert into audit.stock_bar_1m_ts_reverse_delta (range_start, range_end) values (%s::timestamp, %s::timestamp)"
    ]


def test_execute_many_with_migration_journal_validates_allowlist_before_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(client, "_acquire_connection", lambda: pytest.fail("must not acquire a connection"))

    with pytest.raises(ValueError, match="unsupported migration journal fact table"):
        client.execute_many_with_migration_journal(
            "insert into fact.stock_bar_5m values (%s)",
            [("2026-08-22 09:35:00",)],
            fact_table="stock_bar_5m",
            bar_time_index=0,
        )


def test_stock_intraday_writer_journals_1m_and_30m_and_preserves_write_event(monkeypatch: pytest.MonkeyPatch) -> None:
    one_minute = StockQuoteItem(code="600000", trade_time="2026-08-22 09:31:00", freq="1m", close=10.0)
    thirty_minute = StockQuoteItem(code="000001", trade_time="2026-08-22 10:00:00", freq="30m", close=11.0)
    journal_calls: list[tuple[str, int, list[tuple[object, ...]]]] = []
    audit_calls: list[tuple[str, list[tuple[object, ...]]]] = []
    coverage_calls: list[tuple[str, tuple[object, ...]]] = []

    def journal_write(
        query: str,
        params: list[tuple[object, ...]],
        *,
        fact_table: str,
        bar_time_index: int,
    ) -> bool:
        journal_calls.append((fact_table, bar_time_index, params))
        return True

    monkeypatch.setattr(fact_ref_writes, "_complete_stock_1m_items", lambda items: [one_minute])
    monkeypatch.setattr(fact_ref_writes, "execute_many_with_migration_journal", journal_write)
    monkeypatch.setattr(fact_ref_writes, "execute_many", lambda query, params: audit_calls.append((query, params)) or True)
    monkeypatch.setattr(fact_ref_writes, "execute_sql", lambda query, params: coverage_calls.append((query, params)) or True)

    assert fact_ref_writes._upsert_stock_intraday([one_minute, thirty_minute]) is True

    assert [(table, index) for table, index, _ in journal_calls] == [("stock_bar_1m", 2), ("stock_bar_30m", 2)]
    assert [params[0][2] for _, _, params in journal_calls] == ["2026-08-22 09:31:00", "2026-08-22 10:00:00"]
    assert len(audit_calls) == 1
    assert "audit.stock_bar_1m_write_event" in audit_calls[0][0]
    assert len(coverage_calls) == 1
    assert "readmodel.stock_bar_1m_daily_coverage" in coverage_calls[0][0]


def test_stock_30m_row_writer_uses_migration_journal_transaction(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def journal_write(
        query: str,
        params: list[tuple[object, ...]],
        *,
        fact_table: str,
        bar_time_index: int,
    ) -> bool:
        captured.update(query=query, params=params, fact_table=fact_table, bar_time_index=bar_time_index)
        return True

    monkeypatch.setattr(market_reads, "execute_many_with_migration_journal", journal_write)

    assert market_reads.upsert_stock_bar_30m_rows(
        [
            {
                "code": "600000",
                "trade_time": "2026-08-22 10:00:00",
                "open": 10.0,
                "high": 10.1,
                "low": 9.9,
                "close": 10.0,
                "volume": 100,
                "amount": 1000.0,
            }
        ]
    )

    assert captured["fact_table"] == "stock_bar_30m"
    assert captured["bar_time_index"] == 2
    assert "insert into fact.stock_bar_30m" in str(captured["query"])


class FakeCopy:
    def __init__(self) -> None:
        self.rows: list[tuple[object, ...]] = []

    def __enter__(self) -> FakeCopy:
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        return None

    def write_row(self, row: Sequence[object]) -> None:
        self.rows.append(tuple(row))


class FakeFutureCursor:
    def __init__(self) -> None:
        self.queries: list[str] = []
        self.copy_instance = FakeCopy()

    def __enter__(self) -> FakeFutureCursor:
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        return None

    def execute(self, query: str, params: object = ()) -> None:
        self.queries.append(" ".join(query.split()))

    def copy(self, query: str) -> FakeCopy:
        self.queries.append(" ".join(query.split()))
        return self.copy_instance


class FakeFutureConnection:
    def __init__(self) -> None:
        self.cursor_instance = FakeFutureCursor()
        self.commit_calls = 0
        self.rollback_calls = 0

    def cursor(self) -> FakeFutureCursor:
        return self.cursor_instance

    def commit(self) -> None:
        self.commit_calls += 1

    def rollback(self) -> None:
        self.rollback_calls += 1


def test_future_copy_writer_appends_journal_before_commit(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = FakeFutureConnection()
    events: list[tuple[object, ...]] = []
    state = client.MigrationRangeJournalState("future_bar_1m", True, False)
    monkeypatch.setattr(futures, "_acquire_connection", lambda: connection)
    monkeypatch.setattr(futures, "_release_connection", lambda value: events.append(("release", value)))
    monkeypatch.setattr(futures, "discover_migration_range_journals", lambda cursor, fact_table: state)

    def append_journal(
        cursor: object,
        journal_state: client.MigrationRangeJournalState,
        bar_times: Sequence[object],
    ) -> None:
        events.append(("journal", cursor, journal_state, list(bar_times), connection.commit_calls))

    monkeypatch.setattr(futures, "append_migration_range_journals", append_journal)
    item = FutureBar1mItem(
        product_code="IF",
        exchange="CFFEX",
        series_type="main_continuous",
        bar_time="2026-08-22 09:31:00",
        open=4600.0,
        high=4610.0,
        low=4590.0,
        close=4605.0,
        volume=100,
        open_interest=200,
    )

    assert futures.QuoteMuxFutures()._upsert_main_continuous([item]) == 1

    assert events[0] == (
        "journal",
        connection.cursor_instance,
        state,
        ["2026-08-22 09:31:00"],
        0,
    )
    assert connection.commit_calls == 1
    assert connection.rollback_calls == 0
    assert events[1] == ("release", connection)
    set_local_index = connection.cursor_instance.queries.index("set local markethub.explicit_range_journal = 'on'")
    fact_insert_index = next(
        index
        for index, query in enumerate(connection.cursor_instance.queries)
        if query.startswith("insert into fact.future_bar_1m")
    )
    assert set_local_index < fact_insert_index


def test_future_copy_writer_rolls_back_when_journal_append_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = FakeFutureConnection()
    released: list[object] = []
    state = client.MigrationRangeJournalState("future_bar_1m", True, False)
    monkeypatch.setattr(futures, "_acquire_connection", lambda: connection)
    monkeypatch.setattr(futures, "_release_connection", released.append)
    monkeypatch.setattr(futures, "discover_migration_range_journals", lambda cursor, fact_table: state)

    def fail_journal(*_args: object) -> None:
        raise RuntimeError("journal insert failed")

    monkeypatch.setattr(futures, "append_migration_range_journals", fail_journal)
    item = FutureBar1mItem(
        product_code="IF",
        exchange="CFFEX",
        series_type="main_continuous",
        bar_time="2026-08-22 09:31:00",
        open=4600.0,
        high=4610.0,
        low=4590.0,
        close=4605.0,
        volume=100,
        open_interest=200,
    )

    with pytest.raises(RuntimeError, match="journal insert failed"):
        futures.QuoteMuxFutures()._upsert_main_continuous([item])

    assert connection.commit_calls == 0
    assert connection.rollback_calls == 1
    assert released == [connection]
