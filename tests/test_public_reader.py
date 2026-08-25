from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
import os
import subprocess
import sys

import pytest


class _Description:
    def __init__(self, name: str) -> None:
        self.name = name


class _Cursor:
    def __init__(self, rows: list[tuple[object, ...]], *, fail_fetch: bool = False) -> None:
        self.rows = list(rows)
        self.description = (_Description("code"), _Description("trade_time"))
        self.executed: list[tuple[str, tuple[object, ...]]] = []
        self.closed = False
        self.fail_fetch = fail_fetch

    def execute(self, query: str, params: tuple[object, ...] = ()) -> None:
        self.executed.append((query, params))

    def fetchall(self) -> list[tuple[object, ...]]:
        if self.fail_fetch:
            raise RuntimeError("fetch failed")
        rows, self.rows = self.rows, []
        return rows

    def fetchmany(self, size: int) -> list[tuple[object, ...]]:
        if self.fail_fetch:
            raise RuntimeError("fetch failed")
        rows, self.rows = self.rows[:size], self.rows[size:]
        return rows

    def close(self) -> None:
        self.closed = True


class _Connection:
    closed = False

    def __init__(self, cursor: _Cursor) -> None:
        self._cursor = cursor
        self.cursors: list[_Cursor] = []
        self.cancel_count = 0
        self.rollback_count = 0

    def cursor(self, *args, **kwargs) -> _Cursor:
        del args, kwargs
        cursor = _Cursor([]) if not self.cursors else self._cursor
        self.cursors.append(cursor)
        return cursor

    def cancel(self) -> None:
        self.cancel_count += 1

    def rollback(self) -> None:
        self.rollback_count += 1

    def close(self) -> None:
        self.closed = True


def test_public_reader_import_does_not_load_runtime_or_package_installer() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    env = {**os.environ, "PYTHONPATH": str(repo_root / "src")}
    code = """
import sys
from quotemux import QuoteMuxPublicReader
assert QuoteMuxPublicReader.__name__ == 'QuoteMuxPublicReader'
assert 'quotemux.runtime' not in sys.modules
assert 'quotemux.futures' not in sys.modules
assert 'quotemux.package_install' not in sys.modules
assert 'quotemux.fact_ref_writes' not in sys.modules
assert 'quotemux.infra.provider_runtime.core' not in sys.modules
assert 'pandas' not in sys.modules
assert 'pydantic' not in sys.modules
"""
    subprocess.run([sys.executable, "-c", code], check=True, env=env, cwd=repo_root)


def test_read_client_uses_repeatable_read_and_returns_tuple_batch(monkeypatch) -> None:
    from quotemux.infra.db import read_client

    data_cursor = _Cursor([("600000", datetime(2026, 8, 21, 9, 31))])
    connection = _Connection(data_cursor)
    released: list[object] = []
    stages: list[str] = []
    monkeypatch.setattr(read_client, "_acquire_connection", lambda callback=None: connection)
    monkeypatch.setattr(read_client, "_release_connection", lambda value: released.append(value))

    batch = read_client.ReadOnlyClient(stage_callback=lambda stage, _elapsed: stages.append(stage)).query_batch("select 1")

    assert batch.columns == ("code", "trade_time")
    assert batch.rows == (("600000", datetime(2026, 8, 21, 9, 31)),)
    assert "begin isolation level repeatable read read only" in connection.cursors[0].executed[0][0].lower()
    assert connection.rollback_count == 1
    assert connection.cancel_count == 0
    assert released == [connection]
    assert {"pool_wait", "sql_execute", "sql_fetch"}.issubset(stages)


def test_server_cursor_close_early_cancels_rolls_back_and_releases(monkeypatch) -> None:
    from quotemux.infra.db import read_client

    data_cursor = _Cursor([("600000", "09:31"), ("600000", "09:32")])
    connection = _Connection(data_cursor)
    released: list[object] = []
    monkeypatch.setattr(read_client, "_acquire_connection", lambda callback=None: connection)
    monkeypatch.setattr(read_client, "_release_connection", lambda value: released.append(value))

    batches = read_client.ReadOnlyClient().stream_batches("select 1", batch_size=1)
    assert next(batches).rows == (("600000", "09:31"),)
    batches.close()

    assert data_cursor.closed is True
    assert connection.cancel_count == 1
    assert connection.rollback_count == 1
    assert released == [connection]


def test_server_cursor_exception_cancels_and_returns_clean_connection(monkeypatch) -> None:
    from quotemux.infra.db import read_client

    data_cursor = _Cursor([], fail_fetch=True)
    connection = _Connection(data_cursor)
    released: list[object] = []
    monkeypatch.setattr(read_client, "_acquire_connection", lambda callback=None: connection)
    monkeypatch.setattr(read_client, "_release_connection", lambda value: released.append(value))

    with pytest.raises(RuntimeError, match="fetch failed"):
        next(read_client.ReadOnlyClient().stream_batches("select 1"))

    assert connection.cancel_count == 1
    assert connection.rollback_count == 1
    assert released == [connection]


def test_server_cursor_exhaustion_rolls_back_without_cancel(monkeypatch) -> None:
    from quotemux.infra.db import read_client

    data_cursor = _Cursor([("600000", "09:31")])
    connection = _Connection(data_cursor)
    released: list[object] = []
    monkeypatch.setattr(read_client, "_acquire_connection", lambda callback=None: connection)
    monkeypatch.setattr(read_client, "_release_connection", lambda value: released.append(value))

    assert [batch.rows for batch in read_client.ReadOnlyClient().stream_batches("select 1")] == [(("600000", "09:31"),)]

    assert connection.cancel_count == 0
    assert connection.rollback_count == 1
    assert released == [connection]


class _Snapshot:
    def __init__(self, batches) -> None:
        self.calls: list[tuple[str, str, tuple[object, ...]]] = []
        self._batches = batches

    def query_batch(self, query: str, params: tuple[object, ...] = (), *, stage: str = "sql"):
        from quotemux.infra.db.read_client import QueryBatch

        self.calls.append(("query", query, params))
        return QueryBatch(("code", "row_count", "first_trade_time", "last_trade_time"), (("600000", 240, "09:31", "15:00"),))

    def stream_batches(self, query: str, params: tuple[object, ...] = (), *, batch_size: int = 1000, stage: str = "sql"):
        del batch_size, stage
        self.calls.append(("stream", query, params))
        yield from self._batches


class _ReaderClient:
    def __init__(self, batches=()) -> None:
        self.calls: list[tuple[str, str, tuple[object, ...]]] = []
        self.snapshot_value = _Snapshot(batches)

    def query_batch(self, query: str, params: tuple[object, ...] = (), *, stage: str = "sql"):
        from quotemux.infra.db.read_client import QueryBatch

        self.calls.append((stage, query, params))
        return QueryBatch(("code", "trade_time", "pre_close"), (("600000", "2026-08-21", 10.0),))

    def stream_batches(self, query: str, params: tuple[object, ...] = (), *, batch_size: int = 1000, stage: str = "sql"):
        del batch_size
        self.calls.append((stage, query, params))
        yield from self.snapshot_value._batches

    @contextmanager
    def snapshot(self):
        yield self.snapshot_value


def test_daily_reader_pages_and_filters_in_sql_without_models() -> None:
    from quotemux.public_reader import QuoteMuxPublicReader

    client = _ReaderClient()
    batch = QuoteMuxPublicReader(client=client).get_stock_daily_snapshot_batch(
        "2026-08-21", limit=100, offset=200, skip_suspended=True, skip_st=True
    )

    stage, query, params = client.calls[0]
    normalized = query.lower()
    assert stage == "daily_snapshot"
    assert "day_rows.trade_date = %s::date" in normalized
    assert "day_rows.is_suspended is not true" in normalized
    assert "day_rows.is_st is not true" in normalized
    assert normalized.index("limit %s") < normalized.index("left join lateral")
    assert "day_rows.pre_close is null" in normalized
    assert "coalesce(day_rows.pre_close, previous_day.previous_close) as pre_close" in normalized
    assert "coalesce(day_rows.change, day_rows.close - coalesce(day_rows.pre_close, previous_day.previous_close)) as change" in normalized
    assert params == ("2026-08-21", 100, 200)
    assert batch.rows[0][0] == "600000"


def test_daily_local_window_filters_and_pages_before_previous_close_lookup() -> None:
    from quotemux.public_reader import QuoteMuxPublicReader

    client = _ReaderClient()
    QuoteMuxPublicReader(client=client).get_stock_daily_local_window_batch(
        "2026-08-01", "2026-08-21", limit=500, offset=100
    )

    _stage, query, params = client.calls[0]
    normalized = query.lower()
    assert "day_rows.trade_date >= %s::date" in normalized
    assert normalized.index("limit %s") < normalized.index("left join lateral")
    assert "lag(" not in normalized
    assert params == ("2026-08-01", "2026-08-21", 500, 100)


def test_daily_local_window_skip_st_excludes_entire_code_like_legacy_contract() -> None:
    from quotemux.public_reader import QuoteMuxPublicReader

    client = _ReaderClient()
    QuoteMuxPublicReader(client=client).get_stock_daily_local_window_batch(
        "2026-08-01", "2026-08-21", limit=500, offset=100, skip_st=True
    )

    _stage, query, params = client.calls[0]
    normalized = query.lower()
    assert "not exists" in normalized
    assert "st_rows.code = day_rows.code" in normalized
    assert "st_rows.is_st is true" in normalized
    assert params == ("2026-08-01", "2026-08-21", "2026-08-01", "2026-08-21", 500, 100)


def test_daily_tuple_batch_is_field_equivalent_to_legacy_quote_contract() -> None:
    from platform_models import StockQuoteItem
    from quotemux.infra.db.read_client import QueryBatch

    columns = (
        "code", "trade_time", "open", "high", "low", "close", "pre_close",
        "change", "pct_chg", "volume", "amount", "is_suspended", "is_st",
    )
    batch = QueryBatch(columns, (("600000", "2026-08-21", 10.0, 11.0, 9.0, 10.5, 10.0, 0.5, 5.0, 100.0, 1_050.0, False, True),))
    actual = {**batch.as_dicts()[0], "freq": "1d", "adjust": "none"}
    expected = StockQuoteItem(
        code="600000", trade_time="2026-08-21", freq="1d", open=10.0, high=11.0,
        low=9.0, close=10.5, pre_close=10.0, change=0.5, pct_chg=5.0,
        volume=100.0, amount=1_050.0, adjust="none", is_suspended=False, is_st=True,
    ).model_dump()

    assert actual == expected


def test_stock_1m_reader_uses_one_grouped_coverage_and_one_ordered_stream() -> None:
    from quotemux.infra.db.read_client import QueryBatch
    from quotemux.public_reader import QuoteMuxPublicReader

    rows = QueryBatch(("code", "trade_time"), (("000001", "09:31"), ("600000", "09:31")))
    client = _ReaderClient((rows,))
    reader = QuoteMuxPublicReader(client=client)

    with reader.open_stock_1m_batch_stream(
        ["600000", "000001", "600000"], "2026-08-21 09:30:00", "2026-08-21 15:00:00", batch_size=100
    ) as result:
        coverage = result.coverage
        batches = list(result)

    assert coverage.rows[0][1] == 240
    assert batches == [rows]
    assert [call[0] for call in client.snapshot_value.calls] == ["query", "stream"]
    coverage_query = client.snapshot_value.calls[0][1].lower()
    stream_query = client.snapshot_value.calls[1][1].lower()
    assert "from readmodel.stock_bar_1m_daily_coverage coverage" in coverage_query
    assert "sum(coverage.row_count)::bigint" in coverage_query
    assert "group by coverage.code" in coverage_query
    assert "from fact.stock_bar_1m" not in coverage_query
    assert "order by bars.code, bars.bar_time" in stream_query
    assert "bars.code = any(%s::character(6)[])" in stream_query
    assert client.snapshot_value.calls[0][2][0] == ["000001", "600000"]
    assert client.snapshot_value.calls[1][2][0] == ["000001", "600000"]


def test_stock_1m_direct_arrow_stream_is_one_sql_without_dataframe() -> None:
    from quotemux.infra.db.read_client import QueryBatch
    from quotemux.public_reader import QuoteMuxPublicReader

    rows = QueryBatch(("code", "trade_time"), (("600000", "09:31"),))
    client = _ReaderClient((rows,))

    assert list(QuoteMuxPublicReader(client=client).stream_stock_1m_batches(
        ["600000"], "2026-08-21 09:30:00", "2026-08-21 15:00:00"
    )) == [rows]
    assert len(client.calls) == 1
    assert client.calls[0][0] == "stock_1m_stream"
    assert "order by bars.code, bars.bar_time" in client.calls[0][1].lower()


def test_futures_1m_reader_is_strict_read_only_and_normalizes_public_contract() -> None:
    from quotemux.infra.db.read_client import QueryBatch
    from quotemux.public_reader import QuoteMuxPublicReader
    from quotemux.strict_read import strict_public_read_boundary

    class Client:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str, tuple[object, ...]]] = []

        def query_batch(self, query: str, params: tuple[object, ...] = (), *, stage: str = "sql") -> QueryBatch:
            self.calls.append((stage, query, params))
            return QueryBatch(
                (
                    "product_code", "exchange", "series_type", "bar_time", "open", "high", "low", "close",
                    "volume", "open_interest", "adjustment_offset",
                ),
                (("ag", "SHFE", "back_adjusted_continuous", "2018-11-29 13:31:00", 1.0, 2.0, 0.5, 1.5, 10.0, 20.0, 0.0),),
            )

    client = Client()
    with strict_public_read_boundary():
        batch = QuoteMuxPublicReader(client=client).get_futures_quotes_1m_batch(
            " AG,ag,IF ", "back_adjusted_continuous", "2018-11-29 13:31:00", "2018-11-29 13:52:00", limit=999_999
        )

    stage, query, params = client.calls[0]
    normalized = " ".join(query.lower().split())
    assert stage == "futures_1m"
    assert "from fact.future_bar_1m bars" in normalized
    assert "create " not in normalized
    assert "insert " not in normalized
    assert "case when bars.series_type = 'apex_l0_adjusted' then 'back_adjusted_continuous'" in normalized
    assert "order by bars.bar_time, bars.product_code" in normalized
    assert params == (["IF", "ag"], "apex_l0_adjusted", "2018-11-29 13:31:00", "2018-11-29 13:52:00", 500_000)
    assert batch.as_dicts()[0]["series_type"] == "back_adjusted_continuous"


def test_futures_1m_reader_rejects_invalid_public_inputs() -> None:
    from quotemux.public_reader import QuoteMuxPublicReader

    reader = QuoteMuxPublicReader(client=_ReaderClient())
    with pytest.raises(ValueError, match="codes"):
        reader.get_futures_quotes_1m_batch(" , ", "main_continuous", "2026-08-11 09:31:00", "2026-08-11 15:00:00")
    with pytest.raises(ValueError, match="unknown futures product code"):
        reader.get_futures_quotes_1m_batch("UNKNOWN", "main_continuous", "2026-08-11 09:31:00", "2026-08-11 15:00:00")
    with pytest.raises(ValueError, match="series_type"):
        reader.get_futures_quotes_1m_batch("ag", "contract", "2026-08-11 09:31:00", "2026-08-11 15:00:00")
    with pytest.raises(ValueError, match="start_time"):
        reader.get_futures_quotes_1m_batch("ag", "main_continuous", "2026-08-11 15:00:00", "2026-08-11 09:31:00")
    with pytest.raises(ValueError, match="limit"):
        reader.get_futures_quotes_1m_batch("ag", "main_continuous", "2026-08-11 09:31:00", "2026-08-11 15:00:00", limit=True)


def test_futures_coverage_reader_is_strict_local_sorted_and_filtered() -> None:
    from quotemux.infra.db.read_client import QueryBatch
    from quotemux.public_reader import QuoteMuxPublicReader
    from quotemux.strict_read import strict_public_read_boundary

    class Client:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str, tuple[object, ...]]] = []

        def query_batch(self, query: str, params: tuple[object, ...] = (), *, stage: str = "sql") -> QueryBatch:
            self.calls.append((stage, query, params))
            return QueryBatch(
                ("product_code", "exchange", "series_type", "row_count", "first_bar_time", "last_bar_time"),
                (("IF", "CFFEX", "back_adjusted_continuous", 240, "2026-08-21 09:31:00", "2026-08-21 15:00:00"),),
            )

    client = Client()
    with strict_public_read_boundary():
        batch = QuoteMuxPublicReader(client=client).list_futures_coverage_batch("back_adjusted_continuous")

    stage, query, params = client.calls[0]
    normalized = " ".join(query.lower().split())
    assert stage == "futures_coverage"
    assert "from fact.future_bar_1m_coverage" in normalized
    assert "create " not in normalized
    assert "case when coverage.series_type = 'apex_l0_adjusted' then 'back_adjusted_continuous'" in normalized
    assert "order by coverage.series_type, coverage.exchange, coverage.product_code" in normalized
    assert params == ("apex_l0_adjusted", "apex_l0_adjusted")
    assert batch.rows[0][2] == "back_adjusted_continuous"


def test_futures_coverage_reader_all_series_and_rejects_unknown_filter() -> None:
    from quotemux.public_reader import QuoteMuxPublicReader

    client = _ReaderClient()
    reader = QuoteMuxPublicReader(client=client)

    reader.list_futures_coverage_batch()
    assert client.calls[0][2] == ("", "")
    with pytest.raises(ValueError, match="series_type"):
        reader.list_futures_coverage_batch("contract")
    assert len(client.calls) == 1


def test_futures_series_state_reader_pins_one_immutable_generation_per_series() -> None:
    from quotemux.public_reader import QuoteMuxPublicReader

    client = _ReaderClient()
    QuoteMuxPublicReader(client=client).list_futures_series_state_batch("back_adjusted_continuous")

    stage, query, params = client.calls[0]
    normalized = " ".join(query.lower().split())
    assert stage == "futures_series_state"
    assert "from audit.future_bar_1m_series_generation state" in normalized
    assert "distinct on (state.series_type)" in normalized
    assert "order by state.series_type, state.generation desc" in normalized
    assert "case when state.series_type = 'apex_l0_adjusted' then 'back_adjusted_continuous'" in normalized
    assert params == ("apex_l0_adjusted", "apex_l0_adjusted")
