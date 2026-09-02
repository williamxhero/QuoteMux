from __future__ import annotations

from datetime import datetime
from contextlib import nullcontext
from zoneinfo import ZoneInfo

from quotemux.live_bars import (
    CurrentBarNodeAttempt,
    CurrentBarRequest,
    CurrentBarStagingResult,
    LiveBarIngestor,
    NativeCurrentStockBar,
    ProviderCurrentBarsResult,
    PostgresCurrentBarStore,
    CurrentBarFinalizationCandidate,
    CurrentBarFinalizer,
)


SHANGHAI = ZoneInfo("Asia/Shanghai")


class _Provider:
    def fetch(self, codes: tuple[str, ...], effective_now: datetime) -> ProviderCurrentBarsResult:
        assert codes == ("600519",)
        assert effective_now.isoformat() == "2026-09-02T13:30:08+08:00"
        return ProviderCurrentBarsResult(
            bars=(
                NativeCurrentStockBar(
                    code="600519",
                    interval_start=datetime(2026, 9, 2, 13, 30, tzinfo=SHANGHAI),
                    native_trade_time="2026-09-02 13:30:00",
                    open=1400.0,
                    high=1401.0,
                    low=1399.0,
                    close=1400.5,
                    volume=1200,
                    amount=1_680_600.0,
                    unit_conversion="mootdx:volume*1,amount*1",
                ),
            ),
            attempts=(
                CurrentBarNodeAttempt(code="600519", server="180.153.18.172:80", outcome="ok"),
            ),
        )


class _Store:
    def __init__(self) -> None:
        self.events: list[object] = []

    def stage(self, bar: NativeCurrentStockBar, observed_at: datetime, attempts: tuple[CurrentBarNodeAttempt, ...]) -> CurrentBarStagingResult:
        self.events.append((bar, observed_at, attempts))
        return CurrentBarStagingResult(observation_version="42", selected_at=observed_at)

    def record_attempts(self, attempts: tuple[CurrentBarNodeAttempt, ...], observed_at: datetime, interval_start: datetime) -> None:
        self.events.append((attempts, observed_at, interval_start))


def test_live_ingestor_stages_selected_native_bar_before_returning_it() -> None:
    store = _Store()
    ingestor = LiveBarIngestor(provider=_Provider(), store=store, clock=lambda: datetime(2026, 9, 2, 13, 30, 9, tzinfo=SHANGHAI))

    result = ingestor.ingest(
        CurrentBarRequest(codes=("600519",), effective_now=datetime(2026, 9, 2, 13, 30, 8, tzinfo=SHANGHAI))
    )

    assert len(store.events) == 1
    staged_bar, observed_at, attempts = store.events[0]
    assert staged_bar.code == "600519"
    assert observed_at.isoformat() == "2026-09-02T13:30:09+08:00"
    assert attempts[0].outcome == "ok"
    assert result.items[0].observation_version == "42"
    assert result.items[0].is_final is False
    assert result.items[0].provider == "mootdx"


def test_postgres_store_commits_observation_selection_and_attempt_in_one_transaction(monkeypatch) -> None:
    statements: list[str] = []

    class _Cursor:
        fetches = 0

        def execute(self, query: str, params: tuple[object, ...]) -> None:
            del params
            statements.append(" ".join(query.split()))

        def fetchone(self):
            self.fetches += 1
            return {"observation_version": 42} if self.fetches == 1 else {"selected_at": datetime(2026, 9, 2, 13, 30, 9, tzinfo=SHANGHAI)}

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

    class _Connection:
        closed = False

        def transaction(self):
            return nullcontext()

        def cursor(self):
            return _Cursor()

        def rollback(self):
            return None

    connection = _Connection()
    monkeypatch.setattr("quotemux.live_bars.db_client.is_db_available", lambda: True)
    monkeypatch.setattr("quotemux.live_bars.db_client._acquire_connection", lambda: connection)
    monkeypatch.setattr("quotemux.live_bars.db_client._release_connection", lambda current: None)

    result = PostgresCurrentBarStore().stage(
        NativeCurrentStockBar(
            code="600519", interval_start=datetime(2026, 9, 2, 13, 30, tzinfo=SHANGHAI), native_trade_time="2026-09-02 13:30:00",
            open=1400.0, high=1401.0, low=1399.0, close=1400.5, volume=1200, amount=1_680_600.0,
            unit_conversion="mootdx:volume*1,amount*1",
        ),
        datetime(2026, 9, 2, 13, 30, 9, tzinfo=SHANGHAI),
        (CurrentBarNodeAttempt(code="600519", server="180.153.18.172:80", outcome="ok"),),
    )

    assert result.observation_version == "42"
    assert any("insert into live.stock_bar_provider_attempt" in statement for statement in statements)
    assert any("insert into live.stock_bar_observation" in statement for statement in statements)
    assert any("insert into live.stock_bar_selected" in statement for statement in statements)


def test_finalizer_refetches_the_closed_interval_before_atomic_promotion() -> None:
    class _FinalizerProvider:
        def fetch(self, codes: tuple[str, ...], effective_now: datetime) -> ProviderCurrentBarsResult:
            assert codes == ("600519",)
            assert effective_now.isoformat() == "2026-09-02T13:30:00+08:00"
            return ProviderCurrentBarsResult(
                bars=(
                    NativeCurrentStockBar(
                        code="600519", interval_start=effective_now, native_trade_time="2026-09-02 13:30:00",
                        open=1400.0, high=1402.0, low=1399.0, close=1401.0, volume=1500, amount=2_101_500.0,
                        unit_conversion="mootdx:volume*1,amount*1",
                    ),
                ),
                attempts=(CurrentBarNodeAttempt(code="600519", server="180.153.18.172:80", outcome="ok"),),
            )

    class _FinalizerStore:
        def list_due(self, now: datetime, grace_seconds: int) -> tuple[CurrentBarFinalizationCandidate, ...]:
            assert now.isoformat() == "2026-09-02T13:31:07+08:00"
            assert grace_seconds == 7
            return (CurrentBarFinalizationCandidate(market="SHSE", code="600519", interval_start=datetime(2026, 9, 2, 13, 30, tzinfo=SHANGHAI)),)

        def finalize(self, candidate, bar, observed_at, attempts) -> bool:
            assert candidate.code == "600519"
            assert bar.close == 1401.0  # the close-time refetch, not an earlier mutable value
            assert observed_at.isoformat() == "2026-09-02T13:31:07+08:00"
            assert attempts[0].outcome == "ok"
            return True

    finalizer = CurrentBarFinalizer(provider=_FinalizerProvider(), store=_FinalizerStore(), grace_seconds=7)

    result = finalizer.finalize_due(datetime(2026, 9, 2, 13, 31, 7, tzinfo=SHANGHAI))

    assert result == {"candidates": 1, "finalized": 1, "deferred": 0, "failed": 0}


def test_postgres_finalization_writes_history_audit_coverage_and_stage_state_together(monkeypatch) -> None:
    statements: list[str] = []

    class _Cursor:
        def __init__(self) -> None:
            self.fetches = iter(({"state": "staged"}, {"observation_version": 43}))

        def execute(self, query: str, params: tuple[object, ...] = ()) -> None:
            del params
            statements.append(" ".join(query.split()))

        def fetchone(self):
            return next(self.fetches)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

    class _Connection:
        closed = False

        def transaction(self):
            return nullcontext()

        def cursor(self):
            return _Cursor()

        def rollback(self):
            return None

    monkeypatch.setattr("quotemux.live_bars.db_client.is_db_available", lambda: True)
    monkeypatch.setattr("quotemux.live_bars.db_client._acquire_connection", _Connection)
    monkeypatch.setattr("quotemux.live_bars.db_client._release_connection", lambda current: None)
    monkeypatch.setattr("quotemux.live_bars.db_client.discover_migration_range_journals", lambda cursor, table: type("Journal", (), {"has_active_journal": False})())
    monkeypatch.setattr("quotemux.live_bars.db_client.append_migration_range_journals", lambda cursor, state, values: None)

    finalized = PostgresCurrentBarStore().finalize(
        CurrentBarFinalizationCandidate(market="SHSE", code="600519", interval_start=datetime(2026, 9, 2, 13, 30, tzinfo=SHANGHAI)),
        NativeCurrentStockBar(
            code="600519", interval_start=datetime(2026, 9, 2, 13, 30, tzinfo=SHANGHAI), native_trade_time="2026-09-02 13:30:00",
            open=1400.0, high=1402.0, low=1399.0, close=1401.0, volume=1500, amount=2_101_500.0,
            unit_conversion="mootdx:volume*1,amount*1",
        ),
        datetime(2026, 9, 2, 13, 31, 7, tzinfo=SHANGHAI),
        (CurrentBarNodeAttempt(code="600519", server="180.153.18.172:80", outcome="ok"),),
    )

    assert finalized is True
    assert any("insert into fact.stock_bar_1m" in statement for statement in statements)
    assert any("insert into readmodel.stock_bar_1m_daily_coverage" in statement for statement in statements)
    assert any("insert into audit.stock_bar_1m_write_event" in statement for statement in statements)
    assert any("update live.stock_bar_selected" in statement for statement in statements)
