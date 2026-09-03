from __future__ import annotations

from datetime import datetime, timedelta
from contextlib import nullcontext
import io
import json
import sys
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from quotemux.live_bars import (
    CurrentBarNodeAttempt,
    CurrentBarRequest,
    CurrentBarStagingResult,
    LiveBarIngestor,
    NativeCurrentStockBar,
    ProviderCurrentBarsResult,
    PostgresCurrentBarStore,
    CurrentBarFinalizationCandidate,
    FinalizedCorrectionCandidate,
    CurrentBarFinalizer,
    EFinancePriceValidator,
    WholeBarFallbackProvider,
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


def test_live_ingestor_keeps_a_zero_volume_bar_only_when_the_provider_supplies_the_exact_interval() -> None:
    interval = datetime(2026, 9, 2, 13, 30, tzinfo=SHANGHAI)

    class _NoTradeProvider:
        @staticmethod
        def fetch(codes, effective_now):
            assert codes == ("600519",)
            assert effective_now == interval + timedelta(seconds=8)
            return ProviderCurrentBarsResult(
                bars=(NativeCurrentStockBar(
                    code="600519", interval_start=interval, native_trade_time="2026-09-02 13:30:00",
                    open=1400.5, high=1400.5, low=1400.5, close=1400.5, volume=0, amount=0.0,
                    unit_conversion="mootdx:volume*1,amount*1",
                ),),
                attempts=(CurrentBarNodeAttempt(code="600519", server="180.153.18.172:80", outcome="ok"),),
            )

    store = _Store()
    result = LiveBarIngestor(_NoTradeProvider(), store, clock=lambda: interval + timedelta(seconds=9)).ingest(
        CurrentBarRequest(codes=("600519",), effective_now=interval + timedelta(seconds=8))
    )

    assert len(store.events) == 1
    assert result.errors == ()
    assert result.items[0].volume == 0
    assert result.items[0].amount == 0.0


def test_live_ingestor_stages_a_provider_native_current_30m_bar_before_returning_it() -> None:
    interval = datetime(2026, 9, 2, 13, 30, tzinfo=SHANGHAI)

    class _NativeThirtyMinuteProvider:
        @staticmethod
        def fetch(codes, effective_now, freq):
            assert codes == ("600519",)
            assert effective_now == interval + timedelta(minutes=2, seconds=8)
            assert freq == "30m"
            return ProviderCurrentBarsResult(
                bars=(NativeCurrentStockBar(
                    code="600519", interval_start=interval, native_trade_time="2026-09-02 13:30:00",
                    open=1400.0, high=1403.0, low=1399.0, close=1402.5, volume=350, amount=490555.0,
                    unit_conversion="mootdx:volume*1,amount*1", freq="30m",
                ),),
                attempts=(CurrentBarNodeAttempt(code="600519", server="180.153.18.172:80", outcome="ok"),),
            )

    store = _Store()
    result = LiveBarIngestor(_NativeThirtyMinuteProvider(), store, clock=lambda: interval + timedelta(minutes=2, seconds=9)).ingest(
        CurrentBarRequest(codes=("600519",), effective_now=interval + timedelta(minutes=2, seconds=8), freq="30m")
    )

    staged_bar, _observed_at, _attempts = store.events[0]
    assert staged_bar.freq == "30m"
    assert result.items[0].to_dict()["freq"] == "30m"
    assert result.items[0].interval_end == interval + timedelta(minutes=30)


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


def test_finalizer_due_scan_waits_until_the_minute_has_closed_and_grace_has_elapsed(monkeypatch) -> None:
    statements: list[str] = []

    class _Cursor:
        def execute(self, query, params):
            del params
            statements.append(" ".join(query.split()))

        @staticmethod
        def fetchall():
            return []

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

    class _Connection:
        closed = False

        @staticmethod
        def cursor():
            return _Cursor()

        @staticmethod
        def rollback():
            return None

    monkeypatch.setattr("quotemux.live_bars.db_client.is_db_available", lambda: True)
    monkeypatch.setattr("quotemux.live_bars.db_client._acquire_connection", _Connection)
    monkeypatch.setattr("quotemux.live_bars.db_client._release_connection", lambda current: None)

    assert PostgresCurrentBarStore().list_due(datetime(2026, 9, 2, 13, 31, 7, tzinfo=SHANGHAI), 7) == ()

    assert "case freq when '30m' then interval '30 minutes' else interval '1 minute' end" in statements[0]


def test_live_bar_retention_cleanup_keeps_the_latest_five_trading_days(monkeypatch) -> None:
    statements: list[str] = []

    class _Cursor:
        def execute(self, query, params=()):
            del params
            statements.append(" ".join(query.split()))

        @staticmethod
        def fetchone():
            return {"deleted_attempts": 3, "deleted_observations": 2, "deleted_selected": 1}

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

    class _Connection:
        closed = False

        @staticmethod
        def transaction():
            return nullcontext()

        @staticmethod
        def cursor():
            return _Cursor()

        @staticmethod
        def rollback():
            return None

    monkeypatch.setattr("quotemux.live_bars.db_client.is_db_available", lambda: True)
    monkeypatch.setattr("quotemux.live_bars.db_client._acquire_connection", _Connection)
    monkeypatch.setattr("quotemux.live_bars.db_client._release_connection", lambda current: None)

    result = PostgresCurrentBarStore().cleanup_retention(datetime(2026, 9, 9, 16, 0, tzinfo=SHANGHAI))

    assert result == {"deleted_attempts": 3, "deleted_observations": 2, "deleted_selected": 1}
    assert "offset 4" in statements[0]
    assert "delete from live.stock_bar_provider_attempt" in statements[0]


def test_correction_scan_keeps_only_finalized_same_provider_candidates_inside_window(monkeypatch) -> None:
    import pandas as pd

    captured: dict[str, object] = {}
    monkeypatch.setattr(
        "quotemux.live_bars.db_client.query_dataframe",
        lambda query, params: captured.update(query=query, params=params) or pd.DataFrame([{
            "market": "SHSE", "code": "600519", "freq": "1m", "interval_start": pd.Timestamp("2026-09-02 13:30:00"), "provider": "mootdx",
        }]),
    )
    now = datetime(2026, 9, 2, 13, 34, tzinfo=SHANGHAI)

    candidates = PostgresCurrentBarStore().list_correction_candidates(now, 300)

    assert [(item.code, item.freq, item.provider) for item in candidates] == [("600519", "1m", "mootdx")]
    assert "state='finalized'" in captured["query"]
    assert captured["params"] == (now, 300)


def test_correction_writes_revised_fact_and_audit(monkeypatch) -> None:
    statements: list[str] = []
    class _Cursor:
        def __init__(self): self.fetches = iter(({"provider": "mootdx", "observation_hash": "old"}, {"observation_version": 45}))
        def execute(self, query, params=()): del params; statements.append(" ".join(query.split()))
        def fetchone(self): return next(self.fetches)
        def __enter__(self): return self
        def __exit__(self, *args): return None
    class _Connection:
        closed = False
        def transaction(self): return nullcontext()
        def cursor(self): return _Cursor()
        def rollback(self): return None
    monkeypatch.setattr("quotemux.live_bars.db_client.is_db_available", lambda: True)
    monkeypatch.setattr("quotemux.live_bars.db_client._acquire_connection", lambda: _Connection())
    monkeypatch.setattr("quotemux.live_bars.db_client._release_connection", lambda connection: None)
    monkeypatch.setattr("quotemux.live_bars.db_client.discover_migration_range_journals", lambda cursor, table: type("Journal", (), {"has_active_journal": False})())
    monkeypatch.setattr("quotemux.live_bars.db_client.append_migration_range_journals", lambda cursor, state, values: None)
    interval = datetime(2026, 9, 2, 13, 30, tzinfo=SHANGHAI)
    candidate = FinalizedCorrectionCandidate("SHSE", "600519", interval, "1m", "mootdx")
    bar = NativeCurrentStockBar("600519", interval, "2026-09-02 13:30:00", 1400, 1403, 1399, 1402, 1500, 2103000, "mootdx:volume*1,amount*1")
    assert PostgresCurrentBarStore().correct(candidate, bar, interval + timedelta(minutes=1), ()) is True
    assert any("insert into fact.stock_bar_1m" in item for item in statements)
    assert any("quotemux.live_bar_corrector.provider_refetch" in item for item in statements)
    assert any("update live.stock_bar_selected" in item for item in statements)


def test_recovery_finalizes_overdue_staging_and_then_runs_retention(monkeypatch) -> None:
    observed = datetime(2026, 9, 9, 16, 0, tzinfo=SHANGHAI)
    calls: list[str] = []

    class _Store:
        @staticmethod
        def list_correction_candidates(now, window_seconds):
            assert now == observed and window_seconds == 300
            calls.append("corrections")
            return ()

        @staticmethod
        def cleanup_retention(now):
            assert now == observed
            calls.append("cleanup")
            return {"deleted_attempts": 3, "deleted_observations": 2, "deleted_selected": 1}

    class _Finalizer:
        def __init__(self, provider, store, grace_seconds):
            assert isinstance(store, _Store)
            assert grace_seconds == 7

        @staticmethod
        def finalize_due(now):
            assert now == observed
            calls.append("finalize")
            return {"candidates": 2, "finalized": 1, "deferred": 1, "failed": 0}

    monkeypatch.setattr("quotemux.live_bars.PostgresCurrentBarStore", _Store)
    monkeypatch.setattr("quotemux.live_bars.CurrentBarFinalizer", _Finalizer)

    from quotemux.live_bars import recover_due_current_stock_bars

    assert recover_due_current_stock_bars(observed) == {
        "finalizer": {"candidates": 2, "finalized": 1, "deferred": 1, "failed": 0},
        "corrections": {"candidates": 0, "corrected": 0, "unchanged": 0, "failed": 0},
        "retention": {"deleted_attempts": 3, "deleted_observations": 2, "deleted_selected": 1},
    }
    assert calls == ["finalize", "corrections", "cleanup"]


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


def test_postgres_finalization_writes_native_30m_to_its_own_fact_table(monkeypatch) -> None:
    statements: list[str] = []

    class _Cursor:
        def __init__(self): self.fetches = iter(({"state": "staged"}, {"observation_version": 44}))
        def execute(self, query, params=()):
            del params; statements.append(" ".join(query.split()))
        def fetchone(self): return next(self.fetches)
        def __enter__(self): return self
        def __exit__(self, *args): return None

    class _Connection:
        closed = False
        def transaction(self): return nullcontext()
        def cursor(self): return _Cursor()
        def rollback(self): return None

    monkeypatch.setattr("quotemux.live_bars.db_client.is_db_available", lambda: True)
    monkeypatch.setattr("quotemux.live_bars.db_client._acquire_connection", lambda: _Connection())
    monkeypatch.setattr("quotemux.live_bars.db_client._release_connection", lambda connection: None)
    monkeypatch.setattr("quotemux.live_bars.db_client.discover_migration_range_journals", lambda cursor, table: type("Journal", (), {"has_active_journal": False})())
    monkeypatch.setattr("quotemux.live_bars.db_client.append_migration_range_journals", lambda cursor, state, values: None)
    interval = datetime(2026, 9, 2, 13, 30, tzinfo=SHANGHAI)

    assert PostgresCurrentBarStore().finalize(
        CurrentBarFinalizationCandidate("SHSE", "600519", interval, "30m"),
        NativeCurrentStockBar("600519", interval, "2026-09-02 13:30:00", 1400, 1403, 1399, 1402.5, 350, 490555, "mootdx:volume*1,amount*1", freq="30m"),
        interval + timedelta(minutes=30, seconds=7),
        (CurrentBarNodeAttempt("600519", "mootdx", "ok"),),
    ) is True
    assert any("insert into fact.stock_bar_30m" in statement for statement in statements)
    assert not any("stock_bar_1m_daily_coverage" in statement for statement in statements)


def test_whole_bar_fallback_uses_opentdx_only_when_mootdx_has_no_valid_bar() -> None:
    interval = datetime(2026, 9, 2, 13, 30, tzinfo=SHANGHAI)

    class _Primary:
        def fetch(self, codes, effective_now):
            del codes, effective_now
            return ProviderCurrentBarsResult((), (CurrentBarNodeAttempt("600519", "mootdx", "malformed"),))

    class _Fallback:
        def fetch(self, codes, effective_now):
            assert codes == ("600519",)
            return ProviderCurrentBarsResult(
                (NativeCurrentStockBar("600519", interval, "2026-09-02 13:30:00", 1400.0, 1401.0, 1399.0, 1400.5, 1200, 1_680_600.0, "opentdx:volume*1,amount*1", provider="opentdx"),),
                (CurrentBarNodeAttempt("600519", "opentdx", "ok", provider="opentdx"),),
            )

    result = WholeBarFallbackProvider(_Primary(), _Fallback()).fetch(("600519",), interval)

    assert [(bar.provider, bar.close) for bar in result.bars] == [("opentdx", 1400.5)]
    assert [attempt.provider for attempt in result.attempts] == ["mootdx", "opentdx"]


def test_severe_efinance_price_mismatch_replaces_the_whole_selected_bar() -> None:
    interval = datetime(2026, 9, 2, 13, 30, tzinfo=SHANGHAI)
    primary_bar = NativeCurrentStockBar("600519", interval, "2026-09-02 13:30:00", 1400.0, 1401.0, 1399.0, 1400.5, 1200, 1_680_600.0, "mootdx:volume*1,amount*1")
    fallback_bar = NativeCurrentStockBar("600519", interval, "2026-09-02 13:30:00", 1390.0, 1400.0, 1389.0, 1395.0, 1300, 1_813_500.0, "opentdx:volume*1,amount*1", provider="opentdx")

    class _Provider:
        def __init__(self, result): self.result = result
        def fetch(self, codes, effective_now):
            del codes, effective_now
            return self.result

    class _SevereValidator:
        def validate(self, bars, effective_now):
            del bars, effective_now
            return ({"code": "600519", "validator": "efinance", "status": "severe", "price": 1300.0},)

    result = WholeBarFallbackProvider(
        _Provider(ProviderCurrentBarsResult((primary_bar,), ())),
        _Provider(ProviderCurrentBarsResult((fallback_bar,), ())),
        _SevereValidator(),
    ).fetch(("600519",), interval)

    assert [(bar.provider, bar.close) for bar in result.bars] == [("opentdx", 1395.0)]
    assert result.diagnostics[0]["status"] == "severe"


def test_efinance_validation_reports_warning_without_replacing_a_whole_bar(monkeypatch) -> None:
    interval = datetime(2026, 9, 2, 13, 30, tzinfo=SHANGHAI)
    bar = NativeCurrentStockBar("600519", interval, "2026-09-02 13:30:00", 1400.0, 1401.0, 1399.0, 1400.5, 1200, 1_680_600.0, "mootdx:volume*1,amount*1")

    class _Registry:
        @staticmethod
        def get_handler(package_id, capability):
            assert (package_id, capability) == ("efinance", "get_current_stock_price_snapshots")
            return lambda codes, effective_now: [SimpleNamespace(code=codes[0], price=1370.0, source_time=effective_now)]

    monkeypatch.setattr("quotemux.live_bars.get_default_source_package_registry", lambda: _Registry())

    diagnostic = EFinancePriceValidator(warning_ratio=0.01, severe_ratio=0.05).validate((bar,), interval + timedelta(seconds=8))[0]

    assert diagnostic["status"] == "warning"
    assert diagnostic["difference_ratio"] == pytest.approx(0.0222627737)


def test_efinance_validation_does_not_use_a_stale_snapshot_for_source_selection(monkeypatch) -> None:
    interval = datetime(2026, 9, 2, 13, 30, tzinfo=SHANGHAI)
    bar = NativeCurrentStockBar("600519", interval, "2026-09-02 13:30:00", 1400.0, 1401.0, 1399.0, 1400.5, 1200, 1_680_600.0, "mootdx:volume*1,amount*1")

    class _Registry:
        @staticmethod
        def get_handler(package_id, capability):
            assert (package_id, capability) == ("efinance", "get_current_stock_price_snapshots")
            return lambda codes, effective_now: [SimpleNamespace(code=codes[0], price=1300.0, source_time="2026-09-02T13:20:00+08:00")]

    monkeypatch.setattr("quotemux.live_bars.get_default_source_package_registry", lambda: _Registry())

    diagnostic = EFinancePriceValidator(max_age_seconds=300).validate((bar,), interval + timedelta(seconds=8))[0]

    assert diagnostic["status"] == "stale"


def test_efinance_validation_marks_a_malformed_snapshot_unavailable(monkeypatch) -> None:
    interval = datetime(2026, 9, 2, 13, 30, tzinfo=SHANGHAI)
    bar = NativeCurrentStockBar("600519", interval, "2026-09-02 13:30:00", 1400.0, 1401.0, 1399.0, 1400.5, 1200, 1_680_600.0, "mootdx:volume*1,amount*1")

    class _Registry:
        @staticmethod
        def get_handler(package_id, capability):
            assert (package_id, capability) == ("efinance", "get_current_stock_price_snapshots")
            return lambda codes, effective_now: [SimpleNamespace(code=codes[0], price="not-a-price", source_time=effective_now)]

    monkeypatch.setattr("quotemux.live_bars.get_default_source_package_registry", lambda: _Registry())

    diagnostic = EFinancePriceValidator().validate((bar,), interval + timedelta(seconds=8))[0]

    assert diagnostic["status"] == "unavailable"
    assert diagnostic["detail"] == "invalid_snapshot_price"


def test_worker_recover_flag_does_not_require_shell_encoded_json(monkeypatch, capsys) -> None:
    from quotemux import live_bars_worker

    monkeypatch.setattr(sys, "argv", ["quotemux.live_bars_worker", "--recover"])
    monkeypatch.setattr(sys, "stdin", io.StringIO("{not-json}"))
    monkeypatch.setattr(
        live_bars_worker,
        "recover_due_current_stock_bars",
        lambda effective_now: {"recovered": True, "effective_now": effective_now},
    )

    assert live_bars_worker.main() == 0
    assert json.loads(capsys.readouterr().out) == {"recovered": True, "effective_now": None}
