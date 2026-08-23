from __future__ import annotations

from datetime import datetime

import pandas as pd

from quotemux.store import capture
from quotemux.store.capture import CaptureRun, CaptureRunMaintenance


def _running(run_id: int, capability_id: str, started_at: datetime) -> CaptureRun:
    return CaptureRun(
        id=run_id,
        capability_id=capability_id,
        status=capture.CAPTURE_RUNNING,
        planned_time=started_at,
        started_at=started_at,
        finished_at=None,
        row_count=0,
        coverage_count=0,
        error_message="",
        detail_json={"phase": "provider"},
    )


class _RunRepository:
    def __init__(self, rows: tuple[CaptureRun, ...]) -> None:
        self.rows = rows
        self.failed_ids: tuple[int, ...] = ()

    def list_running_started_before(self, started_before: datetime, capability_id: str = "") -> tuple[CaptureRun, ...]:
        return tuple(
            row
            for row in self.rows
            if row.started_at <= started_before and (capability_id == "" or row.capability_id == capability_id)
        )

    def fail_stale_running(self, run_ids: tuple[int, ...], detail_json: dict[str, object]) -> bool:
        self.failed_ids = run_ids
        return True


class _Lock:
    def __init__(self, available: bool) -> None:
        self.available = available
        self.released = False

    def acquire(self) -> bool:
        return self.available

    def release(self) -> None:
        self.released = True


class _Locks:
    def __init__(self, availability: dict[str, bool]) -> None:
        self.availability = availability
        self.items: dict[str, _Lock] = {}

    def create(self, capability_id: str) -> _Lock:
        lock = _Lock(self.availability[capability_id])
        self.items[capability_id] = lock
        return lock


def test_reconcile_marks_unlocked_running_rows_failed() -> None:
    now = datetime(2026, 8, 23, 13, 0)
    runs = _RunRepository((_running(861, "stocks.indicators.money_flow", datetime(2026, 8, 23, 4, 0)),))
    locks = _Locks({"stocks.indicators.money_flow": True})

    result = CaptureRunMaintenance(runs=runs, locks=locks, now_provider=lambda: now).reconcile_stale_running()

    assert result["candidate_count"] == 1
    assert result["reconciled_run_ids"] == [861]
    assert result["active_capability_ids"] == []
    assert runs.failed_ids == (861,)
    assert locks.items["stocks.indicators.money_flow"].released is True


def test_reconcile_does_not_touch_running_rows_while_an_instance_holds_lock() -> None:
    now = datetime(2026, 8, 23, 13, 0)
    runs = _RunRepository((_running(862, "stocks.quotes.intraday", datetime(2026, 8, 23, 12, 0)),))
    locks = _Locks({"stocks.quotes.intraday": False})

    result = CaptureRunMaintenance(runs=runs, locks=locks, now_provider=lambda: now).reconcile_stale_running()

    assert result["reconciled_run_ids"] == []
    assert result["active_capability_ids"] == ["stocks.quotes.intraday"]
    assert runs.failed_ids == ()
    assert locks.items["stocks.quotes.intraday"].released is False


def test_reconcile_rechecks_rows_after_acquiring_lock() -> None:
    now = datetime(2026, 8, 23, 13, 0)

    class _FinishedDuringAcquireRepository(_RunRepository):
        def __init__(self, row: CaptureRun) -> None:
            super().__init__((row,))
            self.calls = 0

        def list_running_started_before(self, started_before: datetime, capability_id: str = "") -> tuple[CaptureRun, ...]:
            self.calls += 1
            return self.rows if self.calls == 1 else ()

    runs = _FinishedDuringAcquireRepository(_running(863, "stocks.quotes.daily", datetime(2026, 8, 23, 12, 0)))
    locks = _Locks({"stocks.quotes.daily": True})

    result = CaptureRunMaintenance(runs=runs, locks=locks, now_provider=lambda: now).reconcile_stale_running()

    assert result["reconciled_run_ids"] == []
    assert runs.failed_ids == ()
    assert locks.items["stocks.quotes.daily"].released is True


def test_capture_run_repository_uses_bounded_conditional_update(monkeypatch) -> None:
    started_before = datetime(2026, 8, 23, 13, 0)
    row = _running(861, "stocks.indicators.money_flow", datetime(2026, 8, 23, 4, 0))
    queries: list[tuple[str, tuple[object, ...]]] = []
    updates: list[tuple[str, tuple[object, ...]]] = []
    monkeypatch.setattr(capture, "_ensure_capture_schema", lambda: True)

    def fake_query(query: str, params: tuple[object, ...]) -> pd.DataFrame:
        queries.append((query, params))
        return pd.DataFrame(
            [
                {
                    "id": row.id,
                    "capability_id": row.capability_id,
                    "status": row.status,
                    "planned_time": row.planned_time,
                    "started_at": row.started_at,
                    "finished_at": row.finished_at,
                    "row_count": row.row_count,
                    "coverage_count": row.coverage_count,
                    "error_message": row.error_message,
                    "detail_json": row.detail_json,
                }
            ]
        )

    monkeypatch.setattr(capture, "query_dataframe", fake_query)
    monkeypatch.setattr(capture, "execute_sql", lambda query, params=(): updates.append((query, params)) is None or True)
    repository = capture.CaptureRunRepository()

    assert repository.list_running_started_before(started_before, row.capability_id) == (row,)
    assert repository.fail_stale_running((row.id,), {"reason": "stale_running_after_process_exit"}) is True

    select_query, select_params = queries[0]
    update_query, update_params = updates[0]
    assert "status = 'running'" in select_query
    assert "started_at <= %s" in select_query
    assert select_params == (row.capability_id, started_before)
    assert "where id = any(%s) and status = 'running'" in update_query
    assert update_params[1] == [row.id]
