from __future__ import annotations

from datetime import datetime, time

import pandas as pd

from quotemux.store.capture import CAPTURE_SUCCESS, CapturePolicy, CaptureRun, QuoteMuxCaptureJob


class _Policies:
    def __init__(self, policy: CapturePolicy) -> None:
        self.policy = policy

    def get(self, capability_id: str) -> CapturePolicy | None:
        return self.policy if capability_id == self.policy.capability_id else None


class _Runs:
    def __init__(self, existing: CaptureRun | None = None) -> None:
        self.existing = existing
        self.fingerprints: list[tuple[str, str]] = []

    def latest_success_for_repair_fingerprint(self, capability_id: str, fingerprint: str) -> CaptureRun | None:
        self.fingerprints.append((capability_id, fingerprint))
        return self.existing


class _Lock:
    def __init__(self) -> None:
        self.released = False

    def acquire(self) -> bool:
        return True

    def release(self) -> None:
        self.released = True


class _Locks:
    def __init__(self) -> None:
        self.value = _Lock()

    def create(self, _capability_id: str) -> _Lock:
        return self.value


class _CachePolicy:
    write_enabled = True


class _Cache:
    def get_policy(self, _capability_id: str) -> _CachePolicy:
        return _CachePolicy()


def _policy() -> CapturePolicy:
    return CapturePolicy("stocks.quotes.intraday", True, "daily", time(20), "Asia/Shanghai", None, None, None, "active_stocks_recent_trading_days", 5, 100, "")


def test_admin_repair_reuses_successful_dataset_scope_fingerprint() -> None:
    planned = datetime(2026, 8, 23, 12)
    existing = CaptureRun(7, "stocks.quotes.intraday", CAPTURE_SUCCESS, planned, planned, planned, 48000, 48000, "", {"repair_fingerprint": "stored"})
    runs = _Runs(existing)
    locks = _Locks()
    job = QuoteMuxCaptureJob(runtime=object(), policies=_Policies(_policy()), runs=runs, locks=locks, cache_store=_Cache())

    result = job.run_repair("stocks.quotes.intraday", {"codes": ["600000", "000001"], "start_date": "2026-08-21", "end_date": "2026-08-21"})

    assert result["id"] == 7
    assert result["repair_reused"] is True
    assert locks.value.released is True
    assert len(runs.fingerprints) == 1


def test_admin_repair_uses_canonical_scope_fingerprint_and_existing_capture_executor(monkeypatch) -> None:
    runs = _Runs()
    locks = _Locks()
    job = QuoteMuxCaptureJob(runtime=object(), policies=_Policies(_policy()), runs=runs, locks=locks, cache_store=_Cache())
    captured: dict[str, object] = {}

    def fake_run(policy, requests, detail, planned_time=None, acquired_lock=None):
        captured.update(policy=policy, requests=requests, detail=detail, planned_time=planned_time, acquired_lock=acquired_lock)
        acquired_lock.release()
        return {"status": CAPTURE_SUCCESS, "detail_json": detail}

    monkeypatch.setattr(job, "_run_capture_requests", fake_run)
    left = job.run_repair("stocks.quotes.intraday", {"end_date": "2026-08-21", "codes": ["600000", "000001"], "start_date": "2026-08-21"})
    right_fingerprint = job.repair_fingerprint("stocks.quotes.intraday", {"codes": ["600000", "000001"], "start_date": "2026-08-21", "end_date": "2026-08-21"})

    assert left["status"] == CAPTURE_SUCCESS
    assert captured["detail"]["mode"] == "repair"
    assert captured["detail"]["repair_fingerprint"] == right_fingerprint
    request = captured["requests"][0]
    assert request.capability_id == "stocks.quotes.intraday"
    assert request.request_identity["codes"] == ["000001", "600000"]


def test_repair_idempotency_lookup_uses_successful_persisted_run(monkeypatch) -> None:
    from quotemux.store import capture

    captured: dict[str, object] = {}
    monkeypatch.setattr(capture, "_ensure_capture_schema", lambda: True)

    def fake_query(query: str, params: tuple[object, ...]) -> pd.DataFrame:
        captured.update(query=query, params=params)
        return pd.DataFrame()

    monkeypatch.setattr(capture, "query_dataframe", fake_query)

    assert capture.CaptureRunRepository().latest_success_for_repair_fingerprint("stocks.quotes.intraday", "abc") is None
    query = str(captured["query"]).lower()
    assert "status = 'success'" in query
    assert "detail_json ->> 'repair_fingerprint' = %s" in query
    assert captured["params"] == ("stocks.quotes.intraday", "abc")
