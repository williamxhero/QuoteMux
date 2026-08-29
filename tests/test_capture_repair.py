from __future__ import annotations

from datetime import datetime, time

import pandas as pd
import pytest

from quotemux.store.capture import CAPTURE_FAILED, CAPTURE_SKIPPED, CAPTURE_SUCCESS, CaptureExecutionResult, CapturePolicy, CaptureRun, QuoteMuxCaptureJob
from quotemux.store.admin import QuoteMuxCaptureAdmin


class _Policies:
    def __init__(self, policy: CapturePolicy) -> None:
        self.policy = policy

    def get(self, capability_id: str) -> CapturePolicy | None:
        return self.policy if capability_id == self.policy.capability_id else None


class _Runs:
    def __init__(self, existing: CaptureRun | None = None, existing_fingerprint: str = "") -> None:
        self.existing = existing
        self.existing_fingerprint = existing_fingerprint
        self.fingerprints: list[tuple[str, str]] = []
        self.finished: list[CaptureRun] = []

    def latest_success_for_repair_fingerprint(self, capability_id: str, fingerprint: str) -> CaptureRun | None:
        self.fingerprints.append((capability_id, fingerprint))
        return self.existing if self.existing_fingerprint in {"", fingerprint} else None

    def get_by_id(self, run_id: int) -> CaptureRun | None:
        return self.existing if self.existing is not None and self.existing.id == run_id else None

    def create(self, capability_id: str, status: str, planned_time: datetime, detail_json: dict[str, object]) -> CaptureRun:
        return CaptureRun(99, capability_id, status, planned_time, planned_time, None, 0, 0, "", detail_json)

    def finish(
        self,
        run_id: int,
        status: str,
        row_count: int,
        coverage_count: int,
        error_message: str,
        detail_json: dict[str, object],
    ) -> None:
        self.finished.append(CaptureRun(run_id, "stocks.quotes.intraday", status, datetime(2026, 8, 23, 12), datetime(2026, 8, 23, 12), datetime(2026, 8, 23, 12), row_count, coverage_count, error_message, detail_json))


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
    def __init__(self, write_enabled: bool = True) -> None:
        self.write_enabled = write_enabled


class _Cache:
    def __init__(self, write_enabled: bool = True) -> None:
        self.write_enabled = write_enabled

    def get_policy(self, _capability_id: str) -> _CachePolicy:
        return _CachePolicy(self.write_enabled)


def _policy(capability_id: str = "stocks.quotes.intraday", enabled: bool = True) -> CapturePolicy:
    return CapturePolicy(capability_id, enabled, "daily", time(20), "Asia/Shanghai", None, None, None, "active_stocks_recent_trading_days", 5, 100, "")


def test_admin_repair_reuses_successful_dataset_scope_fingerprint() -> None:
    planned = datetime(2026, 8, 23, 12)
    existing = CaptureRun(7, "stocks.quotes.intraday", CAPTURE_SUCCESS, planned, planned, planned, 48000, 48000, "", {"repair_fingerprint": "stored"})
    runs = _Runs(existing)
    locks = _Locks()
    job = QuoteMuxCaptureJob(runtime=object(), policies=_Policies(_policy()), runs=runs, locks=locks, cache_store=_Cache())
    runs.existing_fingerprint = job.repair_fingerprint(
        "stocks.quotes.intraday",
        {"codes": ["600000", "000001"], "start_date": "2026-08-21", "end_date": "2026-08-21"},
        "bars-v2",
    )

    result = job.run_repair("stocks.quotes.intraday", {"codes": ["600000", "000001"], "start_date": "2026-08-21", "end_date": "2026-08-21"}, "bars-v2")

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
    left = job.run_repair("stocks.quotes.intraday", {"end_date": "2026-08-21", "codes": ["600000", "000001"], "start_date": "2026-08-21", "dataset_version": "bars-v2"})
    right_fingerprint = job.repair_fingerprint("stocks.quotes.intraday", {"codes": ["600000", "000001"], "start_date": "2026-08-21", "end_date": "2026-08-21"}, "bars-v2")

    assert left["status"] == CAPTURE_SUCCESS
    assert captured["detail"]["mode"] == "repair"
    assert captured["detail"]["repair_fingerprint"] == right_fingerprint
    assert captured["detail"]["repair_dataset_version"] == "bars-v2"
    request = captured["requests"][0]
    assert request.capability_id == "stocks.quotes.intraday"
    assert request.request_identity["codes"] == ["000001", "600000"]
    assert "dataset_version" not in request.request_identity


def test_repair_runs_locked_precondition_before_reuse_or_capture_and_releases_on_failure(monkeypatch) -> None:
    runs = _Runs()
    locks = _Locks()
    job = QuoteMuxCaptureJob(runtime=object(), policies=_Policies(_policy()), runs=runs, locks=locks, cache_store=_Cache())
    events: list[str] = []

    def precondition() -> None:
        assert locks.value.released is False
        events.append("precondition")

    def fake_run(*_args, **_kwargs):
        events.append("capture")
        locks.value.release()
        return {"status": CAPTURE_SUCCESS}

    monkeypatch.setattr(job, "_run_capture_requests", fake_run)
    assert job.run_repair(
        "stocks.quotes.intraday", {"codes": ["600000"], "start_date": "2026-08-21", "end_date": "2026-08-21"},
        "bars-v2", precondition,
    )["status"] == CAPTURE_SUCCESS
    assert events == ["precondition", "capture"]

    failing_locks = _Locks()
    failing_job = QuoteMuxCaptureJob(runtime=object(), policies=_Policies(_policy()), runs=_Runs(), locks=failing_locks, cache_store=_Cache())
    with pytest.raises(RuntimeError, match="stale"):
        failing_job.run_repair(
            "stocks.quotes.intraday", {"codes": ["600000"], "start_date": "2026-08-21", "end_date": "2026-08-21"},
            "bars-v2", lambda: (_ for _ in ()).throw(RuntimeError("stale dataset version")),
        )
    assert failing_locks.value.released is True


def test_capture_admin_preserves_legacy_three_argument_repair_job_call() -> None:
    class LegacyJob:
        def __init__(self) -> None:
            self.args: tuple[object, ...] = ()

        def run_repair(self, dataset, scope, dataset_version):
            self.args = (dataset, scope, dataset_version)
            return {"status": CAPTURE_SUCCESS}

    job = LegacyJob()
    admin = QuoteMuxCaptureAdmin(job=job)

    assert admin.run_repair("stocks.quotes.intraday", {"codes": ["600000"]}, "bars-v2")["status"] == CAPTURE_SUCCESS
    assert job.args == ("stocks.quotes.intraday", {"codes": ["600000"]}, "bars-v2")


def test_repair_different_dataset_version_does_not_reuse_previous_success(monkeypatch) -> None:
    planned = datetime(2026, 8, 23, 12)
    existing = CaptureRun(7, "stocks.quotes.intraday", CAPTURE_SUCCESS, planned, planned, planned, 48000, 48000, "", {"mode": "repair"})
    runs = _Runs(existing)
    locks = _Locks()
    job = QuoteMuxCaptureJob(runtime=object(), policies=_Policies(_policy()), runs=runs, locks=locks, cache_store=_Cache())
    scope = {"codes": ["600000"], "start_date": "2026-08-21", "end_date": "2026-08-21"}
    runs.existing_fingerprint = job.repair_fingerprint("stocks.quotes.intraday", scope, "bars-v1")
    monkeypatch.setattr(job, "_run_capture_requests", lambda *_args, **_kwargs: {"status": CAPTURE_SUCCESS})

    assert job.run_repair("stocks.quotes.intraday", scope, "bars-v2")["status"] == CAPTURE_SUCCESS
    assert runs.fingerprints == [("stocks.quotes.intraday", job.repair_fingerprint("stocks.quotes.intraday", scope, "bars-v2"))]


def test_repair_status_can_be_loaded_by_run_id() -> None:
    planned = datetime(2026, 8, 23, 12)
    existing = CaptureRun(7, "stocks.quotes.intraday", CAPTURE_SUCCESS, planned, planned, planned, 48000, 48000, "", {"mode": "repair", "repair_dataset_version": "bars-v2"})
    job = QuoteMuxCaptureJob(runtime=object(), policies=_Policies(_policy()), runs=_Runs(existing), locks=_Locks(), cache_store=_Cache())

    result = job.get_repair_run(7)

    assert result["id"] == 7
    assert result["detail_json"]["repair_dataset_version"] == "bars-v2"


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


def test_explicit_repair_bypasses_disabled_scheduler_but_not_cache_write_gate(monkeypatch) -> None:
    runs = _Runs()
    locks = _Locks()
    job = QuoteMuxCaptureJob(runtime=object(), policies=_Policies(_policy(enabled=False)), runs=runs, locks=locks, cache_store=_Cache())
    captured: dict[str, object] = {}
    monkeypatch.setattr(job, "_run_capture_requests", lambda *args, **kwargs: captured.update(args=args, kwargs=kwargs) or {"status": CAPTURE_SUCCESS})

    assert job.run_repair("stocks.quotes.intraday", {"codes": ["600000"]})["status"] == CAPTURE_SUCCESS
    assert captured["args"][0].enabled is False

    blocked = QuoteMuxCaptureJob(runtime=object(), policies=_Policies(_policy(enabled=False)), runs=_Runs(), locks=_Locks(), cache_store=_Cache(write_enabled=False))
    result = blocked.run_repair("stocks.quotes.intraday", {"codes": ["600000"]})
    assert result["status"] == CAPTURE_SKIPPED
    assert result["detail_json"]["reason"] == "cache_policy_disabled"


def test_daily_quote_repair_uses_the_formal_capture_path(monkeypatch) -> None:
    job = QuoteMuxCaptureJob(
        runtime=object(),
        policies=_Policies(_policy("stocks.quotes.daily")),
        runs=_Runs(),
        locks=_Locks(),
        cache_store=_Cache(),
    )
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        job,
        "_run_capture_requests",
        lambda policy, requests, detail, *_args, **_kwargs: captured.update(
            policy=policy, requests=requests, detail=detail
        ) or {"status": CAPTURE_SUCCESS, "detail_json": detail},
    )

    result = job.run_repair(
        "stocks.quotes.daily",
        {"codes": ["000635"], "freq": "1d", "start_date": "2026-08-28", "end_date": "2026-08-28"},
        "mhd-v1-repair-baseline",
    )

    assert result["status"] == CAPTURE_SUCCESS
    assert captured["requests"][0].capability_id == "stocks.quotes.daily"
    assert captured["requests"][0].request_identity["codes"] == ["000635"]
    assert captured["detail"]["mode"] == "repair"


def test_repair_without_executable_path_skips_closed_even_if_scheduler_is_disabled() -> None:
    job = QuoteMuxCaptureJob(
        runtime=object(),
        policies=_Policies(_policy("futures.quotes.back_adjusted_continuous.1m", enabled=False)),
        runs=_Runs(),
        locks=_Locks(),
        cache_store=_Cache(),
    )

    result = job.run_repair("futures.quotes.back_adjusted_continuous.1m", {"repair_registry_id": "repair-evidence-001"})

    assert result["status"] == CAPTURE_SKIPPED
    assert result["detail_json"]["reason"] == "repair_path_unavailable"


def test_back_adjusted_repair_uses_only_registered_evidence_and_capture_workflow(monkeypatch) -> None:
    from quotemux.store import futures_back_adjusted_repair

    class Evidence:
        def __init__(self) -> None:
            self.ids: list[str] = []

        def resolve(self, registry_id: str):
            self.ids.append(registry_id)
            return b"immutable-artifact", {"frozen_dataset_version": "mhd-v1"}

    class Guard:
        def require_current(self, capability_id: str, expected_version: str) -> str:
            assert capability_id == "futures.quotes.back_adjusted_continuous.1m"
            assert expected_version == "mhd-v1"
            return expected_version

    class Publisher:
        def __init__(self, guard) -> None:
            assert isinstance(guard, Guard)

        def publish(self, artifact: bytes, manifest: dict[str, object]) -> dict[str, object]:
            assert artifact == b"immutable-artifact"
            assert manifest["frozen_dataset_version"] == "mhd-v1"
            return {"status": "success", "row_count": 2}

    evidence = Evidence()
    monkeypatch.setattr(futures_back_adjusted_repair, "FuturesBackAdjustedRepairPublisher", Publisher)
    job = QuoteMuxCaptureJob(
        runtime=object(),
        policies=_Policies(_policy("futures.quotes.back_adjusted_continuous.1m", enabled=False)),
        runs=_Runs(),
        locks=_Locks(),
        cache_store=_Cache(),
        back_adjusted_repair_evidence=evidence,
        dataset_version_guard=Guard(),
    )

    result = job.run_repair("futures.quotes.back_adjusted_continuous.1m", {"repair_registry_id": "repair-evidence-001"}, "mhd-v1")

    assert result["status"] == CAPTURE_SUCCESS
    assert result["row_count"] == result["coverage_count"] == 2
    assert result["detail_json"]["repair_scope"] == {"repair_registry_id": "repair-evidence-001"}
    assert evidence.ids == ["repair-evidence-001"]


def test_back_adjusted_repair_rejects_unmanaged_scope_before_any_evidence_lookup() -> None:
    job = QuoteMuxCaptureJob(
        runtime=object(),
        policies=_Policies(_policy("futures.quotes.back_adjusted_continuous.1m")),
        runs=_Runs(),
        locks=_Locks(),
        cache_store=_Cache(),
    )

    with pytest.raises(ValueError, match="repair_registry_id"):
        job.run_repair("futures.quotes.back_adjusted_continuous.1m", {"artifact_path": "C:/untrusted.json"})


def test_back_adjusted_repair_fails_before_publish_when_registry_version_is_stale(monkeypatch) -> None:
    class Evidence:
        def resolve(self, _registry_id: str):
            return b"artifact", {"frozen_dataset_version": "mhd-v1"}

    class Guard:
        def require_current(self, *_args):
            pytest.fail("stale evidence must not reach dataset version guard")

    job = QuoteMuxCaptureJob(
        runtime=object(),
        policies=_Policies(_policy("futures.quotes.back_adjusted_continuous.1m")),
        runs=_Runs(),
        locks=_Locks(),
        cache_store=_Cache(),
        back_adjusted_repair_evidence=Evidence(),
        dataset_version_guard=Guard(),
    )

    result = job.run_repair("futures.quotes.back_adjusted_continuous.1m", {"repair_registry_id": "repair-evidence-001"}, "mhd-v2")

    assert result["status"] == CAPTURE_FAILED
    assert "does not match requested dataset_version" in result["detail_json"]["failed_batches"][0]["error"]


def test_explicit_repair_with_zero_return_or_write_is_failed(monkeypatch) -> None:
    job = QuoteMuxCaptureJob(runtime=object(), policies=_Policies(_policy()), runs=_Runs(), locks=_Locks(), cache_store=_Cache())
    monkeypatch.setattr(job, "_execute_requests", lambda *_args: CaptureExecutionResult(0, 0, (), ()))

    result = job.run_repair("stocks.quotes.intraday", {"codes": ["600000"]})

    assert result["status"] == CAPTURE_FAILED
    errors = [item["error"] for item in result["detail_json"]["failed_batches"]]
    assert errors == ["repair returned zero rows", "repair wrote zero rows"]
