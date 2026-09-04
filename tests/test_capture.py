from __future__ import annotations

from dataclasses import replace
from datetime import datetime, time
from zoneinfo import ZoneInfo

import pytest

from quotemux.reports import ContractReport
from quotemux.models import ConceptAliasGroupItem
from platform_models import ConceptQuoteItem, FutureContractCatalogItem, StockQuoteCodeSummary, StockQuoteItem, StockQuotesMeta, StockQuotesQueryResult
from quotemux.store import capture
from quotemux.capabilities import is_independently_configurable_capability_id, list_capability_ids
from quotemux.store.capture import (
    CADENCE_DAILY,
    CADENCE_MONTHLY,
    CADENCE_WEEKLY,
    CADENCE_YEARLY,
    CAPTURE_FAILED,
    CAPTURE_PARTIAL,
    CAPTURE_SKIPPED,
    CAPTURE_SUCCESS,
    PROFILE_CATALOG_SNAPSHOT,
    PROFILE_ACTIVE_STOCKS_RECENT_TRADING_DAYS,
    PROFILE_CONCEPTS_RECENT_TRADING_DAYS,
    PROFILE_DAILY_SNAPSHOT_RECENT_TRADING_DAYS,
    PROFILE_INDEXES_RECENT_TRADING_DAYS,
    PROFILE_TRADING_CALENDAR_YEAR_WINDOW,
    DEFAULT_CAPTURE_POLICY_SPECS,
    RUNTIME_METHODS,
    CapturePolicy,
    CapturePolicyUpdate,
    CaptureRun,
    PostgresAdvisoryLock,
    QuoteMuxCaptureJob,
    is_capture_due,
)
from quotemux.store.capture_gaps import CaptureGap


def _policy(**overrides: object) -> CapturePolicy:
    payload = {
        "capability_id": "stocks.quotes.daily",
        "enabled": True,
        "cadence": CADENCE_DAILY,
        "run_time": time(18, 0),
        "timezone": "Asia/Shanghai",
        "weekday": None,
        "month": None,
        "month_day": None,
        "scope_profile": PROFILE_ACTIVE_STOCKS_RECENT_TRADING_DAYS,
        "window_count": 30,
        "batch_size": 100,
        "notes": "",
    }
    payload.update(overrides)
    return CapturePolicy(**payload)


class MemoryCapturePolicies:
    def __init__(self, policies: tuple[CapturePolicy, ...]) -> None:
        self.items = {policy.capability_id: policy for policy in policies}

    def list(self) -> tuple[CapturePolicy, ...]:
        return tuple(self.items.values())

    def get(self, capability_id: str) -> CapturePolicy | None:
        return self.items.get(capability_id)

    def update(self, policy: CapturePolicy) -> bool:
        self.items[policy.capability_id] = policy
        return True


class MemoryCaptureRuns:
    def __init__(self) -> None:
        self.items: list[CaptureRun] = []
        self.next_id = 1

    def list(self, capability_id: str = "", status: str = "", limit: int = 100) -> tuple[CaptureRun, ...]:
        items = self.items
        if capability_id != "":
            items = [item for item in items if item.capability_id == capability_id]
        if status != "":
            items = [item for item in items if item.status == status]
        return tuple(reversed(items[-limit:]))

    def latest_for_planned_time(self, capability_id: str, planned_time: datetime) -> CaptureRun | None:
        matches = [item for item in self.items if item.capability_id == capability_id and item.planned_time == planned_time]
        return matches[-1] if matches else None

    def latest_success_for_repair_fingerprint(self, _capability_id: str, _fingerprint: str) -> CaptureRun | None:
        return None

    def create(self, capability_id: str, status: str, planned_time: datetime, detail_json: dict[str, object]) -> CaptureRun:
        run = CaptureRun(self.next_id, capability_id, status, planned_time, planned_time, None, 0, 0, "", detail_json)
        self.next_id += 1
        self.items.append(run)
        return run

    def finish(self, run_id: int, status: str, row_count: int, coverage_count: int, error_message: str, detail_json: dict[str, object]) -> bool:
        for index, run in enumerate(self.items):
            if run.id == run_id:
                self.items[index] = replace(run, status=status, finished_at=run.started_at, row_count=row_count, coverage_count=coverage_count, error_message=error_message, detail_json=detail_json)
                return True
        return False

    def update_progress(self, run_id: int, detail_json: dict[str, object]) -> bool:
        for index, run in enumerate(self.items):
            if run.id == run_id and run.status == "running":
                self.items[index] = replace(run, detail_json=detail_json)
                return True
        return False

    def finalize_catalog_repair_publication(self, run_id: int, publication: dict[str, object]) -> CaptureRun:
        for index, run in enumerate(self.items):
            native = run.detail_json.get("publication", {})
            if run.id != run_id:
                continue
            if (
                run.capability_id != "futures.contracts.catalog" or run.status != CAPTURE_SUCCESS
                or run.detail_json.get("mode") != "repair"
                or native.get("snapshot_id") != publication.get("snapshot_id")
                or native.get("content_checksum") != publication.get("content_checksum")
            ):
                raise ValueError("publication mismatch")
            updated = replace(run, detail_json={**run.detail_json, "publication": publication})
            self.items[index] = updated
            return updated
        raise ValueError("unknown run")


class FakeLock:
    def __init__(self, locked: bool) -> None:
        self.locked = locked
        self.released = False

    def acquire(self) -> bool:
        return self.locked

    def release(self) -> None:
        self.released = True


class FakeLocks:
    def __init__(self, locked: bool = True) -> None:
        self.lock = FakeLock(locked)

    def create(self, capability_id: str) -> FakeLock:
        return self.lock


class FakeCachePolicy:
    enabled = True
    write_enabled = True


class FakeCacheStore:
    def get_policy(self, capability_id: str) -> FakeCachePolicy:
        return FakeCachePolicy()


class FakeDisabledCacheStore:
    def get_policy(self, capability_id: str):
        class Policy:
            enabled = True
            write_enabled = False

        return Policy()


class FakeWriteOnlyCacheStore:
    def get_policy(self, capability_id: str):
        class Policy:
            enabled = False
            write_enabled = True

        return Policy()


class FakeCoverageRepo:
    def __init__(self, coverage_map: dict[tuple[str, str], list[object]] | None = None) -> None:
        self.coverage_map = coverage_map or {}

    def find_for_scope(self, capability_id: str, scope_identity: str):
        return self.coverage_map.get((capability_id, scope_identity), [])


class FakeCaptureGaps:
    def __init__(self) -> None:
        self.incomplete: list[dict[str, object]] = []
        self.resolved: list[tuple[str, str, str, int]] = []

    def record_incomplete(
        self,
        capability_id: str,
        code: str,
        trade_date: str,
        expected_count: int,
        actual_count: int,
        provider_results: dict[str, object],
        system_failed: bool,
        last_error: str,
    ) -> None:
        self.incomplete.append(
            {
                "capability_id": capability_id,
                "code": code,
                "trade_date": trade_date,
                "expected_count": expected_count,
                "actual_count": actual_count,
                "provider_results": provider_results,
                "system_failed": system_failed,
                "last_error": last_error,
            }
        )

    def resolve(self, capability_id: str, code: str, trade_date: str, actual_count: int) -> None:
        self.resolved.append((capability_id, code, trade_date, actual_count))

    def record_system_failure(self, capability_id: str, code: str, trade_date: str, last_error: str) -> None:
        self.incomplete.append(
            {
                "capability_id": capability_id,
                "code": code,
                "trade_date": trade_date,
                "system_failed": True,
                "last_error": last_error,
            }
        )


class FakePostgresCursor:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        return None

    def execute(self, query: str, params: tuple[object, ...]) -> None:
        self.queries.append(query)

    def fetchone(self) -> dict[str, bool]:
        return {"locked": True}


class FakePostgresConnection:
    def __init__(self) -> None:
        self.cursor_instance = FakePostgresCursor()
        self.commit_count = 0
        self.close_count = 0

    def cursor(self) -> FakePostgresCursor:
        return self.cursor_instance

    def commit(self) -> None:
        self.commit_count += 1

    def close(self) -> None:
        self.close_count += 1


class FakeStocks:
    def __init__(self, broken: bool = False) -> None:
        self.calls: list[object] = []
        self.broken = broken

    def get_quotes_with_report(self, request):
        self.calls.append(request)
        if self.broken:
            raise RuntimeError("source failed")
        return [object(), object()], ContractReport(contract_name="stocks.quotes.daily").with_store_stats(write=True)

    def get_quotes_query_result_with_report(self, request, *, write_fact_ref=True):
        self.calls.append(request)
        if self.broken:
            raise RuntimeError("source failed")
        items = [
            StockQuoteItem(code=code, trade_time=request.start_date, freq="1d", close=1, volume=1, amount=1)
            for code in request.codes
        ]
        return StockQuotesQueryResult(
            items=items,
            meta=StockQuotesMeta(
                total_rows=len(items),
                returned_rows=len(items),
                complete=True,
                truncated=False,
                codes=[
                    StockQuoteCodeSummary(
                        code=code,
                        row_count=1,
                        expected_bar_count=1,
                        actual_bar_count=1,
                        complete=True,
                        truncated=False,
                    )
                    for code in request.codes
                ],
            ),
        ), ContractReport(contract_name="stocks.quotes.daily").with_store_stats(write=True)


class FakeRuntime:
    def __init__(self, broken: bool = False) -> None:
        self.stocks = FakeStocks(broken)


def _job(policy: CapturePolicy, runtime: FakeRuntime | None = None, locks: FakeLocks | None = None, runs: MemoryCaptureRuns | None = None) -> QuoteMuxCaptureJob:
    return QuoteMuxCaptureJob(
        runtime=runtime or FakeRuntime(),
        policies=MemoryCapturePolicies((policy,)),
        runs=runs or MemoryCaptureRuns(),
        locks=locks or FakeLocks(),
        now_provider=lambda: datetime(2026, 4, 27, 18, 30),
        cache_store=FakeCacheStore(),
        gaps=FakeCaptureGaps(),
    )


def test_capture_policy_update() -> None:
    job = _job(_policy())

    updated = job.update_policy(
        CapturePolicyUpdate(
            capability_id="stocks.quotes.daily",
            enabled=False,
            cadence=CADENCE_WEEKLY,
            run_time=time(19, 0),
            timezone="Asia/Shanghai",
            weekday=1,
            month=None,
            month_day=None,
            scope_profile=PROFILE_ACTIVE_STOCKS_RECENT_TRADING_DAYS,
            window_count=5,
            batch_size=50,
            notes="test",
        )
    )

    assert updated["enabled"] is False
    assert updated["cadence"] == CADENCE_WEEKLY
    assert updated["weekday"] == 1
    assert updated["window_count"] == 5


def test_postgres_advisory_lock_commits_after_acquire(monkeypatch) -> None:
    connections: list[FakePostgresConnection] = []

    def fake_connect(**kwargs: object) -> FakePostgresConnection:
        connection = FakePostgresConnection()
        connections.append(connection)
        return connection

    monkeypatch.setattr(capture.psycopg, "connect", fake_connect)

    lock = PostgresAdvisoryLock("stocks.quotes.daily")

    assert lock.acquire() is True
    assert connections[0].commit_count == 1
    assert connections[0].close_count == 0

    lock.release()

    assert connections[0].commit_count == 2
    assert connections[0].close_count == 1


def test_default_capture_policy_specs_cover_inventory() -> None:
    spec_ids = {spec.capability_id for spec in DEFAULT_CAPTURE_POLICY_SPECS}
    inventory_ids = {capability_id for capability_id in list_capability_ids() if is_independently_configurable_capability_id(capability_id)}

    assert spec_ids == inventory_ids
    assert "markets.calendar.trading.next" not in spec_ids
    assert "markets.calendar.trading.previous" not in spec_ids
    assert "markets.calendar.trading.yearly" not in spec_ids


def test_list_policies_returns_all_capabilities() -> None:
    policies = tuple(
        _policy(
            capability_id=spec.capability_id,
            cadence=spec.cadence,
            scope_profile=spec.scope_profile,
            window_count=spec.window_count,
            batch_size=spec.batch_size,
        )
        for spec in DEFAULT_CAPTURE_POLICY_SPECS
    )
    job = QuoteMuxCaptureJob(
        runtime=FakeRuntime(),
        policies=MemoryCapturePolicies(policies),
        runs=MemoryCaptureRuns(),
        locks=FakeLocks(),
        now_provider=lambda: datetime(2026, 4, 27, 18, 30),
        cache_store=FakeCacheStore(),
    )

    result = job.list_policies()

    independent_ids = {capability_id for capability_id in list_capability_ids() if is_independently_configurable_capability_id(capability_id)}
    assert {str(item["capability_id"]) for item in result} == independent_ids
    assert all("scope_profile_label" in item for item in result)


def test_capture_due_judgement_daily_weekly_monthly() -> None:
    runs = MemoryCaptureRuns()
    now = datetime(2026, 5, 31, 18, 30)

    assert is_capture_due(_policy(cadence=CADENCE_DAILY), runs, now) is True
    assert is_capture_due(_policy(cadence=CADENCE_WEEKLY, weekday=6), runs, now) is True
    assert is_capture_due(_policy(cadence=CADENCE_MONTHLY, month_day=31), runs, now) is True
    assert is_capture_due(_policy(cadence=CADENCE_YEARLY, month=4, month_day=27), runs, now) is True
    assert is_capture_due(_policy(cadence=CADENCE_DAILY, run_time=time(23, 59)), runs, now) is True

    planned = datetime(2026, 5, 31, 18, 0)
    runs.create("stocks.quotes.daily", CAPTURE_SUCCESS, planned, {})
    assert is_capture_due(_policy(cadence=CADENCE_DAILY), runs, now) is False

    partial_runs = MemoryCaptureRuns()
    partial_runs.create("stocks.quotes.daily", CAPTURE_PARTIAL, planned, {})
    assert is_capture_due(_policy(cadence=CADENCE_DAILY), partial_runs, now) is True


def test_capture_due_converts_aware_datetime_to_policy_timezone() -> None:
    runs = MemoryCaptureRuns()
    policy = _policy(run_time=time(20, 0), timezone="Asia/Shanghai")

    assert is_capture_due(policy, runs, datetime(2026, 7, 8, 11, 59, tzinfo=ZoneInfo("UTC"))) is True
    assert is_capture_due(policy, runs, datetime(2026, 7, 8, 12, 0, tzinfo=ZoneInfo("UTC"))) is True


def test_run_capture_success_status(monkeypatch) -> None:
    runtime = FakeRuntime()
    monkeypatch.setattr(QuoteMuxCaptureJob, "_write_fact_ref_items", lambda self, capability_id, items: 1)
    monkeypatch.setattr(
        capture,
        "build_capture_requests",
        lambda policy, now: (capture.CaptureRequest("stocks.quotes.daily", {"codes": ["600000"], "freq": "1d", "start_date": "2026-04-03", "end_date": "2026-04-03"}),),
    )

    result = _job(_policy(), runtime=runtime).run_capture("stocks.quotes.daily")

    assert result["status"] == CAPTURE_SUCCESS
    assert result["row_count"] == 1
    assert result["coverage_count"] == 1
    assert len(runtime.stocks.calls) == 1


def test_daily_capture_is_partial_when_a_provider_batch_omits_a_requested_code_date(monkeypatch) -> None:
    complete_item = StockQuoteItem(
        code="600000",
        trade_time="2026-09-04",
        freq="1d",
        open=10,
        high=11,
        low=9,
        close=10.5,
        volume=100,
        amount=1000,
    )

    class DailyBatchRuntime:
        def __init__(self) -> None:
            self.stocks = self

        def get_quotes_query_result_with_report(self, request, *, write_fact_ref=True):
            assert write_fact_ref is False
            return StockQuotesQueryResult(
                items=[complete_item],
                meta=StockQuotesMeta(
                    total_rows=1,
                    returned_rows=1,
                    complete=False,
                    truncated=False,
                    codes=[
                        StockQuoteCodeSummary(
                            code="600000",
                            row_count=1,
                            expected_bar_count=1,
                            actual_bar_count=1,
                            complete=True,
                            truncated=False,
                        ),
                        StockQuoteCodeSummary(
                            code="600001",
                            row_count=0,
                            expected_bar_count=1,
                            actual_bar_count=0,
                            missing_count=1,
                            complete=False,
                            truncated=False,
                            missing_trade_dates=["2026-09-04"],
                        ),
                    ],
                ),
            ), ContractReport(contract_name="stocks.quotes.daily").with_store_stats(write=True)

    monkeypatch.setattr(
        capture,
        "build_capture_requests",
        lambda policy, now: (
            capture.CaptureRequest(
                "stocks.quotes.daily",
                {
                    "codes": ["600000", "600001"],
                    "freq": "1d",
                    "start_date": "2026-09-04",
                    "end_date": "2026-09-04",
                },
            ),
        ),
    )
    monkeypatch.setattr(QuoteMuxCaptureJob, "_write_fact_ref_items", lambda self, capability_id, items: len(items))

    result = _job(_policy(), runtime=DailyBatchRuntime()).run_capture("stocks.quotes.daily")

    assert result["status"] == CAPTURE_PARTIAL
    assert result["row_count"] == 1
    assert result["coverage_count"] == 1
    assert "600001" in str(result["detail_json"])


def test_concept_member_capture_is_bounded_and_resumes_from_checkpoint(monkeypatch) -> None:
    policy = _policy(capability_id="concepts.members", scope_profile=PROFILE_CONCEPTS_RECENT_TRADING_DAYS, batch_size=2)
    requests = tuple(
        capture.CaptureRequest("concepts.members", {"concept_id": f"C{index}", "trade_date": "2026-04-27"})
        for index in range(5)
    )
    monkeypatch.setattr(capture, "build_capture_requests", lambda *_args: requests)
    runtime = FakeRuntime()
    runs = MemoryCaptureRuns()
    job = _job(policy, runtime=runtime, runs=runs)
    monkeypatch.setattr(job, "_run_capture_batch", lambda _request: capture._CaptureBatchResult((), 1, row_count_override=1))

    first = job.run_capture("concepts.members")
    second = job.run_capture("concepts.members")
    third = job.run_capture("concepts.members")

    assert [item["status"] for item in (first, second, third)] == [CAPTURE_PARTIAL, CAPTURE_PARTIAL, CAPTURE_SUCCESS]
    assert [item["detail_json"]["request_start_index"] for item in (first, second, third)] == [0, 2, 4]
    assert first["detail_json"]["request_next_index"] == 2
    assert runs.items[0].detail_json["completed_request_count"] == 2


def test_catalog_repair_run_persists_its_own_publication_without_rewriting_history(monkeypatch) -> None:
    policy = _policy(capability_id="futures.contracts.catalog", scope_profile=PROFILE_CATALOG_SNAPSHOT)
    snapshots = iter(("snapshot-a", "snapshot-b"))

    class Futures:
        def capture_contract_catalog(self, **_scope):
            snapshot_id = next(snapshots)
            return [FutureContractCatalogItem(
                provider_symbol=f"SHFE.rb-{snapshot_id}", product_code="rb", exchange="SHFE", ins_class="FUTURE",
                price_tick=1.0, price_decs=0, volume_multiple=10.0, snapshot_id=snapshot_id,
                snapshot_complete=True, content_checksum=f"checksum-{snapshot_id}", captured_at="2026-08-24 10:12:00",
                source={"package_id": "shinny_tqsdk", "source_instance_id": "test", "provider_version": {"availability": "unavailable"}},
                catalog_schema_version="future_contract_catalog_v2",
            )]

    runtime = type("Runtime", (), {"futures": Futures()})()
    runs = MemoryCaptureRuns()
    monkeypatch.setattr(capture, "build_capture_requests", lambda *_args: (capture.CaptureRequest("futures.contracts.catalog", {"codes": [], "include_expired": False}),))
    job = _job(policy, runtime=runtime, runs=runs)

    first = job.run_capture("futures.contracts.catalog")
    second = job.run_capture("futures.contracts.catalog")

    assert first["detail_json"]["publication"]["snapshot_id"] == "snapshot-a"
    assert second["detail_json"]["publication"]["snapshot_id"] == "snapshot-b"
    assert runs.items[0].detail_json["publication"]["snapshot_id"] == "snapshot-a"
    assert runs.items[1].detail_json["publication"]["snapshot_id"] == "snapshot-b"


def test_catalog_publication_finalize_updates_exact_run_and_rejects_mismatch(monkeypatch) -> None:
    policy = _policy(capability_id="futures.contracts.catalog", scope_profile=PROFILE_CATALOG_SNAPSHOT)
    snapshots = iter(("snapshot-a", "snapshot-b"))

    class Futures:
        def capture_contract_catalog(self, **_scope):
            snapshot_id = next(snapshots)
            return [FutureContractCatalogItem(
                provider_symbol=f"SHFE.rb-{snapshot_id}", product_code="rb", exchange="SHFE", ins_class="FUTURE",
                price_tick=1.0, price_decs=0, volume_multiple=10.0, snapshot_id=snapshot_id,
                snapshot_complete=True, content_checksum=f"checksum-{snapshot_id}", captured_at="2026-08-24 10:12:00",
                source={"package_id": "shinny_tqsdk", "source_instance_id": "test", "provider_version": {"availability": "unavailable"}},
                catalog_schema_version="future_contract_catalog_v2",
            )]

    runs = MemoryCaptureRuns()
    runtime = type("Runtime", (), {"futures": Futures()})()
    monkeypatch.setattr(capture, "build_capture_requests", lambda *_args: (capture.CaptureRequest("futures.contracts.catalog", {"codes": [], "include_expired": False}),))
    job = _job(policy, runtime=runtime, runs=runs)
    first = job.run_repair("futures.contracts.catalog", {"codes": [], "include_expired": False}, "mhd-v1-a")
    second = job.run_repair("futures.contracts.catalog", {"codes": [], "include_expired": False}, "mhd-v1-b")
    first_publication = dict(first["detail_json"]["publication"])
    finalized = job.finalize_catalog_repair_publication(
        first["id"], {**first_publication, "catalog_dataset_version": "mhd-v1-final"}
    )

    assert finalized["detail_json"]["publication"]["catalog_dataset_version"] == "mhd-v1-final"
    assert runs.items[1].detail_json["publication"]["catalog_dataset_version"] == ""
    with pytest.raises(ValueError, match="mismatch"):
        job.finalize_catalog_repair_publication(second["id"], {**first_publication, "catalog_dataset_version": "wrong-run"})
    assert runs.items[1].detail_json["publication"]["snapshot_id"] == "snapshot-b"


def test_run_capture_failed_status(monkeypatch) -> None:
    monkeypatch.setattr(
        capture,
        "build_capture_requests",
        lambda policy, now: (capture.CaptureRequest("stocks.quotes.daily", {"codes": ["600000"], "freq": "1d"}),),
    )

    result = _job(_policy(), runtime=FakeRuntime(broken=True)).run_capture("stocks.quotes.daily")

    assert result["status"] == CAPTURE_FAILED
    assert result["error_message"] == "部分 batch 采集失败"
    assert result["detail_json"]["failed_batches"][0]["error"] == "source failed"


def test_lock_busy_writes_skipped_run(monkeypatch) -> None:
    runtime = FakeRuntime()
    monkeypatch.setattr(capture, "build_capture_requests", lambda policy, now: ())

    result = _job(_policy(), runtime=runtime, locks=FakeLocks(False)).run_capture("stocks.quotes.daily")

    assert result["status"] == CAPTURE_SKIPPED
    assert result["detail_json"]["reason"] == "advisory_lock_busy"
    assert runtime.stocks.calls == []


def test_disabled_capture_does_not_run(monkeypatch) -> None:
    runtime = FakeRuntime()
    monkeypatch.setattr(
        capture,
        "build_capture_requests",
        lambda policy, now: (capture.CaptureRequest("stocks.quotes.daily", {"codes": ["600000"], "freq": "1d"}),),
    )

    result = _job(_policy(enabled=False), runtime=runtime).run_capture("stocks.quotes.daily")

    assert result["status"] == CAPTURE_SKIPPED
    assert result["detail_json"]["reason"] == "capture_policy_disabled"
    assert runtime.stocks.calls == []


def test_cache_write_disabled_does_not_run(monkeypatch) -> None:
    runtime = FakeRuntime()
    monkeypatch.setattr(capture, "build_capture_requests", lambda policy, now: (capture.CaptureRequest("stocks.quotes.daily", {"codes": ["600000"], "freq": "1d"}),))
    job = QuoteMuxCaptureJob(
        runtime=runtime,
        policies=MemoryCapturePolicies((_policy(),)),
        runs=MemoryCaptureRuns(),
        locks=FakeLocks(),
        now_provider=lambda: datetime(2026, 4, 27, 18, 30),
        cache_store=FakeDisabledCacheStore(),
    )

    result = job.run_capture("stocks.quotes.daily")

    assert result["status"] == CAPTURE_SKIPPED
    assert result["detail_json"]["reason"] == "cache_policy_disabled"
    assert runtime.stocks.calls == []


def test_daily_snapshot_runs_when_generic_cache_write_is_disabled(monkeypatch) -> None:
    class SnapshotRuntime:
        def __init__(self) -> None:
            self.stocks = self
            self.calls: list[object] = []

        def get_daily_snapshot_with_report(self, request):
            self.calls.append(request)
            return [object()], ContractReport(contract_name="stocks.quotes.daily_snapshot").with_store_stats(write=True)

    policy = _policy(
        capability_id="stocks.quotes.daily_snapshot",
        scope_profile=PROFILE_DAILY_SNAPSHOT_RECENT_TRADING_DAYS,
        window_count=1,
        batch_size=1,
    )
    runtime = SnapshotRuntime()
    monkeypatch.setattr(
        capture,
        "build_capture_requests",
        lambda policy, now: (
            capture.CaptureRequest(
                "stocks.quotes.daily_snapshot",
                {"trade_date": "2026-04-27", "limit": 10000, "offset": 0},
            ),
        ),
    )
    job = QuoteMuxCaptureJob(
        runtime=runtime,
        policies=MemoryCapturePolicies((policy,)),
        runs=MemoryCaptureRuns(),
        locks=FakeLocks(),
        now_provider=lambda: datetime(2026, 4, 27, 18, 30),
        cache_store=FakeDisabledCacheStore(),
    )

    result = job.run_capture("stocks.quotes.daily_snapshot")

    assert result["status"] == CAPTURE_SUCCESS
    assert len(runtime.calls) == 1


def test_capture_runs_when_cache_read_is_disabled_but_write_enabled(monkeypatch) -> None:
    runtime = FakeRuntime()
    monkeypatch.setattr(capture, "build_capture_requests", lambda policy, now: (capture.CaptureRequest("stocks.quotes.daily", {"codes": ["600000"], "freq": "1d"}),))
    job = QuoteMuxCaptureJob(
        runtime=runtime,
        policies=MemoryCapturePolicies((_policy(),)),
        runs=MemoryCaptureRuns(),
        locks=FakeLocks(),
        now_provider=lambda: datetime(2026, 4, 27, 18, 30),
        cache_store=FakeWriteOnlyCacheStore(),
    )

    result = job.run_capture("stocks.quotes.daily")

    assert result["status"] == CAPTURE_SUCCESS
    assert len(runtime.stocks.calls) == 1


def test_run_due_captures_runs_all_enabled_missed_schedules(monkeypatch) -> None:
    runtime = FakeRuntime()
    due_policy = _policy(capability_id="stocks.quotes.daily", enabled=True)
    disabled_policy = _policy(capability_id="stocks.quotes.intraday", enabled=False)
    future_policy = _policy(capability_id="indexes.quotes.daily", enabled=True, cadence=CADENCE_WEEKLY, scope_profile=PROFILE_INDEXES_RECENT_TRADING_DAYS)
    monkeypatch.setattr(capture, "build_capture_requests", lambda policy, now: ())
    job = QuoteMuxCaptureJob(
        runtime=runtime,
        policies=MemoryCapturePolicies((due_policy, disabled_policy, future_policy)),
        runs=MemoryCaptureRuns(),
        locks=FakeLocks(),
        now_provider=lambda: datetime(2026, 4, 27, 18, 30),
        cache_store=FakeCacheStore(),
    )

    runs = job.run_due_captures()

    assert [run["capability_id"] for run in runs] == ["stocks.quotes.daily", "indexes.quotes.daily"]


def test_run_due_captures_prioritizes_intraday_before_slow_concepts(monkeypatch) -> None:
    concept_policy = _policy(capability_id="concepts.indicators.money_flow", enabled=True, scope_profile=PROFILE_CONCEPTS_RECENT_TRADING_DAYS)
    intraday_policy = _policy(capability_id="stocks.quotes.intraday", enabled=True)
    snapshot_policy = _policy(capability_id="stocks.quotes.daily_snapshot", enabled=True, scope_profile=PROFILE_DAILY_SNAPSHOT_RECENT_TRADING_DAYS)
    monkeypatch.setattr(capture, "build_capture_requests", lambda policy, now: ())
    job = QuoteMuxCaptureJob(
        runtime=FakeRuntime(),
        policies=MemoryCapturePolicies((concept_policy, intraday_policy, snapshot_policy)),
        runs=MemoryCaptureRuns(),
        locks=FakeLocks(),
        now_provider=lambda: datetime(2026, 4, 27, 18, 30),
        cache_store=FakeCacheStore(),
        gaps=FakeCaptureGaps(),
    )

    runs = job.run_due_captures()

    assert [run["capability_id"] for run in runs] == ["stocks.quotes.intraday", "stocks.quotes.daily_snapshot", "concepts.indicators.money_flow"]


def test_second_phase_profiles_build_requests(monkeypatch) -> None:
    class _Frame:
        empty = False

        def __init__(self, records: list[dict[str, object]]) -> None:
            self.records = records

        def to_dict(self, orient: str):
            return self.records

    monkeypatch.setattr(capture, "load_trade_calendar_frame", lambda *args: _Frame([{"trade_date": "2026-04-24"}, {"trade_date": "2026-04-27"}]))
    monkeypatch.setattr(capture, "load_index_catalog_frame", lambda codes: _Frame([{"index_code": "000001"}, {"index_code": "399001"}]))
    monkeypatch.setattr(capture, "_concept_daily_fact_missing", lambda trade_date: True)
    monkeypatch.setattr("quotemux.concepts._read_alias_groups", lambda trade_date: [ConceptAliasGroupItem(concept_id="C1", canonical_name="概念A"), ConceptAliasGroupItem(concept_id="C2", canonical_name="概念B")])
    now = datetime(2026, 4, 27, 18, 30)

    snapshot = capture.build_capture_requests(_policy(capability_id="stocks.quotes.daily_snapshot", scope_profile=PROFILE_DAILY_SNAPSHOT_RECENT_TRADING_DAYS, window_count=2), now)
    calendar = capture.build_capture_requests(_policy(capability_id="markets.calendar.trading", scope_profile=PROFILE_TRADING_CALENDAR_YEAR_WINDOW, window_count=2), now)
    concept_quotes = capture.build_capture_requests(_policy(capability_id="concepts.quotes.daily", scope_profile=PROFILE_CONCEPTS_RECENT_TRADING_DAYS, window_count=2), now)
    index_members = capture.build_capture_requests(_policy(capability_id="indexes.members", scope_profile=PROFILE_INDEXES_RECENT_TRADING_DAYS, window_count=1), now)
    concept_members = capture.build_capture_requests(_policy(capability_id="concepts.members", scope_profile=PROFILE_CONCEPTS_RECENT_TRADING_DAYS, window_count=1), now)

    assert [item.request_identity["trade_date"] for item in snapshot] == ["2026-04-24", "2026-04-27"]
    assert calendar[0].request_identity["start_date"] == "2026-01-01"
    assert calendar[0].request_identity["end_date"] == "2027-12-31"
    assert [item.request_identity for item in concept_quotes] == [
        {"trade_date": "2026-04-24", "limit": 5000, "offset": 0},
        {"trade_date": "2026-04-27", "limit": 5000, "offset": 0},
    ]
    assert [item.request_identity["index_code"] for item in index_members] == ["000001", "399001"]
    assert [item.request_identity["concept_id"] for item in concept_members] == ["C1", "C2"]


def test_name_history_capture_builds_one_full_market_request() -> None:
    requests = capture.build_capture_requests(
        _policy(
            capability_id="stocks.profile.name_history",
            scope_profile=capture.PROFILE_SINGLE_ENTITY_SNAPSHOT,
            window_count=1,
        ),
        datetime(2026, 7, 17, 18, 30),
    )

    assert requests == (
        capture.CaptureRequest(
            "stocks.profile.name_history",
            {"code": "", "start_date": "", "end_date": ""},
        ),
    )


def test_board_daily_refreshes_when_price_fields_are_missing(monkeypatch) -> None:
    calls: list[tuple[str, tuple[object, ...]]] = []

    class _Frame:
        empty = False

        def __init__(self, records: list[dict[str, object]]) -> None:
            self.records = records

        def to_dict(self, orient: str):
            return self.records

        @property
        def iloc(self):
            return self

        def __getitem__(self, index: int):
            return _FrameRow(self.records[index])

    class _FrameRow:
        def __init__(self, record: dict[str, object]) -> None:
            self.record = record

        def to_dict(self) -> dict[str, object]:
            return self.record

    def fake_query_dataframe(query: str, params: tuple[object, ...] = ()) -> _Frame:
        calls.append((query, params))
        if "fact.board_daily_1d" in query:
            return _Frame([{"row_count": 0}])
        if "count(distinct industry)" in query:
            return _Frame([{"industry_count": 110}])
        return _Frame([{"row_count": 0}])

    monkeypatch.setattr(capture, "query_dataframe", fake_query_dataframe)

    assert capture._board_daily_fact_missing("2026-07-10") is True
    board_query = calls[0][0]
    assert "open is not null" in board_query
    assert "pre_close is not null" in board_query
    assert "change is not null" in board_query


def test_daily_snapshot_only_builds_missing_trade_dates(monkeypatch) -> None:
    class _Frame:
        empty = False

        def __init__(self, records: list[dict[str, object]]) -> None:
            self.records = records

        def to_dict(self, orient: str):
            return self.records

    class _Coverage:
        def __init__(self, start: str, end: str) -> None:
            self.time_start = datetime.strptime(start, "%Y-%m-%d")
            self.time_end = datetime.strptime(end, "%Y-%m-%d")
            self.fresh_until = datetime(2099, 1, 1)

    monkeypatch.setattr(capture, "load_trade_calendar_frame", lambda *args: _Frame([{"trade_date": "2026-04-24"}, {"trade_date": "2026-04-25"}, {"trade_date": "2026-04-27"}]))
    fake_store = type("Store", (), {"get_policy": lambda self, capability_id: type("Policy", (), {"request_scope_fields": ("trade_date",), "ttl_seconds": 3600})(), "coverage": FakeCoverageRepo({("stocks.quotes.daily_snapshot", "trade_date=2026-04-24"): [_Coverage("2026-04-24", "2026-04-24")], ("stocks.quotes.daily_snapshot", "trade_date=2026-04-25"): [_Coverage("2026-04-25", "2026-04-25")]})})()
    monkeypatch.setattr(capture, "get_postgres_cache_store", lambda: fake_store)
    monkeypatch.setattr(capture, "_stock_daily_fact_missing", lambda trade_date: trade_date == "2026-04-27")

    requests = capture.build_capture_requests(_policy(capability_id="stocks.quotes.daily_snapshot", scope_profile=PROFILE_DAILY_SNAPSHOT_RECENT_TRADING_DAYS, window_count=3), datetime(2026, 4, 27, 18, 30))

    assert [item.request_identity["trade_date"] for item in requests] == ["2026-04-27"]


def test_report_period_requests_only_build_missing_periods(monkeypatch) -> None:
    class _Frame:
        empty = False

        def __init__(self, records: list[dict[str, object]]) -> None:
            self.records = records

        def to_dict(self, orient: str):
            return self.records

    class _Coverage:
        def __init__(self, start: str, end: str) -> None:
            self.time_start = datetime.strptime(start, "%Y-%m-%d")
            self.time_end = datetime.strptime(end, "%Y-%m-%d")
            self.fresh_until = datetime(2099, 1, 1)

    monkeypatch.setattr(capture, "load_trade_calendar_frame", lambda *args: _Frame([{"trade_date": "2026-04-27"}]))
    monkeypatch.setattr(capture, "load_stock_active_codes_frame", lambda trade_date: _Frame([{"code": "600000"}]))
    fake_store = type("Store", (), {"get_policy": lambda self, capability_id: type("Policy", (), {"request_scope_fields": ("code",), "ttl_seconds": 3600})(), "coverage": FakeCoverageRepo({("stocks.finance.audits", "code=600000"): [_Coverage("2025-12-31", "2025-12-31")]})})()
    monkeypatch.setattr(capture, "get_postgres_cache_store", lambda: fake_store)

    requests = capture.build_capture_requests(_policy(capability_id="stocks.finance.audits", scope_profile=capture.PROFILE_ACTIVE_STOCKS_RECENT_REPORT_PERIODS, window_count=2), datetime(2026, 4, 27, 18, 30))

    assert [item.request_identity["report_period"] for item in requests] == ["20260331"]


def test_concept_money_flow_requests_only_build_missing_ranges(monkeypatch) -> None:
    class _Frame:
        empty = False

        def __init__(self, records: list[dict[str, object]]) -> None:
            self.records = records

        def to_dict(self, orient: str):
            return self.records

    class _Coverage:
        def __init__(self, start: str, end: str) -> None:
            self.time_start = datetime.strptime(start, "%Y-%m-%d")
            self.time_end = datetime.strptime(end, "%Y-%m-%d")
            self.fresh_until = datetime(2099, 1, 1)

    monkeypatch.setattr(capture, "load_trade_calendar_frame", lambda *args: _Frame([{"trade_date": "2026-04-24"}, {"trade_date": "2026-04-25"}, {"trade_date": "2026-04-27"}]))
    monkeypatch.setattr("quotemux.concepts._read_alias_groups", lambda trade_date: [ConceptAliasGroupItem(concept_id="C1", canonical_name="概念A")])
    fake_store = type(
        "Store",
        (),
        {
            "get_policy": lambda self, capability_id: type("Policy", (), {"request_scope_fields": ("concept_id", "scope"), "ttl_seconds": 3600})(),
            "coverage": FakeCoverageRepo({("concepts.indicators.money_flow", "concept_id=C1|scope=concept"): [_Coverage("2026-04-24", "2026-04-25")]}),
        },
    )()
    monkeypatch.setattr(capture, "get_postgres_cache_store", lambda: fake_store)

    requests = capture.build_capture_requests(_policy(capability_id="concepts.indicators.money_flow", scope_profile=PROFILE_CONCEPTS_RECENT_TRADING_DAYS, window_count=3), datetime(2026, 4, 27, 18, 30))

    assert [item.request_identity["start_date"] for item in requests] == ["2026-04-27"]
    assert [item.request_identity["end_date"] for item in requests] == ["2026-04-27"]


def test_stock_money_flow_batch_requests_only_build_missing_trade_dates(monkeypatch) -> None:
    class _Frame:
        empty = False

        def __init__(self, records: list[dict[str, object]]) -> None:
            self.records = records

        def to_dict(self, orient: str):
            return self.records

    class _Coverage:
        def __init__(self, start: str, end: str) -> None:
            self.time_start = datetime.strptime(start, "%Y-%m-%d")
            self.time_end = datetime.strptime(end, "%Y-%m-%d")
            self.fresh_until = datetime(2099, 1, 1)

    monkeypatch.setattr(capture, "load_trade_calendar_frame", lambda *args: _Frame([{"trade_date": "2026-04-24"}, {"trade_date": "2026-04-25"}, {"trade_date": "2026-04-27"}]))
    monkeypatch.setattr(capture, "load_stock_active_codes_frame", lambda trade_date: _Frame([{"code": "600000"}]))
    fake_store = type(
        "Store",
        (),
        {
            "get_policy": lambda self, capability_id: type("Policy", (), {"request_scope_fields": ("code", "view"), "ttl_seconds": 3600})(),
            "coverage": FakeCoverageRepo({("stocks.indicators.money_flow.batch", "code=600000|view=main"): [_Coverage("2026-04-24", "2026-04-25")]}),
        },
    )()
    monkeypatch.setattr(capture, "get_postgres_cache_store", lambda: fake_store)

    requests = capture.build_capture_requests(_policy(capability_id="stocks.indicators.money_flow.batch", scope_profile=PROFILE_ACTIVE_STOCKS_RECENT_TRADING_DAYS, window_count=3), datetime(2026, 4, 27, 18, 30))

    assert [item.request_identity["trade_date"] for item in requests] == ["2026-04-27"]


def test_shareholder_changes_requests_only_build_missing_ranges(monkeypatch) -> None:
    class _Frame:
        empty = False

        def __init__(self, records: list[dict[str, object]]) -> None:
            self.records = records

        def to_dict(self, orient: str):
            return self.records

    class _Coverage:
        def __init__(self, start: str, end: str) -> None:
            self.time_start = datetime.strptime(start, "%Y-%m-%d")
            self.time_end = datetime.strptime(end, "%Y-%m-%d")
            self.fresh_until = datetime(2099, 1, 1)

    monkeypatch.setattr(capture, "load_trade_calendar_frame", lambda *args: _Frame([{"trade_date": "2026-04-24"}, {"trade_date": "2026-04-25"}, {"trade_date": "2026-04-27"}]))
    monkeypatch.setattr(capture, "load_stock_active_codes_frame", lambda trade_date: _Frame([{"code": "600000"}]))
    fake_store = type(
        "Store",
        (),
        {
            "get_policy": lambda self, capability_id: type("Policy", (), {"request_scope_fields": ("code",), "ttl_seconds": 3600})(),
            "coverage": FakeCoverageRepo({("stocks.ownership.shareholders.changes", "code=600000"): [_Coverage("2026-04-24", "2026-04-25")]}),
        },
    )()
    monkeypatch.setattr(capture, "get_postgres_cache_store", lambda: fake_store)

    requests = capture.build_capture_requests(_policy(capability_id="stocks.ownership.shareholders.changes", scope_profile=capture.PROFILE_OWNERSHIP_RECENT_TRADING_DAYS, window_count=3), datetime(2026, 4, 27, 18, 30))

    assert [item.request_identity["start_date"] for item in requests] == ["2026-04-27"]
    assert [item.request_identity["end_date"] for item in requests] == ["2026-04-27"]


def test_rankings_research_reports_only_build_missing_ranges(monkeypatch) -> None:
    class _Coverage:
        def __init__(self, start: str, end: str) -> None:
            self.time_start = datetime.strptime(start, "%Y-%m-%d")
            self.time_end = datetime.strptime(end, "%Y-%m-%d")
            self.fresh_until = datetime(2099, 1, 1)

    fake_store = type(
        "Store",
        (),
        {
            "get_policy": lambda self, capability_id: type("Policy", (), {"request_scope_fields": (), "ttl_seconds": 3600})(),
            "coverage": FakeCoverageRepo({("rankings.research.reports", ""): [_Coverage("2026-04-25", "2026-04-26")]}),
        },
    )()
    monkeypatch.setattr(capture, "get_postgres_cache_store", lambda: fake_store)

    requests = capture.build_capture_requests(_policy(capability_id="rankings.research.reports", scope_profile=capture.PROFILE_RESEARCH_RECENT_DATES, window_count=3), datetime(2026, 4, 27, 18, 30))

    assert [item.request_identity["start_date"] for item in requests] == ["2026-04-27"]
    assert [item.request_identity["end_date"] for item in requests] == ["2026-04-27"]


def test_news_requests_only_build_missing_trade_dates(monkeypatch) -> None:
    class _Coverage:
        def __init__(self, start: str, end: str) -> None:
            self.time_start = datetime.strptime(start, "%Y-%m-%d")
            self.time_end = datetime.strptime(end, "%Y-%m-%d")
            self.fresh_until = datetime(2099, 1, 1)

    scope_identity = capture.build_scope_identity(
        {"sort_by": "announcement_time", "include_sources": True, "include_content_text": False},
        ("event_type", "stock_code", "sort_by", "include_sources", "include_content_text"),
    )
    fake_store = type(
        "Store",
        (),
        {
            "get_policy": lambda self, capability_id: type("Policy", (), {"request_scope_fields": ("event_type", "stock_code", "sort_by", "include_sources", "include_content_text"), "ttl_seconds": 3600})(),
            "coverage": FakeCoverageRepo({("markets.events.news", scope_identity): [_Coverage("2026-04-25", "2026-04-26")]}),
        },
    )()
    monkeypatch.setattr(capture, "get_postgres_cache_store", lambda: fake_store)

    requests = capture.build_capture_requests(_policy(capability_id="markets.events.news", scope_profile=capture.PROFILE_NEWS_EVENT_UPDATE, window_count=3), datetime(2026, 4, 27, 18, 30))

    assert [item.request_identity["trade_date"] for item in requests] == ["2026-04-27"]


def test_research_month_requests_only_build_missing_months(monkeypatch) -> None:
    monkeypatch.setattr(capture, "_missing_months", lambda capability_id, months: ("202603",))

    requests = capture.build_capture_requests(_policy(capability_id="rankings.research.broker_monthly_picks", scope_profile=capture.PROFILE_RESEARCH_RECENT_MONTHS, window_count=2), datetime(2026, 4, 27, 18, 30))

    assert [item.request_identity["trade_month"] for item in requests] == ["202603"]


def test_second_phase_runtime_dispatch() -> None:
    class Runtime:
        def __init__(self) -> None:
            self.stocks = self
            self.markets = self
            self.indexes = self
            self.concepts = self

        def get_daily_snapshot_with_report(self, request):
            return [object()], ContractReport(contract_name="stocks.quotes.daily_snapshot").with_store_stats(write=True)

        def get_trading_calendar_with_report(self, request):
            return [object(), object()], ContractReport(contract_name="markets.calendar.trading").with_store_stats(write=True)

        def get_members_with_report(self, request):
            return [object(), object(), object()], ContractReport(contract_name="indexes.members").with_store_stats(write=True)

        def get_quotes(self, **kwargs):
            return [object()]

        def get_members(self, **kwargs):
            return [object(), object()]

    job = _job(_policy(), runtime=Runtime())

    snapshot_items, snapshot_report = job._run_runtime_request(capture.CaptureRequest("stocks.quotes.daily_snapshot", {"trade_date": "2026-04-27", "limit": 10000, "offset": 0}))
    calendar_items, calendar_report = job._run_runtime_request(capture.CaptureRequest("markets.calendar.trading", {"exchange": "SSE", "start_date": "2026-01-01", "end_date": "2027-12-31", "is_open": None}))
    index_member_items, index_member_report = job._run_runtime_request(capture.CaptureRequest("indexes.members", {"index_code": "000001", "trade_date": "2026-04-27"}))
    concept_quote_items, concept_quote_report = job._run_runtime_request(capture.CaptureRequest("concepts.quotes.daily", {"concept_ids": ["C1"], "freq": "1d", "trade_date": "", "start_date": "2026-04-27", "end_date": "2026-04-27", "start_time": "", "end_time": "", "count": None, "limit": 5000}))
    concept_member_items, concept_member_report = job._run_runtime_request(capture.CaptureRequest("concepts.members", {"concept_id": "C1", "trade_date": "2026-04-27"}))

    assert len(snapshot_items) == 1
    assert snapshot_report.store_write_count == 1
    assert len(calendar_items) == 2
    assert calendar_report.store_write_count == 1
    assert len(index_member_items) == 3
    assert index_member_report.store_write_count == 1
    assert len(concept_quote_items) == 1
    assert concept_quote_report.store_write_count == 0
    assert len(concept_member_items) == 2
    assert concept_member_report.store_write_count == 0


def test_concept_capture_paths_store_results(monkeypatch) -> None:
    class WriteResult:
        def __init__(self, coverage_count: int) -> None:
            self.coverage_count = coverage_count

    class Runtime:
        def __init__(self) -> None:
            self.concepts = self

        def get_quotes(self, **kwargs):
            return [{"concept_id": "C1", "trade_time": "2026-04-27", "freq": "1d"}]

        def get_members(self, **kwargs):
            return [type("Member", (), {"concept_id": "C1", "code": "600000", "name": "A", "join_date": "", "model_copy": lambda self, update: type("Member", (), {**self.__dict__, **update})()})()]

    stored: list[tuple[str, dict[str, object], list[object]]] = []
    monkeypatch.setattr(capture, "store_result", lambda capability_id, request_identity, items, report: stored.append((capability_id, request_identity, list(items))) or WriteResult(1))
    monkeypatch.setattr(QuoteMuxCaptureJob, "_write_fact_ref_items", lambda self, capability_id, items: 1)

    job = _job(_policy(), runtime=Runtime())
    quote_items, quote_report = job._run_runtime_request(capture.CaptureRequest("concepts.quotes.daily", {"concept_ids": ["C1"], "freq": "1d", "trade_date": "", "start_date": "2026-04-27", "end_date": "2026-04-27", "start_time": "", "end_time": "", "count": None, "limit": 5000}))
    member_items, member_report = job._run_runtime_request(capture.CaptureRequest("concepts.members", {"concept_id": "C1", "trade_date": "2026-04-27"}))

    assert quote_report.store_write_count == 1
    assert member_report.store_write_count == 1
    assert stored[0][0] == "concepts.quotes.daily"
    assert stored[1][0] == "concepts.members"
    assert getattr(member_items[0], "join_date", "") == "2026-04-27"


def test_concept_market_snapshot_uses_current_catalog_as_coverage_baseline(monkeypatch) -> None:
    class WriteResult:
        coverage_count = 9

    class Runtime:
        def __init__(self) -> None:
            self.concepts = self

        def get_market_daily_snapshot(self, **kwargs):
            return [
                ConceptQuoteItem(concept_id=f"C{index}", trade_time="2026-07-20", freq="1d", close=float(index))
                for index in range(1, 10)
            ]

    monkeypatch.setattr(capture, "_concept_ids", lambda: tuple(f"C{index}" for index in range(1, 11)))
    monkeypatch.setattr(capture, "store_result", lambda *args: WriteResult())
    monkeypatch.setattr(QuoteMuxCaptureJob, "_write_fact_ref_items", lambda self, capability_id, items: len(items))

    items, report = _job(_policy(), runtime=Runtime())._run_runtime_request(
        capture.CaptureRequest(
            "concepts.quotes.daily",
            {"trade_date": "2026-07-20", "limit": 5000, "offset": 0},
        )
    )

    assert len(items) == 9
    assert report.store_write_count == 9


def test_generic_adapter_calls_runtime_and_store(monkeypatch) -> None:
    class WriteResult:
        coverage_count = 1

    class Runtime:
        def __init__(self) -> None:
            self.stocks = self
            self.calls: list[dict[str, object]] = []

        def get_daily_basic(self, **kwargs):
            self.calls.append(kwargs)
            return [{"code": "600000", "trade_date": "2026-04-27"}]

    stored: list[tuple[str, dict[str, object], list[object]]] = []
    monkeypatch.setattr(capture, "store_result", lambda capability_id, request_identity, items, report: stored.append((capability_id, request_identity, list(items))) or WriteResult())
    runtime = Runtime()
    job = _job(_policy(), runtime=runtime)

    items, report = job._run_runtime_request(capture.CaptureRequest("stocks.indicators.daily_basic", {"code": "", "codes": "600000", "trade_date": "", "start_date": "2026-04-27", "end_date": "2026-04-27"}))

    assert items == [{"code": "600000", "trade_date": "2026-04-27"}]
    assert runtime.calls == [{"code": "", "codes": "600000", "trade_date": "", "start_date": "2026-04-27", "end_date": "2026-04-27"}]
    assert stored[0][0] == "stocks.indicators.daily_basic"
    assert report.store_write_count == 1


def test_news_updater_run_record(monkeypatch) -> None:
    class Runtime:
        def __init__(self) -> None:
            self.news = self

        def update_events_capture(self, **kwargs):
            return [{"event_id": "n1", "announcement_time": "2026-04-27 09:30:00"}], ContractReport(contract_name="markets.events.news").with_store_stats(write=True)

    monkeypatch.setattr(capture, "build_capture_requests", lambda policy, now: (capture.CaptureRequest("markets.events.news", {"trade_date": "2026-04-27"}),))

    result = _job(_policy(capability_id="markets.events.news", scope_profile=capture.PROFILE_NEWS_EVENT_UPDATE), runtime=Runtime()).run_capture("markets.events.news")

    assert result["status"] == CAPTURE_SUCCESS
    assert result["row_count"] == 1
    assert result["coverage_count"] == 1


def test_intraday_capture_fails_when_fact_ref_not_written(monkeypatch) -> None:
    items = [
        StockQuoteItem(code="600000", trade_time="2026-07-02 09:31:00", freq="1m"),
        StockQuoteItem(code="600000", trade_time="2026-07-02 09:32:00", freq="1m"),
    ]

    class Runtime:
        def __init__(self) -> None:
            self.stocks = self

        def get_quotes_with_report(self, request):
            return items, ContractReport(contract_name="stocks.quotes.intraday")

        def get_quotes_query_result_with_report(self, request, *, write_fact_ref=True):
            assert write_fact_ref is False
            return StockQuotesQueryResult(
                items=items,
                meta=StockQuotesMeta(total_rows=2, returned_rows=2, complete=True, truncated=False),
            ), ContractReport(contract_name="stocks.quotes.intraday")

    monkeypatch.setattr(capture, "build_capture_requests", lambda policy, now: (capture.CaptureRequest("stocks.quotes.intraday", {"codes": ["600000"], "freq": "1m", "start_date": "2026-07-02"}),))
    monkeypatch.setattr(QuoteMuxCaptureJob, "_write_fact_ref_items", lambda self, capability_id, values: 0)

    result = _job(_policy(capability_id="stocks.quotes.intraday"), runtime=Runtime()).run_capture("stocks.quotes.intraday")

    assert result["status"] == CAPTURE_FAILED
    assert result["row_count"] == 2
    assert result["coverage_count"] == 0
    assert "fact.stock_bar_1m" in str(result["detail_json"])


def test_intraday_capture_saves_complete_codes_when_batch_is_incomplete(monkeypatch) -> None:
    write_calls: list[list[StockQuoteItem]] = []
    items = [
        StockQuoteItem(code="600000", trade_time="2026-07-02 09:31:00", freq="1m"),
        StockQuoteItem(code="600001", trade_time="2026-07-02 09:31:00", freq="1m"),
    ]

    class Runtime:
        def __init__(self) -> None:
            self.stocks = self

        def get_quotes_query_result_with_report(self, request, *, write_fact_ref=True):
            assert write_fact_ref is False
            return StockQuotesQueryResult(
                items=items,
                meta=StockQuotesMeta(
                    total_rows=2,
                    returned_rows=2,
                    complete=False,
                    truncated=False,
                    codes=[
                        StockQuoteCodeSummary(
                            code="600000",
                            row_count=240,
                            expected_bar_count=240,
                            actual_bar_count=240,
                            complete=True,
                            truncated=False,
                            missing_trade_times=[],
                        ),
                        StockQuoteCodeSummary(
                            code="600001",
                            row_count=1,
                            expected_bar_count=240,
                            actual_bar_count=1,
                            complete=False,
                            truncated=False,
                            missing_trade_times=["2026-07-02 15:00:00"],
                        )
                    ],
                ),
            ), ContractReport(contract_name="stocks.quotes.intraday").with_store_stats(write=True)

    monkeypatch.setattr(capture, "build_capture_requests", lambda policy, now: (capture.CaptureRequest("stocks.quotes.intraday", {"codes": ["600000", "600001"], "freq": "1m", "start_date": "2026-07-02"}),))
    monkeypatch.setattr(QuoteMuxCaptureJob, "_write_fact_ref_items", lambda self, capability_id, items: write_calls.append(list(items)) or len(items))

    gaps = FakeCaptureGaps()
    job = QuoteMuxCaptureJob(
        runtime=Runtime(),
        policies=MemoryCapturePolicies((_policy(capability_id="stocks.quotes.intraday"),)),
        runs=MemoryCaptureRuns(),
        locks=FakeLocks(),
        now_provider=lambda: datetime(2026, 7, 2, 18, 30),
        cache_store=FakeCacheStore(),
        gaps=gaps,
    )
    result = job.run_capture("stocks.quotes.intraday")

    assert result["status"] == CAPTURE_PARTIAL
    assert result["row_count"] == 1
    assert result["coverage_count"] == 1
    assert "覆盖不完整" in str(result["detail_json"])
    assert [[item.code for item in call] for call in write_calls] == [["600000"]]
    assert [(item[1], item[2]) for item in gaps.resolved] == [("600000", "2026-07-02")]
    assert [(str(item["code"]), str(item["trade_date"])) for item in gaps.incomplete] == [("600001", "2026-07-02")]


def test_intraday_gap_retry_requests_only_persisted_gaps() -> None:
    policy = _policy(capability_id="stocks.quotes.intraday", batch_size=2)
    gaps = (
        CaptureGap("stocks.quotes.intraday", "SHSE", "600000", "2026-07-20", 240, 0, 240, "provider_empty", {}, 1, "", "", "", "", ""),
        CaptureGap("stocks.quotes.intraday", "SZSE", "000001", "2026-07-20", 240, 120, 120, "provider_empty", {}, 2, "", "", "", "", ""),
        CaptureGap("stocks.quotes.intraday", "SZSE", "000002", "2026-07-21", 240, 0, 240, "system_failed", {}, 1, "", "", "", "", ""),
    )

    requests = capture._intraday_gap_requests(policy, gaps)

    assert [request.request_identity["codes"] for request in requests] == [["600000", "000001"], ["000002"]]
    assert [request.request_identity["start_date"] for request in requests] == ["2026-07-20", "2026-07-21"]


def test_intraday_capture_requests_only_missing_traded_codes(monkeypatch) -> None:
    monkeypatch.setattr(capture, "_recent_trading_days", lambda window_count, now: ("2026-07-14", "2026-07-15"))
    monkeypatch.setattr(capture, "_intraday_missing_universe_dates", lambda trading_days: ())
    monkeypatch.setattr(
        capture,
        "_intraday_missing_stock_codes",
        lambda trade_date: ("600000", "600001", "600002") if trade_date == "2026-07-15" else ("600003",),
    )

    requests = capture.build_capture_requests(
        _policy(capability_id="stocks.quotes.intraday", batch_size=2, window_count=2),
        datetime(2026, 7, 16, 18, 30),
    )

    assert [request.request_identity["codes"] for request in requests] == [["600000", "600001"], ["600002"], ["600003"]]
    assert [request.request_identity["start_date"] for request in requests] == ["2026-07-15", "2026-07-15", "2026-07-14"]
    assert [request.request_identity["limit"] for request in requests] == [5000, 5000, 5000]


def test_recent_trading_days_excludes_current_day_before_market_data_ready(monkeypatch) -> None:
    requested_end_dates: list[str] = []

    def load_calendar(market: str, start_date: str, end_date: str, is_open: bool):
        requested_end_dates.append(end_date)
        records = [
            {"trade_date": "2026-07-16"},
            {"trade_date": "2026-07-17"},
        ]
        return capture.pd.DataFrame.from_records(
            [record for record in records if record["trade_date"] <= end_date]
        )

    monkeypatch.setattr(capture, "load_trade_calendar_frame", load_calendar)

    morning_days = capture._recent_trading_days(2, datetime(2026, 7, 17, 4, 0))
    evening_days = capture._recent_trading_days(2, datetime(2026, 7, 17, 20, 0))

    assert requested_end_dates == ["2026-07-16", "2026-07-17"]
    assert morning_days == ("2026-07-16",)
    assert evening_days == ("2026-07-16", "2026-07-17")


def test_daily_capture_requests_include_final_same_day_data_by_1520(monkeypatch) -> None:
    class _Frame:
        empty = False

        def __init__(self, records):
            self.records = records

        def to_dict(self, orient: str):
            return self.records

    monkeypatch.setattr(
        capture,
        "load_trade_calendar_frame",
        lambda market, start_date, end_date, is_open: _Frame(
            [
                {"trade_date": trade_date}
                for trade_date in ("2026-09-03", "2026-09-04")
                if start_date <= trade_date <= end_date
            ]
        ),
    )
    monkeypatch.setattr(capture, "_active_stock_codes", lambda trade_date: ("600000", "000001"))
    monkeypatch.setattr(
        capture,
        "_date_missing_ranges",
        lambda capability_id, request_identity, expected_dates: ((expected_dates[0], expected_dates[-1]),),
    )

    requests = capture.build_capture_requests(
        _policy(window_count=2, batch_size=100),
        datetime(2026, 9, 4, 15, 20),
    )

    assert requests
    assert requests[0].request_identity["end_date"] == "2026-09-04"


def test_catalog_capture_requests_authoritative_full_refresh() -> None:
    requests = capture.build_capture_requests(
        _policy(capability_id="stocks.catalog", scope_profile=capture.PROFILE_CATALOG_SNAPSHOT),
        datetime(2026, 7, 16, 18, 30),
    )

    assert len(requests) == 1
    assert requests[0].request_identity == {
        "codes": [],
        "name": "",
        "exchange": "",
        "list_status": "",
        "include_delisted": True,
        "limit": 10000,
        "offset": 0,
        "refresh": True,
    }


def test_auction_capture_uses_one_market_wide_request_per_missing_range(monkeypatch) -> None:
    monkeypatch.setattr(capture, "_recent_trading_days", lambda window_count, now: ("2026-08-13", "2026-08-14"))
    monkeypatch.setattr(capture, "_active_stock_codes", lambda trade_date: ("600000", "600001", "000001"))
    seen: list[dict[str, object]] = []

    def append_missing(requests, capability_id, request_identity, expected_dates):
        seen.append(request_identity)
        requests.append(capture.CaptureRequest(capability_id, {**request_identity, "start_date": expected_dates[0], "end_date": expected_dates[-1]}))

    monkeypatch.setattr(capture, "_append_missing_range_requests", append_missing)

    requests = capture.build_capture_requests(
        _policy(capability_id="stocks.quotes.auctions", window_count=30, batch_size=100),
        datetime(2026, 8, 14, 18, 30),
    )

    assert len(requests) == 1
    assert seen == [{"code": "", "session": "", "trade_date": "", "start_date": "2026-08-13", "end_date": "2026-08-14"}]
    assert requests[0].request_identity["code"] == ""
    assert requests[0].request_identity["start_date"] == "2026-08-13"
    assert requests[0].request_identity["end_date"] == "2026-08-14"
