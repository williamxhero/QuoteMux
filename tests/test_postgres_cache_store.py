from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta, time

import quotemux
from platform_models import ConnectQuotaItem, HKConnectTargetItem, IndexQuoteItem, NewsEventItem, RankingBrokerPickItem, StockFinancialStatementItem, StockQuoteItem, TradingCalendarItem

from quotemux.reports import ContractReport
from quotemux.requests.stocks import StockDailySnapshotRequest, StockQuotesRequest
from quotemux.requests.markets import NextTradingDaysRequest, PreviousTradingDaysRequest, TradingCalendarRequest, YearlyTradingCalendarRequest
from quotemux.runtime_core.executor import FallbackReport
from quotemux.settings import QuoteMuxSettings
from quotemux.stocks import QuoteMuxStocks
from quotemux.indexes import QuoteMuxIndexes
from quotemux.markets import QuoteMuxMarkets
from quotemux.news import QuoteMuxNews
from quotemux.rankings import QuoteMuxRankings
from quotemux.store import postgres
from quotemux.store.payload_store import CachePayloadRef
from quotemux.store.postgres import CacheCoverage, CachePolicy, UnifiedPostgresCacheStore

_quotemux_import = quotemux


def _policy(**overrides: object) -> CachePolicy:
    payload = {
        "capability_id": "stocks.quotes.daily",
        "enabled": True,
        "read_enabled": True,
        "write_enabled": True,
        "ttl_seconds": 1800,
        "time_field": "trade_time",
        "key_fields": ("code", "freq", "adjust"),
        "request_scope_fields": ("code", "freq", "adjust"),
        "coverage_mode": "trading_day_range",
    }
    payload.update(overrides)
    return CachePolicy(**payload)


def _policy_for(capability_id: str, **overrides: object) -> CachePolicy:
    spec = next(item for item in postgres.DEFAULT_POLICY_SPECS if item.capability_id == capability_id)
    payload = {
        "capability_id": capability_id,
        "enabled": True,
        "read_enabled": True,
        "write_enabled": True,
        "ttl_seconds": spec.ttl_seconds,
        "time_field": spec.time_field,
        "key_fields": spec.key_fields,
        "request_scope_fields": spec.request_scope_fields,
        "coverage_mode": spec.coverage_mode,
    }
    payload.update(overrides)
    return CachePolicy(**payload)


class MemoryPolicyRepository:
    def __init__(self, policy: CachePolicy | None) -> None:
        self.policy = policy

    def get(self, capability_id: str) -> CachePolicy | None:
        if self.policy is None or self.policy.capability_id != capability_id:
            return None
        return self.policy

    def list(self) -> tuple[CachePolicy, ...]:
        return () if self.policy is None else (self.policy,)

    def update(self, policy: CachePolicy) -> bool:
        self.policy = policy
        return True


class MemoryAuditRepository:
    def __init__(self) -> None:
        self.events: list[str] = []

    def write(self, capability_id: str, event_type: str, scope_identity: str, time_start: datetime | None, time_end: datetime | None, detail: dict[str, object]) -> None:
        self.events.append(event_type)


class MemoryCoverageRepository:
    def __init__(self) -> None:
        self.items: dict[tuple[str, str, datetime, datetime], CacheCoverage] = {}

    def find_for_scope(self, capability_id: str, scope_identity: str) -> tuple[CacheCoverage, ...]:
        return tuple(item for key, item in self.items.items() if key[0] == capability_id and key[1] == scope_identity)

    def upsert_many(self, capability_id: str, coverages) -> bool:
        for scope, row_count, fresh_until, source_json in coverages:
            key = (capability_id, scope.scope_identity, scope.time_start, scope.time_end)
            self.items[key] = CacheCoverage(scope.scope_identity, scope.time_start, scope.time_end, fresh_until, row_count, source_json)
        return True


class MemoryRowRepository:
    def __init__(self) -> None:
        self.items: dict[tuple[str, datetime, str], tuple[dict[str, object], datetime]] = {}

    def read(self, capability_id: str, time_start: datetime, time_end: datetime, never_expires: bool) -> tuple[dict[str, object], ...]:
        now = datetime.now()
        return tuple(
            payload
            for (stored_capability_id, time_key, _), (payload, fresh_until) in self.items.items()
            if stored_capability_id == capability_id and time_start <= time_key <= time_end and (never_expires or fresh_until > now)
        )

    def upsert_many(self, capability_id: str, rows) -> bool:
        for time_key, identity_value, payload_json, source_json, fresh_until in rows:
            self.items[(capability_id, time_key, identity_value)] = (payload_json, fresh_until)
        return True


def _store(policy: CachePolicy | None = None) -> UnifiedPostgresCacheStore:
    store = UnifiedPostgresCacheStore()
    store.policies = MemoryPolicyRepository(policy or _policy())
    store.rows = MemoryRowRepository()
    store.coverage = MemoryCoverageRepository()
    store.audit = MemoryAuditRepository()
    return store


def test_empty_scope_value_matches_all_payloads() -> None:
    store = _store(
        _policy_for(
            "stocks.catalog",
            coverage_mode="snapshot",
            request_scope_fields=("code",),
        )
    )

    store.write(
        "stocks.catalog",
        {"code": ""},
        [
            {"code": "600000", "name": "浦发银行", "list_date": "1999-11-10"},
            {"code": "000001", "name": "平安银行", "list_date": "1991-04-03"},
        ],
        ContractReport(contract_name="stocks.catalog"),
    )

    result = store.read("stocks.catalog", {"code": ""})

    assert result.hit
    assert {item["code"] for item in result.items} == {"000001", "600000"}


class _Snapshot:
    profile_id = "profile-default"
    version = "v1"
    source_instances = ()

    def list_enabled_package_ids(self) -> tuple[str, ...]:
        return ()

    def get_contract_source_order(self, contract_name: str, fallback: tuple[str, ...]) -> tuple[str, ...]:
        return fallback

    def get_contract_merge_strategy(self, contract_name: str, fallback: str) -> str:
        return fallback

    def get_contract_mode(self, contract_name: str, fallback: str) -> str:
        return fallback


class _ConfigRuntime:
    def get_active_snapshot(self) -> _Snapshot:
        return _Snapshot()


def _patch_store_context(monkeypatch, store: UnifiedPostgresCacheStore) -> None:
    def load_from_store(capability_id, request_identity, model_type):
        result = store.read(capability_id, request_identity)
        return [model_type(**item) for item in result.items], result

    def write_to_store(capability_id, request_identity, items, report, quarantine_count=0):
        return store.write(capability_id, request_identity, items, report)

    monkeypatch.setattr("quotemux.store.runtime.get_postgres_cache_store", lambda: store)
    monkeypatch.setattr("quotemux.store.admin.get_postgres_cache_store", lambda: store)
    monkeypatch.setattr("quotemux.query_engine.load_store_result", load_from_store)
    monkeypatch.setattr("quotemux.query_engine.store_result", write_to_store)
    monkeypatch.setattr("quotemux.stocks.get_fact_ref_writer", lambda capability_id: None)
    monkeypatch.setattr("quotemux.indexes.get_fact_ref_writer", lambda capability_id: None)
    monkeypatch.setattr("quotemux.boards.get_fact_ref_writer", lambda capability_id: None)
    monkeypatch.setattr("quotemux.markets.get_fact_ref_writer", lambda capability_id: None)
    monkeypatch.setattr("quotemux.stocks.get_local_stock_quotes", lambda codes, freq, trade_date, start_date, end_date, start_time, end_time, count, adjust: [])
    monkeypatch.setattr("quotemux.stocks.get_local_stock_intraday_quotes", lambda codes, freq, trade_date, start_date, end_date, start_time, end_time, count: [])
    monkeypatch.setattr("quotemux.stocks.get_local_stock_daily_snapshot_full", lambda trade_date: [])
    monkeypatch.setattr("quotemux.stocks.load_stock_active_codes_frame", lambda trade_date: __import__("pandas").DataFrame())
    monkeypatch.setattr("quotemux.indexes.get_local_index_quotes", lambda index_codes, freq, trade_date, start_date, end_date, count: [])
    monkeypatch.setattr("quotemux.markets.get_local_trading_calendar", lambda exchange, start_date, end_date, is_open: [])
    monkeypatch.setattr("quotemux.store.runtime.record_provider_event", lambda *args, **kwargs: None)
    monkeypatch.setattr("quotemux.store.runtime.get_config_runtime", lambda: _ConfigRuntime())
    monkeypatch.setattr("quotemux.config_runtime.runtime.get_config_runtime", lambda: _ConfigRuntime())
    monkeypatch.setattr("quotemux.runtime_core.executor.get_config_runtime", lambda: _ConfigRuntime())
    monkeypatch.setattr("quotemux.settings.get_config_runtime", lambda: _ConfigRuntime())


def _item(code: str, trade_time: str = "2026-04-03", close: float = 10.5) -> StockQuoteItem:
    return StockQuoteItem(code=code, trade_time=trade_time, freq="1d", close=close, adjust="none")


def _request(code: str = "600000", start_date: str = "2026-04-03", end_date: str = "2026-04-03") -> dict[str, object]:
    return {"codes": [code], "freq": "1d", "adjust": "none", "start_date": start_date, "end_date": end_date}


def _source_call_stub(responses: dict[tuple[str, str], object]) -> Callable[..., object]:
    def fake_call(package_id: str, handler_name: str, *args: object) -> object:
        value = responses.get((package_id, handler_name), [])
        if isinstance(value, BaseException):
            raise value
        if callable(value):
            return value(*args)
        return value

    return fake_call


def test_cache_hit_and_coverage_hit() -> None:
    store = _store()
    report = ContractReport(contract_name="stocks.quotes.daily")

    store.write("stocks.quotes.daily", _request(), [_item("600000")], report)
    result = store.read("stocks.quotes.daily", _request())

    assert result.hit is True
    assert len(result.items) == 1
    assert "cache_hit" in store.audit.events


def test_cache_write_can_run_when_read_cache_disabled() -> None:
    store = _store(_policy(enabled=False, read_enabled=False, write_enabled=True))
    report = ContractReport(contract_name="stocks.quotes.daily")

    write_result = store.write("stocks.quotes.daily", _request(), [_item("600000")], report)
    read_result = store.read("stocks.quotes.daily", _request())

    assert write_result.status == "write"
    assert read_result.status == "skip"


def test_cache_miss() -> None:
    store = _store()

    result = store.read("stocks.quotes.daily", _request())

    assert result.status == "miss"
    assert result.items == ()


def test_empty_code_list_request_still_builds_single_scope() -> None:
    store = _store(_policy_for("stocks.catalog"))

    result = store.read("stocks.catalog", {"codes": [], "name": "", "exchange": "", "list_status": "", "include_delisted": True})

    assert result.status == "miss"
    assert result.scope_identity == "codes=|name=|exchange=|list_status=|include_delisted=True"


def test_partial_hit() -> None:
    store = _store()
    report = ContractReport(contract_name="stocks.quotes.daily")

    store.write("stocks.quotes.daily", _request(end_date="2026-04-03"), [_item("600000")], report)
    result = store.read("stocks.quotes.daily", _request(start_date="2026-04-03", end_date="2026-04-06"))

    assert result.partial_hit is True
    assert len(result.items) == 1


def test_stale_refresh() -> None:
    store = _store()
    report = ContractReport(contract_name="stocks.quotes.daily")
    store.write("stocks.quotes.daily", _request(), [_item("600000")], report)
    for key, coverage in list(store.coverage.items.items()):
        store.coverage.items[key] = CacheCoverage(coverage.scope_identity, coverage.time_start, coverage.time_end, datetime.now() - timedelta(seconds=1), coverage.row_count, coverage.source_json)

    result = store.read("stocks.quotes.daily", _request())

    assert result.status == "stale"


def test_ttl_minus_one_writes_forever_fresh_until() -> None:
    store = _store(_policy(ttl_seconds=-1))
    report = ContractReport(contract_name="stocks.quotes.daily")

    store.write("stocks.quotes.daily", _request(), [_item("600000")], report)

    assert all(fresh_until == postgres.CACHE_NEVER_EXPIRE_UNTIL for _, fresh_until in store.rows.items.values())
    assert all(coverage.fresh_until == postgres.CACHE_NEVER_EXPIRE_UNTIL for coverage in store.coverage.items.values())


def test_ttl_minus_one_keeps_existing_expired_rows_fresh() -> None:
    store = _store(_policy(ttl_seconds=-1))
    report = ContractReport(contract_name="stocks.quotes.daily")
    expired_at = datetime.now() - timedelta(seconds=1)
    store.write("stocks.quotes.daily", _request(), [_item("600000")], report)
    for key, coverage in list(store.coverage.items.items()):
        store.coverage.items[key] = CacheCoverage(coverage.scope_identity, coverage.time_start, coverage.time_end, expired_at, coverage.row_count, coverage.source_json)
    for key, (payload, _) in list(store.rows.items.items()):
        store.rows.items[key] = (payload, expired_at)

    result = store.read("stocks.quotes.daily", _request())

    assert result.hit is True
    assert len(result.items) == 1


def test_ttl_zero_active_policy_keeps_existing_expired_rows_fresh() -> None:
    store = _store(_policy(ttl_seconds=0, enabled=True, read_enabled=True, write_enabled=True))
    report = ContractReport(contract_name="stocks.quotes.daily")
    expired_at = datetime.now() - timedelta(seconds=1)
    store.write("stocks.quotes.daily", _request(), [_item("600000")], report)
    for key, coverage in list(store.coverage.items.items()):
        store.coverage.items[key] = CacheCoverage(coverage.scope_identity, coverage.time_start, coverage.time_end, expired_at, coverage.row_count, coverage.source_json)
    for key, (payload, _) in list(store.rows.items.items()):
        store.rows.items[key] = (payload, expired_at)

    result = store.read("stocks.quotes.daily", _request())

    assert result.hit is True
    assert len(result.items) == 1


def test_empty_result_cache() -> None:
    store = _store()
    report = ContractReport(contract_name="stocks.quotes.daily")

    store.write("stocks.quotes.daily", _request(), [], report)
    result = store.read("stocks.quotes.daily", _request())

    assert result.hit is True
    assert result.items == ()


def test_capability_independent_switches() -> None:
    disabled = _store(_policy(enabled=False, read_enabled=False, write_enabled=False))
    report = ContractReport(contract_name="stocks.quotes.daily")

    assert disabled.read("stocks.quotes.daily", _request()).status == "skip"
    assert disabled.write("stocks.quotes.daily", _request(), [_item("600000")], report).status == "skip"


def test_upsert_deduplicates_unique_row_key() -> None:
    store = _store()
    report = ContractReport(contract_name="stocks.quotes.daily")

    store.write("stocks.quotes.daily", _request(), [_item("600000", close=10.5)], report)
    store.write("stocks.quotes.daily", _request(), [_item("600000", close=11.5)], report)

    assert len(store.rows.items) == 1
    payload = next(iter(store.rows.items.values()))[0]
    assert payload["close"] == 11.5


def test_postgresql_upsert_sql_uses_documented_unique_key(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(postgres, "_ensure_schema", lambda: True)
    monkeypatch.setattr(postgres, "execute_many", lambda query, params: captured.setdefault("query", query) or True)
    monkeypatch.setattr(postgres, "put_payload", lambda capability_id, time_key, payload_json, source_json: CachePayloadRef("payload-sha", "aa/2026-04/payload-sha.json.gz", "source-sha", "aa/2026-04/source-sha.json.gz"))

    postgres.CacheRowRepository().upsert_many(
        "stocks.quotes.daily",
        [(datetime(2026, 4, 3), "code=600000|freq=1d|adjust=none", {"code": "600000", "trade_time": "2026-04-03", "freq": "1d", "adjust": "none"}, {}, datetime.now())],
    )

    assert "on conflict (capability_id, time_key, identity_value)" in str(captured["query"])
    assert "payload_json" not in str(captured["query"])


def test_default_policy_specs_cover_public_capabilities() -> None:
    from quotemux.capabilities import is_independently_configurable_capability_id, list_capability_ids

    capability_ids = {capability_id for capability_id in list_capability_ids() if is_independently_configurable_capability_id(capability_id)}
    policy_ids = {item.capability_id for item in postgres.DEFAULT_POLICY_SPECS}

    assert capability_ids == policy_ids
    assert all(item.enabled for item in postgres.DEFAULT_POLICY_SPECS)


def test_derived_calendar_apis_read_root_trading_calendar_store(monkeypatch) -> None:
    settings = QuoteMuxSettings(enabled_sources=("tushare", "akshare"))
    calendar_store = _store(_policy_for("markets.calendar.trading"))
    calendar_items = [
        TradingCalendarItem(exchange="SSE", trade_date="2026-04-02", is_open=True),
        TradingCalendarItem(exchange="SSE", trade_date="2026-04-03", is_open=True),
        TradingCalendarItem(exchange="SSE", trade_date="2026-04-07", is_open=True),
    ]
    calendar_store.write(
        "markets.calendar.trading",
        {"exchange": "SSE", "start_date": "2025-01-01", "end_date": "2027-12-31", "is_open": True},
        calendar_items,
        ContractReport(contract_name="markets.calendar.trading"),
    )
    calendar_store.write(
        "markets.calendar.trading",
        {"exchange": "SSE", "start_date": "2025-01-01", "end_date": "2027-12-31", "is_open": None},
        calendar_items,
        ContractReport(contract_name="markets.calendar.trading"),
    )
    _patch_store_context(monkeypatch, calendar_store)
    monkeypatch.setattr("quotemux.markets._source_package_call", _source_call_stub({("tushare", "get_trading_calendar"): AssertionError("source should not run"), ("akshare", "get_trading_calendar"): AssertionError("source should not run")}))

    markets = QuoteMuxMarkets(settings)

    assert markets.get_previous_trading_days(PreviousTradingDaysRequest(exchange="SSE", trade_date="2026-04-03", n=1))[0].trade_date == "2026-04-02"
    assert markets.get_next_trading_days(NextTradingDaysRequest(exchange="SSE", trade_date="2026-04-03", n=1))[0].trade_date == "2026-04-07"
    assert [item.trade_date for item in markets.get_yearly_trading_calendar(YearlyTradingCalendarRequest(exchange="SSE", start_year=2026, end_year=2026))] == ["2026-04-02", "2026-04-03", "2026-04-07"]


def test_cache_admin_updates_policy(monkeypatch) -> None:
    from quotemux.store.admin import CachePolicyUpdate, QuoteMuxCacheAdmin

    store = _store()
    monkeypatch.setattr("quotemux.store.admin.get_postgres_cache_store", lambda: store)

    updated = QuoteMuxCacheAdmin().update_policy(CachePolicyUpdate("stocks.quotes.daily", False, 60))

    assert updated["enabled"] is False
    assert updated["read_enabled"] is False
    assert updated["write_enabled"] is False
    assert updated["ttl_seconds"] == 60


def test_cache_admin_preserves_ttl_when_payload_omits_it(monkeypatch) -> None:
    from quotemux.store.admin import CachePolicyUpdate, QuoteMuxCacheAdmin

    store = _store(_policy(ttl_seconds=1800))
    monkeypatch.setattr("quotemux.store.admin.get_postgres_cache_store", lambda: store)

    updated = QuoteMuxCacheAdmin().update_policy(CachePolicyUpdate("stocks.quotes.daily", False, None))

    assert updated["enabled"] is False
    assert updated["read_enabled"] is False
    assert updated["write_enabled"] is False
    assert updated["ttl_seconds"] == 1800


def test_capture_admin_keeps_cache_enabled_and_preserves_ttl(monkeypatch) -> None:
    from quotemux.store.admin import CapturePolicyPayload, QuoteMuxCaptureAdmin

    class FakeJob:
        def update_policy(self, update) -> dict[str, object]:
            return {"capability_id": update.capability_id, "enabled": update.enabled, "cadence": update.cadence}

    store = _store(_policy(ttl_seconds=172800))
    monkeypatch.setattr("quotemux.store.admin.get_postgres_cache_store", lambda: store)
    payload = CapturePolicyPayload("stocks.quotes.daily", True, "daily", time(0, 0), "Asia/Shanghai", None, None, None, "active_stocks_recent_trading_days", 5, 50, "")

    updated = QuoteMuxCaptureAdmin(job=FakeJob()).update_policy(payload)
    disabled_cache = store.get_policy("stocks.quotes.daily")
    restored = QuoteMuxCaptureAdmin(job=FakeJob()).update_policy(CapturePolicyPayload("stocks.quotes.daily", False, "daily", time(0, 0), "Asia/Shanghai", None, None, None, "active_stocks_recent_trading_days", 5, 50, ""))
    restored_cache = store.get_policy("stocks.quotes.daily")

    assert updated["enabled"] is True
    assert disabled_cache is not None
    assert disabled_cache.enabled is True
    assert disabled_cache.read_enabled is True
    assert disabled_cache.write_enabled is True
    assert disabled_cache.ttl_seconds == 172800
    assert restored["enabled"] is False
    assert restored_cache is not None
    assert restored_cache.enabled is True
    assert restored_cache.read_enabled is True
    assert restored_cache.write_enabled is True
    assert restored_cache.ttl_seconds == 172800


def test_capture_admin_enables_cache_when_ttl_zero(monkeypatch) -> None:
    from quotemux.store.admin import CapturePolicyPayload, QuoteMuxCaptureAdmin

    class FakeJob:
        def update_policy(self, update) -> dict[str, object]:
            return {"capability_id": update.capability_id, "enabled": update.enabled, "cadence": update.cadence}

    store = _store(_policy(ttl_seconds=0, enabled=False, read_enabled=False, write_enabled=False))
    monkeypatch.setattr("quotemux.store.admin.get_postgres_cache_store", lambda: store)
    payload = CapturePolicyPayload("stocks.quotes.daily", True, "daily", time(0, 0), "Asia/Shanghai", None, None, None, "active_stocks_recent_trading_days", 5, 50, "")

    QuoteMuxCaptureAdmin(job=FakeJob()).update_policy(payload)
    enabled_cache = store.get_policy("stocks.quotes.daily")
    QuoteMuxCaptureAdmin(job=FakeJob()).update_policy(CapturePolicyPayload("stocks.quotes.daily", False, "daily", time(0, 0), "Asia/Shanghai", None, None, None, "active_stocks_recent_trading_days", 5, 50, ""))
    disabled_cache = store.get_policy("stocks.quotes.daily")

    assert enabled_cache is not None
    assert enabled_cache.enabled is True
    assert enabled_cache.read_enabled is True
    assert enabled_cache.write_enabled is True
    assert enabled_cache.ttl_seconds == 0
    assert disabled_cache is not None
    assert disabled_cache.enabled is False
    assert disabled_cache.read_enabled is False
    assert disabled_cache.write_enabled is False
    assert disabled_cache.ttl_seconds == 0


def test_capture_admin_keeps_never_expire_ttl_writable(monkeypatch) -> None:
    from quotemux.store.admin import CapturePolicyPayload, QuoteMuxCaptureAdmin

    class FakeJob:
        def update_policy(self, update) -> dict[str, object]:
            return {"capability_id": update.capability_id, "enabled": update.enabled, "cadence": update.cadence}

    store = _store(_policy(ttl_seconds=-1))
    monkeypatch.setattr("quotemux.store.admin.get_postgres_cache_store", lambda: store)
    payload = CapturePolicyPayload("stocks.quotes.daily", True, "daily", time(0, 0), "Asia/Shanghai", None, None, None, "active_stocks_recent_trading_days", 5, 50, "")

    QuoteMuxCaptureAdmin(job=FakeJob()).update_policy(payload)
    disabled_cache = store.get_policy("stocks.quotes.daily")
    QuoteMuxCaptureAdmin(job=FakeJob()).update_policy(CapturePolicyPayload("stocks.quotes.daily", False, "daily", time(0, 0), "Asia/Shanghai", None, None, None, "active_stocks_recent_trading_days", 5, 50, ""))
    restored_cache = store.get_policy("stocks.quotes.daily")

    assert disabled_cache is not None
    assert disabled_cache.enabled is True
    assert disabled_cache.read_enabled is True
    assert disabled_cache.write_enabled is True
    assert disabled_cache.ttl_seconds == -1
    assert restored_cache is not None
    assert restored_cache.enabled is True
    assert restored_cache.read_enabled is True
    assert restored_cache.write_enabled is True
    assert restored_cache.ttl_seconds == -1


def test_missing_planner_modes() -> None:
    from quotemux.store.planner import CacheMissingPlanner, CacheMissingRange

    planner = CacheMissingPlanner()
    start = datetime(2026, 4, 1)
    end = datetime(2026, 4, 3)
    covered = (CacheMissingRange(datetime(2026, 4, 1), datetime(2026, 4, 1)),)

    date_ranges = planner.plan("date_range", start, end, covered)
    trading_ranges = planner.plan("trading_day_range", start, end, covered, (datetime(2026, 4, 1), datetime(2026, 4, 3)))
    snapshot_ranges = planner.plan("snapshot", start, end, ())
    minute_ranges = planner.plan("minute_range", datetime(2026, 4, 1, 9, 30), datetime(2026, 4, 1, 9, 31), ())
    period_ranges = planner.plan("period_range", start, end, covered)
    event_ranges = planner.plan("event_range", start, end, covered)

    assert date_ranges == (CacheMissingRange(datetime(2026, 4, 2), datetime(2026, 4, 3)),)
    assert trading_ranges == (CacheMissingRange(datetime(2026, 4, 3), datetime(2026, 4, 3)),)
    assert snapshot_ranges == (CacheMissingRange(start, end),)
    assert minute_ranges == (CacheMissingRange(datetime(2026, 4, 1, 9, 30), datetime(2026, 4, 1, 9, 31)),)
    assert period_ranges == (CacheMissingRange(datetime(2026, 4, 2), datetime(2026, 4, 3)),)
    assert event_ranges == (CacheMissingRange(datetime(2026, 4, 2), datetime(2026, 4, 3)),)


def test_minute_planner_respects_trading_sessions() -> None:
    from quotemux.store.planner import CacheMissingPlanner, CacheMissingRange

    planner = CacheMissingPlanner()
    session_ranges = (
        CacheMissingRange(datetime(2026, 4, 1, 9, 30), datetime(2026, 4, 1, 11, 30)),
        CacheMissingRange(datetime(2026, 4, 1, 13, 0), datetime(2026, 4, 1, 15, 0)),
    )

    minute_ranges = planner.plan(
        "minute_range",
        datetime(2026, 4, 1, 11, 29),
        datetime(2026, 4, 1, 13, 1),
        (),
        session_ranges=session_ranges,
    )

    assert minute_ranges == (
        CacheMissingRange(datetime(2026, 4, 1, 11, 29), datetime(2026, 4, 1, 11, 30)),
        CacheMissingRange(datetime(2026, 4, 1, 13, 0), datetime(2026, 4, 1, 13, 1)),
    )


def test_first_batch_runtime_capabilities_hit_store(monkeypatch) -> None:
    settings = QuoteMuxSettings(enabled_sources=("tushare", "efinance", "mootdx", "akshare", "opentdx"))

    daily_store = _store(_policy_for("stocks.quotes.daily"))
    daily_store.write(
        "stocks.quotes.daily",
        {"codes": ["600000"], "freq": "1d", "adjust": "none", "start_date": "2026-04-03", "end_date": "2026-04-03"},
        [StockQuoteItem(code="600000", trade_time="2026-04-03", freq="1d", close=10.5, adjust="none")],
        ContractReport(contract_name="stocks.quotes.daily"),
    )
    _patch_store_context(monkeypatch, daily_store)
    monkeypatch.setattr("quotemux.stocks._source_package_call", _source_call_stub({("tushare", "get_stock_quotes"): AssertionError("source should not run")}))
    items, report = QuoteMuxStocks(settings).get_quotes_with_report(StockQuotesRequest(codes=["600000"], freq="1d", start_date="2026-04-03", end_date="2026-04-03"))
    assert items[0].code == "600000"
    assert report.store_hit_count == 1

    intraday_store = _store(_policy_for("stocks.quotes.intraday"))
    intraday_store.write(
        "stocks.quotes.intraday",
        {"codes": ["600000"], "freq": "1m", "adjust": "none", "start_time": "2026-04-03 09:30:00", "end_time": "2026-04-03 09:30:00"},
        [StockQuoteItem(code="600000", trade_time="2026-04-03 09:30", freq="1m", close=10.5, adjust="none")],
        ContractReport(contract_name="stocks.quotes.intraday"),
    )
    _patch_store_context(monkeypatch, intraday_store)
    monkeypatch.setattr("quotemux.stocks._source_package_call", _source_call_stub({("opentdx", "get_stock_quotes"): AssertionError("source should not run")}))
    items, report = QuoteMuxStocks(settings).get_quotes_with_report(StockQuotesRequest(codes=["600000"], freq="1m", start_time="2026-04-03 09:30:00", end_time="2026-04-03 09:30:00"))
    assert items[0].trade_time == "2026-04-03 09:30"
    assert report.store_hit_count == 1

    snapshot_store = _store(_policy_for("stocks.quotes.daily_snapshot"))
    snapshot_store.write(
        "stocks.quotes.daily_snapshot",
        {"trade_date": "2026-04-03"},
        [StockQuoteItem(code="600000", trade_time="2026-04-03", freq="1d", close=10.5, adjust="none")],
        ContractReport(contract_name="stocks.quotes.daily_snapshot"),
    )
    _patch_store_context(monkeypatch, snapshot_store)
    monkeypatch.setattr("quotemux.stocks._source_package_call", _source_call_stub({("tushare", "get_stock_daily_snapshot_full"): AssertionError("source should not run")}))
    items, report = QuoteMuxStocks(settings).get_daily_snapshot_with_report(StockDailySnapshotRequest(trade_date="2026-04-03"))
    assert items[0].code == "600000"
    assert report.store_hit_count == 1

    index_store = _store(_policy_for("indexes.quotes.daily"))
    index_store.write(
        "indexes.quotes.daily",
        {"index_codes": ["000001"], "freq": "1d", "start_date": "2026-04-03", "end_date": "2026-04-03"},
        [IndexQuoteItem(index_code="000001", trade_time="2026-04-03", freq="1d", close=3305.0)],
        ContractReport(contract_name="indexes.quotes.daily"),
    )
    _patch_store_context(monkeypatch, index_store)
    monkeypatch.setattr("quotemux.indexes._source_package_call", _source_call_stub({("mootdx", "get_index_quotes"): AssertionError("source should not run")}))
    items, report = QuoteMuxIndexes(settings).get_quotes_with_report(quotemux.IndexQuotesRequest(index_codes=["000001"], start_date="2026-04-03", end_date="2026-04-03"))
    assert items[0].index_code == "000001"
    assert report.store_hit_count == 1

    calendar_store = _store(_policy_for("markets.calendar.trading"))
    calendar_store.write(
        "markets.calendar.trading",
        {"exchange": "SSE", "start_date": "2025-01-01", "end_date": "2027-12-31", "is_open": None},
        [
            TradingCalendarItem(exchange="SSE", trade_date="2026-04-02", is_open=True),
            TradingCalendarItem(exchange="SSE", trade_date="2026-04-03", is_open=True),
            TradingCalendarItem(exchange="SSE", trade_date="2026-04-07", is_open=True),
        ],
        ContractReport(contract_name="markets.calendar.trading"),
    )
    _patch_store_context(monkeypatch, calendar_store)
    monkeypatch.setattr("quotemux.markets._source_package_call", _source_call_stub({("tushare", "get_trading_calendar"): AssertionError("source should not run")}))
    items, report = QuoteMuxMarkets(settings).get_trading_calendar_with_report(TradingCalendarRequest(exchange="SSE", start_date="2026-04-03", end_date="2026-04-03", is_open=True))
    assert items[0].trade_date == "2026-04-03"
    assert report.store_hit_count == 1

    assert QuoteMuxMarkets(settings).get_previous_trading_days(PreviousTradingDaysRequest(exchange="SSE", trade_date="2026-04-03", n=1))[0].trade_date == "2026-04-02"
    assert QuoteMuxMarkets(settings).get_next_trading_days(NextTradingDaysRequest(exchange="SSE", trade_date="2026-04-03", n=1))[0].trade_date == "2026-04-07"
    assert [item.trade_date for item in QuoteMuxMarkets(settings).get_yearly_trading_calendar(YearlyTradingCalendarRequest(exchange="SSE", start_year=2026, end_year=2026))] == ["2026-04-02", "2026-04-03", "2026-04-07"]


def test_empty_daily_snapshot_does_not_write_store(monkeypatch) -> None:
    class StoreRead:
        hit = False
        partial_hit = False
        status = "miss"

    stored: list[tuple[str, dict[str, object], list[object]]] = []

    monkeypatch.setattr("quotemux.stocks.load_store_result", lambda capability_id, identity, model_type: ([], StoreRead()))
    monkeypatch.setattr("quotemux.stocks.store_result", lambda capability_id, identity, items, report, quarantine_count=0: stored.append((capability_id, identity, list(items))))
    monkeypatch.setattr("quotemux.stocks.run_fallback_chain_with_report", lambda *args, **kwargs: ([], FallbackReport("stocks.quotes.daily_snapshot", "", "", ())))

    items, report = QuoteMuxStocks(QuoteMuxSettings(enabled_sources=("tushare",))).get_daily_snapshot_with_report(StockDailySnapshotRequest(trade_date="2026-06-10"))

    assert items == []
    assert stored == []
    assert report.store_miss_count == 1
    assert report.store_write_count == 0


def test_remaining_capability_families_store_roundtrip(monkeypatch) -> None:
    settings = QuoteMuxSettings(enabled_sources=("tushare",))

    finance_store = _store(_policy_for("stocks.finance.statements"))
    _patch_store_context(monkeypatch, finance_store)
    finance_item = StockFinancialStatementItem(code="600000", report_period="20251231", report_type="income_statement", announce_date="2026-03-31")
    monkeypatch.setattr("quotemux.stocks._source_package_call", _source_call_stub({("tushare", "get_stock_financial_statements"): [finance_item]}))
    stocks = QuoteMuxStocks(settings)
    assert stocks.get_financial_statements(["600000"], "20251231", "", "", "income_statement")[0].code == "600000"
    monkeypatch.setattr("quotemux.stocks._source_package_call", _source_call_stub({("tushare", "get_stock_financial_statements"): AssertionError("source should not run")}))
    assert stocks.get_financial_statements(["600000"], "20251231", "", "", "income_statement")[0].report_period == "2025-12-31"

    reference_store = _store(_policy_for("stocks.reference.hk_connect_targets"))
    _patch_store_context(monkeypatch, reference_store)
    reference_item = HKConnectTargetItem(code="600000", name="浦发银行", direction="south", status="active", effective_date="2026-04-03")
    monkeypatch.setattr("quotemux.stocks._source_package_call", _source_call_stub({("tushare", "get_hk_connect_targets"): [reference_item]}))
    assert QuoteMuxStocks(settings).get_hk_connect_targets("south", "active", "2026-04-03")[0].code == "600000"

    connect_store = _store(_policy_for("markets.connect.quotas"))
    _patch_store_context(monkeypatch, connect_store)
    quota_item = ConnectQuotaItem(trade_date="2026-04-03", market="north", quota_total=1.0, quota_balance=0.5, quota_used=0.5)
    monkeypatch.setattr("quotemux.markets._source_package_call", _source_call_stub({("tushare", "get_connect_quotas"): [quota_item]}))
    assert QuoteMuxMarkets(settings).get_connect_quotas("2026-04-03", "", "", "north")[0].market == "north"

    news_cache = _store(_policy_for("markets.events.news"))
    _patch_store_context(monkeypatch, news_cache)
    news_item = NewsEventItem(
        event_id="evt-1",
        trade_date="2026-04-03",
        announcement_time="2026-04-03 09:30:00",
        crawl_time="2026-04-03 09:31:00",
        session_tag="open",
        event_type="announcement",
        title="公告",
        summary="摘要",
        importance_score=80,
        sentiment="neutral",
        source_name="news",
        primary_detail_url="https://example.com",
        related_stock_codes=["600000"],
    )
    news_cache.write(
        "markets.events.news",
        {
            "trade_date": "2026-04-03",
            "announcement_date": "",
            "crawl_date": "",
            "stock_code": "600000",
            "event_type": "announcement",
            "min_importance_score": None,
            "sort_by": "announcement_time",
            "limit": 20,
            "offset": 0,
            "include_sources": False,
            "include_content_text": False,
        },
        [news_item],
        ContractReport(contract_name="markets.events.news"),
    )
    assert QuoteMuxNews(settings).get_events("2026-04-03", "", "", "600000", "announcement", None, "announcement_time", 20, 0, False, False).events[0].event_id == "evt-1"

    empty_news_cache = _store(_policy_for("markets.events.news"))
    _patch_store_context(monkeypatch, empty_news_cache)
    assert QuoteMuxNews(settings).get_events("2026-04-04", "", "", "600000", "announcement", None, "announcement_time", 20, 0, False, False).events == []

    ranking_store = _store(_policy_for("rankings.research.broker_monthly_picks"))
    _patch_store_context(monkeypatch, ranking_store)
    ranking_item = RankingBrokerPickItem(trade_month="202604", code="600000", name="浦发银行", institution="Broker", rank=1)
    monkeypatch.setattr("quotemux.rankings._source_package_call", _source_call_stub({("tushare", "get_rank_broker_monthly_picks"): [ranking_item]}))
    assert QuoteMuxRankings(settings).get_broker_monthly_picks("202604", 20)[0].trade_month == "202604"


def test_policy_specs_for_remaining_capabilities_use_runtime_fields() -> None:
    specs = {item.capability_id: item for item in postgres.DEFAULT_POLICY_SPECS}

    assert specs["stocks.finance.audits"].time_field == "report_period"
    assert specs["stocks.finance.disclosure_dates"].time_field == "report_period"
    assert specs["stocks.corporate_actions.share_changes"].time_field == "change_date"
    assert specs["stocks.corporate_actions.unlock_schedules"].time_field == "unlock_date"
    assert specs["stocks.reference.hk_connect_targets"].time_field == "effective_date"
    assert specs["stocks.research.reports"].time_field == "report_date"
    assert specs["stocks.research.surveys"].time_field == "survey_date"
    assert specs["markets.connect.quotas"].key_fields == ("market", "trade_date")
    assert specs["markets.events.news"].request_scope_fields == ("event_type", "stock_code", "sort_by", "include_sources", "include_content_text")
    assert specs["rankings.research.broker_monthly_picks"].time_field == "trade_month"


def test_schema_upsert_preserves_existing_ttl_seconds() -> None:
    schema_text = "\n".join(item for item in postgres._ensure_schema.__code__.co_consts if isinstance(item, str))

    assert "ttl_seconds = excluded.ttl_seconds" not in schema_text
