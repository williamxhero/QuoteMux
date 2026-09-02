from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import hashlib
import json
import math
import os
from typing import Callable, Protocol
from zoneinfo import ZoneInfo

from quotemux.infra.common import normalize_stock_code, stock_market_name
from quotemux.infra.db import client as db_client
from quotemux.source_packages.registry import get_default_source_package_registry
from quotemux.strict_read import reject_in_strict_public_read


SHANGHAI = ZoneInfo("Asia/Shanghai")


class LiveBarPersistenceError(RuntimeError):
    """The current Bar was not committed, so it must never be returned."""


@dataclass(frozen=True)
class CurrentBarRequest:
    codes: tuple[str, ...]
    effective_now: datetime


@dataclass(frozen=True)
class CurrentBarNodeAttempt:
    code: str
    server: str
    outcome: str
    detail: str = ""
    provider: str = "mootdx"


@dataclass(frozen=True)
class NativeCurrentStockBar:
    code: str
    interval_start: datetime
    native_trade_time: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    amount: float
    unit_conversion: str
    provider: str = "mootdx"


@dataclass(frozen=True)
class ProviderCurrentBarsResult:
    bars: tuple[NativeCurrentStockBar, ...]
    attempts: tuple[CurrentBarNodeAttempt, ...]
    diagnostics: tuple[dict[str, object], ...] = ()


@dataclass(frozen=True)
class CurrentBarStagingResult:
    observation_version: str
    selected_at: datetime


@dataclass(frozen=True)
class CurrentBarFinalizationCandidate:
    market: str
    code: str
    interval_start: datetime


@dataclass(frozen=True)
class CurrentStockBarItem:
    code: str
    market: str
    interval_start: datetime
    interval_end: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    amount: float
    observed_at: datetime
    provider: str
    observation_version: str
    source_semantics: str = "native"
    is_final: bool = False
    selection_reason: str = "provider_priority"

    def to_dict(self) -> dict[str, object]:
        return {
            "code": self.code,
            "market": self.market,
            "trade_time": self.interval_start.isoformat(),
            "freq": "1m",
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "amount": self.amount,
            "adjust": "none",
            "is_suspended": False,
            "is_st": False,
            "interval_start": self.interval_start.isoformat(),
            "interval_end": self.interval_end.isoformat(),
            "is_final": self.is_final,
            "observed_at": self.observed_at.isoformat(),
            "last_trade_at": self.interval_start.isoformat(),
            "provider": self.provider,
            "source_semantics": self.source_semantics,
            "observation_version": self.observation_version,
            # This result is returned directly after its staging transaction;
            # cache-age calculation is deliberately a later gateway concern.
            "freshness_ms": 0,
            "degraded": False,
            "market_status": "trading",
        }


@dataclass(frozen=True)
class LiveIngestResult:
    items: tuple[CurrentStockBarItem, ...]
    attempts: tuple[CurrentBarNodeAttempt, ...]
    errors: tuple[dict[str, str], ...]
    diagnostics: tuple[dict[str, object], ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            "items": [item.to_dict() for item in self.items],
            "attempts": [attempt.__dict__ for attempt in self.attempts],
            "errors": list(self.errors),
            "diagnostics": list(self.diagnostics),
        }


class CurrentBarProvider(Protocol):
    def fetch(self, codes: tuple[str, ...], effective_now: datetime) -> ProviderCurrentBarsResult: ...


class CurrentBarStore(Protocol):
    def stage(
        self,
        bar: NativeCurrentStockBar,
        observed_at: datetime,
        attempts: tuple[CurrentBarNodeAttempt, ...],
    ) -> CurrentBarStagingResult: ...

    def record_attempts(self, attempts: tuple[CurrentBarNodeAttempt, ...], observed_at: datetime, interval_start: datetime) -> None: ...


class CurrentBarFinalizationStore(Protocol):
    def list_due(self, now: datetime, grace_seconds: int) -> tuple[CurrentBarFinalizationCandidate, ...]: ...

    def finalize(
        self,
        candidate: CurrentBarFinalizationCandidate,
        bar: NativeCurrentStockBar,
        observed_at: datetime,
        attempts: tuple[CurrentBarNodeAttempt, ...],
    ) -> bool: ...


def _as_shanghai(value: datetime) -> datetime:
    localized = value.replace(tzinfo=SHANGHAI) if value.tzinfo is None else value.astimezone(SHANGHAI)
    return localized.replace(microsecond=0)


def _positive_env_float(name: str, default: float) -> float:
    try:
        value = float(os.getenv(name, str(default)))
    except ValueError:
        return default
    return value if value > 0 else default


def _parse_timestamp(value: str) -> datetime | None:
    try:
        return _as_shanghai(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except (TypeError, ValueError):
        return None


class PackageCurrentBarProvider:
    """Adapter from the provider-owned native current-Bar type to QuoteMux."""

    def __init__(self, package_id: str) -> None:
        self._package_id = package_id

    def fetch(self, codes: tuple[str, ...], effective_now: datetime) -> ProviderCurrentBarsResult:
        handler = get_default_source_package_registry().get_handler(self._package_id, "get_current_stock_bars")
        raw_result = handler(list(codes), _as_shanghai(effective_now).isoformat())
        bars = tuple(
            NativeCurrentStockBar(
                code=normalize_stock_code(raw.code).zfill(6),
                interval_start=_as_shanghai(raw.interval_start),
                native_trade_time=str(raw.native_trade_time),
                open=float(raw.open), high=float(raw.high), low=float(raw.low), close=float(raw.close),
                volume=int(raw.volume), amount=float(raw.amount), unit_conversion=str(raw.unit_conversion), provider=self._package_id,
            )
            for raw in raw_result.bars
        )
        attempts = tuple(
            CurrentBarNodeAttempt(code=normalize_stock_code(getattr(raw, "code", "")).zfill(6), server=str(raw.server), outcome=str(raw.outcome), detail=str(raw.detail), provider=self._package_id)
            for raw in raw_result.attempts
        )
        return ProviderCurrentBarsResult(bars=bars, attempts=attempts)


class MootdxCurrentBarProvider(PackageCurrentBarProvider):
    def __init__(self) -> None:
        super().__init__("mootdx")


class OpenTdxCurrentBarProvider(PackageCurrentBarProvider):
    def __init__(self) -> None:
        super().__init__("opentdx")


class EFinancePriceValidator:
    def __init__(self, warning_ratio: float | None = None, severe_ratio: float | None = None, max_age_seconds: float | None = None) -> None:
        self._warning_ratio = warning_ratio if warning_ratio is not None else _positive_env_float("MHK_LIVE_EFINANCE_WARNING_RATIO", 0.01)
        self._severe_ratio = severe_ratio if severe_ratio is not None else _positive_env_float("MHK_LIVE_EFINANCE_SEVERE_RATIO", 0.05)
        self._max_age_seconds = max_age_seconds if max_age_seconds is not None else _positive_env_float("MHK_LIVE_EFINANCE_MAX_AGE_SECONDS", 300)
        if self._warning_ratio > self._severe_ratio:
            raise ValueError("eFinance warning ratio cannot exceed severe ratio")

    def validate(self, bars: tuple[NativeCurrentStockBar, ...], effective_now: datetime) -> tuple[dict[str, object], ...]:
        if not bars:
            return ()
        try:
            handler = get_default_source_package_registry().get_handler("efinance", "get_current_stock_price_snapshots")
            snapshots = handler([bar.code for bar in bars], _as_shanghai(effective_now).isoformat())
        except Exception as exc:
            return tuple({"code": bar.code, "validator": "efinance", "status": "unavailable", "detail": str(exc)} for bar in bars)
        snapshot_by_code = {normalize_stock_code(item.code).zfill(6): item for item in snapshots}
        diagnostics: list[dict[str, object]] = []
        for bar in bars:
            snapshot = snapshot_by_code.get(bar.code)
            if snapshot is None:
                diagnostics.append({"code": bar.code, "validator": "efinance", "status": "unavailable"})
                continue
            source_time = str(getattr(snapshot, "source_time", ""))
            snapshot_time = _parse_timestamp(source_time)
            if snapshot_time is None:
                diagnostics.append({"code": bar.code, "validator": "efinance", "status": "unavailable", "detail": "missing_or_invalid_snapshot_time"})
                continue
            age_seconds = (_as_shanghai(effective_now) - snapshot_time).total_seconds()
            if age_seconds > self._max_age_seconds:
                diagnostics.append({"code": bar.code, "validator": "efinance", "status": "stale", "source_time": snapshot_time.isoformat(), "age_seconds": age_seconds})
                continue
            try:
                price = float(snapshot.price)
            except (TypeError, ValueError):
                diagnostics.append({"code": bar.code, "validator": "efinance", "status": "unavailable", "detail": "invalid_snapshot_price"})
                continue
            if not math.isfinite(price) or price <= 0:
                diagnostics.append({"code": bar.code, "validator": "efinance", "status": "unavailable", "detail": "invalid_snapshot_price"})
                continue
            ratio = abs(bar.close - price) / max(abs(price), 1e-12)
            status = "severe" if ratio >= self._severe_ratio else "warning" if ratio >= self._warning_ratio else "ok"
            diagnostics.append({"code": bar.code, "validator": "efinance", "status": status, "price": price, "difference_ratio": ratio, "source_time": snapshot_time.isoformat(), "age_seconds": age_seconds})
        return tuple(diagnostics)


class WholeBarFallbackProvider:
    def __init__(self, primary: CurrentBarProvider | None = None, fallback: CurrentBarProvider | None = None, validator: EFinancePriceValidator | None = None) -> None:
        self._primary = primary or MootdxCurrentBarProvider()
        self._fallback = fallback or OpenTdxCurrentBarProvider()
        self._validator = validator or EFinancePriceValidator()

    def fetch(self, codes: tuple[str, ...], effective_now: datetime) -> ProviderCurrentBarsResult:
        primary = self._primary.fetch(codes, effective_now)
        diagnostics = self._validator.validate(primary.bars, effective_now)
        severe = {str(item["code"]) for item in diagnostics if item.get("status") == "severe"}
        hit_codes = {bar.code for bar in primary.bars}
        missing = tuple(code for code in codes if code not in hit_codes or code in severe)
        if not missing:
            return ProviderCurrentBarsResult(primary.bars, primary.attempts, diagnostics)
        fallback = self._fallback.fetch(missing, effective_now)
        selected_primary = tuple(bar for bar in primary.bars if bar.code not in severe)
        return ProviderCurrentBarsResult(bars=selected_primary + fallback.bars, attempts=primary.attempts + fallback.attempts, diagnostics=diagnostics)


class PostgresCurrentBarStore:
    """Durably store mutable provider observations and the selected whole Bar."""

    def _run_transaction(self, operation: Callable[[object], CurrentBarStagingResult | None]) -> CurrentBarStagingResult | None:
        reject_in_strict_public_read("live_bar_store")
        if not db_client.is_db_available():
            raise LiveBarPersistenceError("live Bar database is unavailable")
        connection = db_client._acquire_connection()
        try:
            with connection.transaction():
                with connection.cursor() as cursor:
                    result = operation(cursor)
            return result
        except Exception as exc:
            try:
                connection.rollback()
            except Exception:
                pass
            raise LiveBarPersistenceError(f"live Bar staging transaction failed: {exc}") from exc
        finally:
            db_client._release_connection(connection)

    @staticmethod
    def _write_attempts(cursor: object, attempts: tuple[CurrentBarNodeAttempt, ...], observed_at: datetime, interval_start: datetime) -> None:
        for attempt in attempts:
            cursor.execute(
                """
                insert into live.stock_bar_provider_attempt
                  (provider, market, code, freq, interval_start, observed_at, server, outcome, detail)
                values (%s, %s, %s, '1m', %s, %s, %s, %s, %s)
                """,
                (attempt.provider, stock_market_name(attempt.code), attempt.code, interval_start, observed_at, attempt.server, attempt.outcome, attempt.detail),
            )

    def record_attempts(self, attempts: tuple[CurrentBarNodeAttempt, ...], observed_at: datetime, interval_start: datetime) -> None:
        if attempts == ():
            return
        self._run_transaction(lambda cursor: self._write_attempts(cursor, attempts, observed_at, interval_start))

    def stage(
        self,
        bar: NativeCurrentStockBar,
        observed_at: datetime,
        attempts: tuple[CurrentBarNodeAttempt, ...],
    ) -> CurrentBarStagingResult:
        canonical = {
            "provider": bar.provider, "market": stock_market_name(bar.code), "code": bar.code,
            "freq": "1m", "interval_start": bar.interval_start.isoformat(), "native_trade_time": bar.native_trade_time,
            "open": bar.open, "high": bar.high, "low": bar.low, "close": bar.close,
            "volume": bar.volume, "amount": bar.amount, "unit_conversion": bar.unit_conversion,
        }
        observation_hash = hashlib.sha256(json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()

        def _stage(cursor: object) -> CurrentBarStagingResult:
            self._write_attempts(cursor, attempts, observed_at, bar.interval_start)
            cursor.execute(
                """
                insert into live.stock_bar_observation
                  (provider, market, code, freq, interval_start, observed_at, native_trade_time,
                   open, high, low, close, volume, amount, unit_conversion, observation_hash)
                values (%s, %s, %s, '1m', %s, %s, %s::timestamp,
                        %s, %s, %s, %s, %s, %s, %s, %s)
                on conflict (provider, market, code, freq, interval_start, observation_hash) do update set
                  observed_at = excluded.observed_at
                returning observation_version
                """,
                (bar.provider, stock_market_name(bar.code), bar.code, bar.interval_start, observed_at, bar.native_trade_time,
                 bar.open, bar.high, bar.low, bar.close, bar.volume, bar.amount, bar.unit_conversion, observation_hash),
            )
            row = cursor.fetchone()
            cursor.execute(
                """
                insert into live.stock_bar_selected
                  (market, code, freq, interval_start, provider, observation_version, selection_reason, selected_at)
                values (%s, %s, '1m', %s, %s, %s, %s, %s)
                on conflict (market, code, freq, interval_start) do update set
                  provider = excluded.provider,
                  observation_version = excluded.observation_version,
                  selection_reason = excluded.selection_reason,
                  selected_at = excluded.selected_at
                returning selected_at
                """,
                (stock_market_name(bar.code), bar.code, bar.interval_start, bar.provider, row["observation_version"], f"provider_priority:{bar.provider}", observed_at),
            )
            selected = cursor.fetchone()
            return CurrentBarStagingResult(observation_version=str(row["observation_version"]), selected_at=_as_shanghai(selected["selected_at"]))

        result = self._run_transaction(_stage)
        if not isinstance(result, CurrentBarStagingResult):
            raise LiveBarPersistenceError("live Bar staging returned no selection")
        return result

    def list_due(self, now: datetime, grace_seconds: int) -> tuple[CurrentBarFinalizationCandidate, ...]:
        reject_in_strict_public_read("live_bar_finalization_list")
        if not db_client.is_db_available():
            raise LiveBarPersistenceError("live Bar database is unavailable")
        connection = db_client._acquire_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    select market,btrim(code) as code,interval_start
                    from live.stock_bar_selected
                    where state='staged' and interval_start + interval '1 minute' + (%s * interval '1 second') <= %s
                    order by interval_start,code
                    for update skip locked
                    """,
                    (grace_seconds, now),
                )
                rows = cursor.fetchall()
            connection.rollback()
        except Exception as exc:
            try:
                connection.rollback()
            except Exception:
                pass
            raise LiveBarPersistenceError(f"live Bar finalization scan failed: {exc}") from exc
        finally:
            db_client._release_connection(connection)
        return tuple(
            CurrentBarFinalizationCandidate(
                market=str(row["market"]), code=str(row["code"]).strip(), interval_start=_as_shanghai(row["interval_start"]),
            )
            for row in rows
        )

    def finalize(
        self,
        candidate: CurrentBarFinalizationCandidate,
        bar: NativeCurrentStockBar,
        observed_at: datetime,
        attempts: tuple[CurrentBarNodeAttempt, ...],
    ) -> bool:
        if _as_shanghai(bar.interval_start) != _as_shanghai(candidate.interval_start):
            return False
        canonical = {
            "provider": bar.provider, "market": candidate.market, "code": candidate.code, "freq": "1m",
            "interval_start": candidate.interval_start.isoformat(), "native_trade_time": bar.native_trade_time,
            "open": bar.open, "high": bar.high, "low": bar.low, "close": bar.close,
            "volume": bar.volume, "amount": bar.amount, "unit_conversion": bar.unit_conversion,
        }
        observation_hash = hashlib.sha256(json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()

        def _finalize(cursor: object) -> bool:
            cursor.execute(
                """
                select state from live.stock_bar_selected
                where market=%s and code=%s and freq='1m' and interval_start=%s
                for update
                """,
                (candidate.market, candidate.code, candidate.interval_start),
            )
            selected = cursor.fetchone()
            if selected is None or str(selected["state"]) != "staged":
                return False
            self._write_attempts(cursor, attempts, observed_at, candidate.interval_start)
            cursor.execute(
                """
                insert into live.stock_bar_observation
                  (provider, market, code, freq, interval_start, observed_at, native_trade_time,
                   open, high, low, close, volume, amount, unit_conversion, observation_hash)
                values (%s, %s, %s, '1m', %s, %s, %s::timestamp,
                        %s, %s, %s, %s, %s, %s, %s, %s)
                on conflict (provider, market, code, freq, interval_start, observation_hash) do update set
                  observed_at = excluded.observed_at
                returning observation_version
                """,
                (bar.provider, candidate.market, candidate.code, candidate.interval_start, observed_at, bar.native_trade_time,
                 bar.open, bar.high, bar.low, bar.close, bar.volume, bar.amount, bar.unit_conversion, observation_hash),
            )
            observation = cursor.fetchone()
            journal_state = db_client.discover_migration_range_journals(cursor, "stock_bar_1m")
            if journal_state.has_active_journal:
                db_client.enable_explicit_range_journaling(cursor)
            cursor.execute(
                """
                insert into fact.stock_bar_1m (market, code, bar_time, open, high, low, close, volume, amount)
                values (%s, %s, %s::timestamptz at time zone 'Asia/Shanghai', %s, %s, %s, %s, %s, %s)
                on conflict (market, code, bar_time) do update set
                  open=excluded.open, high=excluded.high, low=excluded.low, close=excluded.close,
                  volume=excluded.volume, amount=excluded.amount, loaded_at=now()
                """,
                (candidate.market, candidate.code, candidate.interval_start, bar.open, bar.high, bar.low, bar.close, bar.volume, bar.amount),
            )
            db_client.append_migration_range_journals(
                cursor,
                journal_state,
                [candidate.interval_start.astimezone(SHANGHAI).replace(tzinfo=None)],
            )
            cursor.execute(
                """
                with refreshed as (
                  select market,code,bar_time::date as trade_date,count(*)::bigint as row_count,
                         min(bar_time) as first_bar_time,max(bar_time) as last_bar_time
                  from fact.stock_bar_1m
                  where market=%s and code=%s
                    and bar_time >= (%s::timestamptz at time zone 'Asia/Shanghai')::date
                    and bar_time < ((%s::timestamptz at time zone 'Asia/Shanghai')::date + interval '1 day')
                  group by market,code,bar_time::date
                )
                insert into readmodel.stock_bar_1m_daily_coverage
                  (market,code,trade_date,row_count,first_bar_time,last_bar_time,updated_at)
                select market,code,trade_date,row_count,first_bar_time,last_bar_time,now() from refreshed
                on conflict (market,code,trade_date) do update set
                  row_count=excluded.row_count,first_bar_time=excluded.first_bar_time,
                  last_bar_time=excluded.last_bar_time,updated_at=excluded.updated_at
                """,
                (candidate.market, candidate.code, candidate.interval_start, candidate.interval_start),
            )
            cursor.execute(
                """
                insert into audit.stock_bar_1m_write_event(source_semantics,min_bar_time,max_bar_time,row_count)
                values ('quotemux.live_bar_finalizer.provider_refetch',
                        %s::timestamptz at time zone 'Asia/Shanghai',
                        %s::timestamptz at time zone 'Asia/Shanghai', 1)
                """,
                (candidate.interval_start, candidate.interval_start),
            )
            cursor.execute(
                """
                update live.stock_bar_selected
                set provider=%s,observation_version=%s,selection_reason=%s,
                    state='finalized',selected_at=%s,updated_at=now()
                where market=%s and code=%s and freq='1m' and interval_start=%s
                """,
                (bar.provider, observation["observation_version"], f"provider_refetch:{bar.provider}", observed_at, candidate.market, candidate.code, candidate.interval_start),
            )
            cursor.execute(
                "update live.stock_bar_observation set finalized_at=%s where observation_version=%s",
                (observed_at, observation["observation_version"]),
            )
            return True

        return bool(self._run_transaction(_finalize))


class LiveBarIngestor:
    def __init__(self, provider: CurrentBarProvider, store: CurrentBarStore, clock: Callable[[], datetime] | None = None) -> None:
        self._provider = provider
        self._store = store
        self._clock = clock or (lambda: datetime.now(tz=SHANGHAI))

    def ingest(self, request: CurrentBarRequest) -> LiveIngestResult:
        reject_in_strict_public_read("live_bar_ingest")
        effective_now = _as_shanghai(request.effective_now)
        target_interval = effective_now.replace(second=0, microsecond=0)
        codes = tuple(dict.fromkeys(normalize_stock_code(code).zfill(6) for code in request.codes if normalize_stock_code(code)))
        provider_result = self._provider.fetch(codes, effective_now)
        observed_at = _as_shanghai(self._clock())
        bars_by_code = {bar.code: bar for bar in provider_result.bars if _as_shanghai(bar.interval_start) == target_interval}
        errors: list[dict[str, str]] = []
        items: list[CurrentStockBarItem] = []
        attempts_by_code: dict[str, tuple[CurrentBarNodeAttempt, ...]] = {
            code: tuple(attempt for attempt in provider_result.attempts if attempt.code == code) for code in codes
        }
        for code in codes:
            bar = bars_by_code.get(code)
            attempts = attempts_by_code[code]
            if bar is None:
                self._store.record_attempts(attempts, observed_at, target_interval)
                errors.append({"code": code, "message": "no exact current 1m Bar from configured whole-Bar providers"})
                continue
            staged = self._store.stage(bar, observed_at, attempts)
            items.append(
                CurrentStockBarItem(
                    code=code, market=stock_market_name(code), interval_start=target_interval,
                    interval_end=target_interval + timedelta(minutes=1), open=bar.open, high=bar.high, low=bar.low,
                    close=bar.close, volume=bar.volume, amount=bar.amount, observed_at=observed_at,
                    provider=bar.provider, observation_version=staged.observation_version,
                )
            )
        return LiveIngestResult(items=tuple(items), attempts=provider_result.attempts, errors=tuple(errors), diagnostics=provider_result.diagnostics)


def ingest_current_stock_bars(request: CurrentBarRequest) -> LiveIngestResult:
    provider = WholeBarFallbackProvider()
    store = PostgresCurrentBarStore()
    # Request-driven finalization keeps the first release progressing even
    # before the periodic recovery loop is deployed; finalization failures are
    # reflected in its durable staging state and never expose a partial Bar.
    CurrentBarFinalizer(provider, store).finalize_due(request.effective_now)
    return LiveBarIngestor(provider=provider, store=store).ingest(request)


class CurrentBarFinalizer:
    def __init__(self, provider: CurrentBarProvider, store: CurrentBarFinalizationStore, grace_seconds: int = 7) -> None:
        if grace_seconds < 5 or grace_seconds > 10:
            raise ValueError("current Bar finalization grace must be between 5 and 10 seconds")
        self._provider = provider
        self._store = store
        self._grace_seconds = grace_seconds

    def finalize_due(self, now: datetime | None = None) -> dict[str, int]:
        observed_at = _as_shanghai(now or datetime.now(tz=SHANGHAI))
        candidates = self._store.list_due(observed_at, self._grace_seconds)
        finalized = deferred = failed = 0
        for candidate in candidates:
            try:
                refetched = self._provider.fetch((candidate.code,), candidate.interval_start)
                bar = next((item for item in refetched.bars if item.code == candidate.code and _as_shanghai(item.interval_start) == candidate.interval_start), None)
                if bar is None:
                    deferred += 1
                    continue
                if self._store.finalize(candidate, bar, observed_at, refetched.attempts):
                    finalized += 1
                else:
                    deferred += 1
            except Exception:
                failed += 1
        return {"candidates": len(candidates), "finalized": finalized, "deferred": deferred, "failed": failed}


def finalize_due_current_stock_bars(now: datetime | None = None, grace_seconds: int = 7) -> dict[str, int]:
    return CurrentBarFinalizer(WholeBarFallbackProvider(), PostgresCurrentBarStore(), grace_seconds).finalize_due(now)
