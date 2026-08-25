"""Fail-closed publication of immutable derived futures back-adjusted 1m repairs.

This is deliberately a publication seam: it never loads a provider or derives a
price.  ``derived_core`` supplies an immutable JSON artifact plus its manifest;
MarketHub supplies the version guard described by :class:`DatasetVersionGuard`.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re
from typing import Any, Protocol

from quotemux.infra.db.client import _acquire_connection, _release_connection


CAPABILITY_ID = "futures.quotes.back_adjusted_continuous.1m"
STORAGE_SERIES_TYPE = "apex_l0_adjusted"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class FuturesBackAdjustedRepairError(ValueError):
    pass


class DatasetVersionGuard(Protocol):
    """MarketHub-owned guard, invoked after the capability lock and before any write.

    It must atomically reject a stale expected version against the current
    ``future_bar_1m`` publication version, and return that current version.
    """

    def require_current(self, capability_id: str, expected_version: str) -> str: ...


@dataclass(frozen=True)
class StagedRepair:
    artifact_sha256: str
    rows: tuple[dict[str, object], ...]
    manifest: Mapping[str, object]


def _text(value: object, field: str) -> str:
    text = str(value).strip()
    if not text:
        raise FuturesBackAdjustedRepairError(f"{field} must be non-empty")
    return text


def _sha(value: object, field: str) -> str:
    value = _text(value, field)
    if not _SHA256.fullmatch(value):
        raise FuturesBackAdjustedRepairError(f"{field} must be a lowercase SHA-256")
    return value


def _number(value: object, field: str) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise FuturesBackAdjustedRepairError(f"{field} must be finite numeric") from exc
    if not number.is_finite():
        raise FuturesBackAdjustedRepairError(f"{field} must be finite numeric")
    return number


def load_staged_repair(artifact_bytes: bytes, manifest: Mapping[str, object]) -> StagedRepair:
    """Verify the immutable artifact and all source/derivation identities before staging."""
    try:
        payload = json.loads(artifact_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FuturesBackAdjustedRepairError("staged artifact must be UTF-8 JSON") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != "futures_back_adjusted_1m_staged_artifact_v1":
        raise FuturesBackAdjustedRepairError("unsupported staged artifact schema_version")
    rows = payload.get("rows")
    if not isinstance(rows, list) or not rows:
        raise FuturesBackAdjustedRepairError("staged artifact rows must be non-empty")
    artifact_sha256 = hashlib.sha256(artifact_bytes).hexdigest()
    if artifact_sha256 != _sha(manifest.get("staged_artifact_sha256"), "staged_artifact_sha256"):
        raise FuturesBackAdjustedRepairError("staged artifact SHA-256 mismatch")
    if manifest.get("schema_version") != "futures_back_adjusted_1m_derivation_v1":
        raise FuturesBackAdjustedRepairError("unsupported derivation manifest schema_version")
    frozen_version = _text(manifest.get("frozen_dataset_version"), "frozen_dataset_version")
    _sha(manifest.get("ruleset_sha256"), "ruleset_sha256")
    _sha(manifest.get("gap_ranges_artifact_sha256"), "gap_ranges_artifact_sha256")
    for label in ("source_capture", "contract_mapping_capture"):
        capture = manifest.get(label)
        if not isinstance(capture, Mapping):
            raise FuturesBackAdjustedRepairError(f"{label} must be an immutable capture manifest")
        _text(capture.get("source"), f"{label}.source")
        _text(capture.get("capture_id"), f"{label}.capture_id")
        _text(capture.get("version"), f"{label}.version")
        _sha(capture.get("artifact_sha256"), f"{label}.artifact_sha256")
        _sha(capture.get("rowset_sha256"), f"{label}.rowset_sha256")
        if not isinstance(capture.get("request_ranges"), list) or not capture["request_ranges"]:
            raise FuturesBackAdjustedRepairError(f"{label}.request_ranges must be non-empty")
        if not isinstance(capture.get("timestamp_contract"), Mapping) or not capture["timestamp_contract"]:
            raise FuturesBackAdjustedRepairError(f"{label}.timestamp_contract must be non-empty")
    expected_keys = manifest.get("exact_missing_keys")
    if not isinstance(expected_keys, list) or not expected_keys:
        raise FuturesBackAdjustedRepairError("manifest exact_missing_keys must be a non-empty list")
    expected = {(str(item.get("product_code", "")), str(item.get("bar_time", ""))) for item in expected_keys if isinstance(item, Mapping)}
    if len(expected) != len(expected_keys) or any(not product_code or not bar_time for product_code, bar_time in expected):
        raise FuturesBackAdjustedRepairError("manifest exact_missing_keys contains invalid or duplicate keys")
    expected_order = [(str(item["product_code"]), str(item["bar_time"])) for item in expected_keys if isinstance(item, Mapping)]
    if expected_order != sorted(expected_order):
        raise FuturesBackAdjustedRepairError("manifest exact_missing_keys must be sorted")
    normalized: list[dict[str, object]] = []
    actual: set[tuple[str, str]] = set()
    actual_order: list[tuple[str, str]] = []
    for raw in rows:
        if not isinstance(raw, Mapping):
            raise FuturesBackAdjustedRepairError("staged artifact row must be an object")
        product_code = _text(raw.get("product_code"), "row.product_code")
        bar_time = _text(raw.get("bar_time"), "row.bar_time")
        key = (product_code, bar_time)
        if key in actual:
            raise FuturesBackAdjustedRepairError(f"duplicate staged key {key}")
        actual.add(key)
        actual_order.append(key)
        if raw.get("series_type") != "back_adjusted_continuous":
            raise FuturesBackAdjustedRepairError("row.series_type must be back_adjusted_continuous")
        values = {name: _number(raw.get(name), f"row.{name}") for name in ("open", "high", "low", "close", "volume", "open_interest", "adjustment_offset")}
        if values["high"] < max(values["open"], values["close"]) or values["low"] > min(values["open"], values["close"]) or values["high"] < values["low"]:
            raise FuturesBackAdjustedRepairError(f"invalid OHLC ordering at {key}")
        if values["volume"] < 0 or values["open_interest"] < 0:
            raise FuturesBackAdjustedRepairError(f"negative volume/open_interest at {key}")
        normalized.append({"product_code": product_code, "bar_time": bar_time, "source_key": _text(raw.get("source_key"), "row.source_key"), **values})
    if actual != expected:
        raise FuturesBackAdjustedRepairError("staged keys do not exactly equal declared missing keys")
    if actual_order != sorted(actual_order):
        raise FuturesBackAdjustedRepairError("staged artifact rows must be sorted by product_code and bar_time")
    if int(manifest.get("row_count", 0)) != len(normalized):
        raise FuturesBackAdjustedRepairError("manifest row_count does not match staged artifact")
    # Keep this local binding explicit: callers must pass the same frozen
    # identity to the MarketHub version guard before any write.
    if frozen_version == "":  # pragma: no cover - _text already proves this
        raise FuturesBackAdjustedRepairError("frozen dataset version missing")
    return StagedRepair(artifact_sha256, tuple(normalized), manifest)


class FuturesBackAdjustedRepairPublisher:
    def __init__(self, version_guard: DatasetVersionGuard, connection_factory: Callable[[], Any] = _acquire_connection) -> None:
        self._version_guard = version_guard
        self._connection_factory = connection_factory

    def publish(self, artifact_bytes: bytes, manifest: Mapping[str, object]) -> dict[str, object]:
        staged = load_staged_repair(artifact_bytes, manifest)
        expected_version = _text(manifest.get("frozen_dataset_version"), "frozen_dataset_version")
        connection = self._connection_factory()
        try:
            with connection.cursor() as cursor:
                cursor.execute("select pg_try_advisory_xact_lock(hashtext(%s))", (CAPABILITY_ID,))
                lock_row = cursor.fetchone()
                if not lock_row or not bool(next(iter(lock_row.values())) if isinstance(lock_row, Mapping) else lock_row[0]):
                    raise FuturesBackAdjustedRepairError("future back-adjusted repair advisory lock busy")
                current_version = self._version_guard.require_current(CAPABILITY_ID, expected_version)
                if current_version != expected_version:
                    raise FuturesBackAdjustedRepairError("future_bar_1m dataset version is stale")
                self._stage_and_publish(cursor, staged)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            _release_connection(connection)
        return {"status": "success", "row_count": len(staged.rows), "artifact_sha256": staged.artifact_sha256, "dataset_version": expected_version}

    @staticmethod
    def _stage_and_publish(cursor: Any, staged: StagedRepair) -> None:
        cursor.execute("create temporary table future_back_adjusted_repair_stage (product_code text, bar_time timestamp, open double precision, high double precision, low double precision, close double precision, volume double precision, open_interest double precision, adjustment_offset double precision, source_key text) on commit drop")
        with cursor.copy("copy future_back_adjusted_repair_stage (product_code, bar_time, open, high, low, close, volume, open_interest, adjustment_offset, source_key) from stdin") as copy:
            for row in staged.rows:
                copy.write_row(tuple(row[name] for name in ("product_code", "bar_time", "open", "high", "low", "close", "volume", "open_interest", "adjustment_offset", "source_key")))
        cursor.execute("select count(*) from fact.future_bar_1m bars join future_back_adjusted_repair_stage stage on stage.product_code = bars.product_code and stage.bar_time = bars.bar_time where bars.series_type = %s", (STORAGE_SERIES_TYPE,))
        if int(cursor.fetchone()[0]) != 0:
            raise FuturesBackAdjustedRepairError("repair conflicts with existing future_bar_1m keys")
        cursor.execute("insert into fact.future_bar_1m (product_code, exchange, series_type, bar_time, open, high, low, close, volume, open_interest, adjustment_offset, source_key) select stage.product_code, series.exchange, %s, stage.bar_time, stage.open, stage.high, stage.low, stage.close, stage.volume, stage.open_interest, stage.adjustment_offset, stage.source_key from future_back_adjusted_repair_stage stage join ref.future_series series on series.product_code = stage.product_code and series.series_type = %s", (STORAGE_SERIES_TYPE, STORAGE_SERIES_TYPE))
        if cursor.rowcount != len(staged.rows):
            raise FuturesBackAdjustedRepairError("repair insert count differs from exact staged plan")
        cursor.execute("select count(*) from fact.future_bar_1m bars join future_back_adjusted_repair_stage stage on stage.product_code = bars.product_code and stage.bar_time = bars.bar_time where bars.series_type = %s", (STORAGE_SERIES_TYPE,))
        if int(cursor.fetchone()[0]) != len(staged.rows):
            raise FuturesBackAdjustedRepairError("post-publication audit did not resolve every exact missing key")
        source_capture = staged.manifest["source_capture"]
        mapping_capture = staged.manifest["contract_mapping_capture"]
        cursor.execute("create table if not exists audit.future_back_adjusted_1m_repair_publications (artifact_sha256 text primary key, source_capture_id text not null, mapping_capture_id text not null, frozen_dataset_version text not null, ruleset_sha256 text not null, row_count bigint not null check (row_count > 0), published_at timestamp with time zone not null default now())")
        cursor.execute("insert into audit.future_back_adjusted_1m_repair_publications (artifact_sha256, source_capture_id, mapping_capture_id, frozen_dataset_version, ruleset_sha256, row_count) values (%s, %s, %s, %s, %s, %s)", (staged.artifact_sha256, source_capture["capture_id"], mapping_capture["capture_id"], staged.manifest["frozen_dataset_version"], staged.manifest["ruleset_sha256"], len(staged.rows)))
