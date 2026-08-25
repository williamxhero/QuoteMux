from __future__ import annotations

import hashlib
import json

import pytest

from quotemux.store.futures_back_adjusted_repair import FuturesBackAdjustedRepairError, load_staged_repair


def _artifact(rows: list[dict[str, object]]) -> bytes:
    return json.dumps({"schema_version": "futures_back_adjusted_1m_staged_artifact_v1", "rows": rows}, separators=(",", ":")).encode()


def _row() -> dict[str, object]:
    return {"product_code": "ag", "series_type": "back_adjusted_continuous", "bar_time": "2026-02-02 09:02:00", "open": "4999", "high": "5002", "low": "4997", "close": "5000", "volume": "12", "open_interest": "34", "adjustment_offset": "1000", "source_key": "derived_core:formal:capture-1:AG2604.SHF"}


def _manifest(artifact: bytes, rows: list[dict[str, object]]) -> dict[str, object]:
    capture = {"source": "formal_source", "capture_id": "capture-1", "version": "2026-08-25", "artifact_sha256": "a" * 64, "rowset_sha256": "b" * 64, "request_ranges": [{"start_time": "2026-02-02 09:01:00", "end_time": "2026-02-02 09:03:00"}], "timestamp_contract": {"timezone": "Asia/Shanghai", "frequency": "1m"}}
    return {"schema_version": "futures_back_adjusted_1m_derivation_v1", "staged_artifact_sha256": hashlib.sha256(artifact).hexdigest(), "frozen_dataset_version": "mhd-v1-frozen", "ruleset_sha256": "c" * 64, "gap_ranges_artifact_sha256": "d" * 64, "source_capture": capture, "contract_mapping_capture": {**capture, "capture_id": "mapping-1"}, "exact_missing_keys": [{"product_code": item["product_code"], "bar_time": item["bar_time"]} for item in rows], "row_count": len(rows)}


def test_staged_repair_requires_immutable_lineage_and_exact_missing_keys() -> None:
    rows = [_row()]
    artifact = _artifact(rows)

    staged = load_staged_repair(artifact, _manifest(artifact, rows))

    assert staged.artifact_sha256 == hashlib.sha256(artifact).hexdigest()
    assert staged.rows[0]["product_code"] == "ag"


def test_staged_repair_rejects_sha_mismatch_duplicate_or_extra_key() -> None:
    rows = [_row()]
    artifact = _artifact(rows)
    manifest = _manifest(artifact, rows)
    manifest["staged_artifact_sha256"] = "0" * 64
    with pytest.raises(FuturesBackAdjustedRepairError, match="SHA-256"):
        load_staged_repair(artifact, manifest)

    duplicate = [_row(), _row()]
    duplicate_artifact = _artifact(duplicate)
    duplicate_manifest = _manifest(duplicate_artifact, [_row()])
    with pytest.raises(FuturesBackAdjustedRepairError, match="duplicate staged key"):
        load_staged_repair(duplicate_artifact, duplicate_manifest)

    extra = _row()
    extra["bar_time"] = "2026-02-02 09:03:00"
    extra_rows = [_row(), extra]
    extra_artifact = _artifact(extra_rows)
    with pytest.raises(FuturesBackAdjustedRepairError, match="exactly equal"):
        load_staged_repair(extra_artifact, _manifest(extra_artifact, [_row()]))


def test_staged_repair_rejects_invalid_market_contract_values() -> None:
    row = _row()
    row["high"] = "4998"
    artifact = _artifact([row])

    with pytest.raises(FuturesBackAdjustedRepairError, match="OHLC"):
        load_staged_repair(artifact, _manifest(artifact, [row]))
