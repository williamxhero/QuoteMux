"""Evidence-first Pyramid classifier and immutable QuoteMux facts importer."""
from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import re
from typing import Any, Iterator

import psycopg

from quotemux.futures_partial_contract import (
    FACT_NORMALIZATION_VERSION, PRODUCT_EXCHANGE, PRODUCTS, PYRAMID_SOURCE_KEY,
    SERIES_TYPE, candidate_row, candidate_sha256, canonical_json_bytes,
)
from quotemux.infra.db.client import _acquire_connection, _release_connection

SOURCE_KEY = PYRAMID_SOURCE_KEY
FACT_TRANSFORM_VERSION = FACT_NORMALIZATION_VERSION
_SHA = re.compile(r"^[0-9a-f]{64}$")
_PARQUET_FIELDS = ("product_code", "exchange", "bar_time", "open", "high", "low", "close", "volume", "open_interest", "adjustment_offset", "source_key")
_CORRECTED_RAW_AGGREGATE_SHA256 = "0afda7afcfa0749ab5ebf243c577a639793e29809a6ca0dc8aa51d3602781149"


class FuturesPyramidImportError(ValueError):
    pass


@dataclass(frozen=True)
class PyramidBundle:
    path: Path
    manifest: dict[str, object]
    source_normalized_rowset_sha256: str
    canonical_fact_rowset_sha256: str
    normalized_row_count: int
    rows: tuple[dict[str, object], ...] = ()


def _sha(value: object, name: str) -> str:
    text = str(value or "")
    if not _SHA.fullmatch(text):
        raise FuturesPyramidImportError(f"{name} must be a lowercase SHA-256")
    return text


def _hash_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _strict_authorization(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise FuturesPyramidImportError("Pyramid authorization is required")
    actual = dict(value)
    required = {"status": "private_research_authorized", "source_thread_id": "01a031ef-006c-7de0-a585-68eecdf769c7", "private_server_retention": True, "transformation": True, "private_research": True, "redistribution": False}
    for key, expected in required.items():
        if actual.get(key) != expected:
            raise FuturesPyramidImportError(f"authorization.{key} must be {expected!r}")
    if not isinstance(actual.get("evidence"), str) or not actual["evidence"].strip():
        raise FuturesPyramidImportError("authorization.evidence must be non-empty")
    return actual


def _verify_inventory(bundle_path: Path, entries: object, *, kind: str) -> None:
    if not isinstance(entries, list) or not entries:
        raise FuturesPyramidImportError(f"{kind} inventory is missing")
    for entry in entries:
        if not isinstance(entry, Mapping):
            raise FuturesPyramidImportError(f"{kind} inventory entry is invalid")
        relative = str(entry.get("path", ""))
        if not relative or Path(relative).is_absolute() or ".." in Path(relative).parts:
            raise FuturesPyramidImportError(f"unsafe {kind} inventory path")
        path = bundle_path / relative
        if not path.is_file() or path.stat().st_size != int(entry.get("size_bytes", -1)):
            raise FuturesPyramidImportError(f"{kind} inventory mismatch: {relative}")
        if _hash_path(path) != _sha(entry.get("sha256"), f"{kind} sha256"):
            raise FuturesPyramidImportError(f"{kind} hash mismatch: {relative}")


def _arrow_value(value: object) -> object:
    return value.isoformat(sep=" ") if hasattr(value, "isoformat") else value


def _rows(path: Path, *, fields: tuple[str, ...]) -> Iterator[dict[str, object]]:
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover
        raise FuturesPyramidImportError("pyarrow is required to read Pyramid evidence") from exc
    parquet = pq.ParquetFile(path)
    if tuple(parquet.schema_arrow.names) != fields:
        raise FuturesPyramidImportError(f"unexpected parquet schema: {path.name}")
    for batch in parquet.iter_batches(batch_size=100_000, columns=list(fields)):
        for row in batch.to_pylist():
            yield {key: _arrow_value(row.get(key)) for key in fields}


def _fact_from_parquet(row: Mapping[str, object]) -> dict[str, object]:
    product = str(row.get("product_code", ""))
    if product not in PRODUCTS or product == "TL" or row.get("exchange") != PRODUCT_EXCHANGE[product]:
        raise FuturesPyramidImportError("fact parquet product/exchange contract mismatch")
    if row.get("source_key") != SOURCE_KEY or row.get("open_interest") is not None:
        raise FuturesPyramidImportError("fact parquet source/OI contract mismatch")
    if any(row.get(name) is None for name in ("bar_time", "open", "high", "low", "close", "volume", "adjustment_offset")):
        raise FuturesPyramidImportError("fact parquet has missing required fields")
    fact = candidate_row(dict(row))
    numbers = [float(fact[name]) for name in ("open","high","low","close","volume","adjustment_offset")]
    if not all(math.isfinite(value) for value in numbers) or float(fact["volume"]) < 0 or float(fact["high"]) < max(float(fact["open"]),float(fact["close"]),float(fact["low"])) or float(fact["low"]) > min(float(fact["open"]),float(fact["close"]),float(fact["high"])):
        raise FuturesPyramidImportError("fact parquet OHLCV/offset contract mismatch")
    return fact


def load_pyramid_filesystem_bundle(bundle_path: Path) -> PyramidBundle:
    """Verify all physical evidence before a database connection is made."""
    try:
        manifest = json.loads((bundle_path / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise FuturesPyramidImportError("Pyramid bundle manifest is unreadable") from exc
    if not isinstance(manifest, dict) or manifest.get("schema_version") != "futures_user_pyramid_archive_bundle_v1":
        raise FuturesPyramidImportError("unsupported Pyramid bundle schema")
    _strict_authorization(manifest.get("authorization"))
    lineage = manifest.get("source_lineage")
    if not isinstance(lineage, Mapping) or lineage.get("source_class") != "user_provided" or lineage.get("source_identity") != "pyramid_post_adjusted_20260714" or lineage.get("vendor_entitlement") != "unknown_not_asserted":
        raise FuturesPyramidImportError("Pyramid lineage/entitlement contract mismatch")
    _verify_inventory(bundle_path, manifest.get("raw_files"), kind="raw")
    _verify_inventory(bundle_path, manifest.get("evidence_files"), kind="evidence")
    raw_inventory = manifest.get("raw_byte_inventory")
    if not isinstance(raw_inventory, list) or len(raw_inventory) != 23:
        raise FuturesPyramidImportError("exact 23-file raw inventory is required")
    if _sha(manifest.get("raw_aggregate_sha256"), "raw_aggregate_sha256") != _CORRECTED_RAW_AGGREGATE_SHA256:
        raise FuturesPyramidImportError("corrected raw aggregate hash is not the authorized archive")
    if hashlib.sha256(canonical_json_bytes(manifest.get("raw_files"))).hexdigest() != _sha(manifest.get("raw_aggregate_sha256"), "raw_aggregate_sha256"):
        raise FuturesPyramidImportError("raw aggregate hash mismatch")
    normalization = manifest.get("fact_normalization")
    if not isinstance(normalization, Mapping) or normalization.get("version") != FACT_TRANSFORM_VERSION or normalization.get("source_key") != SOURCE_KEY:
        raise FuturesPyramidImportError("fact_normalization version/source_key mismatch")
    normalized_path, fact_path = bundle_path / "normalized.parquet", bundle_path / "fact_normalized.parquet"
    if not normalized_path.is_file() or _hash_path(normalized_path) != _sha(manifest.get("normalized_artifact_sha256"), "normalized_artifact_sha256"):
        raise FuturesPyramidImportError("normalized artifact hash mismatch")
    if not fact_path.is_file() or _hash_path(fact_path) != _sha(normalization.get("fact_normalized_artifact_sha256"), "fact_normalized_artifact_sha256"):
        raise FuturesPyramidImportError("fact-normalized artifact hash mismatch")
    staged_path, intervals_path = bundle_path / "staged.parquet", bundle_path / "intervals.jsonl"
    interval_meta = manifest.get("interval_artifact")
    if not staged_path.is_file() or _hash_path(staged_path) != _sha(manifest.get("staged_artifact_sha256"), "staged_artifact_sha256"):
        raise FuturesPyramidImportError("staged artifact hash mismatch")
    if not isinstance(interval_meta, Mapping) or not intervals_path.is_file() or _hash_path(intervals_path) != _sha(interval_meta.get("sha256"), "interval artifact sha256"):
        raise FuturesPyramidImportError("interval artifact hash mismatch")
    source_digest, package_fact_digest, database_fact_digest, previous, count = hashlib.sha256(), hashlib.sha256(), hashlib.sha256(), None, 0
    for source, fact in zip(_rows(normalized_path, fields=_PARQUET_FIELDS), _rows(fact_path, fields=_PARQUET_FIELDS), strict=True):
        canonical = _fact_from_parquet(fact); key = (canonical["product_code"], canonical["exchange"], canonical["bar_time"])
        if previous is not None and key <= previous:
            raise FuturesPyramidImportError("fact parquet primary keys must be sorted/unique")
        previous = key
        if source.get("product_code") != canonical["product_code"] or source.get("exchange") != canonical["exchange"] or str(source.get("bar_time")) != str(canonical["bar_time"]):
            raise FuturesPyramidImportError("normalized/fact parquet key mismatch")
        source_digest.update(canonical_json_bytes(source) + b"\n")
        # The archive's fact artifact has the package NORMALIZED_SCHEMA (no
        # storage series field).  QuoteMux derives a separately named DB-fact
        # rowset by adding only the fixed storage series/source contract.
        package_fact_digest.update(canonical_json_bytes(fact) + b"\n")
        database_fact_digest.update(canonical_json_bytes(canonical) + b"\n"); count += 1
    if count != int(manifest.get("normalized_row_count", -1)):
        raise FuturesPyramidImportError("normalized row count mismatch")
    source_sha, package_fact_sha, fact_sha = source_digest.hexdigest(), package_fact_digest.hexdigest(), database_fact_digest.hexdigest()
    if source_sha != _sha(manifest.get("source_normalized_rowset_sha256"), "source_normalized_rowset_sha256") or package_fact_sha != _sha(normalization.get("fact_normalized_rowset_sha256"), "fact_normalized_rowset_sha256"):
        raise FuturesPyramidImportError("rowset hash mismatch")
    return PyramidBundle(bundle_path, manifest, source_sha, fact_sha, count)


def load_pyramid_bundle(artifact_bytes: bytes, manifest: Mapping[str, object]) -> PyramidBundle:
    """Small-fixture compatibility helper; production must use filesystem bundle."""
    try: payload = json.loads(artifact_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc: raise FuturesPyramidImportError("Pyramid bundle must be UTF-8 JSON") from exc
    rows = payload.get("rows") if isinstance(payload, Mapping) else None
    if not isinstance(rows, list): raise FuturesPyramidImportError("Pyramid bundle rows must be a list")
    digest, legacy_digest, previous, canonical_rows = hashlib.sha256(), hashlib.sha256(), None, []
    for row in rows:
        if not isinstance(row, Mapping): raise FuturesPyramidImportError("Pyramid bundle row is invalid")
        product = str(row.get("product_code", ""))
        if product not in PRODUCTS or product == "TL": raise FuturesPyramidImportError(f"unsupported Pyramid product_code: {product}")
        if row.get("exchange") != PRODUCT_EXCHANGE[product]: raise FuturesPyramidImportError(f"Pyramid exchange mismatch for {product}")
        fact = candidate_row(dict(row)); key = (fact["product_code"], fact["exchange"], fact["bar_time"])
        if previous is not None and key <= previous: raise FuturesPyramidImportError("Pyramid bundle primary keys must be sorted/unique")
        previous = key; canonical_rows.append(fact); digest.update(canonical_json_bytes(fact) + b"\n")
    legacy_digest.update(canonical_json_bytes(canonical_rows))
    normalization = manifest.get("fact_normalization") if isinstance(manifest, Mapping) else None
    expected = normalization.get("fact_normalized_rowset_sha256") if isinstance(normalization, Mapping) else manifest.get("canonical_fact_rowset_sha256")
    expected_sha = _sha(expected, "canonical_fact_rowset_sha256")
    if digest.hexdigest() != expected_sha and legacy_digest.hexdigest() != expected_sha: raise FuturesPyramidImportError("canonical fact rowset SHA-256 mismatch")
    return PyramidBundle(Path("."), dict(manifest), _sha(manifest.get("source_normalized_rowset_sha256"), "source_normalized_rowset_sha256"), digest.hexdigest(), len(rows), tuple(canonical_rows))


def _publisher_connection() -> Any:
    values = {name: os.getenv(f"QUOTEMUX_PUBLISH_DB_{name}", "").strip() for name in ("HOST", "PORT", "NAME", "USER", "PASSWORD")}
    if not all(values.values()): raise FuturesPyramidImportError("QUOTEMUX_PUBLISH_DB_HOST/PORT/NAME/USER/PASSWORD are required")
    try: port = int(values["PORT"])
    except ValueError as exc: raise FuturesPyramidImportError("QUOTEMUX_PUBLISH_DB_PORT must be an integer") from exc
    return psycopg.connect(host=values["HOST"], port=port, dbname=values["NAME"], user=values["USER"], password=values["PASSWORD"])


def _write_plan(path: Path, header: dict[str, object], rows: Iterator[dict[str, object]]) -> dict[str, object]:
    if path.exists(): raise FuturesPyramidImportError("refusing to overwrite a disposition artifact")
    digest, count, counts = hashlib.sha256(), 0, {"missing_valid": 0, "already_present_equivalent": 0, "existing_conflict": 0}
    partial = path.with_suffix(path.suffix + ".records.partial")
    if partial.exists(): raise FuturesPyramidImportError("refusing to overwrite a partial disposition artifact")
    try:
        with partial.open("x", encoding="utf-8", newline="\n") as stream:
            for row in rows:
                text = json.dumps(row, sort_keys=True, separators=(",", ":"), allow_nan=False); stream.write(text + "\n"); digest.update(text.encode() + b"\n")
                count += 1; counts[str(row["disposition"])] += 1
        complete = {**header, "disposition_count": count, "disposition_sha256": digest.hexdigest(), "counts": counts}
        with gzip.open(path, "xt", encoding="utf-8", newline="\n") as stream, partial.open("r", encoding="utf-8") as records:
            stream.write(json.dumps({"kind": "header", **complete}, sort_keys=True, separators=(",", ":")) + "\n")
            for line in records: stream.write(line)
    finally:
        if partial.exists(): partial.unlink()
    return {**complete, "artifact": str(path)}


def _read_plan(path: Path) -> tuple[dict[str, object], Iterator[dict[str, object]]]:
    stream = gzip.open(path, "rt", encoding="utf-8")
    try: head = json.loads(next(stream))
    except Exception: stream.close(); raise FuturesPyramidImportError("invalid disposition artifact")
    if not isinstance(head, dict) or head.get("kind") != "header": stream.close(); raise FuturesPyramidImportError("disposition artifact header missing")
    def records() -> Iterator[dict[str, object]]:
        try:
            for line in stream:
                row = json.loads(line)
                if not isinstance(row, dict): raise FuturesPyramidImportError("invalid disposition record")
                yield row
        finally: stream.close()
    return head, records()


class FuturesPyramidImporter:
    def __init__(self, connection_factory: Callable[[], Any] = _acquire_connection) -> None: self._connection_factory = connection_factory

    def classify_filesystem_bundle(self, bundle_path: Path, plan_path: Path) -> dict[str, object]:
        """Read-only deterministic classification; it does not write temp tables."""
        bundle = load_pyramid_filesystem_bundle(bundle_path); connection = self._connection_factory(); owns = self._connection_factory is _acquire_connection
        try:
            with connection.cursor() as cursor:
                cursor.execute("begin isolation level repeatable read read only")
                cursor.execute("select generation,row_count,first_bar_time::text,last_bar_time::text from audit.future_bar_1m_series_generation where series_type=%s order by generation desc limit 1", (SERIES_TYPE,)); generation = cursor.fetchone()
                if not generation: raise FuturesPyramidImportError("future series generation is absent")
                stream = connection.cursor(name="future_pyramid_classify_existing")
                stream.execute("select product_code,exchange,bar_time::text,open,high,low,close,volume,adjustment_offset,open_interest,source_key from fact.future_bar_1m where series_type=%s and product_code=any(%s::text[]) order by product_code collate \"C\",exchange collate \"C\",bar_time", (SERIES_TYPE, list(PRODUCTS)))
                def existing_rows() -> Iterator[tuple[object, ...]]:
                    try:
                        while rows := stream.fetchmany(100_000):
                            yield from rows
                    finally:
                        stream.close()
                existing = existing_rows(); current = next(existing, None)
                def dispositions() -> Iterator[dict[str, object]]:
                    nonlocal current
                    for candidate in _rows(bundle.path / "fact_normalized.parquet", fields=_PARQUET_FIELDS):
                        fact = _fact_from_parquet(candidate); key = (str(fact["product_code"]), str(fact["exchange"]), str(fact["bar_time"]))
                        while current is not None and tuple(map(str, current[:3])) < key: current = next(existing, None)
                        same_key = current is not None and tuple(map(str, current[:3])) == key
                        equivalent = same_key and all(current[index] == fact[name] for index, name in enumerate(("product_code", "exchange", "bar_time", "open", "high", "low", "close", "volume", "adjustment_offset")))
                        existing_payload = None if not same_key else {"product_code":current[0],"exchange":current[1],"bar_time":str(current[2]),"open":current[3],"high":current[4],"low":current[5],"close":current[6],"volume":current[7],"adjustment_offset":current[8],"open_interest":current[9],"series_type":SERIES_TYPE,"source_key":current[10]}
                        yield {**fact, "candidate_sha256": candidate_sha256(fact), "disposition": "already_present_equivalent" if equivalent else ("existing_conflict" if same_key else "missing_valid"), "existing_source_key": None if existing_payload is None else current[10], "existing_fact_sha256": None if existing_payload is None else hashlib.sha256(canonical_json_bytes(existing_payload)).hexdigest()}
                header = {"bundle_source_normalized_rowset_sha256": bundle.source_normalized_rowset_sha256, "bundle_canonical_fact_rowset_sha256": bundle.canonical_fact_rowset_sha256, "fact_transform_version": FACT_TRANSFORM_VERSION, "expected_generation": int(generation[0]), "expected_generation_row_count": int(generation[1]), "expected_generation_first": str(generation[2]), "expected_generation_last": str(generation[3]), "candidate_count": bundle.normalized_row_count}
                result = _write_plan(plan_path, header, dispositions())
            connection.rollback(); return result
        except Exception: connection.rollback(); raise
        finally:
            if owns: _release_connection(connection)

    def publish_filesystem_bundle(self, bundle_path: Path, plan_path: Path) -> dict[str, object]:
        bundle = load_pyramid_filesystem_bundle(bundle_path); header, records = _read_plan(plan_path)
        if header.get("bundle_source_normalized_rowset_sha256") != bundle.source_normalized_rowset_sha256 or header.get("bundle_canonical_fact_rowset_sha256") != bundle.canonical_fact_rowset_sha256 or header.get("fact_transform_version") != FACT_TRANSFORM_VERSION: raise FuturesPyramidImportError("plan does not bind this exact verified bundle")
        plan_payload = {key: value for key, value in header.items() if key not in {"kind", "artifact"}}; payload_sha = hashlib.sha256(canonical_json_bytes(plan_payload)).hexdigest()
        qmi_id = "qmi-v1-" + hashlib.sha256(canonical_json_bytes({"source": bundle.source_normalized_rowset_sha256, "fact": bundle.canonical_fact_rowset_sha256, "transform": FACT_TRANSFORM_VERSION, "plan": payload_sha})).hexdigest()
        connection = self._connection_factory(); owns = self._connection_factory is _acquire_connection
        try:
            with connection.cursor() as cursor:
                cursor.execute("select pg_advisory_xact_lock(hashtext('future_pyramid_import:' || %s))", (bundle.canonical_fact_rowset_sha256,))
                cursor.execute("select payload_sha256,inserted_count,equivalent_count,conflict_count from audit.future_bar_1m_import_publication where qmi_id=%s", (qmi_id,)); prior = cursor.fetchone()
                if prior:
                    if prior[0] != payload_sha: raise FuturesPyramidImportError("existing qmi receipt differs")
                    connection.rollback(); return {"status":"idempotent","qmi_id":qmi_id,"inserted_count":prior[1],"equivalent_count":prior[2],"conflict_count":prior[3]}
                cursor.execute("create temporary table future_pyramid_stage (product_code text not null,exchange text not null,bar_time timestamp not null,open double precision not null,high double precision not null,low double precision not null,close double precision not null,volume double precision not null,adjustment_offset double precision not null,candidate_sha256 text not null,planned_disposition text not null,existing_source_key text,existing_fact_sha256 text,actual_disposition text) on commit drop")
                digest, count = hashlib.sha256(), 0
                with cursor.copy("copy future_pyramid_stage (product_code,exchange,bar_time,open,high,low,close,volume,adjustment_offset,candidate_sha256,planned_disposition,existing_source_key,existing_fact_sha256) from stdin") as copy:
                    verified_candidates = _rows(bundle.path / "fact_normalized.parquet", fields=_PARQUET_FIELDS)
                    for record, candidate in zip(records, verified_candidates, strict=True):
                        fact = _fact_from_parquet(candidate)
                        if any(record.get(name) != fact.get(name) for name in ("product_code","exchange","bar_time","open","high","low","close","volume","open_interest","adjustment_offset","series_type","source_key")) or record.get("candidate_sha256") != candidate_sha256(fact):
                            raise FuturesPyramidImportError("disposition record does not match the verified fact artifact")
                        text = json.dumps(record,sort_keys=True,separators=(",",":"),allow_nan=False); digest.update(text.encode()+b"\n"); count += 1
                        copy.write_row(tuple(record[name] for name in ("product_code","exchange","bar_time","open","high","low","close","volume","adjustment_offset","candidate_sha256","disposition","existing_source_key","existing_fact_sha256")))
                if count != int(header.get("disposition_count", -1)) or digest.hexdigest() != header.get("disposition_sha256"): raise FuturesPyramidImportError("disposition artifact byte integrity mismatch")
                cursor.execute("select generation,row_count,first_bar_time::text,last_bar_time::text from audit.future_bar_1m_series_generation where series_type=%s order by generation desc limit 1", (SERIES_TYPE,)); actual = cursor.fetchone()
                if not actual or tuple(map(str,actual)) != tuple(map(str,(header["expected_generation"],header["expected_generation_row_count"],header["expected_generation_first"],header["expected_generation_last"]))): raise FuturesPyramidImportError("frozen plan generation is stale")
                cursor.execute("""select count(*) from (select case when bars.product_code is null then 'missing_valid' when bars.exchange is not distinct from stage.exchange and bars.open is not distinct from stage.open and bars.high is not distinct from stage.high and bars.low is not distinct from stage.low and bars.close is not distinct from stage.close and bars.volume is not distinct from stage.volume and bars.adjustment_offset is not distinct from stage.adjustment_offset then 'already_present_equivalent' else 'existing_conflict' end actual,stage.planned_disposition from future_pyramid_stage stage left join fact.future_bar_1m bars on bars.product_code=stage.product_code and bars.exchange=stage.exchange and bars.series_type=%s and bars.bar_time=stage.bar_time) classified where actual<>planned_disposition""",(SERIES_TYPE,))
                if int(cursor.fetchone()[0]): raise FuturesPyramidImportError("frozen plan changed during publication; reclassify")
                cursor.execute("create temporary table future_pyramid_inserted(product_code text,exchange text,bar_time timestamp) on commit drop")
                cursor.execute("with inserted as (insert into fact.future_bar_1m(product_code,exchange,series_type,bar_time,open,high,low,close,volume,open_interest,adjustment_offset,source_key) select product_code,exchange,%s,bar_time,open,high,low,close,volume,null,adjustment_offset,%s from future_pyramid_stage where planned_disposition='missing_valid' on conflict do nothing returning product_code,exchange,bar_time) insert into future_pyramid_inserted select product_code,exchange,bar_time from inserted",(SERIES_TYPE,SOURCE_KEY))
                cursor.execute("""update future_pyramid_stage stage set actual_disposition=case when inserted.product_code is not null then 'inserted' when bars.exchange is not distinct from stage.exchange and bars.open is not distinct from stage.open and bars.high is not distinct from stage.high and bars.low is not distinct from stage.low and bars.close is not distinct from stage.close and bars.volume is not distinct from stage.volume and bars.adjustment_offset is not distinct from stage.adjustment_offset then 'already_present_equivalent' else 'existing_conflict' end from fact.future_bar_1m bars left join future_pyramid_inserted inserted on inserted.product_code=bars.product_code and inserted.exchange=bars.exchange and inserted.bar_time=bars.bar_time where bars.product_code=stage.product_code and bars.exchange=stage.exchange and bars.series_type=%s and bars.bar_time=stage.bar_time""",(SERIES_TYPE,))
                cursor.execute("select count(*) from future_pyramid_stage where actual_disposition is null or (planned_disposition='missing_valid' and actual_disposition='existing_conflict')")
                if int(cursor.fetchone()[0]): raise FuturesPyramidImportError("post-insert fact equality/race verification failed")
                cursor.execute("select count(*) filter(where actual_disposition='inserted'),count(*) filter(where actual_disposition='already_present_equivalent'),count(*) filter(where actual_disposition='existing_conflict') from future_pyramid_stage"); inserted,equivalent,conflict = map(int,cursor.fetchone())
                receipt = {"qmi_id":qmi_id,"bundle_manifest":bundle.manifest,"bundle_source_normalized_rowset_sha256":bundle.source_normalized_rowset_sha256,"bundle_archive_fact_normalized_rowset_sha256":bundle.manifest["fact_normalization"]["fact_normalized_rowset_sha256"],"bundle_canonical_fact_rowset_sha256":bundle.canonical_fact_rowset_sha256,"fact_transform_version":FACT_TRANSFORM_VERSION,"plan":plan_payload}
                cursor.execute("insert into audit.future_bar_1m_import_publication(qmi_id,source_normalized_rowset_sha256,fact_transform_version,canonical_fact_rowset_sha256,payload_sha256,manifest_json,inserted_count,equivalent_count,conflict_count) values(%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s)",(qmi_id,bundle.source_normalized_rowset_sha256,FACT_TRANSFORM_VERSION,bundle.canonical_fact_rowset_sha256,payload_sha,json.dumps(receipt,sort_keys=True),inserted,equivalent,conflict))
                cursor.execute("insert into audit.future_bar_1m_import_disposition(qmi_id,product_code,exchange,series_type,bar_time,candidate_sha256,disposition,existing_source_key,existing_fact_sha256) select %s,product_code,exchange,%s,bar_time,candidate_sha256,case when actual_disposition='inserted' then 'missing_valid' else actual_disposition end,existing_source_key,existing_fact_sha256 from future_pyramid_stage",(qmi_id,SERIES_TYPE))
                cursor.execute("insert into audit.future_bar_1m_import_admission(qmi_id,product_code,exchange,series_type,bar_time,candidate_sha256,disposition) select %s,product_code,exchange,%s,bar_time,candidate_sha256,actual_disposition from future_pyramid_stage where actual_disposition in ('inserted','already_present_equivalent')",(qmi_id,SERIES_TYPE))
            connection.commit(); return {"status":"success","qmi_id":qmi_id,"inserted_count":inserted,"equivalent_count":equivalent,"conflict_count":conflict,"plan_sha256":payload_sha}
        except Exception: connection.rollback(); raise
        finally:
            if owns: _release_connection(connection)


def _main() -> int:
    import argparse
    parser = argparse.ArgumentParser(description="Classify/publish an authorized Pyramid facts bundle"); parser.add_argument("command",choices=("classify","publish")); parser.add_argument("--bundle",type=Path,required=True); parser.add_argument("--plan",type=Path,required=True)
    args=parser.parse_args(); importer=FuturesPyramidImporter(_publisher_connection)
    result=importer.classify_filesystem_bundle(args.bundle,args.plan) if args.command=="classify" else importer.publish_filesystem_bundle(args.bundle,args.plan)
    print(json.dumps(result,sort_keys=True)); return 0


if __name__ == "__main__": raise SystemExit(_main())
