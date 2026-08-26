"""Idempotent, existing-first import of the user-authorized Pyramid archive.

The archive is evidence, not a provider runtime.  It can add missing facts but
can never replace a QuoteMux fact already present under the canonical key.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
import hashlib
import json
import re
from typing import Any
from pathlib import Path
import os

import psycopg

from quotemux.infra.db.client import _acquire_connection, _release_connection

STORAGE_SERIES_TYPE = "apex_l0_adjusted"
SOURCE_KEY = "pyramid_back_adjusted_20260714"
FACT_TRANSFORM_VERSION = "quotemux_fact_source_key_v1"
_SHA = re.compile(r"^[0-9a-f]{64}$")
_PRODUCTS = frozenset(("ag", "al", "AP", "CF", "cu", "hc", "i", "j", "m", "MA", "ni", "p", "ru", "sc", "T", "TA", "TF", "v", "y", "lh", "SA", "ao", "si"))


class FuturesPyramidImportError(ValueError):
    pass


@dataclass(frozen=True)
class PyramidBundle:
    rows: tuple[dict[str, object], ...]
    manifest: Mapping[str, object]
    source_normalized_rowset_sha256: str
    canonical_fact_rowset_sha256: str
    bundle_path: Path | None = None


def _sha(value: object, name: str) -> str:
    text = str(value or "")
    if not _SHA.fullmatch(text):
        raise FuturesPyramidImportError(f"{name} must be a lowercase SHA-256")
    return text


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _fact_row(raw: Mapping[str, object]) -> dict[str, object]:
    product = str(raw.get("product_code", ""))
    if product not in _PRODUCTS or product == "TL":
        raise FuturesPyramidImportError(f"unsupported Pyramid product_code: {product}")
    required = ("exchange", "bar_time", "open", "high", "low", "close", "volume", "adjustment_offset")
    if any(raw.get(name) is None for name in required):
        raise FuturesPyramidImportError("Pyramid canonical row has a missing required field")
    row = {name: raw[name] for name in ("product_code", "exchange", "bar_time", "open", "high", "low", "close", "volume", "adjustment_offset")}
    row.update(series_type=STORAGE_SERIES_TYPE, source_key=SOURCE_KEY, open_interest=None)
    return row


def load_pyramid_bundle(artifact_bytes: bytes, manifest: Mapping[str, object]) -> PyramidBundle:
    try:
        payload = json.loads(artifact_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FuturesPyramidImportError("Pyramid bundle must be UTF-8 JSON") from exc
    if not isinstance(payload, Mapping) or payload.get("schema_version") != "futures_user_pyramid_archive_bundle_v1":
        raise FuturesPyramidImportError("unsupported Pyramid bundle schema")
    if manifest.get("schema_version") != "futures_user_pyramid_archive_manifest_v1":
        raise FuturesPyramidImportError("unsupported Pyramid manifest schema")
    if manifest.get("lineage") != "user_provided/pyramid_post_adjusted_20260714":
        raise FuturesPyramidImportError("Pyramid lineage must remain user_provided")
    authorization = manifest.get("authorization")
    if not isinstance(authorization, Mapping) or authorization.get("status") != "private_research_authorized":
        raise FuturesPyramidImportError("Pyramid private-research authorization is required")
    rows = payload.get("rows")
    if not isinstance(rows, list) or not rows:
        raise FuturesPyramidImportError("Pyramid bundle rows must be non-empty")
    source_hash = _sha(manifest.get("source_normalized_rowset_sha256"), "source_normalized_rowset_sha256")
    fact_hash = _sha(manifest.get("canonical_fact_rowset_sha256"), "canonical_fact_rowset_sha256")
    if manifest.get("fact_transform_version") != FACT_TRANSFORM_VERSION:
        raise FuturesPyramidImportError("unexpected Pyramid fact transform version")
    normalized = tuple(_fact_row(row) for row in rows if isinstance(row, Mapping))
    if len(normalized) != len(rows) or len({(r["product_code"], r["bar_time"]) for r in normalized}) != len(normalized):
        raise FuturesPyramidImportError("Pyramid bundle has invalid or duplicate keys")
    if hashlib.sha256(_canonical_bytes(list(normalized))).hexdigest() != fact_hash:
        raise FuturesPyramidImportError("canonical fact rowset SHA-256 mismatch")
    return PyramidBundle(normalized, manifest, source_hash, fact_hash)


def load_pyramid_filesystem_bundle(bundle_path: Path) -> PyramidBundle:
    """Validate the package-produced Parquet bundle without reading it into RAM."""
    manifest_path = bundle_path / "manifest.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise FuturesPyramidImportError("Pyramid bundle manifest is unreadable") from exc
    if not isinstance(manifest, Mapping) or manifest.get("schema_version") != "futures_user_pyramid_archive_bundle_v1":
        raise FuturesPyramidImportError("unsupported Pyramid filesystem bundle schema")
    if not isinstance(manifest.get("authorization"), Mapping) or manifest["authorization"].get("status") != "private_research_authorized":
        raise FuturesPyramidImportError("Pyramid private-research authorization is required")
    facts = bundle_path / "fact_normalized.parquet"
    if not facts.is_file():
        raise FuturesPyramidImportError("fact_normalized.parquet is missing")
    actual = hashlib.sha256(facts.read_bytes()).hexdigest()
    normalization = manifest.get("fact_normalization")
    if not isinstance(normalization, Mapping) or actual != _sha(normalization.get("fact_normalized_artifact_sha256"), "fact_normalized_artifact_sha256"):
        raise FuturesPyramidImportError("fact-normalized artifact SHA-256 mismatch")
    return PyramidBundle((), manifest, _sha(manifest.get("source_normalized_rowset_sha256"), "source_normalized_rowset_sha256"), _sha(normalization.get("fact_normalized_rowset_sha256"), "fact_normalized_rowset_sha256"), bundle_path)


def _main() -> int:
    """Administrative command: classify/publish an immutable on-disk bundle."""
    import argparse
    parser = argparse.ArgumentParser(description="Classify and publish a Pyramid futures evidence bundle")
    parser.add_argument("command", choices=("classify", "publish"))
    parser.add_argument("--bundle", type=Path, required=True)
    args = parser.parse_args()
    bundle = load_pyramid_filesystem_bundle(args.bundle)
    if args.command == "classify":
        print(json.dumps({"source_normalized_rowset_sha256": bundle.source_normalized_rowset_sha256, "canonical_fact_rowset_sha256": bundle.canonical_fact_rowset_sha256, "rows": bundle.manifest.get("normalized_row_count")}, sort_keys=True)); return 0
    print(json.dumps(FuturesPyramidImporter(_publisher_connection).publish_filesystem_bundle(args.bundle), sort_keys=True)); return 0


def _publisher_connection() -> Any:
    """Fail-closed admin connection; never reuse the legacy application DSN."""
    required = {name: os.getenv(f"QUOTEMUX_PUBLISH_DB_{name}", "").strip() for name in ("HOST", "PORT", "NAME", "USER", "PASSWORD")}
    if not all(required.values()):
        raise FuturesPyramidImportError("QUOTEMUX_PUBLISH_DB_HOST/PORT/NAME/USER/PASSWORD are required")
    try:
        port = int(required["PORT"])
    except ValueError as exc:
        raise FuturesPyramidImportError("QUOTEMUX_PUBLISH_DB_PORT must be an integer") from exc
    return psycopg.connect(host=required["HOST"], port=port, dbname=required["NAME"], user=required["USER"], password=required["PASSWORD"])


if __name__ == "__main__":
    raise SystemExit(_main())


class FuturesPyramidImporter:
    def __init__(self, connection_factory: Callable[[], Any] = _acquire_connection) -> None:
        self._connection_factory = connection_factory

    def publish(self, artifact_bytes: bytes, manifest: Mapping[str, object]) -> dict[str, object]:
        bundle = load_pyramid_bundle(artifact_bytes, manifest)
        connection = self._connection_factory()
        owns_connection = self._connection_factory is _acquire_connection
        try:
            with connection.cursor() as cursor:
                cursor.execute("select qmi_id, payload_sha256 from audit.future_bar_1m_import_publication where source_normalized_rowset_sha256=%s and fact_transform_version=%s and canonical_fact_rowset_sha256=%s", (bundle.source_normalized_rowset_sha256, FACT_TRANSFORM_VERSION, bundle.canonical_fact_rowset_sha256))
                existing = cursor.fetchone()
                payload_hash = hashlib.sha256(_canonical_bytes(manifest)).hexdigest()
                if existing:
                    if existing[1] != payload_hash:
                        raise FuturesPyramidImportError("existing qmi payload differs")
                    connection.rollback()
                    return {"status": "idempotent", "qmi_id": existing[0]}
                self._stage(cursor, bundle)
                cursor.execute("select count(*) filter (where disposition='missing_valid'), count(*) filter (where disposition='already_present_equivalent'), count(*) filter (where disposition='existing_conflict') from future_pyramid_stage")
                missing, equivalent, conflict = cursor.fetchone()
                if missing:
                    cursor.execute("""insert into fact.future_bar_1m (product_code,exchange,series_type,bar_time,open,high,low,close,volume,open_interest,adjustment_offset,source_key)
                    select product_code,exchange,%s,bar_time,open,high,low,close,volume,null,adjustment_offset,%s from future_pyramid_stage where disposition='missing_valid'""", (STORAGE_SERIES_TYPE, SOURCE_KEY))
                qmi_id = "qmi-" + hashlib.sha256(_canonical_bytes({"source": bundle.source_normalized_rowset_sha256, "fact": bundle.canonical_fact_rowset_sha256, "transform": FACT_TRANSFORM_VERSION})).hexdigest()
                cursor.execute("""insert into audit.future_bar_1m_import_publication (qmi_id,source_normalized_rowset_sha256,fact_transform_version,canonical_fact_rowset_sha256,payload_sha256,manifest_json,inserted_count,equivalent_count,conflict_count)
                values (%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s)""", (qmi_id,bundle.source_normalized_rowset_sha256,FACT_TRANSFORM_VERSION,bundle.canonical_fact_rowset_sha256,payload_hash,json.dumps(manifest, sort_keys=True),missing,equivalent,conflict))
            connection.commit()
            return {"status": "success", "qmi_id": qmi_id, "inserted_count": missing, "equivalent_count": equivalent, "conflict_count": conflict}
        except Exception:
            connection.rollback()
            raise
        finally:
            if owns_connection:
                _release_connection(connection)

    def publish_filesystem_bundle(self, bundle_path: Path) -> dict[str, object]:
        """COPY Parquet record batches through a temporary stage, never a huge JSON list."""
        bundle = load_pyramid_filesystem_bundle(bundle_path)
        connection = self._connection_factory(); owns_connection = self._connection_factory is _acquire_connection
        try:
            with connection.cursor() as cursor:
                cursor.execute("select pg_advisory_xact_lock(hashtext('future_pyramid_import:' || %s))", (bundle.canonical_fact_rowset_sha256,))
                cursor.execute("select qmi_id, payload_sha256 from audit.future_bar_1m_import_publication where source_normalized_rowset_sha256=%s and fact_transform_version=%s and canonical_fact_rowset_sha256=%s", (bundle.source_normalized_rowset_sha256, FACT_TRANSFORM_VERSION, bundle.canonical_fact_rowset_sha256))
                existing = cursor.fetchone()
                payload_hash = hashlib.sha256(_canonical_bytes(bundle.manifest)).hexdigest()
                if existing:
                    if existing[1] != payload_hash: raise FuturesPyramidImportError("existing qmi payload differs")
                    connection.rollback(); return {"status":"idempotent","qmi_id":existing[0]}
                self._stage_parquet(cursor, bundle.bundle_path / "fact_normalized.parquet")
                cursor.execute("""update future_pyramid_stage stage set disposition=case when bars.product_code is null then 'missing_valid'
                    when bars.exchange is not distinct from stage.exchange and bars.open is not distinct from stage.open and bars.high is not distinct from stage.high and bars.low is not distinct from stage.low and bars.close is not distinct from stage.close and bars.volume is not distinct from stage.volume and bars.adjustment_offset is not distinct from stage.adjustment_offset then 'already_present_equivalent' else 'existing_conflict' end
                    from fact.future_bar_1m bars where bars.product_code=stage.product_code and bars.exchange=stage.exchange and bars.bar_time=stage.bar_time and bars.series_type=%s""", (STORAGE_SERIES_TYPE,))
                cursor.execute("update future_pyramid_stage set disposition='missing_valid' where disposition is null")
                cursor.execute("select count(*) filter(where disposition='missing_valid'),count(*) filter(where disposition='already_present_equivalent'),count(*) filter(where disposition='existing_conflict') from future_pyramid_stage")
                missing,equivalent,conflict=cursor.fetchone()
                if missing:
                    cursor.execute("insert into fact.future_bar_1m (product_code,exchange,series_type,bar_time,open,high,low,close,volume,open_interest,adjustment_offset,source_key) select product_code,exchange,%s,bar_time,open,high,low,close,volume,null,adjustment_offset,%s from future_pyramid_stage where disposition='missing_valid'", (STORAGE_SERIES_TYPE,SOURCE_KEY))
                qmi_id="qmi-v1-"+hashlib.sha256(_canonical_bytes({"source":bundle.source_normalized_rowset_sha256,"fact":bundle.canonical_fact_rowset_sha256,"transform":FACT_TRANSFORM_VERSION})).hexdigest()
                cursor.execute("insert into audit.future_bar_1m_import_publication(qmi_id,source_normalized_rowset_sha256,fact_transform_version,canonical_fact_rowset_sha256,payload_sha256,manifest_json,inserted_count,equivalent_count,conflict_count) values(%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s)", (qmi_id,bundle.source_normalized_rowset_sha256,FACT_TRANSFORM_VERSION,bundle.canonical_fact_rowset_sha256,payload_hash,json.dumps(bundle.manifest,sort_keys=True),missing,equivalent,conflict))
            connection.commit(); return {"status":"success","qmi_id":qmi_id,"inserted_count":missing,"equivalent_count":equivalent,"conflict_count":conflict}
        except Exception:
            connection.rollback(); raise
        finally:
            if owns_connection: _release_connection(connection)

    @staticmethod
    def _stage_parquet(cursor: Any, path: Path) -> None:
        import pyarrow.parquet as pq
        cursor.execute("create temporary table future_pyramid_stage (product_code text,exchange text,bar_time timestamp,open double precision,high double precision,low double precision,close double precision,volume double precision,adjustment_offset double precision,disposition text) on commit drop")
        with cursor.copy("copy future_pyramid_stage (product_code,exchange,bar_time,open,high,low,close,volume,adjustment_offset) from stdin") as copy:
            for batch in pq.ParquetFile(path).iter_batches(batch_size=100_000, columns=["product_code","exchange","bar_time","open","high","low","close","volume","adjustment_offset"]):
                for row in batch.to_pylist(): copy.write_row(tuple(row[name] for name in ("product_code","exchange","bar_time","open","high","low","close","volume","adjustment_offset")))

    @staticmethod
    def _stage(cursor: Any, bundle: PyramidBundle) -> None:
        cursor.execute("create temporary table future_pyramid_stage (product_code text,exchange text,bar_time timestamp,open double precision,high double precision,low double precision,close double precision,volume double precision,adjustment_offset double precision,disposition text) on commit drop")
        with cursor.copy("copy future_pyramid_stage (product_code,exchange,bar_time,open,high,low,close,volume,adjustment_offset) from stdin") as copy:
            for row in bundle.rows:
                copy.write_row(tuple(row[key] for key in ("product_code","exchange","bar_time","open","high","low","close","volume","adjustment_offset")))
        cursor.execute("""update future_pyramid_stage stage set disposition=case when bars.product_code is null then 'missing_valid'
          when bars.exchange is not distinct from stage.exchange and bars.open is not distinct from stage.open and bars.high is not distinct from stage.high and bars.low is not distinct from stage.low and bars.close is not distinct from stage.close and bars.volume is not distinct from stage.volume and bars.adjustment_offset is not distinct from stage.adjustment_offset then 'already_present_equivalent' else 'existing_conflict' end
          from fact.future_bar_1m bars where bars.product_code=stage.product_code and bars.bar_time=stage.bar_time and bars.series_type='apex_l0_adjusted'""")
        cursor.execute("update future_pyramid_stage set disposition='missing_valid' where disposition is null")
