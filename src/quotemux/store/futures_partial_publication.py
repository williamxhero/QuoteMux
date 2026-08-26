"""Immutable S000012 partial publication derived only from QuoteMux facts."""
from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime, timedelta
import hashlib
import json
from typing import Any

from psycopg.rows import tuple_row
from quotemux.futures_partial_contract import (
    APEX_SOURCE_KEY, DATASET_ID, INVALID_APEX_KEYS, PRODUCT_EXCHANGES, PRODUCTS, PYRAMID_SOURCE_KEY,
    SERIES_TYPE, SHINNY_SOURCE_KEY, admitted_rows_cte, canonical_json_bytes,
)
from quotemux.infra.db.client import _acquire_connection, _release_connection
from quotemux.store.futures_pyramid_import import verify_persisted_qmi_children


def canonical_identity(prefix: str, payload: Mapping[str, object]) -> str:
    if prefix not in {"qmp", "qmc", "qmg"}: raise ValueError("unsupported publication prefix")
    return f"{prefix}-v1-" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def validate_identity(value: str, prefix: str) -> str:
    marker = f"{prefix}-v1-"
    if not value.startswith(marker) or len(value) != len(marker) + 64: raise ValueError(f"invalid {prefix} identity")
    try: int(value[len(marker):], 16)
    except ValueError as exc: raise ValueError(f"invalid {prefix} identity") from exc
    return value


def _payload_sha(payload: Mapping[str, object]) -> str: return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _row_payload(row: tuple[object, ...]) -> dict[str, object]:
    keys = ("product_code","exchange","series_type","bar_time","open","high","low","close","volume","open_interest","adjustment_offset","source_key","pyramid_candidate_sha256")
    return dict(zip(keys, row, strict=True))


def _boundary_id(row: Mapping[str, object]) -> str:
    return "qmb-v1-" + hashlib.sha256(canonical_json_bytes(row)).hexdigest()


def _interval_id(row: Mapping[str, object]) -> str:
    return "qci-v1-" + hashlib.sha256(canonical_json_bytes(row)).hexdigest()


def _collect_admitted(connection: Any, qmi_id: str) -> tuple[list[dict[str, object]], list[dict[str, object]], dict[str, object]]:
    """Derive actual source islands and observed one-minute runs from one CTE."""
    boundaries: list[dict[str, object]] = []; intervals: list[dict[str, object]] = []
    current_boundary: dict[str, object] | None = None; current_interval: dict[str, object] | None = None
    boundary_digest: Any | None = None; interval_digest: Any | None = None
    warmup: dict[str, str] = {}
    def close_boundary() -> None:
        nonlocal current_boundary, boundary_digest
        if current_boundary is not None:
            current_boundary["eligible_rowset_sha256"] = boundary_digest.hexdigest(); current_boundary["boundary_id"] = _boundary_id({key:value for key,value in current_boundary.items() if key != "boundary_id"}); boundaries.append(current_boundary)
        current_boundary = None; boundary_digest = None
    def close_interval() -> None:
        nonlocal current_interval, interval_digest
        if current_interval is not None:
            current_interval["observed_rowset_sha256"] = interval_digest.hexdigest(); current_interval["interval_id"] = _interval_id({key:value for key,value in current_interval.items() if key != "interval_id"}); intervals.append(current_interval)
        current_interval = None; interval_digest = None
    # The admitted relation must run once.  Filtering it once per product can
    # make PostgreSQL re-evaluate tens of millions of facts/admissions 23x.
    cursor = connection.cursor(name="future_partial_admitted_rows", row_factory=tuple_row)
    try:
        cursor.execute(admitted_rows_cte(qmi_expression="%s") + " select product_code,exchange,series_type,bar_time::text,open,high,low,close,volume,open_interest,adjustment_offset,source_key,pyramid_candidate_sha256 from admitted_rows order by product_code collate \"C\",exchange collate \"C\",bar_time", (qmi_id,qmi_id))
        while rows := cursor.fetchmany(100_000):
          for raw in rows:
            row = _row_payload(raw); product = str(row["product_code"]); timestamp = datetime.fromisoformat(str(row["bar_time"]))
            warmup.setdefault(product, str(row["bar_time"]))
            bkey = (product, row["exchange"], row["series_type"], row["source_key"])
            if current_boundary is None or tuple(current_boundary[key] for key in ("product_code","exchange","series_type","source_key")) != bkey:
                close_boundary(); current_boundary = {"product_code":product,"exchange":row["exchange"],"series_type":row["series_type"],"source_key":row["source_key"],"start_time":str(row["bar_time"]),"end_time":str(row["bar_time"]),"eligible_row_count":0,"quality_predicate_version":"future_partial_admitted_v2","pyramid_admission":"exact_qmi_key_candidate_hash" if row["source_key"] == PYRAMID_SOURCE_KEY else "legacy_source_and_quality"}; boundary_digest = hashlib.sha256()
            current_boundary["end_time"] = str(row["bar_time"]); current_boundary["eligible_row_count"] = int(current_boundary["eligible_row_count"]) + 1; boundary_digest.update(canonical_json_bytes(row) + b"\n")
            ikey = (product, row["exchange"])
            start_new = current_interval is None or (current_interval["product_code"],current_interval["exchange"]) != ikey or timestamp != datetime.fromisoformat(str(current_interval["end_time"])) + timedelta(minutes=1)
            if start_new:
                close_interval(); current_interval = {"product_code":product,"exchange":row["exchange"],"start_time":str(row["bar_time"]),"end_time":str(row["bar_time"]),"status":"accepted","observed_count":0,"residual_json":{"missing_bar_semantics":"skip","meaning":"observed consecutive one-minute facts only; session completeness is not asserted","open_interest":"unavailable_or_null"}}; interval_digest = hashlib.sha256()
            current_interval["end_time"] = str(row["bar_time"]); current_interval["observed_count"] = int(current_interval["observed_count"]) + 1; interval_digest.update(canonical_json_bytes(row) + b"\n")
    finally:
        cursor.close()
    close_boundary(); close_interval()
    # The three known legacy exclusions are product-specific evidence; all
    # other gaps are represented by absent observed runs, never fabricated.
    exclusions: dict[str, list[str]] = {}
    for product, timestamp in INVALID_APEX_KEYS: exclusions.setdefault(product, []).append(timestamp)
    for interval in intervals:
        product = str(interval["product_code"])
        if product in exclusions: interval["residual_json"]["excluded_legacy_apex_keys"] = exclusions[product]
    return boundaries, intervals, {"warmup_first_observed": warmup, "accepted_interval_semantics":"consecutive_observed_minutes_only", "residual_semantics":"excluded_or_missing_rows_are_skipped", "excluded_legacy_apex_keys": exclusions}


def _manifest(rows: list[dict[str, object]]) -> dict[str, object]:
    digest = hashlib.sha256()
    for row in rows: digest.update(canonical_json_bytes(row) + b"\n")
    return {"count":len(rows),"sha256":digest.hexdigest()}


def _verified_catalog_identity(cursor: Any) -> str:
    """Freeze the exact QuoteMux 23-product reference mapping into qmp."""
    cursor.execute("select product_code,exchange,series_type,display_name from ref.future_series where series_type=%s and product_code=any(%s::text[]) order by product_code collate \"C\",exchange collate \"C\",series_type collate \"C\"", (SERIES_TYPE, list(PRODUCTS)))
    rows = [
        {"product_code": str(product), "exchange": str(exchange), "series_type": str(series), "display_name": str(display)}
        for product, exchange, series, display in cursor.fetchall()
    ]
    expected = set(PRODUCT_EXCHANGES)
    if len(rows) != len(expected) or {(row["product_code"], row["exchange"]) for row in rows} != expected or any(row["series_type"] != SERIES_TYPE for row in rows):
        raise ValueError("QuoteMux futures catalog does not contain the exact S000012 product mapping")
    return "qmf-catalog-v1-" + hashlib.sha256(canonical_json_bytes({"products": rows, "series_type": SERIES_TYPE})).hexdigest()


def _verify_persisted_manifest(connection: Any, *, qmp_id: str, qmc_id: str, boundaries: list[dict[str, object]], intervals: list[dict[str, object]]) -> None:
    """Stream persisted immutable rows and reject stale extras or hash drift."""
    expected_boundaries = sorted(boundaries, key=lambda row: (str(row["product_code"]),str(row["start_time"]),str(row["end_time"]),str(row["boundary_id"])))
    expected_intervals = sorted(intervals, key=lambda row: (str(row["product_code"]),str(row["start_time"]),str(row["end_time"]),str(row["interval_id"])))
    cursor = connection.cursor(name="future_partial_persisted_boundaries", row_factory=tuple_row)
    boundary_digest, boundary_count = hashlib.sha256(), 0
    try:
        cursor.execute("select boundary_id,product_code,exchange,series_type,source_key,start_time::text,end_time::text,evidence_json from audit.future_bar_1m_partial_source_boundary where qmp_id=%s order by product_code collate \"C\",start_time,end_time,boundary_id collate \"C\"",(qmp_id,))
        while rows := cursor.fetchmany(100_000):
            for boundary_id,product,exchange,series,source,start,end,evidence in rows:
                detail=evidence if isinstance(evidence,dict) else json.loads(str(evidence))
                actual={"product_code":product,"exchange":exchange,"series_type":series,"source_key":source,"start_time":start,"end_time":end,"eligible_row_count":detail["eligible_row_count"],"quality_predicate_version":detail["quality_predicate_version"],"pyramid_admission":detail["pyramid_admission"],"eligible_rowset_sha256":detail["eligible_rowset_sha256"],"boundary_id":boundary_id}
                boundary_digest.update(canonical_json_bytes(actual)+b"\n"); boundary_count += 1
    finally:
        cursor.close()
    cursor = connection.cursor(name="future_partial_persisted_intervals", row_factory=tuple_row)
    interval_digest, interval_count = hashlib.sha256(), 0
    try:
        cursor.execute("select interval_id,product_code,exchange,start_time::text,end_time::text,status,observed_count,residual_json from audit.future_bar_1m_partial_revision_interval where qmc_id=%s order by product_code collate \"C\",start_time,end_time,interval_id collate \"C\"",(qmc_id,))
        while rows := cursor.fetchmany(100_000):
            for interval_id,product,exchange,start,end,status,count,residual in rows:
                full=residual if isinstance(residual,dict) else json.loads(str(residual)); detail={key:value for key,value in full.items() if key!="observed_rowset_sha256"}
                actual={"product_code":product,"exchange":exchange,"start_time":start,"end_time":end,"status":status,"observed_count":count,"residual_json":detail,"observed_rowset_sha256":full["observed_rowset_sha256"],"interval_id":interval_id}
                interval_digest.update(canonical_json_bytes(actual)+b"\n"); interval_count += 1
    finally:
        cursor.close()
    if boundary_count != len(expected_boundaries) or interval_count != len(expected_intervals) or boundary_digest.hexdigest() != _manifest(expected_boundaries)["sha256"] or interval_digest.hexdigest() != _manifest(expected_intervals)["sha256"]:
        raise ValueError("persisted immutable boundary/interval manifest differs from plan")


class FuturesPartialPublisher:
    """Plans read-only; publication writes immutable metadata, never duplicate bars."""
    def __init__(self, connection_factory: Callable[[], Any] = _acquire_connection) -> None: self._connection_factory = connection_factory

    def plan(self, *, qmi_id: str, catalog_identity: str | None = None, expected_generation: int | None = None) -> dict[str, object]:
        connection = self._connection_factory(); owns = self._connection_factory is _acquire_connection
        try:
            with connection.cursor(row_factory=tuple_row) as cursor:
                cursor.execute("begin isolation level repeatable read read only")
                cursor.execute("select canonical_fact_rowset_sha256,manifest_json,inserted_count,equivalent_count,conflict_count from audit.future_bar_1m_import_publication where qmi_id=%s",(qmi_id,)); receipt = cursor.fetchone()
                cursor.execute("select generation,row_count,first_bar_time::text,last_bar_time::text from audit.future_bar_1m_series_generation where series_type=%s order by generation desc limit 1",(SERIES_TYPE,)); generation = cursor.fetchone()
                if not receipt or not generation: raise ValueError("required qmi receipt or future generation is absent")
                if expected_generation is not None and int(generation[0]) != expected_generation: raise ValueError("future generation is stale")
                verified_catalog_identity = _verified_catalog_identity(cursor)
                if catalog_identity is not None and catalog_identity != verified_catalog_identity:
                    raise ValueError("catalog_identity does not match the current QuoteMux S000012 catalog")
                boundaries, intervals, coverage = _collect_admitted(connection,qmi_id)
                qmg_payload = {"dataset_id":DATASET_ID,"series_type":SERIES_TYPE,"generation":int(generation[0]),"row_count":int(generation[1]),"first_bar_time":str(generation[2]),"last_bar_time":str(generation[3])}; qmg_id = canonical_identity("qmg",qmg_payload)
                boundary_manifest, interval_manifest = _manifest(boundaries), _manifest(intervals)
                import_receipt = receipt[1] if isinstance(receipt[1],dict) else json.loads(str(receipt[1])); pyramid_manifest=import_receipt.get("bundle_manifest",{}); qmi_child_manifests = verify_persisted_qmi_children(connection, qmi_id=qmi_id, receipt=import_receipt, inserted=int(receipt[2]), equivalent=int(receipt[3]), conflict=int(receipt[4]))
                qmp_payload = {"dataset_id":DATASET_ID,"qmi_id":qmi_id,"qmi_fact_rowset_sha256":receipt[0],"catalog_identity":verified_catalog_identity,"qmg_id":qmg_id,"sources":[{"source_key":APEX_SOURCE_KEY,"lineage":"legacy_reconstructed","entitlement":"unverified","admission":"generic_quality_with_three_exact_exclusions"},{"source_key":PYRAMID_SOURCE_KEY,"lineage":"user_provided/pyramid_post_adjusted_20260714","entitlement":"unknown_not_asserted","admission":"exact_qmi_key_candidate_hash","bundle_manifest_sha256":hashlib.sha256(canonical_json_bytes(pyramid_manifest)).hexdigest(),"raw_aggregate_sha256":pyramid_manifest.get("raw_aggregate_sha256"),"authorization":pyramid_manifest.get("authorization"),"qmi_child_manifests":qmi_child_manifests},{"source_key":SHINNY_SOURCE_KEY,"lineage":"derived_back_adjusted","admission":"generic_quality","actual_contract_mapping":"recorded_in_source_key"}],"qmi_counts":{"inserted":int(receipt[2]),"equivalent":int(receipt[3]),"conflict":int(receipt[4])},"source_boundary_manifest":boundary_manifest,"lineage_limitations":"Pyramid vendor entitlement is unknown/not asserted; legacy Apex raw artifact is unavailable; no source is claimed complete"}; qmp_id = canonical_identity("qmp",qmp_payload)
                qmc_payload = {"dataset_id":DATASET_ID,"qmp_id":qmp_id,"qmg_id":qmg_id,"timezone":"Asia/Shanghai","interval_bounds":"inclusive_local_naive","coverage_semantics":"observed_admitted_runs_only","missing_bar_semantics":"skip","open_interest":"null_or_unavailable","session_grid":"not_asserted_complete","accepted_interval_manifest":interval_manifest,"warmup":coverage,"partial_contract_satisfied":"verified_immutable_identity_and_observed_skip_contract"}; qmc_id = canonical_identity("qmc",qmc_payload)
                return {"dataset_id":DATASET_ID,"qmi_id":qmi_id,"catalog_identity":verified_catalog_identity,"expected_generation":int(generation[0]),"qmg_id":qmg_id,"qmp_id":qmp_id,"qmc_id":qmc_id,"generation_payload":qmg_payload,"publication_payload":qmp_payload,"revision_payload":qmc_payload,"boundaries":boundaries,"intervals":intervals}
        finally:
            try: connection.rollback()
            finally:
                if owns: _release_connection(connection)

    def publish(self, frozen_plan: Mapping[str, object]) -> dict[str, str]:
        qmi_id, catalog_identity, expected = str(frozen_plan.get("qmi_id","")), str(frozen_plan.get("catalog_identity","")), frozen_plan.get("expected_generation")
        current = self.plan(qmi_id=qmi_id,catalog_identity=catalog_identity,expected_generation=int(expected))
        if canonical_json_bytes(current) != canonical_json_bytes(dict(frozen_plan)): raise ValueError("frozen partial plan differs from current facts; re-plan")
        connection = self._connection_factory(); owns = self._connection_factory is _acquire_connection
        try:
            with connection.cursor(row_factory=tuple_row) as cursor:
                cursor.execute("select pg_advisory_xact_lock(hashtext(%s))",(DATASET_ID,))
                # Re-plan under the publication lock to close the check/write race.
                cursor.execute("select generation from audit.future_bar_1m_series_generation where series_type=%s order by generation desc limit 1",(SERIES_TYPE,)); locked_generation=cursor.fetchone()
                if not locked_generation or int(locked_generation[0]) != int(expected): raise ValueError("future generation changed before immutable publication")
                qmp, qmc = dict(current["publication_payload"]), dict(current["revision_payload"])
                cursor.execute("select payload_sha256 from audit.future_bar_1m_partial_publication where qmp_id=%s",(current["qmp_id"],)); existing_qmp=cursor.fetchone()
                cursor.execute("select payload_sha256 from audit.future_bar_1m_partial_revision where qmc_id=%s",(current["qmc_id"],)); existing_qmc=cursor.fetchone()
                if existing_qmp or existing_qmc:
                    if not existing_qmp or not existing_qmc or existing_qmp[0] != _payload_sha(qmp) or existing_qmc[0] != _payload_sha(qmc): raise ValueError("partial publication identity collision or orphan")
                    _verify_persisted_manifest(connection,qmp_id=str(current["qmp_id"]),qmc_id=str(current["qmc_id"]),boundaries=list(current["boundaries"]),intervals=list(current["intervals"]))
                    connection.commit(); return {"qmp_id":str(current["qmp_id"]),"qmc_id":str(current["qmc_id"]),"qmg_id":str(current["qmg_id"])}
                for table, key, payload, extra in (("audit.future_bar_1m_partial_publication","qmp_id",qmp,(DATASET_ID,)),("audit.future_bar_1m_partial_revision","qmc_id",qmc,(current["qmp_id"],))):
                    identity = str(current[key]); digest = _payload_sha(payload)
                    if table.endswith("publication"):
                        cursor.execute(f"insert into {table} (qmp_id,dataset_id,payload_json,payload_sha256) values (%s,%s,%s::jsonb,%s) on conflict do nothing",(identity,*extra,json.dumps(payload,sort_keys=True),digest))
                    else:
                        cursor.execute(f"insert into {table} (qmc_id,qmp_id,payload_json,payload_sha256) values (%s,%s,%s::jsonb,%s) on conflict do nothing",(identity,*extra,json.dumps(payload,sort_keys=True),digest))
                    cursor.execute(f"select payload_sha256 from {table} where {key}=%s",(identity,)); stored=cursor.fetchone()
                    if not stored or stored[0] != digest: raise ValueError("immutable publication identity collision")
                for boundary in current["boundaries"]:
                    evidence = {key:value for key,value in boundary.items() if key not in {"boundary_id","product_code","exchange","series_type","source_key","start_time","end_time"}}
                    cursor.execute("insert into audit.future_bar_1m_partial_source_boundary(qmp_id,boundary_id,product_code,exchange,series_type,source_key,start_time,end_time,evidence_json) values(%s,%s,%s,%s,%s,%s,%s::timestamp,%s::timestamp,%s::jsonb) on conflict do nothing",(current["qmp_id"],boundary["boundary_id"],boundary["product_code"],boundary["exchange"],boundary["series_type"],boundary["source_key"],boundary["start_time"],boundary["end_time"],json.dumps(evidence,sort_keys=True)))
                for interval in current["intervals"]:
                    cursor.execute("insert into audit.future_bar_1m_partial_revision_interval(qmc_id,interval_id,product_code,exchange,start_time,end_time,status,observed_count,residual_json) values(%s,%s,%s,%s,%s::timestamp,%s::timestamp,%s,%s,%s::jsonb) on conflict do nothing",(current["qmc_id"],interval["interval_id"],interval["product_code"],interval["exchange"],interval["start_time"],interval["end_time"],interval["status"],interval["observed_count"],json.dumps({**interval["residual_json"],"observed_rowset_sha256":interval["observed_rowset_sha256"]},sort_keys=True)))
                _verify_persisted_manifest(connection,qmp_id=str(current["qmp_id"]),qmc_id=str(current["qmc_id"]),boundaries=list(current["boundaries"]),intervals=list(current["intervals"]))
            connection.commit(); return {"qmp_id":str(current["qmp_id"]),"qmc_id":str(current["qmc_id"]),"qmg_id":str(current["qmg_id"])}
        except Exception: connection.rollback(); raise
        finally:
            if owns: _release_connection(connection)

    def verify_published(self, frozen_plan: Mapping[str, object]) -> dict[str, str]:
        """Read-only proof that immutable parents and exact child sets exist."""
        current=self.plan(qmi_id=str(frozen_plan.get("qmi_id","")),catalog_identity=str(frozen_plan.get("catalog_identity","")),expected_generation=int(frozen_plan["expected_generation"]))
        if canonical_json_bytes(current)!=canonical_json_bytes(dict(frozen_plan)): raise ValueError("frozen partial plan differs from current facts")
        connection=self._connection_factory(); owns=self._connection_factory is _acquire_connection
        try:
            with connection.cursor(row_factory=tuple_row) as cursor:
                cursor.execute("begin isolation level repeatable read read only")
                cursor.execute("select payload_sha256 from audit.future_bar_1m_partial_publication where qmp_id=%s",(current["qmp_id"],)); qmp=cursor.fetchone()
                cursor.execute("select payload_sha256 from audit.future_bar_1m_partial_revision where qmc_id=%s",(current["qmc_id"],)); qmc=cursor.fetchone()
                if not qmp or not qmc or qmp[0] != _payload_sha(dict(current["publication_payload"])) or qmc[0] != _payload_sha(dict(current["revision_payload"])): raise ValueError("partial parents are absent or mismatched")
                _verify_persisted_manifest(connection,qmp_id=str(current["qmp_id"]),qmc_id=str(current["qmc_id"]),boundaries=list(current["boundaries"]),intervals=list(current["intervals"]))
            connection.rollback(); return {"status":"verified","qmp_id":str(current["qmp_id"]),"qmc_id":str(current["qmc_id"]),"qmg_id":str(current["qmg_id"])}
        except Exception: connection.rollback(); raise
        finally:
            if owns: _release_connection(connection)


def _main() -> int:
    """Administrative plan/freeze/publish/verify command; no implicit writes."""
    import argparse
    from pathlib import Path
    from quotemux.store.futures_pyramid_import import _publisher_connection
    parser = argparse.ArgumentParser(description="Publish immutable QuoteMux futures partial metadata")
    parser.add_argument("command", choices=("plan", "publish", "verify"))
    parser.add_argument("--qmi-id"); parser.add_argument("--catalog-identity"); parser.add_argument("--expected-generation", type=int)
    parser.add_argument("--plan", type=Path, required=True)
    args = parser.parse_args(); publisher = FuturesPartialPublisher(_publisher_connection)
    if args.command == "plan":
        if not args.qmi_id: raise ValueError("plan requires --qmi-id")
        result = publisher.plan(qmi_id=args.qmi_id,catalog_identity=args.catalog_identity,expected_generation=args.expected_generation)
        with args.plan.open("x", encoding="utf-8") as stream: stream.write(canonical_json_bytes(result).decode("utf-8"))
    else:
        frozen=json.loads(args.plan.read_text(encoding="utf-8"))
        if not isinstance(frozen,dict): raise ValueError("plan must be a JSON object")
        if args.command == "publish": result=publisher.publish(frozen)
        else:
            result=publisher.verify_published(frozen)
    print(json.dumps(result,sort_keys=True)); return 0


if __name__ == "__main__":
    raise SystemExit(_main())
