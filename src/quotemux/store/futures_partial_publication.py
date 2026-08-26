"""Immutable metadata-only partial publication identities for S000012."""
from __future__ import annotations
import hashlib
import json
from collections.abc import Mapping
from typing import Any, Callable

from quotemux.infra.db.client import _acquire_connection, _release_connection

DATASET_ID = "future_1m_partial_s000012_quotemux"

def canonical_identity(prefix: str, payload: Mapping[str, object]) -> str:
    if prefix not in {"qmp", "qmc", "qmg"}:
        raise ValueError("unsupported publication prefix")
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    return f"{prefix}-v1-" + hashlib.sha256(encoded).hexdigest()

def validate_identity(value: str, prefix: str) -> str:
    expected = f"{prefix}-v1-"
    if not value.startswith(expected) or len(value) != len(expected) + 64:
        raise ValueError(f"invalid {prefix} identity")
    int(value[len(expected):], 16)
    return value


def _payload_hash(payload: Mapping[str, object]) -> str:
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


class FuturesPartialPublisher:
    """Create a read contract from existing QuoteMux facts, never a duplicate bar store."""
    def __init__(self, connection_factory: Callable[[], Any] = _acquire_connection) -> None:
        self._connection_factory = connection_factory

    def publish(self, *, qmi_id: str, catalog_identity: str, expected_generation: int | None = None) -> dict[str, str]:
        connection = self._connection_factory(); owns = self._connection_factory is _acquire_connection
        try:
            with connection.cursor() as cursor:
                cursor.execute("select pg_advisory_xact_lock(hashtext('future_1m_partial_s000012_quotemux'))")
                cursor.execute("select canonical_fact_rowset_sha256, manifest_json from audit.future_bar_1m_import_publication where qmi_id=%s", (qmi_id,))
                imported = cursor.fetchone()
                if not imported: raise ValueError("qmi receipt is absent")
                cursor.execute("select generation,row_count,first_bar_time,last_bar_time from audit.future_bar_1m_series_generation where series_type='apex_l0_adjusted' order by generation desc limit 1")
                generation = cursor.fetchone()
                if not generation: raise ValueError("future generation is absent")
                if expected_generation is not None and int(generation[0]) != expected_generation: raise ValueError("future generation is stale")
                qmg_payload = {"dataset_id": DATASET_ID, "generation": int(generation[0]), "row_count": int(generation[1]), "first": str(generation[2]), "last": str(generation[3])}
                qmg_id = canonical_identity("qmg", qmg_payload)
                qmp_payload = {"dataset_id": DATASET_ID, "qmi_id": qmi_id, "qmi_fact_rowset": imported[0], "catalog_identity": catalog_identity, "qmg_id": qmg_id, "missing_bar_semantics": "skip", "open_interest": "unavailable_or_null", "sources": [
                    {"source_key":"apex_l0_import","lineage":"legacy_reconstructed","raw_artifact":"unavailable","entitlement":"unverified","admission":"row_quality_gate"},
                    {"source_key":"pyramid_back_adjusted_20260714","lineage":"user_provided/pyramid_post_adjusted_20260714","admission":"qmi_exact_key_candidate_hash"},
                    {"source_key":"shinny_edb_derived_back_adjusted_20260811","lineage":"derived_back_adjusted","admission":"row_quality_gate","actual_contract_mapping":"recorded_in_source_key"},
                ]}
                qmp_id = canonical_identity("qmp", qmp_payload)
                qmc_payload = {"dataset_id": DATASET_ID, "qmp_id": qmp_id, "qmg_id": qmg_id, "contract": "observed_admitted_only_skip_gaps", "timezone":"Asia/Shanghai", "interval_bounds":"inclusive_local_naive", "missing_bar_semantics":"skip", "oi":"null_or_unavailable", "session_grid":"not asserted_complete", "partial_contract_satisfied":"identity_and_skip_semantics_only"}
                qmc_id = canonical_identity("qmc", qmc_payload)
                cursor.execute("insert into audit.future_bar_1m_partial_publication (qmp_id,dataset_id,payload_json,payload_sha256) values (%s,%s,%s::jsonb,%s) on conflict (qmp_id) do update set payload_json=excluded.payload_json where audit.future_bar_1m_partial_publication.payload_sha256=excluded.payload_sha256", (qmp_id,DATASET_ID,json.dumps(qmp_payload,sort_keys=True),_payload_hash(qmp_payload)))
                cursor.execute("insert into audit.future_bar_1m_partial_revision (qmc_id,qmp_id,payload_json,payload_sha256) values (%s,%s,%s::jsonb,%s) on conflict (qmc_id) do update set payload_json=excluded.payload_json where audit.future_bar_1m_partial_revision.payload_sha256=excluded.payload_sha256", (qmc_id,qmp_id,json.dumps(qmc_payload,sort_keys=True),_payload_hash(qmc_payload)))
                # Maximal observed source runs, based on actual source ownership.
                cursor.execute("""insert into audit.future_bar_1m_partial_source_boundary(qmp_id,boundary_id,product_code,exchange,series_type,source_key,start_time,end_time,evidence_json)
                with changes as (select product_code,exchange,series_type,source_key,bar_time,
                   case when lag(source_key) over (partition by product_code,exchange,series_type order by bar_time) is not distinct from source_key then 0 else 1 end as changed from fact.future_bar_1m where series_type='apex_l0_adjusted' and open is not null and high is not null and low is not null and close is not null and volume is not null and volume>=0 and high>=greatest(open,close,low) and low<=least(open,close,high) and (product_code,bar_time) not in (('TA','2010-11-17 09:05:00'::timestamp),('y','2010-11-11 14:01:00'::timestamp),('y','2011-02-23 11:22:00'::timestamp))), ordered as
                   (select *,sum(changed) over (partition by product_code,exchange,series_type order by bar_time) as run_id from changes), runs as
                   (select product_code,exchange,series_type,source_key,run_id,min(bar_time) start_time,max(bar_time) end_time from ordered group by product_code,exchange,series_type,source_key,run_id)
                select %s, 'qmb-v1-'||md5(product_code||'|'||exchange||'|'||source_key||'|'||start_time::text||'|'||end_time::text),product_code,exchange,series_type,source_key,start_time,end_time,jsonb_build_object('meaning','maximal actual per-row source ownership run') from runs on conflict do nothing""", (qmp_id,))
                cursor.execute("""insert into audit.future_bar_1m_partial_revision_interval(qmc_id,interval_id,product_code,start_time,end_time,status,observed_count,residual_json)
                select %s, 'qmi-v1-'||md5(product_code||'|'||min(bar_time)::text||'|'||max(bar_time)::text), product_code,min(bar_time),max(bar_time),'accepted',count(*),jsonb_build_object('missing_bar_semantics','skip','oi','unavailable_or_null','excluded_exact_keys',jsonb_build_array('TA@2010-11-17 09:05:00','y@2010-11-11 14:01:00','y@2011-02-23 11:22:00')) from fact.future_bar_1m where series_type='apex_l0_adjusted' and open is not null and high is not null and low is not null and close is not null and volume is not null and volume>=0 and high>=greatest(open,close,low) and low<=least(open,close,high) and (product_code,bar_time) not in (('TA','2010-11-17 09:05:00'::timestamp),('y','2010-11-11 14:01:00'::timestamp),('y','2011-02-23 11:22:00'::timestamp)) group by product_code on conflict do nothing""", (qmc_id,))
            connection.commit(); return {"qmp_id":qmp_id,"qmc_id":qmc_id,"qmg_id":qmg_id}
        except Exception:
            connection.rollback(); raise
        finally:
            if owns: _release_connection(connection)

    def plan(self, *, qmi_id: str, catalog_identity: str, expected_generation: int | None = None) -> dict[str, object]:
        """Read-only deterministic preflight; deliberately rolls back its snapshot."""
        connection = self._connection_factory(); owns = self._connection_factory is _acquire_connection
        try:
            with connection.cursor() as cursor:
                cursor.execute("begin isolation level repeatable read read only")
                cursor.execute("select canonical_fact_rowset_sha256 from audit.future_bar_1m_import_publication where qmi_id=%s", (qmi_id,)); imported=cursor.fetchone()
                cursor.execute("select generation,row_count,first_bar_time,last_bar_time from audit.future_bar_1m_series_generation where series_type='apex_l0_adjusted' order by generation desc limit 1"); generation=cursor.fetchone()
                if not imported or not generation: raise ValueError("required import receipt or generation is absent")
                if expected_generation is not None and int(generation[0]) != expected_generation: raise ValueError("future generation is stale")
                qmg=canonical_identity("qmg",{"dataset_id":DATASET_ID,"generation":int(generation[0]),"row_count":int(generation[1]),"first":str(generation[2]),"last":str(generation[3])})
                qmp=canonical_identity("qmp",{"dataset_id":DATASET_ID,"qmi_id":qmi_id,"qmi_fact_rowset":imported[0],"catalog_identity":catalog_identity,"qmg_id":qmg,"missing_bar_semantics":"skip","open_interest":"unavailable_or_null","sources":[{"source_key":"apex_l0_import","lineage":"legacy_reconstructed","raw_artifact":"unavailable","entitlement":"unverified","admission":"row_quality_gate"},{"source_key":"pyramid_back_adjusted_20260714","lineage":"user_provided/pyramid_post_adjusted_20260714","admission":"qmi_exact_key_candidate_hash"},{"source_key":"shinny_edb_derived_back_adjusted_20260811","lineage":"derived_back_adjusted","admission":"row_quality_gate","actual_contract_mapping":"recorded_in_source_key"}]})
                qmc=canonical_identity("qmc",{"dataset_id":DATASET_ID,"qmp_id":qmp,"qmg_id":qmg,"contract":"observed_admitted_only_skip_gaps","timezone":"Asia/Shanghai","interval_bounds":"inclusive_local_naive","missing_bar_semantics":"skip","oi":"null_or_unavailable","session_grid":"not_asserted_complete","partial_contract_satisfied":"identity_and_skip_semantics_only"})
                return {"dataset_id":DATASET_ID,"qmi_id":qmi_id,"catalog_identity":catalog_identity,"expected_generation":int(generation[0]),"qmp_id":qmp,"qmc_id":qmc,"qmg_id":qmg}
        finally:
            try: connection.rollback()
            finally:
                if owns: _release_connection(connection)


def _main() -> int:
    import argparse
    from quotemux.store.futures_pyramid_import import _publisher_connection
    parser = argparse.ArgumentParser(description="Plan or publish QuoteMux futures partial metadata")
    parser.add_argument("command", choices=("plan", "publish"))
    parser.add_argument("--qmi-id")
    parser.add_argument("--catalog-identity")
    parser.add_argument("--manifest")
    parser.add_argument("--expected-generation", type=int)
    parser.add_argument("--out")
    args = parser.parse_args()
    publisher = FuturesPartialPublisher(_publisher_connection)
    if args.manifest:
        from pathlib import Path
        frozen = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
        if not isinstance(frozen, dict): raise ValueError("manifest must be an object")
        qmi_id, catalog_identity, expected = str(frozen.get("qmi_id","")), str(frozen.get("catalog_identity","")), frozen.get("expected_generation")
        actual = publisher.plan(qmi_id=qmi_id, catalog_identity=catalog_identity, expected_generation=int(expected))
        if json.dumps(actual,sort_keys=True,separators=(",",":")) != json.dumps(frozen,sort_keys=True,separators=(",",":")):
            raise ValueError("manifest differs from current deterministic partial plan")
        result = actual if args.command == "plan" else publisher.publish(qmi_id=qmi_id, catalog_identity=catalog_identity, expected_generation=int(expected))
    else:
        if not args.qmi_id or not args.catalog_identity: raise ValueError("--qmi-id and --catalog-identity are required without --manifest")
        result = publisher.plan(qmi_id=args.qmi_id, catalog_identity=args.catalog_identity, expected_generation=args.expected_generation) if args.command == "plan" else publisher.publish(qmi_id=args.qmi_id, catalog_identity=args.catalog_identity, expected_generation=args.expected_generation)
    if args.out:
        from pathlib import Path
        Path(args.out).write_text(json.dumps(result, sort_keys=True), encoding="utf-8")
    print(json.dumps({"mode": args.command, **result}, sort_keys=True)); return 0


if __name__ == "__main__":
    raise SystemExit(_main())
