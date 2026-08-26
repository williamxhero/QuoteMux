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
                qmp_payload = {"dataset_id": DATASET_ID, "qmi_id": qmi_id, "qmi_fact_rowset": imported[0], "catalog_identity": catalog_identity, "qmg_id": qmg_id, "missing_bar_semantics": "skip", "open_interest": "unavailable_or_null"}
                qmp_id = canonical_identity("qmp", qmp_payload)
                qmc_payload = {"dataset_id": DATASET_ID, "qmp_id": qmp_id, "qmg_id": qmg_id, "contract": "observed_admitted_only_skip_gaps"}
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
