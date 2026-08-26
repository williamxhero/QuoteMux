from __future__ import annotations

import hashlib
import json

import pytest

from quotemux.infra.db.read_client import QueryBatch
from quotemux.public_reader import FuturesPartialPublicationQueryError, QuoteMuxPublicReader
from quotemux.store.futures_partial_publication import canonical_identity, validate_identity
from quotemux.store.futures_pyramid_import import FACT_TRANSFORM_VERSION, FuturesPyramidImportError, load_pyramid_bundle


def _artifact(product: str = "T") -> tuple[bytes, dict[str, object]]:
    row = {"product_code": product, "exchange": "CFFEX", "bar_time": "2026-07-14 15:00:00", "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 9.0, "adjustment_offset": 0.0}
    artifact = json.dumps({"schema_version": "futures_user_pyramid_archive_bundle_v1", "rows": [row]}, separators=(",", ":")).encode()
    fact = [{**row, "series_type": "apex_l0_adjusted", "source_key": "pyramid_back_adjusted_20260714", "open_interest": None}]
    manifest = {"schema_version": "futures_user_pyramid_archive_manifest_v1", "lineage": "user_provided/pyramid_post_adjusted_20260714", "authorization": {"status": "private_research_authorized"}, "source_normalized_rowset_sha256": "a" * 64, "fact_transform_version": FACT_TRANSFORM_VERSION, "canonical_fact_rowset_sha256": hashlib.sha256(json.dumps(fact, sort_keys=True, separators=(",", ":")).encode()).hexdigest()}
    return artifact, manifest


def test_pyramid_bundle_requires_canonical_t_and_rejects_tl() -> None:
    artifact, manifest = _artifact()
    assert load_pyramid_bundle(artifact, manifest).rows[0]["product_code"] == "T"
    bad_artifact, bad_manifest = _artifact("TL")
    with pytest.raises(FuturesPyramidImportError, match="unsupported"):
        load_pyramid_bundle(bad_artifact, bad_manifest)


def test_pyramid_bundle_rejects_ao_gfex_exchange() -> None:
    artifact, manifest = _artifact("ao")
    with pytest.raises(FuturesPyramidImportError, match="exchange mismatch"):
        load_pyramid_bundle(artifact, manifest)


def test_partial_identity_has_prefixed_64_hex_digest() -> None:
    value = canonical_identity("qmc", {"dataset": "future_1m_partial_s000012_quotemux"})
    assert value.startswith("qmc-v1-") and len(value) == 71
    assert validate_identity(value, "qmc") == value


def test_partial_catalog_identity_is_derived_from_exact_quotemux_reference_rows() -> None:
    from quotemux.futures_partial_contract import PRODUCT_EXCHANGES, SERIES_TYPE
    from quotemux.store.futures_partial_publication import _verified_catalog_identity

    class Cursor:
        def execute(self, _query, _params=()): pass
        def fetchall(self): return [(product, exchange, SERIES_TYPE, "") for product, exchange in PRODUCT_EXCHANGES]

    identity = _verified_catalog_identity(Cursor())
    assert identity.startswith("qmf-catalog-v1-") and len(identity) == 79
    class Missing(Cursor):
        def fetchall(self): return super().fetchall()[:-1]
    with pytest.raises(ValueError, match="exact S000012"):
        _verified_catalog_identity(Missing())


def test_partial_reader_binds_generation_and_rejects_tl() -> None:
    class Client:
        def query_batch(self, _query, _params=(), *, stage="sql"):
            return QueryBatch(("product_code",), ())
    qmp = canonical_identity("qmp", {"p": 1}); qmc = canonical_identity("qmc", {"p": 1}); qmg = canonical_identity("qmg", {"p": 1})
    reader = QuoteMuxPublicReader(client=Client())
    with pytest.raises(FuturesPartialPublicationQueryError, match="TL"):
        reader.read_futures_1m_partial_page("TL", "2026-07-14 09:01:00", "2026-07-14 15:00:00", qmp_id=qmp, qmc_id=qmc, qmg_id=qmg)


def test_partial_metadata_requires_verified_identity_and_exposes_skip_contract() -> None:
    qmg_payload = {"dataset_id": "future_1m_partial_s000012_quotemux", "series_type": "apex_l0_adjusted", "generation": 7, "row_count": 9, "first_bar_time": "2020-01-01 09:01:00", "last_bar_time": "2020-01-01 09:09:00"}
    qmg = canonical_identity("qmg", qmg_payload)
    publication = {"dataset_id": "future_1m_partial_s000012_quotemux", "qmg_id": qmg, "qmi_id": "qmi-v1-" + "1" * 64, "catalog_identity": "mhd-v1-catalog", "sources": [], "source_boundary_manifest": {"count": 0, "sha256": "0" * 64}, "lineage_limitations": "known"}
    qmp = canonical_identity("qmp", publication)
    revision = {"dataset_id": "future_1m_partial_s000012_quotemux", "qmp_id": qmp, "qmg_id": qmg, "timezone": "Asia/Shanghai", "interval_bounds": "inclusive_local_naive", "coverage_semantics": "observed_admitted_runs_only", "missing_bar_semantics": "skip", "open_interest": "null_or_unavailable", "session_grid": "not_asserted_complete", "warmup": {"residual_semantics": "skip"}}
    qmc = canonical_identity("qmc", revision)
    class Client:
        def query_batch(self, _query, _params=(), *, stage="sql"):
            import hashlib
            encoded = lambda value: hashlib.sha256(json.dumps(value,sort_keys=True,separators=(",",":")).encode()).hexdigest()
            return QueryBatch(("publication",), ((publication, encoded(publication), revision, encoded(revision), 7, 9, "2020-01-01 09:01:00", "2020-01-01 09:09:00"),))
    metadata = QuoteMuxPublicReader(client=Client()).get_futures_1m_partial_metadata(qmp_id=qmp,qmc_id=qmc,qmg_id=qmg)
    assert metadata["publication_verified"] is True
    assert metadata["missing_bar_semantics"] == "skip"


def test_partial_sql_keeps_exact_exclusions_and_boundary_evidence() -> None:
    from quotemux.store import futures_partial_publication as publication
    from quotemux import futures_partial_contract as contract
    from quotemux.store import futures_partial_migration as migration
    from quotemux import futures
    source = open(publication.__file__, encoding="utf-8").read()
    schema = "\n".join(futures.FUTURE_SCHEMA_SQL)
    shared_relation = contract.admitted_rows_cte(qmi_expression="%s")
    assert "pyramid_admission" in shared_relation
    assert "pyramid_conflicts" in shared_relation
    assert "TA" in repr(contract.INVALID_APEX_KEYS)
    assert "eligible_rowset_sha256" in source
    assert "qmg_id" in source
    assert "after truncate on fact.future_bar_1m" not in schema.lower()
    assert "security definer" in "\n".join(migration.HARDENED_FUNCTION_DDL).lower()


def test_futures_partial_migration_grants_trigger_and_receipt_path() -> None:
    from quotemux.store import futures_partial_migration as migration
    text = open(migration.__file__, encoding="utf-8").read()
    assert "audit.future_bar_1m_import_admission" in text
    assert "audit.future_bar_1m_series_generation" in text
    assert "quotemux_futures_partial_publisher" in text
    assert "quotemux_futures_owner" in text
    assert "OWNER_RUNTIME_GRANTS" in text
    assert "revoke insert,update,delete,truncate on fact.future_bar_1m" in text
    assert "for statement in OWNER_RUNTIME_GRANTS: cursor.execute(statement)\n            for statement in HARDENED_FUNCTION_DDL" in text
    assert "partial_revision_interval (qmc_id text not null" in text and "exchange text not null" in text


def test_partial_role_provisioning_forces_tuple_rows_on_dict_default_connection() -> None:
    from psycopg.rows import tuple_row
    from quotemux.store.futures_partial_migration import provision_futures_partial_roles

    class Cursor:
        def __init__(self, row_factory): self.row_factory = row_factory; self.last_sql = ""
        def __enter__(self): return self
        def __exit__(self, *_args): return False
        def execute(self, statement, _params=None): self.last_sql = str(statement)
        def fetchone(self):
            if "current_database" in self.last_sql:
                return ("datalake_dev",) if self.row_factory is tuple_row else {"current_database": "datalake_dev"}
            return None

    class Connection:
        def __init__(self): self.row_factories = []; self.commits = 0
        def cursor(self, *, row_factory=None, **_kwargs): self.row_factories.append(row_factory); return Cursor(row_factory)
        def commit(self): self.commits += 1
        def rollback(self): pass

    connection = Connection()
    provision_futures_partial_roles("publisher-secret", "reader-secret", lambda: connection)
    assert connection.commits == 2
    assert connection.row_factories and all(factory is tuple_row for factory in connection.row_factories)


def test_partial_import_and_publication_force_tuple_rows_on_default_pool_connections() -> None:
    from quotemux.store import futures_partial_publication as publication
    from quotemux.store import futures_pyramid_import as importer

    publication_source = open(publication.__file__, encoding="utf-8").read()
    importer_source = open(importer.__file__, encoding="utf-8").read()
    assert "connection.cursor()" not in publication_source
    assert "connection.cursor()" not in importer_source
    assert publication_source.count("row_factory=tuple_row") >= 4
    assert importer_source.count("row_factory=tuple_row") >= 4


def test_disposition_plan_persists_actual_stream_hash_and_count(tmp_path) -> None:
    from quotemux.store.futures_pyramid_import import _read_plan, _write_plan
    path = tmp_path / "plan.jsonl.gz"
    result = _write_plan(path, {"candidate_count": 2}, iter((
        {"product_code": "T", "disposition": "missing_valid"},
        {"product_code": "ag", "disposition": "existing_conflict"},
    )))
    header, rows = _read_plan(path)
    assert header["disposition_count"] == 2
    assert header["disposition_sha256"] == result["disposition_sha256"]
    assert list(rows)[1]["disposition"] == "existing_conflict"


def test_disposition_plan_failure_never_publishes_a_partial_final_artifact(tmp_path) -> None:
    from quotemux.store.futures_pyramid_import import _write_plan
    path = tmp_path / "plan.jsonl.gz"
    def broken_rows():
        yield {"product_code": "T", "disposition": "missing_valid"}
        raise RuntimeError("fixture failure")
    with pytest.raises(RuntimeError, match="fixture failure"):
        _write_plan(path, {}, broken_rows())
    assert not path.exists()
    assert not path.with_suffix(path.suffix + ".records.partial").exists()
    assert not path.with_suffix(path.suffix + ".gzip.partial").exists()


def test_prior_qmi_child_manifest_mismatch_fails_closed(monkeypatch) -> None:
    from quotemux.store import futures_pyramid_import as importer
    plan = {"disposition_count": 2, "disposition_sha256": "f" * 64}
    expected = {
        "dispositions": {"count": 2, "sha256": "d" * 64},
        "admissions": {"count": 1, "sha256": "a" * 64},
    }
    receipt = {"plan": plan, "child_manifests": expected}
    monkeypatch.setattr(importer, "_persisted_child_manifests", lambda *_args: expected)
    importer._verify_prior_qmi(object(), qmi_id="qmi-v1-" + "1" * 64, receipt=receipt, plan_payload=plan, inserted=1, equivalent=0, conflict=1)
    monkeypatch.setattr(importer, "_persisted_child_manifests", lambda *_args: {**expected, "admissions": {"count": 0, "sha256": "0" * 64}})
    with pytest.raises(FuturesPyramidImportError, match="child sets"):
        importer._verify_prior_qmi(object(), qmi_id="qmi-v1-" + "1" * 64, receipt=receipt, plan_payload=plan, inserted=1, equivalent=0, conflict=1)




def test_partial_coverage_binds_all_sql_placeholders() -> None:
    qmg_payload={"dataset_id":"future_1m_partial_s000012_quotemux","series_type":"apex_l0_adjusted","generation":1,"row_count":1,"first_bar_time":"2020-01-01 09:01:00","last_bar_time":"2020-01-01 09:01:00"}; qmg=canonical_identity("qmg",qmg_payload)
    publication={"dataset_id":"future_1m_partial_s000012_quotemux","qmg_id":qmg}; qmp=canonical_identity("qmp",publication); revision={"dataset_id":"future_1m_partial_s000012_quotemux","qmp_id":qmp,"qmg_id":qmg}; qmc=canonical_identity("qmc",revision)
    class Client:
        def __init__(self): self.calls=[]
        def query_batch(self, query, params=(), *, stage="sql"):
            self.calls.append((query,params,stage))
            if stage == "futures_partial_identity":
                encoded=lambda value: hashlib.sha256(json.dumps(value,sort_keys=True,separators=(",",":")).encode()).hexdigest()
                return QueryBatch(("x",),((publication,encoded(publication),revision,encoded(revision),1,1,"2020-01-01 09:01:00","2020-01-01 09:01:00"),))
            return QueryBatch(("x",),())
    client=Client(); QuoteMuxPublicReader(client=client).read_futures_1m_partial_coverage_page("T","2020-01-01 09:01:00","2020-01-01 09:02:00",qmp_id=qmp,qmc_id=qmc,qmg_id=qmg)
    query,params,stage=client.calls[-1]
    assert stage == "futures_partial_coverage" and query.count("%s") == len(params) == 14


def test_partial_coverage_keyset_uses_the_same_clipped_bounds_as_cursor_output() -> None:
    from quotemux import public_reader
    query = public_reader._FUTURES_PARTIAL_COVERAGE_QUERY
    assert "with clipped_intervals" in query
    assert "(product_code, start_time, end_time, status, interval_id)" in query
    assert "interval_row.start_time, interval_row.end_time, interval_row.status" not in query
    assert "interval_row.exchange" in query
    assert "join ref.future_series series" not in query


def test_partial_coverage_cursor_binds_clipped_interval_bounds_for_next_page() -> None:
    qmg_payload={"dataset_id":"future_1m_partial_s000012_quotemux","series_type":"apex_l0_adjusted","generation":1,"row_count":1,"first_bar_time":"2020-01-01 09:01:00","last_bar_time":"2020-01-01 09:01:00"}; qmg=canonical_identity("qmg",qmg_payload)
    publication={"dataset_id":"future_1m_partial_s000012_quotemux","qmg_id":qmg}; qmp=canonical_identity("qmp",publication); revision={"dataset_id":"future_1m_partial_s000012_quotemux","qmp_id":qmp,"qmg_id":qmg}; qmc=canonical_identity("qmc",revision)
    encoded=lambda value: hashlib.sha256(json.dumps(value,sort_keys=True,separators=(",",":")).encode()).hexdigest()
    first=("T","CFFEX","2020-01-01 09:01:00","2020-01-01 09:02:00","accepted",2,"qci-v1-" + "1" * 64,{})
    second=("ag","SHFE","2020-01-01 09:01:00","2020-01-01 09:02:00","accepted",2,"qci-v1-" + "2" * 64,{})
    class Client:
        def __init__(self): self.coverage_params=[]
        def query_batch(self, _query, params=(), *, stage="sql"):
            if stage == "futures_partial_identity": return QueryBatch(("x",),((publication,encoded(publication),revision,encoded(revision),1,1,"2020-01-01 09:01:00","2020-01-01 09:01:00"),))
            self.coverage_params.append(params)
            return QueryBatch(("product_code",), (first, second) if len(self.coverage_params) == 1 else (second,))
    client=Client(); reader=QuoteMuxPublicReader(client=client)
    _page, cursor = reader.read_futures_1m_partial_coverage_page(("T","ag"),"2020-01-01 09:01:00","2020-01-01 09:02:00",qmp_id=qmp,qmc_id=qmc,qmg_id=qmg,limit=1)
    page, next_cursor = reader.read_futures_1m_partial_coverage_page(("T","ag"),"2020-01-01 09:01:00","2020-01-01 09:02:00",qmp_id=qmp,qmc_id=qmc,qmg_id=qmg,cursor=cursor,limit=1)
    assert page.rows == (second,) and next_cursor is None
    assert client.coverage_params[1][7:13] == ("T", "T", "2020-01-01 09:01:00", "2020-01-01 09:02:00", "accepted", first[6])


def test_partial_manifest_order_uses_c_collation_for_mixed_case_products() -> None:
    from quotemux.store import futures_partial_publication as publication
    source = open(publication.__file__, encoding="utf-8").read()
    # Python's Unicode/codepoint sort agrees with PostgreSQL COLLATE "C";
    # it deliberately differs from the live database's en_US default here.
    assert sorted(("ag", "AP", "CF", "ao")) == ["AP", "CF", "ag", "ao"]
    assert source.count(r'product_code collate \"C\"') >= 3
    assert r'boundary_id collate \"C\"' in source and r'interval_id collate \"C\"' in source
    assert 'name="future_partial_persisted_boundaries"' in source
    assert 'name="future_partial_persisted_intervals"' in source


def test_partial_verify_requires_persisted_parents() -> None:
    from quotemux.store.futures_partial_publication import FuturesPartialPublisher

    publication = {"dataset_id": "future_1m_partial_s000012_quotemux"}
    revision = {"dataset_id": "future_1m_partial_s000012_quotemux"}
    plan = {
        "qmi_id": "qmi-v1-" + "1" * 64, "catalog_identity": "catalog", "expected_generation": 1,
        "qmp_id": "qmp-v1-" + "2" * 64, "qmc_id": "qmc-v1-" + "3" * 64, "qmg_id": "qmg-v1-" + "4" * 64,
        "publication_payload": publication, "revision_payload": revision, "boundaries": [], "intervals": [],
    }
    encoded = lambda value: hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    class Cursor:
        def __init__(self, parents): self.parents = parents; self.last_sql = ""
        def __enter__(self): return self
        def __exit__(self, *_args): return False
        def execute(self, query, _params=()): self.last_sql = str(query)
        def close(self): pass
        def fetchone(self):
            if "partial_publication where" in self.last_sql: return (encoded(publication),) if self.parents else None
            if "partial_revision where" in self.last_sql: return (encoded(revision),) if self.parents else None
            return None
        def fetchmany(self, _size): return []

    class Connection:
        def __init__(self, parents): self.parents = parents
        def cursor(self, **_kwargs): return Cursor(self.parents)
        def rollback(self): pass

    class Publisher(FuturesPartialPublisher):
        def plan(self, **_kwargs): return plan

    assert Publisher(lambda: Connection(True)).verify_published(plan)["status"] == "verified"
    with pytest.raises(ValueError, match="parents are absent"):
        Publisher(lambda: Connection(False)).verify_published(plan)


def test_partial_queries_reject_second_precision_windows() -> None:
    reader = QuoteMuxPublicReader(client=object())
    with pytest.raises(FuturesPartialPublicationQueryError, match="minute-aligned"):
        reader.read_futures_1m_partial_page(
            "T", "2020-01-01 09:01:01", "2020-01-01 09:02:00",
            qmp_id="qmp-v1-" + "1" * 64, qmc_id="qmc-v1-" + "2" * 64, qmg_id="qmg-v1-" + "3" * 64,
        )
