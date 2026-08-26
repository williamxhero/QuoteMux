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
    publication = {"qmg_id": qmg, "qmi_id": "qmi-v1-" + "1" * 64, "catalog_identity": "mhd-v1-catalog", "sources": [], "source_boundary_manifest": {"count": 0, "sha256": "0" * 64}, "lineage_limitations": "known"}
    qmp = canonical_identity("qmp", publication)
    revision = {"qmp_id": qmp, "timezone": "Asia/Shanghai", "interval_bounds": "inclusive_local_naive", "coverage_semantics": "observed_admitted_runs_only", "missing_bar_semantics": "skip", "open_interest": "null_or_unavailable", "session_grid": "not_asserted_complete", "warmup": {"residual_semantics": "skip"}}
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
