from __future__ import annotations

import pandas as pd

from quotemux.infra.db import availability


def test_fact_ref_availability_skips_missing_coverage_column(monkeypatch) -> None:
    executed_queries: list[str] = []

    def fake_query_dataframe(query: str, params: tuple[object, ...] = ()) -> pd.DataFrame:
        executed_queries.append(query)
        if "information_schema.tables" in query:
            return pd.DataFrame.from_records([{"full_name": "ref.concept"}])
        if "pg_indexes" in query:
            return pd.DataFrame.from_records([{"indexname": "concept_pkey"}])
        if "information_schema.columns" in query:
            return pd.DataFrame.from_records(
                [
                    {"full_name": "ref.concept", "column_name": "concept_id"},
                    {"full_name": "ref.concept", "column_name": "name"},
                    {"full_name": "ref.concept", "column_name": "updated_at"},
                ]
            )
        if "pg_class" in query:
            return pd.DataFrame.from_records([{"row_count": 12}])
        raise AssertionError(f"不应执行覆盖列查询: {query}")

    monkeypatch.setattr(availability, "query_dataframe", fake_query_dataframe)
    monkeypatch.setattr(
        availability,
        "OBJECT_SPECS",
        (availability.FactRefObjectSpec("ref", "concept", ("concept_pkey",), "listed_date"),),
    )

    payload = availability.get_fact_ref_availability()

    assert payload["status"] == "ok"
    assert payload["objects"] == [
        {"name": "ref.concept", "exists": True, "missing_indexes": [], "row_count": 12, "min_value": "", "max_value": ""}
    ]
    assert all("listed_date" not in query for query in executed_queries)


def test_fact_ref_availability_counts_small_tables_when_estimate_is_zero(monkeypatch) -> None:
    def fake_query_dataframe(query: str, params: tuple[object, ...] = ()) -> pd.DataFrame:
        if "information_schema.tables" in query:
            return pd.DataFrame.from_records([{"full_name": "ref.index"}])
        if "pg_indexes" in query:
            return pd.DataFrame.from_records([{"indexname": "index_pkey"}])
        if "information_schema.columns" in query:
            return pd.DataFrame.from_records([{"full_name": "ref.index", "column_name": "list_date"}])
        if "pg_class" in query:
            return pd.DataFrame.from_records([{"row_count": 0}])
        if "count(*)" in query:
            return pd.DataFrame.from_records([{"row_count": 8}])
        if "order by list_date" in query:
            return pd.DataFrame()
        raise AssertionError(f"未预期查询 {query}")

    monkeypatch.setattr(availability, "query_dataframe", fake_query_dataframe)
    monkeypatch.setattr(
        availability,
        "OBJECT_SPECS",
        (availability.FactRefObjectSpec("ref", "index", ("index_pkey",), "list_date"),),
    )

    payload = availability.get_fact_ref_availability()

    assert payload["status"] == "ok"
    assert payload["objects"] == [
        {"name": "ref.index", "exists": True, "missing_indexes": [], "row_count": 8, "min_value": "", "max_value": ""}
    ]
