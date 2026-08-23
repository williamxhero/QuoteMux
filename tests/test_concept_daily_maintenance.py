from __future__ import annotations

from datetime import datetime

import pandas as pd

from platform_models import ConceptMemberItem, ConceptQuoteItem
from quotemux import concept_runtime
from quotemux import fact_ref_writes
from quotemux.infra.db import reference_reads
from quotemux.settings import QuoteMuxSettings
from quotemux.store import capture


def _quote(concept_id: str, trade_date: str) -> ConceptQuoteItem:
    return ConceptQuoteItem(
        concept_id=concept_id,
        concept_name=concept_id,
        trade_time=trade_date,
        freq="1d",
        pct_chg=1.25,
        amount=100.0,
    )


def test_derivable_concept_universe_matches_canonical_membership_and_stock_requirements(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_query(query: str, params: tuple[object, ...]) -> pd.DataFrame:
        captured["query"] = query
        captured["params"] = params
        return pd.DataFrame([{"concept_id": "C1"}])

    monkeypatch.setattr(reference_reads, "query_dataframe", fake_query)

    frame = reference_reads.load_derivable_concept_ids_frame(["C1", "C2"], "2026-08-18")

    assert frame.to_dict("records") == [{"concept_id": "C1"}]
    query = str(captured["query"])
    assert "max(membership.valid_from)" in query
    assert "stock_rows.trade_date = %s::date" in query
    assert "stock_rows.pct_chg is not null" in query
    assert "previous_rows.close is not null" in query


def test_concept_coverage_requires_exact_derivable_universe() -> None:
    missing = capture._missing_derivable_concept_ids([_quote("C1", "2026-08-18")], ("C1", "C2"))

    assert missing == ("C2",)


def test_scheduled_membership_refresh_ignores_existing_cache_coverage(monkeypatch) -> None:
    monkeypatch.setattr(capture, "_recent_trading_days", lambda window_count, now: ("2026-08-18",))
    monkeypatch.setattr(capture, "_concept_ids", lambda: ("C1", "C2"))
    monkeypatch.setattr(capture, "_single_date_missing", lambda capability_id, identity: False)
    policy = type("Policy", (), {"window_count": 1})()

    requests = capture._concept_member_requests(policy, "concepts.members", datetime(2026, 8, 23, 18, 30))

    assert [request.request_identity for request in requests] == [
        {"concept_id": "C1", "trade_date": "2026-08-18"},
        {"concept_id": "C2", "trade_date": "2026-08-18"},
    ]


def test_rebuild_concept_daily_facts_uses_derived_items_and_official_writer(monkeypatch) -> None:
    runtime = object.__new__(concept_runtime.QuoteMuxConceptRuntime)
    runtime._settings = QuoteMuxSettings(enabled_sources=("derived_core",))
    runtime._concepts = type(
        "Concepts",
        (),
        {"list_alias_groups": lambda self, trade_date: [type("Group", (), {"concept_id": "C1"})()]},
    )()
    writes: list[list[ConceptQuoteItem]] = []
    monkeypatch.setattr(concept_runtime, "_expected_trade_dates", lambda start, end, settings: ["2026-08-18", "2026-08-19"])
    monkeypatch.setattr(
        concept_runtime,
        "load_derivable_concept_ids_frame",
        lambda concept_ids, trade_date: pd.DataFrame([{"concept_id": "C1"}]),
    )
    monkeypatch.setattr(runtime, "_get_derived_snapshot_items", lambda concept_ids, trade_date: [_quote("C1", trade_date)])
    monkeypatch.setattr(concept_runtime, "get_fact_ref_writer", lambda capability_id: lambda items: writes.append(list(items)) is None or True)

    result = runtime.rebuild_daily_facts("2026-08-18", "2026-08-19")

    assert result["trade_dates"] == ["2026-08-18", "2026-08-19"]
    assert result["written_row_count"] == 2
    assert [[item.trade_time for item in batch] for batch in writes] == [["2026-08-18"], ["2026-08-19"]]


def test_membership_snapshot_write_prunes_stale_members_and_invalidates_derived_rows(monkeypatch) -> None:
    many_calls: list[tuple[str, list[tuple[object, ...]]]] = []
    sql_calls: list[tuple[str, tuple[object, ...]]] = []
    monkeypatch.setattr(fact_ref_writes, "_ensure_concept_membership_table", lambda: True)
    monkeypatch.setattr(fact_ref_writes, "_table_exists", lambda schema, table: True)
    monkeypatch.setattr(
        fact_ref_writes,
        "_filter_concept_member_params",
        lambda params: [("C1", "SHSE", "600000", "2026-07-08", None)],
    )
    monkeypatch.setattr(fact_ref_writes, "execute_many", lambda query, params: many_calls.append((query, params)) is None or True)
    monkeypatch.setattr(fact_ref_writes, "execute_sql", lambda query, params=(): sql_calls.append((query, params)) is None or True)
    item = ConceptMemberItem(concept_id="C1", code="600000", name="浦发银行", weight=None, join_date="2026-07-08")

    assert fact_ref_writes._upsert_concept_members([item]) is True

    assert any("delete from ref.concept_stock_membership" in query and "not exists" in query for query, _ in sql_calls)
    assert any("delete from fact.concept_daily_1d" in query for query, _ in many_calls)


def test_membership_filter_has_no_concept_daily_circular_dependency(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_query(query: str, params: tuple[object, ...]) -> pd.DataFrame:
        captured["query"] = query
        return pd.DataFrame([{"concept_id": "C1", "stock_market": "SHSE", "stock_code": "600000", "valid_from": "2026-07-08", "weight": None}])

    monkeypatch.setattr(fact_ref_writes, "query_dataframe", fake_query)

    fact_ref_writes._filter_concept_member_params([("C1", "SHSE", "600000", "2026-07-08", None)])

    assert "fact.concept_daily_1d" not in str(captured["query"])


def test_derived_concept_writer_preserves_metrics_it_cannot_recompute(monkeypatch) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(fact_ref_writes, "_table_exists", lambda schema, table: True)
    monkeypatch.setattr(
        fact_ref_writes,
        "_existing_columns",
        lambda schema, table: {"pre_close", "change", "pct_chg"},
    )

    def fake_many(query: str, params: list[tuple[object, ...]]) -> bool:
        captured["query"] = query
        captured["params"] = params
        return True

    monkeypatch.setattr(fact_ref_writes, "execute_many", fake_many)

    assert fact_ref_writes._upsert_concept_daily([_quote("C1", "2026-08-18")]) is True

    query = str(captured["query"])
    assert "insert into fact.concept_daily_1d as existing" in query
    assert "open = coalesce(excluded.open, existing.open)" in query
    assert "pre_close = coalesce(excluded.pre_close, existing.pre_close)" in query
    assert "pct_chg = coalesce(excluded.pct_chg, existing.pct_chg)" in query
