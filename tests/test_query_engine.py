from __future__ import annotations

from pydantic import BaseModel

from quotemux.query_engine import CapabilityQuerySpec, execute_capability_query
from quotemux.runtime_core.executor import FallbackReport, ProviderStep


class DemoItem(BaseModel):
    code: str
    trade_time: str


class StoreRead:
    status = "miss"
    hit = False


def _spec(writer):
    return CapabilityQuerySpec(
        capability_id="stocks.quotes.intraday",
        store_identity={"code": "600000"},
        model_type=DemoItem,
        key_fields=("code", "trade_time"),
        sort_fields=("code", "trade_time"),
        request_builder=lambda items: [("600000",)] if items == [] else [],
        provider_steps=(ProviderStep("tushare", lambda code: []),),
        source_order=("tushare",),
        fact_ref_writer=writer,
    )


def test_fact_ref_writer_success_counts_as_store_write(monkeypatch) -> None:
    item = DemoItem(code="600000", trade_time="2026-07-02 09:31:00")
    written: list[DemoItem] = []

    monkeypatch.setattr("quotemux.query_engine.load_store_result", lambda capability_id, identity, model_type: ([], StoreRead()))
    monkeypatch.setattr("quotemux.query_engine.run_fallback_chain_with_report", lambda *args, **kwargs: ([item], FallbackReport("stocks.quotes.intraday", "", "", ())))

    items, report = execute_capability_query(_spec(lambda rows: written.extend(rows) is None or True))

    assert items == [item]
    assert written == [item]
    assert report.store_write_count == 1


def test_fact_ref_writer_failure_raises(monkeypatch) -> None:
    item = DemoItem(code="600000", trade_time="2026-07-02 09:31:00")

    monkeypatch.setattr("quotemux.query_engine.load_store_result", lambda capability_id, identity, model_type: ([], StoreRead()))
    monkeypatch.setattr("quotemux.query_engine.run_fallback_chain_with_report", lambda *args, **kwargs: ([item], FallbackReport("stocks.quotes.intraday", "", "", ())))

    try:
        execute_capability_query(_spec(lambda rows: False))
    except RuntimeError as exc:
        assert "fact ref 写入失败" in str(exc)
    else:
        raise AssertionError("fact ref 写入失败时必须抛出异常")


def test_cache_hit_deduplicates_declared_identity(monkeypatch) -> None:
    first = DemoItem(code="600000", trade_time="2026-07-02 09:31:00")
    duplicate = first.model_copy()

    class CacheHit:
        status = "hit"
        hit = True

    monkeypatch.setattr(
        "quotemux.query_engine.load_store_result",
        lambda capability_id, identity, model_type: ([first, duplicate], CacheHit()),
    )
    spec = CapabilityQuerySpec(
        capability_id="stocks.catalog",
        store_identity={"include_delisted": True},
        model_type=DemoItem,
        key_fields=("code", "trade_time"),
        sort_fields=("code", "trade_time"),
        request_builder=lambda items: [],
        provider_steps=(),
        source_order=(),
    )

    items, report = execute_capability_query(spec)

    assert items == [first]
    assert report.store_hit_count == 1
