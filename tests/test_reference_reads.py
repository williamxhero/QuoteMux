from __future__ import annotations

import pandas as pd

from quotemux.infra.db import reference_reads


def test_active_stock_query_excludes_b_shares_without_full_daily_provider_contract(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_query_dataframe(query: str, params: object = None):
        captured["query"] = query
        captured["params"] = params
        return pd.DataFrame()

    monkeypatch.setattr(reference_reads, "query_dataframe", fake_query_dataframe)

    reference_reads.load_stock_active_codes_frame("2026-08-17")

    assert "market = 'SHSE' and left(code, 3) = '900'" in str(captured["query"])
    assert "market = 'SZSE' and left(code, 3) = '200'" in str(captured["query"])
    assert captured["params"] == ("2026-08-17", "2026-08-17")


def test_stock_catalog_query_returns_one_current_identity_per_code(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_query_dataframe(query: str, params: tuple[object, ...]) -> pd.DataFrame:
        captured["query"] = " ".join(query.split())
        captured["params"] = params
        return pd.DataFrame()

    monkeypatch.setattr(reference_reads, "query_dataframe", fake_query_dataframe)

    reference_reads.load_stock_catalog_frame([], "", "", "")

    assert "select distinct on (code)" in captured["query"]
    assert (
        "order by code, (delisted_date is null) desc, listed_date desc, market"
        in captured["query"]
    )
    assert captured["params"] == ()
