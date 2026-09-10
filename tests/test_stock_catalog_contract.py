from __future__ import annotations

from quotemux.models import StockBasicInfo
from quotemux.settings import QuoteMuxSettings
from quotemux.stock_reference_authority import StockReferenceReconciliationResult
from quotemux.stocks import QuoteMuxStocks


def _stock(code: str, status: str) -> StockBasicInfo:
    return StockBasicInfo(
        code=code,
        name=code,
        exchange="SHSE",
        market="main_board",
        list_status=status,
        list_date="2000-01-01",
        delist_date="2006-10-20" if status == "D" else "",
    )


def test_stock_catalog_exposes_only_standard_six_digit_codes(monkeypatch) -> None:
    items = [_stock("600018", "L"), _stock("000003", "D"), _stock("T600018", "D")]
    monkeypatch.setattr(
        "quotemux.stocks.get_local_stock_catalog",
        lambda *args: items,
    )
    monkeypatch.setattr(
        "quotemux.stocks._source_package_call",
        lambda *args: (_ for _ in ()).throw(
            AssertionError("public catalog must not call providers")
        ),
    )

    result = QuoteMuxStocks(QuoteMuxSettings()).get_catalog([], "", "", "", True, 200, 0)

    assert [item.code for item in result] == ["600018", "000003"]


def test_full_refresh_publishes_only_reconciled_local_catalog(monkeypatch) -> None:
    reconciliation = StockReferenceReconciliationResult(
        status="committed",
        run_key="stable-run",
        input_id="a" * 64,
        candidate_count=2,
        existing_count=2,
        provisional_count=0,
        inserted_count=0,
        promoted_count=2,
        renamed_count=0,
        missing_count=0,
        conflict_count=0,
        normalized_output_sha256="b" * 64,
        audit_content_sha256="c" * 64,
    )
    monkeypatch.setattr(
        "quotemux.stocks._refresh_tushare_authoritative_catalog",
        lambda settings: reconciliation,
    )
    monkeypatch.setattr(
        "quotemux.stocks.get_local_stock_catalog",
        lambda *args: [_stock("301699", "L"), _stock("920268", "L")],
    )
    monkeypatch.setattr(
        "quotemux.stocks.execute_capability_query",
        lambda spec: (_ for _ in ()).throw(AssertionError("generic catalog path must not publish")),
    )

    result = QuoteMuxStocks(QuoteMuxSettings()).get_catalog(
        [],
        "",
        "",
        "",
        True,
        10_000,
        0,
        refresh=True,
    )

    assert [item.code for item in result] == ["301699", "920268"]
    assert "identity_status" not in result[0].model_dump()
    assert "authority_input_id" not in result[0].model_dump()
