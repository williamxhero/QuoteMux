from __future__ import annotations

from quotemux.models import StockBasicInfo
from quotemux.reports import ContractReport
from quotemux.settings import QuoteMuxSettings
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
        "quotemux.stocks.execute_capability_query",
        lambda spec: (items, ContractReport(contract_name="stocks.catalog")),
    )

    result = QuoteMuxStocks(QuoteMuxSettings()).get_catalog([], "", "", "", True, 200, 0)

    assert [item.code for item in result] == ["600018", "000003"]
