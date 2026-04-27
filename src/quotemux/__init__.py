from __future__ import annotations

import os
from importlib.util import find_spec
from pathlib import Path
import sys


def _activate_markethub_packages_project() -> None:
    if find_spec("markethub_packages") is not None:
        return
    root_text = os.getenv("MARKETHUB_PACKAGES_ROOT", "")
    package_root = Path(root_text) if root_text != "" else Path(__file__).resolve().parents[3] / "MarketHub_Packages"
    if not package_root.is_dir():
        return
    package_root_text = str(package_root)
    if package_root_text not in sys.path:
        sys.path.insert(0, package_root_text)


_activate_markethub_packages_project()

from quotemux.reports import ContractReport
from quotemux.requests import IndexBar1dRequest, IndexMembersRequest, IndexQuotesRequest, NextTradingDaysRequest, PreviousTradingDaysRequest, StockBar1mRequest, StockDailyOhlcvaRepairRequest, StockDailySnapshotRequest, StockQuotesRequest, TradingCalendarRequest, YearlyTradingCalendarRequest
from quotemux.runtime import QuoteMux
from quotemux.settings import QuoteMuxSettings

__all__ = [
    "ContractReport",
    "IndexBar1dRequest",
    "IndexMembersRequest",
    "IndexQuotesRequest",
    "NextTradingDaysRequest",
    "PreviousTradingDaysRequest",
    "QuoteMux",
    "QuoteMuxSettings",
    "StockBar1mRequest",
    "StockDailyOhlcvaRepairRequest",
    "StockDailySnapshotRequest",
    "StockQuotesRequest",
    "TradingCalendarRequest",
    "YearlyTradingCalendarRequest",
]
