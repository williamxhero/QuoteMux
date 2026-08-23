from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from quotemux.public_reader import QuoteMuxPublicReader
    from quotemux.runtime import QuoteMux

__all__ = [
    "ContractReport",
    "IndexBar1dRequest",
    "IndexMembersRequest",
    "IndexQuotesRequest",
    "NextTradingDaysRequest",
    "PreviousTradingDaysRequest",
    "QuoteMux",
    "QuoteMuxPublicReader",
    "QuoteMuxSettings",
    "StockBar1mRequest",
    "StockDailyOhlcvaRepairRequest",
    "StockDailySnapshotRequest",
    "StockDailyLocalWindowRequest",
    "StockQuotesRequest",
    "EtfDailyQuotesRequest",
    "TradingCalendarRequest",
    "YearlyTradingCalendarRequest",
    "PackageInstallResult",
    "install_all_packages",
]


_LAZY_EXPORTS = {
    "ContractReport": ("quotemux.reports", "ContractReport"),
    "QuoteMux": ("quotemux.runtime", "QuoteMux"),
    "QuoteMuxPublicReader": ("quotemux.public_reader", "QuoteMuxPublicReader"),
    "QuoteMuxSettings": ("quotemux.settings", "QuoteMuxSettings"),
    "PackageInstallResult": ("quotemux.package_install", "PackageInstallResult"),
    "install_all_packages": ("quotemux.package_install", "install_all_packages"),
}
for _request_name in (
    "EtfDailyQuotesRequest",
    "IndexBar1dRequest",
    "IndexMembersRequest",
    "IndexQuotesRequest",
    "NextTradingDaysRequest",
    "PreviousTradingDaysRequest",
    "StockBar1mRequest",
    "StockDailyLocalWindowRequest",
    "StockDailyOhlcvaRepairRequest",
    "StockDailySnapshotRequest",
    "StockQuotesRequest",
    "TradingCalendarRequest",
    "YearlyTradingCalendarRequest",
):
    _LAZY_EXPORTS[_request_name] = ("quotemux.requests", _request_name)


def __getattr__(name: str):
    try:
        module_name, attribute_name = _LAZY_EXPORTS[name]
    except KeyError as exc:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from exc
    value = getattr(import_module(module_name), attribute_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted({*globals(), *_LAZY_EXPORTS})
