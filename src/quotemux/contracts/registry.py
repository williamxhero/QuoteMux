from __future__ import annotations

from dataclasses import dataclass

from platform_models import IndexMemberItem, IndexQuoteItem, StockQuoteItem, TradingCalendarItem
from quotemux.contracts.policies import get_contract_policy
from quotemux.requests import IndexBar1dRequest, IndexMembersRequest, IndexQuotesRequest, StockBar1mRequest, StockDailyOhlcvaRepairRequest, StockDailySnapshotRequest, StockQuotesRequest, TradingCalendarRequest


MANIFEST_CONTRACT_NAMES = (
    "boards.money_flow",
    "boards.quotes",
    "boards.reference",
    "indexes.members",
    "indexes.quotes",
    "indexes.quotes.daily",
    "indexes.reference",
    "markets.events.news",
    "markets.topics",
    "markets.trading.sessions",
    "markets.trading_calendar",
    "reference",
    "stocks.corporate_actions",
    "stocks.daily_snapshot",
    "stocks.finance",
    "stocks.finance.statements",
    "stocks.indicators",
    "stocks.indicators.chips",
    "stocks.indicators.daily_basic",
    "stocks.indicators.daily_market_value",
    "stocks.indicators.daily_valuation",
    "stocks.money_flow",
    "stocks.ownership",
    "stocks.profile",
    "stocks.quotes",
    "stocks.quotes.daily",
    "stocks.quotes.intraday",
    "stocks.research",
    "updater",
    "updater.index_bar_1d",
    "updater.stock_bar_1m",
    "updater.stock_daily_1d.ohlcva",
)


@dataclass(frozen=True)
class ContractDefinition:
    name: str
    request_type: type[object]
    result_type: type[object]
    key_fields: tuple[str, ...]
    source_order: tuple[str, ...]
    degraded: bool
    degraded_policy: str
    missing_request_builder: str


def _build_contract_definition(
    name: str,
    request_type: type[object],
    result_type: type[object],
    key_fields: tuple[str, ...],
    degraded: bool,
    missing_request_builder: str,
) -> ContractDefinition:
    policy = get_contract_policy(name)
    return ContractDefinition(
        name=name,
        request_type=request_type,
        result_type=result_type,
        key_fields=key_fields,
        source_order=policy.source_order,
        degraded=degraded,
        degraded_policy=policy.mode,
        missing_request_builder=missing_request_builder,
    )


CONTRACT_DEFINITIONS = {
    "stocks.quotes.intraday": _build_contract_definition("stocks.quotes.intraday", StockQuotesRequest, StockQuoteItem, ("code", "trade_time", "freq"), False, "stocks.quote_ranges"),
    "stocks.quotes.daily": _build_contract_definition("stocks.quotes.daily", StockQuotesRequest, StockQuoteItem, ("code", "trade_time", "freq"), False, "stocks.quote_ranges"),
    "stocks.daily_snapshot": _build_contract_definition("stocks.daily_snapshot", StockDailySnapshotRequest, StockQuoteItem, ("code", "trade_time", "freq"), False, "stocks.snapshot_codes"),
    "indexes.quotes.daily": _build_contract_definition("indexes.quotes.daily", IndexQuotesRequest, IndexQuoteItem, ("index_code", "trade_time", "freq"), False, "indexes.quote_ranges"),
    "indexes.members": _build_contract_definition("indexes.members", IndexMembersRequest, IndexMemberItem, ("index_code", "code"), True, "indexes.members_request"),
    "markets.trading_calendar": _build_contract_definition("markets.trading_calendar", TradingCalendarRequest, TradingCalendarItem, ("exchange", "trade_date"), True, "markets.calendar_ranges"),
    "updater.stock_bar_1m": _build_contract_definition("updater.stock_bar_1m", StockBar1mRequest, StockQuoteItem, ("code", "trade_time", "freq"), False, "updater.stock_bar_ranges"),
    "updater.index_bar_1d": _build_contract_definition("updater.index_bar_1d", IndexBar1dRequest, IndexQuoteItem, ("index_code", "trade_time", "freq"), False, "updater.index_bar_ranges"),
    "updater.stock_daily_1d.ohlcva": _build_contract_definition("updater.stock_daily_1d.ohlcva", StockDailyOhlcvaRepairRequest, StockQuoteItem, ("code", "trade_time", "freq"), False, "updater.daily_ohlcva_codes"),
}

CONTRACT_NAMES = tuple(sorted({*MANIFEST_CONTRACT_NAMES, *CONTRACT_DEFINITIONS.keys()}))


def get_contract_definition(contract_name: str) -> ContractDefinition:
    definition = CONTRACT_DEFINITIONS.get(contract_name)
    if definition is None:
        raise KeyError(f"未知 contract: {contract_name}")
    return definition


def list_contract_definitions() -> tuple[ContractDefinition, ...]:
    return tuple(CONTRACT_DEFINITIONS.values())


def list_contract_names() -> tuple[str, ...]:
    return CONTRACT_NAMES


def is_known_contract_name(contract_name: str) -> bool:
    return contract_name in CONTRACT_NAMES
