from __future__ import annotations

from dataclasses import dataclass

from platform_models import IndexMemberItem, IndexQuoteItem, StockQuoteItem, TradingCalendarItem
from quotemux.capabilities import get_capability_definition, is_independently_configurable_capability_id, is_known_capability_id, list_capability_definitions, list_capability_ids, normalize_capability_id
from quotemux.contracts.strategies import allowed_merge_strategies
from quotemux.requests import IndexMembersRequest, IndexQuotesRequest, StockDailySnapshotRequest, StockQuotesRequest, TradingCalendarRequest


@dataclass(frozen=True)
class ContractDefinition:
    name: str
    request_type: type[object]
    result_type: type[object]
    result_shape: str
    key_fields: tuple[str, ...]
    source_order: tuple[str, ...]
    degraded: bool
    degraded_policy: str
    merge_strategy: str
    allowed_merge_strategies: tuple[str, ...]
    missing_request_builder: str
    api_paths: tuple[str, ...]


_REQUEST_TYPES = {
    "indexes.members": IndexMembersRequest,
    "indexes.quotes.daily": IndexQuotesRequest,
    "markets.calendar.trading": TradingCalendarRequest,
    "markets.calendar.trading.next": TradingCalendarRequest,
    "markets.calendar.trading.previous": TradingCalendarRequest,
    "markets.calendar.trading.yearly": TradingCalendarRequest,
    "stocks.quotes.daily": StockQuotesRequest,
    "stocks.quotes.daily_snapshot": StockDailySnapshotRequest,
    "stocks.quotes.intraday": StockQuotesRequest,
}

_RESULT_TYPES = {
    "indexes.members": IndexMemberItem,
    "indexes.quotes.daily": IndexQuoteItem,
    "markets.calendar.trading": TradingCalendarItem,
    "markets.calendar.trading.next": TradingCalendarItem,
    "markets.calendar.trading.previous": TradingCalendarItem,
    "markets.calendar.trading.yearly": TradingCalendarItem,
    "stocks.quotes.daily": StockQuoteItem,
    "stocks.quotes.daily_snapshot": StockQuoteItem,
    "stocks.quotes.intraday": StockQuoteItem,
}

_MISSING_REQUEST_BUILDERS = {
    "indexes.members": "indexes.members_request",
    "indexes.quotes.daily": "indexes.quote_ranges",
    "markets.calendar.trading": "markets.calendar_ranges",
    "markets.calendar.trading.next": "markets.calendar_ranges",
    "markets.calendar.trading.previous": "markets.calendar_ranges",
    "markets.calendar.trading.yearly": "markets.calendar_ranges",
    "stocks.quotes.daily": "stocks.quote_ranges",
    "stocks.quotes.daily_snapshot": "stocks.snapshot_codes",
    "stocks.quotes.intraday": "stocks.quote_ranges",
}


def _build_definition(capability_id: str) -> ContractDefinition:
    from quotemux.contracts.policies import get_contract_policy

    definition = get_capability_definition(capability_id)
    policy = get_contract_policy(capability_id)
    return ContractDefinition(
        name=definition.capability_id,
        request_type=_REQUEST_TYPES.get(definition.capability_id, object),
        result_type=_RESULT_TYPES.get(definition.capability_id, object),
        result_shape=definition.result_shape,
        key_fields=definition.key_fields,
        source_order=policy.source_order,
        degraded=policy.mode == "degraded",
        degraded_policy=policy.mode,
        merge_strategy=policy.merge_strategy,
        allowed_merge_strategies=allowed_merge_strategies(definition.result_shape),
        missing_request_builder=_MISSING_REQUEST_BUILDERS.get(definition.capability_id, ""),
        api_paths=definition.api_paths,
    )


def get_contract_result_shape(contract_name: str) -> str:
    return get_capability_definition(contract_name).result_shape


def get_contract_allowed_merge_strategies(contract_name: str) -> tuple[str, ...]:
    return allowed_merge_strategies(get_contract_result_shape(contract_name))


def get_contract_definition(contract_name: str) -> ContractDefinition:
    normalized = normalize_capability_id(contract_name)
    if normalized not in _REQUEST_TYPES:
        raise KeyError(f"未知 capability 定义: {contract_name}")
    return _build_definition(normalized)


def list_contract_definitions() -> tuple[ContractDefinition, ...]:
    return tuple(_build_definition(capability_id) for capability_id in _REQUEST_TYPES)


def list_contract_names() -> tuple[str, ...]:
    return tuple(capability_id for capability_id in list_capability_ids() if is_independently_configurable_capability_id(capability_id))


def is_known_contract_name(contract_name: str) -> bool:
    return is_known_capability_id(contract_name)
