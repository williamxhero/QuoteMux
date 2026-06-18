from __future__ import annotations

from datetime import datetime, timedelta

from platform_models import AuctionItem, BlockTradeItem, ConnectActiveTop10Item, ConnectCapitalFlowItem, ConnectQuotaItem, DragonTigerInstitutionItem, DragonTigerItem, HotMoneyDetailItem, HotMoneyProfileItem, MarketCapitalFlowItem, TradingCalendarItem, TradingSessionItem
from quotemux.infra.common import format_date_value, parse_date_text
from quotemux.runtime_core.executor import ProviderStep, SourceInstanceExecutor, run_fallback_chain_with_report
from quotemux.common import ensure_limit
from quotemux.fact_ref_writes import get_fact_ref_writer
from quotemux.local_store import get_local_trading_calendar
from quotemux.query_engine import CapabilityQuerySpec, execute_capability_query
from quotemux.reports import ContractReport
from quotemux.requests.markets import NextTradingDaysRequest, PreviousTradingDaysRequest, TradingCalendarRequest, YearlyTradingCalendarRequest
from quotemux.source_packages.registry import get_default_source_package_registry
from quotemux.settings import QuoteMuxSettings


def _source_package_call(package_id: str, handler_name: str, *args: object) -> object:
    handler = get_default_source_package_registry().get_handler(package_id, handler_name)
    return handler(*args)


def _today_text() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _payloads_with_as_of_date(items: list[object]) -> list[dict[str, object]]:
    return [{**item.model_dump(), "as_of_date": _today_text()} for item in items if hasattr(item, "model_dump")]


def _resolve_calendar_range(start_date: str, end_date: str) -> tuple[str, str]:
    actual_end = end_date or datetime.now().strftime("%Y-%m-%d")
    actual_start = start_date or (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    return actual_start, actual_end


def _build_missing_calendar_ranges(start_date: str, end_date: str, existing_dates: set[str]) -> list[tuple[str, str]]:
    start_day = parse_date_text(start_date)
    end_day = parse_date_text(end_date)
    if start_day is None or end_day is None or start_day > end_day:
        return []
    items: list[tuple[str, str]] = []
    current_start = None
    current_day = start_day
    while current_day <= end_day:
        current_text = current_day.strftime("%Y-%m-%d")
        if current_text not in existing_dates:
            if current_start is None:
                current_start = current_day
        elif current_start is not None:
            items.append((current_start.strftime("%Y-%m-%d"), (current_day - timedelta(days=1)).strftime("%Y-%m-%d")))
            current_start = None
        current_day += timedelta(days=1)
    if current_start is not None:
        items.append((current_start.strftime("%Y-%m-%d"), end_day.strftime("%Y-%m-%d")))
    return items


def _build_market_flow_requests(items: list[MarketCapitalFlowItem], trade_date: str, start_date: str, end_date: str) -> list[tuple[object, ...]]:
    actual_trade_date = trade_date or (start_date if start_date != "" and start_date == end_date else "")
    if actual_trade_date == "":
        return [()] if items == [] else []
    complete = any(item.trade_date == actual_trade_date and item.net_inflow is not None and item.main_inflow is not None and item.main_outflow is not None for item in items)
    return [] if complete else [()]


def _build_connect_flow_requests(items: list[ConnectCapitalFlowItem], trade_date: str, start_date: str, end_date: str) -> list[tuple[object, ...]]:
    actual_trade_date = trade_date or (start_date if start_date != "" and start_date == end_date else "")
    if actual_trade_date == "":
        return [()] if items == [] else []
    complete = any(item.trade_date == actual_trade_date and item.market == "northbound" and item.net_amount is not None and item.buy_amount is not None and item.sell_amount is not None for item in items)
    return [] if complete else [()]


def _build_named_event_requests(items: list[object]) -> list[tuple[object, ...]]:
    if items == []:
        return [()]
    if any(getattr(item, "name", "") == "" for item in items):
        return [()]
    return []


def _dedupe_market_flow_items(items: list[MarketCapitalFlowItem]) -> list[MarketCapitalFlowItem]:
    keyed_items = {(format_date_value(item.trade_date), item.market): item.model_copy(update={"trade_date": format_date_value(item.trade_date)}) for item in items}
    return [keyed_items[key] for key in sorted(keyed_items)]


def _sum_optional_values(first: float | None, second: float | None) -> float | None:
    if first is None or second is None:
        return None
    return first + second


def _dedupe_connect_flow_items(items: list[ConnectCapitalFlowItem]) -> list[ConnectCapitalFlowItem]:
    keyed_items = {(format_date_value(item.trade_date), item.market): item.model_copy(update={"trade_date": format_date_value(item.trade_date)}) for item in items}
    return [keyed_items[key] for key in sorted(keyed_items)]


def _connect_flow_source_order(settings: QuoteMuxSettings) -> tuple[str, ...]:
    if settings.enabled_sources != ():
        return tuple(source_name for source_name in ("tushare", "akshare") if source_name in settings.enabled_sources)
    return ("tushare", "akshare")


class QuoteMuxMarkets:
    def __init__(self, settings: QuoteMuxSettings) -> None:
        self._settings = settings

    def _store_list(
        self,
        capability_id: str,
        store_identity: dict[str, object],
        model_type: type[object],
        unique_fields: tuple[str, ...],
        sort_fields: tuple[str, ...],
        fetcher,
        payload_builder=None,
    ) -> list[object]:
        items, _ = execute_capability_query(
            CapabilityQuerySpec(
                capability_id=capability_id,
                store_identity=store_identity,
                model_type=model_type,
                key_fields=unique_fields,
                sort_fields=sort_fields,
                request_builder=lambda current_items: [()] if current_items == [] else [],
                provider_steps=(ProviderStep(name="provider", fetcher=lambda: list(fetcher())),),
                source_order=("provider",),
                payload_builder=payload_builder,
            )
        )
        return list(items)

    def _source_list(self, capability_id: str, handlers: dict[str, object], source_order: tuple[str, ...], key_fields: tuple[str, ...]) -> list[object]:
        items, _ = run_fallback_chain_with_report(
            capability_id,
            [],
            key_fields,
            lambda current_items: [()] if current_items == [] else [],
            SourceInstanceExecutor(self._settings).build_steps(capability_id, handlers, source_order),
            self._settings.get_contract_source_order(capability_id, source_order),
        )
        return items

    def get_main_capital_flow(self, trade_date: str, start_date: str, end_date: str) -> list[MarketCapitalFlowItem]:
        store_identity = {"trade_date": trade_date, "start_date": start_date, "end_date": end_date}
        handlers = {
            "get_market_capital_flow": lambda instance: lambda: _source_package_call(instance.package_id, "get_market_capital_flow", trade_date, start_date, end_date),
        }
        items, _ = execute_capability_query(
            CapabilityQuerySpec(
                capability_id="markets.indicators.main_capital_flow",
                store_identity=store_identity,
                model_type=MarketCapitalFlowItem,
                key_fields=("market", "trade_date"),
                sort_fields=("trade_date", "market"),
                request_builder=lambda current_items: _build_market_flow_requests(current_items, trade_date, start_date, end_date),
                provider_steps=lambda: SourceInstanceExecutor(self._settings).build_steps("markets.indicators.main_capital_flow", handlers, ("tushare", "akshare")),
                source_order=self._settings.get_contract_source_order("markets.indicators.main_capital_flow", ("tushare", "akshare")),
            )
        )
        return _dedupe_market_flow_items(items)

    def get_trading_calendar(self, request: TradingCalendarRequest) -> list[TradingCalendarItem]:
        items, _ = self.get_trading_calendar_with_report(request)
        return items

    def get_trading_calendar_with_report(self, request: TradingCalendarRequest) -> tuple[list[TradingCalendarItem], ContractReport]:
        actual_start, actual_end = _resolve_calendar_range(request.start_date, request.end_date)
        store_identity = {
            "exchange": request.exchange,
            "start_date": actual_start,
            "end_date": actual_end,
            "is_open": None,
        }
        handlers = {
            "get_trading_calendar": lambda instance: lambda missing_start, missing_end: _source_package_call(instance.package_id, "get_trading_calendar", request.exchange, missing_start, missing_end, None),
        }
        merged_items, report = execute_capability_query(
            CapabilityQuerySpec(
                capability_id="markets.calendar.trading",
                store_identity=store_identity,
                model_type=TradingCalendarItem,
                key_fields=("exchange", "trade_date"),
                sort_fields=("trade_date",),
                request_builder=lambda items: [(actual_start, actual_end)] if items == [] else _build_missing_calendar_ranges(actual_start, actual_end, {item.trade_date for item in items}),
                provider_steps=lambda: SourceInstanceExecutor(self._settings).build_steps("markets.calendar.trading", handlers, ("tushare", "akshare")),
                source_order=self._settings.get_contract_source_order("markets.calendar.trading", ("tushare", "akshare")),
                base_items=get_local_trading_calendar(request.exchange, actual_start, actual_end, None),
                base_source_name="ref.trade_calendar",
                fact_ref_writer=get_fact_ref_writer("markets.calendar.trading"),
            )
        )
        if request.is_open is not None:
            merged_items = [item for item in merged_items if item.is_open == request.is_open]
        return sorted(merged_items, key=lambda item: item.trade_date), report

    def get_previous_trading_days(self, request: PreviousTradingDaysRequest) -> list[TradingCalendarItem]:
        handlers = {
            "get_previous_trading_days": lambda instance: lambda: _source_package_call(instance.package_id, "get_previous_trading_days", request.exchange, request.trade_date, request.n),
        }
        derived_settings = QuoteMuxSettings(enabled_sources=("derived_core",))
        items, _ = run_fallback_chain_with_report(
            "markets.calendar.trading.previous",
            [],
            ("exchange", "trade_date"),
            lambda current_items: [()] if current_items == [] else [],
            SourceInstanceExecutor(derived_settings).build_steps("markets.calendar.trading.previous", handlers, ("derived_core",)),
            ("derived_core",),
        )
        return items

    def get_next_trading_days(self, request: NextTradingDaysRequest) -> list[TradingCalendarItem]:
        handlers = {
            "get_next_trading_days": lambda instance: lambda: _source_package_call(instance.package_id, "get_next_trading_days", request.exchange, request.trade_date, request.n),
        }
        derived_settings = QuoteMuxSettings(enabled_sources=("derived_core",))
        items, _ = run_fallback_chain_with_report(
            "markets.calendar.trading.next",
            [],
            ("exchange", "trade_date"),
            lambda current_items: [()] if current_items == [] else [],
            SourceInstanceExecutor(derived_settings).build_steps("markets.calendar.trading.next", handlers, ("derived_core",)),
            ("derived_core",),
        )
        return items

    def get_yearly_trading_calendar(self, request: YearlyTradingCalendarRequest) -> list[TradingCalendarItem]:
        handlers = {
            "get_yearly_trading_calendar": lambda instance: lambda: _source_package_call(instance.package_id, "get_yearly_trading_calendar", request.exchange, request.start_year, request.end_year),
        }
        derived_settings = QuoteMuxSettings(enabled_sources=("derived_core",))
        items, _ = run_fallback_chain_with_report(
            "markets.calendar.trading.yearly",
            [],
            ("exchange", "trade_date"),
            lambda current_items: [()] if current_items == [] else [],
            SourceInstanceExecutor(derived_settings).build_steps("markets.calendar.trading.yearly", handlers, ("derived_core",)),
            ("derived_core",),
        )
        return items

    def get_connect_capital_flow(self, trade_date: str, start_date: str, end_date: str) -> list[ConnectCapitalFlowItem]:
        store_identity = {"trade_date": trade_date, "start_date": start_date, "end_date": end_date}
        handlers = {
            "get_connect_capital_flow": lambda instance: lambda: _source_package_call(instance.package_id, "get_connect_capital_flow", trade_date, start_date, end_date),
        }
        package_order = _connect_flow_source_order(self._settings)
        source_order = self._settings.get_contract_source_order("markets.connect.capital_flow", package_order)
        items, _ = execute_capability_query(
            CapabilityQuerySpec(
                capability_id="markets.connect.capital_flow",
                store_identity=store_identity,
                model_type=ConnectCapitalFlowItem,
                key_fields=("market", "trade_date"),
                sort_fields=("trade_date", "market"),
                request_builder=lambda current_items: _build_connect_flow_requests(current_items, trade_date, start_date, end_date),
                provider_steps=lambda: SourceInstanceExecutor(self._settings).build_steps("markets.connect.capital_flow", handlers, package_order),
                source_order=source_order,
            )
        )
        return _dedupe_connect_flow_items(items)

    def get_connect_quotas(self, trade_date: str, start_date: str, end_date: str, market_type: str) -> list[ConnectQuotaItem]:
        store_identity = {"trade_date": trade_date, "start_date": start_date, "end_date": end_date, "market_type": market_type}
        handlers = {
            "get_connect_quotas": lambda instance: lambda: _source_package_call(instance.package_id, "get_connect_quotas", trade_date, start_date, end_date, market_type),
        }
        return self._store_list(
            "markets.connect.quotas",
            store_identity,
            ConnectQuotaItem,
            ("market", "trade_date"),
            ("trade_date", "market"),
            lambda: self._source_list("markets.connect.quotas", handlers, ("tushare",), ("market", "trade_date")),
        )

    def get_connect_active_top10(self, trade_date: str, start_date: str, end_date: str, market_type: str, limit: int) -> list[ConnectActiveTop10Item]:
        store_identity = {"trade_date": trade_date, "start_date": start_date, "end_date": end_date, "market_type": market_type, "limit": limit}
        handlers = {
            "get_connect_active_top10": lambda instance: lambda: _source_package_call(instance.package_id, "get_connect_active_top10", trade_date, start_date, end_date, market_type, ensure_limit(limit)),
        }
        items = self._store_list(
            "markets.connect.active_top10",
            store_identity,
            ConnectActiveTop10Item,
            ("market", "trade_date", "code", "rank"),
            ("trade_date", "market", "rank", "code"),
            lambda: self._source_list("markets.connect.active_top10", handlers, ("tushare",), ("market", "trade_date", "code", "rank")),
        )
        return items[: ensure_limit(limit)]

    def get_block_trades(self, trade_date: str, start_date: str, end_date: str, code: str, limit: int) -> list[BlockTradeItem]:
        store_identity = {"trade_date": trade_date, "start_date": start_date, "end_date": end_date, "code": code, "limit": limit}
        handlers = {
            "get_block_trades": lambda instance: lambda: _source_package_call(instance.package_id, "get_block_trades", trade_date, start_date, end_date, code, ensure_limit(limit)),
        }
        items, _ = execute_capability_query(
            CapabilityQuerySpec(
                capability_id="markets.events.block_trades",
                store_identity=store_identity,
                model_type=BlockTradeItem,
                key_fields=("trade_date", "code", "buyer", "seller"),
                sort_fields=("trade_date", "code", "buyer", "seller"),
                request_builder=_build_named_event_requests,
                provider_steps=lambda: SourceInstanceExecutor(self._settings).build_steps("markets.events.block_trades", handlers, ("tushare", "akshare")),
                source_order=self._settings.get_contract_source_order("markets.events.block_trades", ("tushare", "akshare")),
            )
        )
        return list(items)[: ensure_limit(limit)]

    def get_dragon_tiger(self, trade_date: str, start_date: str, end_date: str, code: str, limit: int) -> list[DragonTigerItem]:
        store_identity = {"trade_date": trade_date, "start_date": start_date, "end_date": end_date, "code": code, "limit": limit}
        handlers = {
            "get_dragon_tiger": lambda instance: lambda: _source_package_call(instance.package_id, "get_dragon_tiger", trade_date, start_date, end_date, code, ensure_limit(limit)),
        }
        items = self._store_list(
            "markets.participants.dragon_tiger",
            store_identity,
            DragonTigerItem,
            ("trade_date", "code", "reason"),
            ("trade_date", "code", "reason"),
            lambda: self._source_list("markets.participants.dragon_tiger", handlers, ("tushare", "akshare", "efinance"), ("trade_date", "code", "reason")),
        )
        return items[: ensure_limit(limit)]

    def get_dragon_tiger_institutions(self, trade_date: str, start_date: str, end_date: str, code: str, limit: int) -> list[DragonTigerInstitutionItem]:
        store_identity = {"trade_date": trade_date, "start_date": start_date, "end_date": end_date, "code": code, "limit": limit}
        handlers = {
            "get_dragon_tiger_institutions": lambda instance: lambda: _source_package_call(instance.package_id, "get_dragon_tiger_institutions", trade_date, start_date, end_date, code, ensure_limit(limit)),
        }
        items = self._store_list(
            "markets.participants.dragon_tiger.institutions",
            store_identity,
            DragonTigerInstitutionItem,
            ("trade_date", "code", "institution_count"),
            ("trade_date", "code"),
            lambda: self._source_list("markets.participants.dragon_tiger.institutions", handlers, ("tushare", "akshare"), ("trade_date", "code", "institution_count")),
        )
        return items[: ensure_limit(limit)]

    def get_hot_money(self, name: str, tag: str, limit: int, offset: int) -> list[HotMoneyProfileItem]:
        store_identity = {"name": name, "tag": tag, "limit": limit, "offset": offset}
        handlers = {
            "get_hot_money_profiles": lambda instance: lambda: _source_package_call(instance.package_id, "get_hot_money_profiles", name),
        }
        items = self._store_list(
            "markets.participants.hot_money",
            store_identity,
            HotMoneyProfileItem,
            ("name",),
            ("name",),
            lambda: self._source_list("markets.participants.hot_money", handlers, ("tushare",), ("name",)),
            _payloads_with_as_of_date,
        )
        if tag:
            items = [item for item in items if item.tag == tag]
        return items[offset: offset + ensure_limit(limit)]

    def get_hot_money_details(self, trade_date: str, start_date: str, end_date: str, name: str, limit: int, offset: int) -> list[HotMoneyDetailItem]:
        store_identity = {"trade_date": trade_date, "start_date": start_date, "end_date": end_date, "name": name, "limit": limit, "offset": offset}
        handlers = {
            "get_hot_money_details": lambda instance: lambda: _source_package_call(instance.package_id, "get_hot_money_details", trade_date, start_date, end_date, name, ensure_limit(limit)),
        }
        items = self._store_list(
            "markets.participants.hot_money.details",
            store_identity,
            HotMoneyDetailItem,
            ("trade_date", "name", "code"),
            ("trade_date", "name", "code"),
            lambda: self._source_list("markets.participants.hot_money.details", handlers, ("tushare", "akshare"), ("trade_date", "name", "code")),
        )
        return items[offset: offset + ensure_limit(limit)]

    def get_open_auctions(self, codes: str, trade_date: str) -> list[AuctionItem]:
        store_identity = {"code": codes, "trade_date": trade_date}
        handlers = {
            "get_market_open_auctions": lambda instance: lambda: _source_package_call(instance.package_id, "get_market_open_auctions", codes, trade_date),
        }
        return self._store_list(
            "markets.trading.open_auctions",
            store_identity,
            AuctionItem,
            ("code", "trade_date", "auction_time", "session"),
            ("trade_date", "code", "auction_time"),
            lambda: self._source_list("markets.trading.open_auctions", handlers, ("tushare", "akshare"), ("code", "trade_date", "auction_time", "session")),
        )

    def get_sessions(self, codes: str) -> list[TradingSessionItem]:
        store_identity = {"codes": codes}
        handlers = {
            "get_market_sessions": lambda instance: lambda: _source_package_call(instance.package_id, "get_market_sessions", codes),
        }
        return self._store_list(
            "markets.trading.sessions",
            store_identity,
            TradingSessionItem,
            ("code", "session_name", "start_time"),
            ("code", "start_time", "session_name"),
            lambda: self._source_list("markets.trading.sessions", handlers, ("tushare",), ("code", "session_name", "start_time")),
            _payloads_with_as_of_date,
        )
