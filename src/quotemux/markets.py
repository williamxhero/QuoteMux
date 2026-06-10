from __future__ import annotations

from datetime import date, datetime, timedelta

from platform_models import AuctionItem, BlockTradeItem, ConnectActiveTop10Item, ConnectCapitalFlowItem, ConnectQuotaItem, DragonTigerInstitutionItem, DragonTigerItem, HotMoneyDetailItem, HotMoneyProfileItem, MarketCapitalFlowItem, TradingCalendarItem, TradingSessionItem
from quotemux.infra.common import parse_date_text
from quotemux.runtime_core.executor import SourceInstanceExecutor, run_fallback_chain_with_report
from quotemux.common import ensure_limit
from quotemux.reports import ContractReport
from quotemux.requests.markets import NextTradingDaysRequest, PreviousTradingDaysRequest, TradingCalendarRequest, YearlyTradingCalendarRequest
from quotemux.source_packages.registry import get_default_source_package_registry
from quotemux.settings import QuoteMuxSettings
from quotemux.store import load_store_result, store_result


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
        store_items, store_read = load_store_result(capability_id, store_identity, model_type)
        if store_read.hit:
            return list(store_items)
        fetched_items = list(fetcher())
        merged_items = store_items if store_read.partial_hit else []
        if merged_items != []:
            from quotemux.common import merge_model_lists

            merged_items = merge_model_lists(store_items, fetched_items, unique_fields)
        else:
            merged_items = fetched_items
        sorted_items = sorted(merged_items, key=lambda item: tuple(getattr(item, field) for field in sort_fields)) if sort_fields else merged_items
        payload_items = payload_builder(sorted_items) if payload_builder is not None else sorted_items
        store_result(capability_id, store_identity, payload_items, ContractReport(contract_name=capability_id))
        return sorted_items

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
        return self._store_list(
            "markets.indicators.main_capital_flow",
            store_identity,
            MarketCapitalFlowItem,
            ("market", "trade_date"),
            ("trade_date", "market"),
            lambda: self._source_list("markets.indicators.main_capital_flow", handlers, ("tushare", "akshare"), ("market", "trade_date")),
        )

    def get_trading_calendar(self, request: TradingCalendarRequest) -> list[TradingCalendarItem]:
        items, _ = self.get_trading_calendar_with_report(request)
        return items

    def get_trading_calendar_with_report(self, request: TradingCalendarRequest) -> tuple[list[TradingCalendarItem], ContractReport]:
        actual_start, actual_end = _resolve_calendar_range(request.start_date, request.end_date)
        store_identity = {
            "exchange": request.exchange,
            "start_date": actual_start,
            "end_date": actual_end,
            "is_open": request.is_open,
        }
        store_items, store_read = load_store_result("markets.calendar.trading", store_identity, TradingCalendarItem)
        if store_read.hit:
            if request.is_open is not None:
                store_items = [item for item in store_items if item.is_open == request.is_open]
            from quotemux.config_runtime.runtime import get_config_runtime

            active_snapshot = get_config_runtime().get_active_snapshot()
            return sorted(store_items, key=lambda item: item.trade_date), ContractReport(
                contract_name="markets.calendar.trading",
                profile_id=active_snapshot.profile_id,
                profile_version=active_snapshot.version,
            ).with_store_stats(hit=True)
        handlers = {
            "get_trading_calendar": lambda instance: lambda missing_start, missing_end: _source_package_call(instance.package_id, "get_trading_calendar", request.exchange, missing_start, missing_end, None),
        }
        merged_items, fallback_report = run_fallback_chain_with_report(
            "markets.calendar.trading",
            store_items if store_read.partial_hit else [],
            ("exchange", "trade_date"),
            lambda items: [(actual_start, actual_end)] if items == [] else _build_missing_calendar_ranges(actual_start, actual_end, {item.trade_date for item in items}),
            SourceInstanceExecutor(self._settings).build_steps("markets.calendar.trading", handlers, ("tushare", "akshare")),
            self._settings.get_contract_source_order("markets.calendar.trading", ("tushare", "akshare")),
        )
        if request.is_open is not None:
            merged_items = [item for item in merged_items if item.is_open == request.is_open]
        sorted_items = sorted(merged_items, key=lambda item: item.trade_date)
        report = ContractReport.from_fallback_report("markets.calendar.trading", fallback_report, degraded=True)
        store_write = store_result("markets.calendar.trading", store_identity, sorted_items, report, report.quarantine_count)
        report = report.with_store_stats(partial_hit=store_read.partial_hit, miss=store_read.status in {"miss", "skip"}, stale=store_read.status == "stale", write=store_write.status == "write")
        return sorted_items, report

    def get_previous_trading_days(self, request: PreviousTradingDaysRequest) -> list[TradingCalendarItem]:
        items = self.get_trading_calendar(TradingCalendarRequest(exchange=request.exchange, start_date="", end_date=request.trade_date, is_open=True))
        return [item for item in items if item.trade_date < request.trade_date][-request.n:]

    def get_next_trading_days(self, request: NextTradingDaysRequest) -> list[TradingCalendarItem]:
        trade_day = parse_date_text(request.trade_date)
        end_date = ""
        if trade_day is not None:
            try:
                next_year_day = trade_day.replace(year=trade_day.year + 1)
            except ValueError:
                next_year_day = date(trade_day.year + 1, 2, 28)
            end_date = next_year_day.strftime("%Y-%m-%d")
        items = self.get_trading_calendar(TradingCalendarRequest(exchange=request.exchange, start_date=request.trade_date, end_date=end_date, is_open=True))
        return [item for item in items if item.trade_date > request.trade_date][: request.n]

    def get_yearly_trading_calendar(self, request: YearlyTradingCalendarRequest) -> list[TradingCalendarItem]:
        return self.get_trading_calendar(TradingCalendarRequest(exchange=request.exchange, start_date=f"{request.start_year}-01-01", end_date=f"{request.end_year}-12-31", is_open=None))

    def get_connect_capital_flow(self, trade_date: str, start_date: str, end_date: str) -> list[ConnectCapitalFlowItem]:
        store_identity = {"trade_date": trade_date, "start_date": start_date, "end_date": end_date}
        handlers = {
            "get_connect_capital_flow": lambda instance: lambda: _source_package_call(instance.package_id, "get_connect_capital_flow", trade_date, start_date, end_date),
        }
        return self._store_list(
            "markets.connect.capital_flow",
            store_identity,
            ConnectCapitalFlowItem,
            ("market", "trade_date"),
            ("trade_date", "market"),
            lambda: self._source_list("markets.connect.capital_flow", handlers, ("tushare", "akshare"), ("market", "trade_date")),
        )

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
        items = self._store_list(
            "markets.events.block_trades",
            store_identity,
            BlockTradeItem,
            ("trade_date", "code", "buyer", "seller"),
            ("trade_date", "code", "buyer", "seller"),
            lambda: self._source_list("markets.events.block_trades", handlers, ("tushare", "akshare"), ("trade_date", "code", "buyer", "seller")),
        )
        return items[: ensure_limit(limit)]

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
            lambda: self._source_list("markets.participants.hot_money.details", handlers, ("tushare",), ("trade_date", "name", "code")),
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
            lambda: self._source_list("markets.trading.open_auctions", handlers, ("tushare",), ("code", "trade_date", "auction_time", "session")),
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
            ("market", "session"),
            ("market", "session"),
            lambda: self._source_list("markets.trading.sessions", handlers, ("tushare",), ("market", "session")),
            _payloads_with_as_of_date,
        )




