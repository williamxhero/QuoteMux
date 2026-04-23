from __future__ import annotations

from datetime import date, datetime, timedelta

from platform_models import AuctionItem, BlockTradeItem, ConnectActiveTop10Item, ConnectCapitalFlowItem, ConnectQuotaItem, DragonTigerInstitutionItem, DragonTigerItem, HotMoneyDetailItem, HotMoneyProfileItem, MarketCapitalFlowItem, TradingCalendarItem, TradingSessionItem
from quotemux.infra.common import parse_date_text
from quotemux.runtime_core.executor import SourceInstanceExecutor, run_fallback_chain_with_report
from quotemux.common import ensure_limit
from quotemux.reports import ContractReport
from quotemux.requests.markets import NextTradingDaysRequest, PreviousTradingDaysRequest, TradingCalendarRequest, YearlyTradingCalendarRequest
from quotemux.runtime_core.registry import SourceProxy
from quotemux.settings import QuoteMuxSettings


_akshare_provider = SourceProxy("akshare")
_datalake_reference = SourceProxy("datalake_reference")
_local_topics = SourceProxy("local_topics")
_tushare_provider = SourceProxy("tushare")


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

    def get_main_capital_flow(self, trade_date: str, start_date: str, end_date: str) -> list[MarketCapitalFlowItem]:
        if not self._settings.is_source_enabled("tushare"):
            return []
        return _tushare_provider.get_market_capital_flow(trade_date, start_date, end_date)

    def get_trading_calendar(self, request: TradingCalendarRequest) -> list[TradingCalendarItem]:
        items, _ = self.get_trading_calendar_with_report(request)
        return items

    def get_trading_calendar_with_report(self, request: TradingCalendarRequest) -> tuple[list[TradingCalendarItem], ContractReport]:
        actual_start, actual_end = _resolve_calendar_range(request.start_date, request.end_date)
        handlers = {
            "datalake_reference": ("get_trading_calendar", lambda instance: lambda missing_start, missing_end: _datalake_reference.get_trading_calendar(request.exchange, missing_start, missing_end, None)),
            "tushare": ("get_trading_calendar", lambda instance: lambda missing_start, missing_end: _tushare_provider.get_trading_calendar(request.exchange, missing_start, missing_end, None)),
            "akshare": ("get_trading_calendar", lambda instance: lambda missing_start, missing_end: _akshare_provider.get_trading_calendar(request.exchange, missing_start, missing_end, None)),
        }
        merged_items, fallback_report = run_fallback_chain_with_report(
            "markets.trading_calendar",
            [],
            ("exchange", "trade_date"),
            lambda items: [(actual_start, actual_end)] if items == [] else _build_missing_calendar_ranges(actual_start, actual_end, {item.trade_date for item in items}),
            SourceInstanceExecutor(self._settings).build_steps("markets.trading_calendar", handlers, ("datalake_reference", "tushare", "akshare")),
            self._settings.get_contract_source_order("markets.trading_calendar", ("datalake_reference", "tushare", "akshare")),
        )
        if request.is_open is not None:
            merged_items = [item for item in merged_items if item.is_open == request.is_open]
        sorted_items = sorted(merged_items, key=lambda item: item.trade_date)
        report = ContractReport.from_fallback_report("markets.trading_calendar", fallback_report, degraded=True)
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
        if not self._settings.is_source_enabled("tushare"):
            return []
        return _tushare_provider.get_connect_capital_flow(trade_date, start_date, end_date)

    def get_connect_quotas(self, trade_date: str, start_date: str, end_date: str, market_type: str) -> list[ConnectQuotaItem]:
        if not self._settings.is_source_enabled("tushare"):
            return []
        return _tushare_provider.get_connect_quotas(trade_date, start_date, end_date, market_type)

    def get_connect_active_top10(self, trade_date: str, start_date: str, end_date: str, market_type: str, limit: int) -> list[ConnectActiveTop10Item]:
        if not self._settings.is_source_enabled("tushare"):
            return []
        return _tushare_provider.get_connect_active_top10(trade_date, start_date, end_date, market_type, ensure_limit(limit))

    def get_block_trades(self, trade_date: str, start_date: str, end_date: str, code: str, limit: int) -> list[BlockTradeItem]:
        if not self._settings.is_source_enabled("tushare"):
            return []
        return _tushare_provider.get_block_trades(trade_date, start_date, end_date, code, ensure_limit(limit))

    def get_dragon_tiger(self, trade_date: str, start_date: str, end_date: str, code: str, limit: int) -> list[DragonTigerItem]:
        if not self._settings.is_source_enabled("tushare"):
            return []
        return _tushare_provider.get_dragon_tiger(trade_date, start_date, end_date, code, ensure_limit(limit))

    def get_dragon_tiger_institutions(self, trade_date: str, start_date: str, end_date: str, code: str, limit: int) -> list[DragonTigerInstitutionItem]:
        if not self._settings.is_source_enabled("tushare"):
            return []
        return _tushare_provider.get_dragon_tiger_institutions(trade_date, start_date, end_date, code, ensure_limit(limit))

    def get_hot_money(self, name: str, tag: str, limit: int, offset: int) -> list[HotMoneyProfileItem]:
        if not self._settings.is_source_enabled("tushare"):
            return []
        items = _tushare_provider.get_hot_money_profiles(name)
        if tag:
            items = [item for item in items if item.tag == tag]
        return items[offset: offset + ensure_limit(limit)]

    def get_hot_money_details(self, trade_date: str, start_date: str, end_date: str, name: str, limit: int, offset: int) -> list[HotMoneyDetailItem]:
        if not self._settings.is_source_enabled("tushare"):
            return []
        items = _tushare_provider.get_hot_money_details(trade_date, start_date, end_date, name, ensure_limit(limit))
        return items[offset: offset + ensure_limit(limit)]

    def get_open_auctions(self, codes: str, trade_date: str) -> list[AuctionItem]:
        if not self._settings.is_source_enabled("tushare"):
            return []
        return _tushare_provider.get_market_open_auctions(codes, trade_date)

    def get_sessions(self, codes: str) -> list[TradingSessionItem]:
        if not self._settings.is_source_enabled("local_topics"):
            return []
        return _local_topics.get_market_sessions(codes)




