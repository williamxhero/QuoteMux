from __future__ import annotations

from datetime import timedelta

from platform_models import BoardCatalogItem, BoardCategoryItem, BoardMemberHistoryItem, BoardMemberItem, BoardMoneyFlowItem, BoardQuoteItem
from quotemux.infra.common import format_date_value, parse_date_text
from quotemux.common import build_missing_expected_date_ranges, ensure_limit, has_enough_stock_quote_rows, trim_items_per_key
from quotemux.fact_ref_writes import get_fact_ref_writer
from quotemux.local_store import get_local_board_catalog, get_local_board_member_history, get_local_board_members, get_local_board_profile, get_local_board_quotes
from quotemux.query_engine import CapabilityQuerySpec, execute_capability_query
from quotemux.runtime_core.executor import SourceInstanceExecutor, run_fallback_chain_with_report
from quotemux.source_packages.registry import get_default_source_package_registry
from quotemux.settings import QuoteMuxSettings
from quotemux.reports import ContractReport
from quotemux.store import load_store_result, store_result


def _source_package_call(package_id: str, handler_name: str, *args: object) -> object:
    handler = get_default_source_package_registry().get_handler(package_id, handler_name)
    return handler(*args)


def _today_text() -> str:
    from datetime import datetime

    return datetime.now().strftime("%Y-%m-%d")


def _payloads_with_as_of_date(items: list[object]) -> list[dict[str, object]]:
    return [{**item.model_dump(), "as_of_date": _today_text()} for item in items if hasattr(item, "model_dump")]


def _board_member_store_payloads(items: list[BoardMemberItem], trade_date: str) -> list[dict[str, object]]:
    actual_date = format_date_value(trade_date) or _today_text()
    payloads = []
    for item in items:
        payload = item.model_dump()
        if payload.get("join_date", "") == "":
            payload["join_date"] = actual_date
        payloads.append(payload)
    return payloads


def _build_missing_date_ranges(start_date: str, end_date: str, existing_dates: set[str]) -> list[tuple[str, str]]:
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


def _build_missing_expected_date_ranges(expected_dates: list[str], existing_dates: set[str]) -> list[tuple[str, str]]:
    if expected_dates == []:
        return []
    items: list[tuple[str, str]] = []
    current_start = ""
    current_end = ""
    for expected_date in expected_dates:
        if expected_date in existing_dates:
            if current_start != "":
                items.append((current_start, current_end))
                current_start = ""
                current_end = ""
            continue
        if current_start == "":
            current_start = expected_date
        current_end = expected_date
    if current_start != "":
        items.append((current_start, current_end))
    return items


def _expected_trade_dates(start_date: str, end_date: str, settings: QuoteMuxSettings) -> list[str]:
    from quotemux.markets import QuoteMuxMarkets
    from quotemux.requests.markets import TradingCalendarRequest

    items = QuoteMuxMarkets(settings).get_trading_calendar(
        TradingCalendarRequest(exchange="SSE", start_date=start_date, end_date=end_date, is_open=True)
    )
    return [item.trade_date for item in items]


def _build_missing_quote_requests(board_codes: list[str], items: list[BoardQuoteItem], freq: str, trade_date: str, start_date: str, end_date: str, count: int | None, settings: QuoteMuxSettings) -> list[tuple[list[str], str, str]]:
    if trade_date == "" and start_date == "" and end_date == "" and count:
        if has_enough_stock_quote_rows(items, board_codes, count, "board_code"):
            return []
        missing_codes = [board_code for board_code in board_codes if sum(1 for item in items if item.board_code == board_code) < count]
        return [(missing_codes, "", "")] if missing_codes else []
    actual_trade_date = format_date_value(trade_date)
    actual_start_date = actual_trade_date or format_date_value(start_date)
    actual_end_date = actual_trade_date or format_date_value(end_date)
    if actual_start_date == "" and actual_end_date == "":
        return [(board_codes, "", "")] if items == [] else []
    if actual_start_date == "":
        actual_start_date = actual_end_date
    if actual_end_date == "":
        actual_end_date = actual_start_date
    expected_trade_dates = _expected_trade_dates(actual_start_date, actual_end_date, settings) if freq == "1d" else []
    grouped_ranges: dict[tuple[str, str], list[str]] = {}
    for board_code in board_codes:
        existing_dates = {item.trade_time for item in items if item.board_code == board_code and item.freq == freq}
        missing_ranges = build_missing_expected_date_ranges(expected_trade_dates, existing_dates)
        if missing_ranges == [] and expected_trade_dates == []:
            missing_ranges = _build_missing_date_ranges(actual_start_date, actual_end_date, existing_dates)
        for missing_start, missing_end in missing_ranges:
            grouped_ranges.setdefault((missing_start, missing_end), []).append(board_code)
    return [(range_codes, range_start, range_end) for (range_start, range_end), range_codes in grouped_ranges.items()]


class QuoteMuxBoards:
    def __init__(self, settings: QuoteMuxSettings) -> None:
        self._settings = settings

    def _build_money_flow_requests(self, items: list[BoardMoneyFlowItem], trade_date: str, start_date: str, end_date: str) -> list[tuple[str, str]]:
        actual_trade_date = format_date_value(trade_date)
        if actual_trade_date:
            return [] if any(item.trade_date == actual_trade_date for item in items) else [(actual_trade_date, actual_trade_date)]
        actual_start_date = format_date_value(start_date)
        actual_end_date = format_date_value(end_date)
        if actual_start_date == "" and actual_end_date == "":
            return [("", "")] if items == [] else []
        if actual_start_date == "":
            actual_start_date = actual_end_date
        if actual_end_date == "":
            actual_end_date = actual_start_date
        expected_trade_dates: list[str] = []
        existing_dates = {item.trade_date for item in items}
        missing_ranges = _build_missing_expected_date_ranges(expected_trade_dates, existing_dates)
        if missing_ranges == [] and expected_trade_dates == []:
            return _build_missing_date_ranges(actual_start_date, actual_end_date, existing_dates)
        return missing_ranges

    def get_quotes(
        self,
        board_codes: list[str],
        freq: str,
        trade_date: str,
        start_date: str,
        end_date: str,
        start_time: str,
        end_time: str,
        count: int | None,
        limit: int,
    ) -> list[BoardQuoteItem]:
        store_identity = {"board_codes": list(board_codes), "freq": freq, "trade_date": trade_date, "start_date": start_date, "end_date": end_date, "start_time": start_time, "end_time": end_time, "count": count}
        handlers = {
            "get_board_quotes": lambda instance: lambda request_board_codes, missing_start, missing_end: _source_package_call(instance.package_id, "get_board_quotes", request_board_codes, freq, "", missing_start, missing_end, start_time, end_time, count),
        }
        items, _ = execute_capability_query(
            CapabilityQuerySpec(
                capability_id="boards.quotes.daily",
                store_identity=store_identity,
                model_type=BoardQuoteItem,
                key_fields=("board_code", "trade_time", "freq"),
                sort_fields=("board_code", "trade_time"),
                request_builder=lambda current_items: _build_missing_quote_requests(board_codes, current_items, freq, trade_date, start_date, end_date, count, self._settings),
                provider_steps=lambda: SourceInstanceExecutor(self._settings).build_steps("boards.quotes.daily", handlers, ("tushare", "akshare")),
                source_order=self._settings.get_contract_source_order("boards.quotes.daily", ("tushare", "akshare")),
                base_items=get_local_board_quotes(board_codes, freq, trade_date, start_date, end_date, count),
                base_source_name="fact.board_daily_1d",
                fact_ref_writer=get_fact_ref_writer("boards.quotes.daily") if freq == "1d" else None,
            )
        )
        if count:
            items = trim_items_per_key(items, "board_code", "trade_time", count)
        return sorted(items, key=lambda item: (item.board_code, item.trade_time))[: ensure_limit(limit)]

    def get_catalog(self, category: str, market: str, status: str, limit: int, offset: int) -> list[BoardCatalogItem]:
        store_identity = {"category": category, "market": market, "status": status}
        handlers = {
            "get_board_catalog": lambda instance: lambda: _source_package_call(instance.package_id, "get_board_catalog", category, market, status, ensure_limit(limit), offset),
        }
        items, _ = execute_capability_query(
            CapabilityQuerySpec(
                capability_id="boards.catalog",
                store_identity=store_identity,
                model_type=BoardCatalogItem,
                key_fields=("board_code",),
                sort_fields=("board_code",),
                request_builder=lambda current_items: [()] if current_items == [] else [],
                provider_steps=lambda: SourceInstanceExecutor(self._settings).build_steps("boards.catalog", handlers, ("tushare", "akshare")),
                source_order=self._settings.get_contract_source_order("boards.catalog", ("tushare", "akshare")),
                base_items=get_local_board_catalog(status),
                base_source_name="ref.board",
                payload_builder=_payloads_with_as_of_date,
                fact_ref_writer=get_fact_ref_writer("boards.catalog"),
            )
        )
        filtered = [item for item in items if (category == "" or item.category == category) and (market == "" or item.market == market) and (status == "" or item.status == status)]
        return filtered[offset: offset + ensure_limit(limit)]

    def get_profile(self, board_code: str) -> BoardCatalogItem | None:
        store_identity = {"board_code": board_code}
        handlers = {
            "get_board_profile": lambda instance: lambda: [item for item in [_source_package_call(instance.package_id, "get_board_profile", board_code)] if item is not None],
        }
        items, _ = execute_capability_query(
            CapabilityQuerySpec(
                capability_id="boards.profile",
                store_identity=store_identity,
                model_type=BoardCatalogItem,
                key_fields=("board_code",),
                sort_fields=("board_code",),
                request_builder=lambda current_items: [()] if current_items == [] else [],
                provider_steps=lambda: SourceInstanceExecutor(self._settings).build_steps("boards.profile", handlers, ("tushare", "akshare")),
                source_order=self._settings.get_contract_source_order("boards.profile", ("tushare", "akshare")),
                base_items=get_local_board_profile(board_code),
                base_source_name="ref.board",
                payload_builder=_payloads_with_as_of_date,
                fact_ref_writer=get_fact_ref_writer("boards.profile"),
            )
        )
        return items[0] if items else None

    def get_members(self, board_code: str, trade_date: str) -> list[BoardMemberItem]:
        store_identity = {"board_code": board_code, "trade_date": trade_date}
        handlers = {
            "get_board_members": lambda instance: lambda: _source_package_call(instance.package_id, "get_board_members", board_code, trade_date),
        }
        items, _ = execute_capability_query(
            CapabilityQuerySpec(
                capability_id="boards.members",
                store_identity=store_identity,
                model_type=BoardMemberItem,
                key_fields=("board_code", "code"),
                sort_fields=("board_code", "code"),
                request_builder=lambda current_items: [()] if current_items == [] else [],
                provider_steps=lambda: SourceInstanceExecutor(self._settings).build_steps("boards.members", handlers, ("tushare", "akshare")),
                source_order=self._settings.get_contract_source_order("boards.members", ("tushare", "akshare")),
                base_items=get_local_board_members(board_code, trade_date),
                base_source_name="ref.board_stock_membership",
                payload_builder=lambda payload_items: _board_member_store_payloads(payload_items, trade_date),
                fact_ref_writer=get_fact_ref_writer("boards.members"),
            )
        )
        return items

    def get_member_history(self, board_code: str, start_date: str, end_date: str) -> list[BoardMemberHistoryItem]:
        store_identity = {"board_code": board_code, "start_date": start_date, "end_date": end_date}
        handlers = {
            "get_board_member_history": lambda instance: lambda: _source_package_call(instance.package_id, "get_board_member_history", board_code, start_date, end_date),
        }
        items, _ = execute_capability_query(
            CapabilityQuerySpec(
                capability_id="boards.members.history",
                store_identity=store_identity,
                model_type=BoardMemberHistoryItem,
                key_fields=("board_code", "code", "effective_date", "action"),
                sort_fields=("board_code", "code", "effective_date"),
                request_builder=lambda current_items: [()] if current_items == [] else [],
                provider_steps=lambda: SourceInstanceExecutor(self._settings).build_steps("boards.members.history", handlers, ("tushare", "akshare")),
                source_order=self._settings.get_contract_source_order("boards.members.history", ("tushare", "akshare")),
                base_items=get_local_board_member_history(board_code),
                base_source_name="ref.board_stock_membership",
                fact_ref_writer=get_fact_ref_writer("boards.members.history"),
            )
        )
        return items

    def get_money_flow(self, board_code: str, trade_date: str, start_date: str, end_date: str, scope: str) -> list[BoardMoneyFlowItem]:
        store_identity = {"board_code": board_code, "trade_date": trade_date, "start_date": start_date, "end_date": end_date, "scope": scope}
        handlers = {
            "get_board_money_flow": lambda instance: lambda missing_start, missing_end: _source_package_call(instance.package_id, "get_board_money_flow", board_code, trade_date, missing_start, missing_end, scope),
        }
        sorted_items, _ = execute_capability_query(
            CapabilityQuerySpec(
                capability_id="boards.indicators.money_flow",
                store_identity=store_identity,
                model_type=BoardMoneyFlowItem,
                key_fields=("board_code", "trade_date", "scope"),
                sort_fields=("board_code", "trade_date"),
                request_builder=lambda items: self._build_money_flow_requests(items, trade_date, start_date, end_date),
                provider_steps=lambda: SourceInstanceExecutor(self._settings).build_steps("boards.indicators.money_flow", handlers, ("tushare", "akshare")),
                source_order=self._settings.get_contract_source_order("boards.indicators.money_flow", ("tushare", "akshare")),
            )
        )
        return sorted_items

    def get_market_money_flow(self, trade_date: str, scope: str, limit: int, offset: int) -> list[BoardMoneyFlowItem]:
        store_identity = {"board_code": "", "trade_date": trade_date, "scope": scope}
        handlers = {
            "get_board_daily_money_flow_snapshot": lambda instance: lambda: _source_package_call(instance.package_id, "get_board_daily_money_flow_snapshot", trade_date, scope, limit, offset),
        }
        items, _ = execute_capability_query(
            CapabilityQuerySpec(
                capability_id="boards.indicators.money_flow.snapshot",
                store_identity=store_identity,
                model_type=BoardMoneyFlowItem,
                key_fields=("board_code", "trade_date", "scope"),
                sort_fields=("board_code", "trade_date"),
                request_builder=lambda current_items: [()] if current_items == [] else [],
                provider_steps=lambda: SourceInstanceExecutor(self._settings).build_steps("boards.indicators.money_flow.snapshot", handlers, ("tushare", "akshare")),
                source_order=self._settings.get_contract_source_order("boards.indicators.money_flow.snapshot", ("tushare", "akshare")),
            )
        )
        return items[offset: offset + ensure_limit(limit)]


    def get_market_daily_snapshot(self, trade_date: str, limit: int, offset: int) -> list[BoardQuoteItem]:
        """获取指定交易日全市场板块快照"""
        store_identity = {"board_codes": [], "trade_date": trade_date, "snapshot": True}
        handlers = {
            "get_board_daily_snapshot": lambda instance: lambda: _source_package_call(instance.package_id, "get_board_daily_snapshot", trade_date, limit, offset),
        }
        items, _ = execute_capability_query(
            CapabilityQuerySpec(
                capability_id="boards.quotes.daily.snapshot",
                store_identity=store_identity,
                model_type=BoardQuoteItem,
                key_fields=("board_code", "trade_time", "freq"),
                sort_fields=("board_code", "trade_time"),
                request_builder=lambda current_items: [()] if current_items == [] else [],
                provider_steps=lambda: SourceInstanceExecutor(self._settings).build_steps("boards.quotes.daily.snapshot", handlers, ("tushare", "akshare")),
                source_order=self._settings.get_contract_source_order("boards.quotes.daily.snapshot", ("tushare", "akshare")),
            )
        )
        return items[offset: offset + ensure_limit(limit)]

    def get_categories(self, parent_code: str, level: int | None) -> list[BoardCategoryItem]:
        store_identity = {"parent_code": parent_code, "level": level}
        handlers = {
            "get_board_categories": lambda instance: lambda: _source_package_call(instance.package_id, "get_board_categories", parent_code, level),
        }
        items, _ = execute_capability_query(
            CapabilityQuerySpec(
                capability_id="boards.reference.categories",
                store_identity=store_identity,
                model_type=BoardCategoryItem,
                key_fields=("category_code",),
                sort_fields=("category_code",),
                request_builder=lambda current_items: [()] if current_items == [] else [],
                provider_steps=lambda: SourceInstanceExecutor(self._settings).build_steps("boards.reference.categories", handlers, ("tushare", "akshare")),
                source_order=self._settings.get_contract_source_order("boards.reference.categories", ("tushare", "akshare")),
                payload_builder=_payloads_with_as_of_date,
            )
        )
        return items
