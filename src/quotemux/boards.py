from __future__ import annotations

from datetime import timedelta

from platform_models import BoardCatalogItem, BoardCategoryItem, BoardMemberHistoryItem, BoardMemberItem, BoardMoneyFlowItem, BoardQuoteItem
from quotemux.infra.common import format_date_value, parse_date_text
from quotemux.common import ensure_limit
from quotemux.runtime_core.executor import SourceInstanceExecutor, run_fallback_chain_with_report
from quotemux.runtime_core.registry import SourceProxy
from quotemux.settings import QuoteMuxSettings


_datalake = SourceProxy("datalake")
_datalake_reference = SourceProxy("datalake_reference")
_tushare_provider = SourceProxy("tushare")


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
        expected_trade_dates = []
        if self._settings.is_source_enabled("datalake_reference"):
            trading_calendar_items = _datalake_reference.get_trading_calendar("SSE", actual_start_date, actual_end_date, True)
            expected_trade_dates = [item.trade_date for item in trading_calendar_items]
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
        instances = self._settings.get_contract_source_instances("boards.quotes", ("datalake",))
        if not any(item.package_id == "datalake" for item in instances):
            return []
        items = _datalake.get_board_quotes(board_codes, freq, trade_date, start_date, end_date, start_time, end_time, count)
        items = sorted(items, key=lambda item: (item.board_code, item.trade_time))
        if count:
            grouped: dict[str, list[BoardQuoteItem]] = {}
            for item in items:
                grouped.setdefault(item.board_code, []).append(item)
            trimmed: list[BoardQuoteItem] = []
            for _, group_items in grouped.items():
                trimmed.extend(group_items[-count:])
            items = sorted(trimmed, key=lambda item: (item.board_code, item.trade_time))
        return items[: ensure_limit(limit)]

    def get_catalog(self, category: str, market: str, status: str, limit: int, offset: int) -> list[BoardCatalogItem]:
        if not self._settings.is_source_enabled("datalake_reference"):
            return []
        return _datalake_reference.get_board_catalog(category, market, status, ensure_limit(limit), offset)

    def get_profile(self, board_code: str) -> BoardCatalogItem | None:
        if not self._settings.is_source_enabled("datalake_reference"):
            return None
        return _datalake_reference.get_board_profile(board_code)

    def get_members(self, board_code: str, trade_date: str) -> list[BoardMemberItem]:
        if not self._settings.is_source_enabled("datalake_reference"):
            return []
        return _datalake_reference.get_board_members(board_code, trade_date)

    def get_member_history(self, board_code: str, start_date: str, end_date: str) -> list[BoardMemberHistoryItem]:
        if not self._settings.is_source_enabled("datalake_reference"):
            return []
        return _datalake_reference.get_board_member_history(board_code, start_date, end_date)

    def get_money_flow(self, board_code: str, trade_date: str, start_date: str, end_date: str, scope: str) -> list[BoardMoneyFlowItem]:
        handlers = {
            "datalake": ("get_board_money_flow", lambda instance: lambda missing_start, missing_end: _datalake.get_board_money_flow(board_code, "", missing_start, missing_end, scope)),
            "tushare": ("get_board_money_flow", lambda instance: lambda missing_start, missing_end: _tushare_provider.get_board_money_flow(board_code, "", missing_start, missing_end, scope)),
        }
        merged_items, _ = run_fallback_chain_with_report(
            "boards.money_flow",
            [],
            ("board_code", "trade_date", "scope"),
            lambda items: self._build_money_flow_requests(items, trade_date, start_date, end_date),
            SourceInstanceExecutor(self._settings).build_steps("boards.money_flow", handlers, ("datalake", "tushare")),
            self._settings.get_contract_source_order("boards.money_flow", ("datalake", "tushare")),
        )
        return sorted(merged_items, key=lambda item: (item.board_code, item.trade_date))

    def get_market_money_flow(self, trade_date: str, scope: str, limit: int, offset: int) -> list[BoardMoneyFlowItem]:
        if not self._settings.is_source_enabled("datalake"):
            return []
        return _datalake.get_board_daily_money_flow_snapshot(trade_date, scope, limit, offset)

    def get_categories(self, parent_code: str, level: int | None) -> list[BoardCategoryItem]:
        if not self._settings.is_source_enabled("datalake_reference"):
            return []
        return _datalake_reference.get_board_categories(parent_code, level)





