from __future__ import annotations

from datetime import timedelta

import pandas as pd

from platform_models import AdjFactorItem, AuditItem, AuctionItem, BSECodeMappingItem, CcassHoldingDetailItem, CcassHoldingItem, ChipDistributionItem, ChipPerformanceItem, DisclosureDateItem, DividendItem, ExpressItem, ForecastItem, HKConnectHoldingItem, HKConnectTargetItem, HLSignalItem, MainBusinessItem, ManagementRewardItem, NameHistoryItem, NineTurnItem, PledgeDetailItem, PledgeStatItem, RepurchaseItem, RightsIssueItem, ShareChangeItem, ShareholderCountItem, ShareholderTop10Item, StockAHComparisonItem, StockArchiveItem, StockBasicInfo, StockDailyBasicItem, StockDailyMarketValueItem, StockDailyValuationItem, StockFinanceIndicatorItem, StockFinancialStatementItem, StockManagerItem, StockMoneyFlowItem, StockPremarketItem, StockProfileItem, StockQuoteItem, StockRiskFlagItem, SurveyItem, UnlockScheduleItem
from quotemux.infra.common import add_quote_metrics, aggregate_ohlc, build_time_bounds, format_date_value, format_datetime_value, normalize_stock_code, parse_date_text
from quotemux.runtime_core.executor import ProviderStep, SourceInstanceExecutor, run_fallback_chain_with_report
from quotemux.infra.tushare.helpers import normalize_date_range
from quotemux.common import MARKET_DAILY_SNAPSHOT_LIMIT, build_missing_expected_date_ranges, ensure_limit, has_enough_stock_quote_rows, merge_model_lists, sort_items, trim_items_per_key
from quotemux.reports import ContractReport
from quotemux.requests.stocks import StockDailySnapshotRequest, StockQuotesRequest
from quotemux.runtime_core.registry import SourceProxy
from quotemux.settings import QuoteMuxSettings


MAX_DAILY_INDICATOR_CODES = 200
akshare_provider = SourceProxy("akshare")
datalake = SourceProxy("datalake")
datalake_reference = SourceProxy("datalake_reference")
efinance_provider = SourceProxy("efinance")
mootdx_provider = SourceProxy("mootdx")
opentdx_provider = SourceProxy("opentdx")
tushare_provider = SourceProxy("tushare")
tushare_stock_chips = SourceProxy("tushare_stock_chips")
tushare_stock_finance = SourceProxy("tushare_stock_finance")
tushare_stock_ownership = SourceProxy("tushare_stock_ownership")
tushare_stocks = SourceProxy("tushare_stocks")


def _fallback_quote_freq(freq: str) -> str:
    if freq in {"1w", "1mo"}:
        return "1d"
    return freq


def _fallback_quote_count(freq: str, count: int | None) -> int | None:
    if not count:
        return count
    if freq == "1w":
        return count * 10
    if freq == "1mo":
        return count * 35
    return count


def _aggregate_stock_quotes(items: list[StockQuoteItem], freq: str, adjust: str) -> list[StockQuoteItem]:
    if freq not in {"1w", "1mo"} or items == []:
        return items
    frame = pd.DataFrame([item.model_dump() for item in items])
    frame["trade_time"] = pd.to_datetime(frame["trade_time"], errors="coerce")
    for column in ["open", "high", "low", "close", "volume", "amount"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(subset=["trade_time"])
    if frame.empty:
        return []
    result: list[StockQuoteItem] = []
    for stock_code, group in frame.groupby("code", sort=False):
        aggregated = add_quote_metrics(aggregate_ohlc(group[["trade_time", "open", "high", "low", "close", "volume", "amount"]], freq))
        for _, row in aggregated.iterrows():
            result.append(
                StockQuoteItem(
                    code=str(stock_code),
                    trade_time=format_datetime_value(row["trade_time"], freq),
                    freq=freq,
                    open=float(row["open"]) if pd.notna(row["open"]) else None,
                    high=float(row["high"]) if pd.notna(row["high"]) else None,
                    low=float(row["low"]) if pd.notna(row["low"]) else None,
                    close=float(row["close"]) if pd.notna(row["close"]) else None,
                    pre_close=float(row["pre_close"]) if pd.notna(row["pre_close"]) else None,
                    change=float(row["change"]) if pd.notna(row["change"]) else None,
                    pct_chg=float(row["pct_chg"]) if pd.notna(row["pct_chg"]) else None,
                    volume=float(row["volume"]) if pd.notna(row["volume"]) else None,
                    amount=float(row["amount"]) if pd.notna(row["amount"]) else None,
                    adjust=adjust,
                )
            )
    return result


def _build_missing_date_ranges(start_date: str, end_date: str, existing_dates: set[str]) -> list[tuple[str, str]]:
    start_day = parse_date_text(start_date)
    end_day = parse_date_text(end_date)
    if start_day is None or end_day is None or start_day > end_day:
        return []
    missing_ranges: list[tuple[str, str]] = []
    current_start = None
    current_day = start_day
    while current_day <= end_day:
        current_text = current_day.strftime("%Y-%m-%d")
        if current_text not in existing_dates:
            if current_start is None:
                current_start = current_day
        elif current_start is not None:
            missing_ranges.append((current_start.strftime("%Y-%m-%d"), (current_day - timedelta(days=1)).strftime("%Y-%m-%d")))
            current_start = None
        current_day += timedelta(days=1)
    if current_start is not None:
        missing_ranges.append((current_start.strftime("%Y-%m-%d"), end_day.strftime("%Y-%m-%d")))
    return missing_ranges


def _build_missing_quote_requests(
    codes: list[str],
    current_items: list[StockQuoteItem],
    freq: str,
    trade_date: str,
    start_date: str,
    end_date: str,
    start_time: str,
    end_time: str,
    count: int | None,
) -> list[tuple[list[str], str, str]]:
    if trade_date == "" and start_date == "" and end_date == "" and start_time == "" and end_time == "" and count:
        if has_enough_stock_quote_rows(current_items, codes, count, "code"):
            return []
        missing_codes = [code for code in codes if sum(1 for item in current_items if item.code == code) < count]
        return [(missing_codes, "", "")] if missing_codes else []
    if freq != "1d":
        if has_enough_stock_quote_rows(current_items, codes, count, "code"):
            return []
        missing_codes = [code for code in codes if sum(1 for item in current_items if item.code == code) < (count or 1)]
        if missing_codes == []:
            return []
        actual_start_date = trade_date or start_date
        actual_end_date = trade_date or end_date
        if actual_start_date == "" and actual_end_date == "":
            return [(missing_codes, "", "")]
        if actual_start_date == "":
            actual_start_date = actual_end_date
        if actual_end_date == "":
            actual_end_date = actual_start_date
        return [(missing_codes, actual_start_date, actual_end_date)]
    request_start_dt, request_end_dt = build_time_bounds(trade_date, start_date, end_date, start_time, end_time, count, False)
    actual_start_date = request_start_dt.strftime("%Y-%m-%d") if request_start_dt is not None else ""
    actual_end_date = request_end_dt.strftime("%Y-%m-%d") if request_end_dt is not None else ""
    if actual_start_date == "" and actual_end_date == "":
        if has_enough_stock_quote_rows(current_items, codes, count, "code"):
            return []
        missing_codes = [code for code in codes if sum(1 for item in current_items if item.code == code) < (count or 1)]
        return [(missing_codes, "", "")] if missing_codes else []
    if actual_start_date == "":
        actual_start_date = actual_end_date
    if actual_end_date == "":
        actual_end_date = actual_start_date
    expected_trade_dates = []
    trading_calendar_items = datalake_reference.get_trading_calendar("SSE", actual_start_date, actual_end_date, True)
    expected_trade_dates = [item.trade_date for item in trading_calendar_items]
    grouped_ranges: dict[tuple[str, str], list[str]] = {}
    for code in codes:
        existing_dates = {item.trade_time for item in current_items if item.code == code and item.freq == "1d"}
        missing_ranges = build_missing_expected_date_ranges(expected_trade_dates, existing_dates)
        if missing_ranges == [] and expected_trade_dates == []:
            missing_ranges = _build_missing_date_ranges(actual_start_date, actual_end_date, existing_dates)
        for missing_start, missing_end in missing_ranges:
            grouped_ranges.setdefault((missing_start, missing_end), []).append(code)
    return [(range_codes, range_start, range_end) for (range_start, range_end), range_codes in grouped_ranges.items()]


def _missing_snapshot_codes(trade_date: str, items: list[StockQuoteItem]) -> list[str]:
    expected_codes = datalake_reference.get_stock_active_codes(trade_date)
    if expected_codes == []:
        return []
    existing_codes = {item.code for item in items if item.trade_time == trade_date and item.freq == "1d"}
    return [code for code in expected_codes if code not in existing_codes]


def _build_steps(freq: str, request_freq: str, request_count: int | None, actual_adjust: str, settings: QuoteMuxSettings) -> tuple[ProviderStep[StockQuoteItem], ...]:
    handlers = {
        "datalake": ("get_stock_quotes", lambda instance: lambda missing_codes, missing_start, missing_end: datalake.get_stock_quotes(missing_codes, request_freq, "", missing_start, missing_end, "", "", request_count, actual_adjust)),
        "efinance": ("get_stock_quotes", lambda instance: lambda missing_codes, missing_start, missing_end: efinance_provider.get_stock_quotes(missing_codes, request_freq, "", missing_start, missing_end, "", "", request_count, actual_adjust)),
        "mootdx": ("get_stock_quotes", lambda instance: lambda missing_codes, missing_start, missing_end: mootdx_provider.get_stock_quotes(missing_codes, request_freq, "", missing_start, missing_end, "", "", request_count, actual_adjust)),
        "akshare": ("get_stock_quotes", lambda instance: lambda missing_codes, missing_start, missing_end: akshare_provider.get_stock_quotes(missing_codes, request_freq, "", missing_start, missing_end, "", "", request_count, actual_adjust)),
    }
    if freq == "1d":
        handlers["tushare"] = ("get_stock_quotes", lambda instance: lambda missing_codes, missing_start, missing_end: tushare_provider.get_stock_quotes(missing_codes, request_freq, "", missing_start, missing_end, "", "", request_count, actual_adjust))
        fallback_order = ("datalake", "tushare", "efinance", "mootdx", "akshare")
    else:
        handlers["opentdx"] = ("get_stock_quotes", lambda instance: lambda missing_codes, missing_start, missing_end: opentdx_provider.get_stock_quotes(missing_codes, request_freq, "", missing_start, missing_end, "", "", request_count, actual_adjust))
        fallback_order = ("datalake", "opentdx", "efinance", "mootdx", "akshare")
    return SourceInstanceExecutor(settings).build_steps("stocks.quotes.daily" if freq == "1d" else "stocks.quotes.intraday", handlers, fallback_order)


def _build_daily_snapshot_steps(settings: QuoteMuxSettings) -> tuple[ProviderStep[StockQuoteItem], ...]:
    handlers = {
        "datalake": ("get_stock_daily_snapshot_full", lambda instance: lambda missing_codes, request_trade_date: datalake.get_stock_daily_snapshot_full(request_trade_date)),
        "tushare": ("get_stock_quotes", lambda instance: lambda missing_codes, request_trade_date: tushare_provider.get_stock_quotes(missing_codes, "1d", request_trade_date, "", "", "", "", None, "none")),
        "efinance": ("get_stock_quotes", lambda instance: lambda missing_codes, request_trade_date: efinance_provider.get_stock_quotes(missing_codes, "1d", request_trade_date, "", "", "", "", None, "none")),
        "mootdx": ("get_stock_quotes", lambda instance: lambda missing_codes, request_trade_date: mootdx_provider.get_stock_quotes(missing_codes, "1d", request_trade_date, "", "", "", "", None, "none")),
        "akshare": ("get_stock_quotes", lambda instance: lambda missing_codes, request_trade_date: akshare_provider.get_stock_quotes(missing_codes, "1d", request_trade_date, "", "", "", "", None, "none")),
    }
    return SourceInstanceExecutor(settings).build_steps("stocks.daily_snapshot", handlers, ("datalake", "tushare", "efinance", "mootdx", "akshare"))


def _indicator_codes_from_params(code: str, codes: str) -> list[str]:
    items: list[str] = []
    if code:
        items.append(normalize_stock_code(code))
    if codes:
        items.extend(normalize_stock_code(item) for item in str(codes).split(","))
    return [item for item in dict.fromkeys(items) if item]


def _single_day_indicator_request(trade_date: str, start_date: str, end_date: str) -> str:
    actual_trade_date = format_date_value(trade_date)
    if actual_trade_date:
        return actual_trade_date
    actual_start = format_date_value(start_date)
    actual_end = format_date_value(end_date)
    if actual_start and not actual_end:
        return actual_start
    if actual_end and not actual_start:
        return actual_end
    if actual_start and actual_end and actual_start == actual_end:
        return actual_start
    return ""


class QuoteMuxStocks:
    def __init__(self, settings: QuoteMuxSettings) -> None:
        self._settings = settings

    def get_quotes(self, request: StockQuotesRequest) -> list[StockQuoteItem]:
        items, _ = self.get_quotes_with_report(request)
        return items

    def get_quotes_with_report(self, request: StockQuotesRequest) -> tuple[list[StockQuoteItem], ContractReport]:
        if request.codes == []:
            return [], ContractReport.empty("stocks.quotes")
        actual_limit = ensure_limit(request.limit)
        actual_freq = request.freq or "1d"
        actual_adjust = request.adjust or "none"
        request_freq = _fallback_quote_freq(actual_freq)
        request_count = _fallback_quote_count(actual_freq, request.count)
        contract_name = "stocks.quotes.daily" if request_freq == "1d" else "stocks.quotes.intraday"
        merged_items, fallback_report = run_fallback_chain_with_report(
            contract_name,
            [],
            ("code", "trade_time", "freq"),
            lambda items: _build_missing_quote_requests(request.codes, items, request_freq, request.trade_date, request.start_date, request.end_date, request.start_time, request.end_time, request_count),
            _build_steps(request_freq, request_freq, request_count, actual_adjust, self._settings),
            self._settings.get_contract_source_order(contract_name, ("datalake", "tushare", "opentdx", "efinance", "mootdx", "akshare")),
        )
        if actual_freq in {"1w", "1mo"}:
            merged_items = _aggregate_stock_quotes(merged_items, actual_freq, actual_adjust)
        trimmed_items = trim_items_per_key(merged_items, "code", "trade_time", request.count)
        sorted_items = sort_items(trimmed_items, ("code", "trade_time"))
        report = ContractReport.from_fallback_report(contract_name, fallback_report)
        return sorted_items[:actual_limit], report

    def get_daily_snapshot(self, request: StockDailySnapshotRequest) -> list[StockQuoteItem]:
        items, _ = self.get_daily_snapshot_with_report(request)
        return items

    def get_daily_snapshot_with_report(self, request: StockDailySnapshotRequest) -> tuple[list[StockQuoteItem], ContractReport]:
        actual_trade_date = format_date_value(request.trade_date)
        if actual_trade_date == "":
            raise ValueError("trade_date 涓嶈兘涓虹┖锛屼笖蹇呴』鏄崟涓氦鏄撴棩")
        if request.limit < 1 or request.limit > MARKET_DAILY_SNAPSHOT_LIMIT:
            raise ValueError("limit 瓒呭嚭鍏佽鑼冨洿")
        if request.offset < 0:
            raise ValueError("offset 涓嶈兘灏忎簬 0")
        merged_items, fallback_report = run_fallback_chain_with_report(
            "stocks.daily_snapshot",
            [],
            ("code", "trade_time", "freq"),
            lambda items: [(_missing_snapshot_codes(actual_trade_date, items), actual_trade_date)] if items == [] or _missing_snapshot_codes(actual_trade_date, items) else [],
            _build_daily_snapshot_steps(self._settings),
            self._settings.get_contract_source_order("stocks.daily_snapshot", ("datalake", "tushare", "efinance", "mootdx", "akshare")),
        )
        sorted_items = sort_items(merged_items, ("code", "trade_time"))
        report = ContractReport.from_fallback_report("stocks.daily_snapshot", fallback_report)
        return sorted_items[request.offset: request.offset + request.limit], report

    def _build_missing_money_flow_requests(self, items: list[StockMoneyFlowItem], trade_date: str, start_date: str, end_date: str) -> list[tuple[str, str]]:
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
            trading_calendar_items = datalake_reference.get_trading_calendar("SSE", actual_start_date, actual_end_date, True)
            expected_trade_dates = [item.trade_date for item in trading_calendar_items]
        existing_dates = {item.trade_date for item in items}
        missing_ranges = build_missing_expected_date_ranges(expected_trade_dates, existing_dates)
        if missing_ranges == [] and expected_trade_dates == []:
            return _build_missing_date_ranges(actual_start_date, actual_end_date, existing_dates)
        return missing_ranges

    def get_money_flow(self, code: str, trade_date: str, start_date: str, end_date: str, view: str) -> list[StockMoneyFlowItem]:
        handlers = {
            "datalake": ("get_stock_money_flow", lambda instance: lambda missing_start, missing_end: datalake.get_stock_money_flow(code, "", missing_start, missing_end, view)),
            "tushare": ("get_stock_money_flow", lambda instance: lambda missing_start, missing_end: tushare_provider.get_stock_money_flow(code, "", missing_start, missing_end, view)),
        }
        merged_items, _ = run_fallback_chain_with_report(
            "stocks.money_flow",
            [],
            ("code", "trade_date", "view"),
            lambda items: self._build_missing_money_flow_requests(items, trade_date, start_date, end_date),
            SourceInstanceExecutor(self._settings).build_steps("stocks.money_flow", handlers, ("datalake", "tushare")),
            self._settings.get_contract_source_order("stocks.money_flow", ("datalake", "tushare")),
        )
        return sorted(merged_items, key=lambda item: (item.code, item.trade_date))

    def get_financial_statements(self, codes: list[str], report_period: str, start_period: str, end_period: str, report_type: str) -> list[StockFinancialStatementItem]:
        if not self._settings.is_source_enabled("tushare"):
            return []
        ts_items = tushare_provider.get_stock_financial_statements(codes, report_period, start_period, end_period, report_type)
        return sorted(ts_items, key=lambda item: (item.code, item.report_period, item.announce_date, item.report_type))

    def get_finance_indicators(self, code: str, codes: str, report_period: str, start_period: str, end_period: str) -> list[StockFinanceIndicatorItem]:
        if not self._settings.is_source_enabled("tushare_stocks"):
            return []
        return tushare_stocks.get_stock_finance_indicators(code, codes, report_period, start_period, end_period)

    def get_catalog(self, codes: list[str], name: str, exchange: str, list_status: str, include_delisted: bool, limit: int, offset: int) -> list[StockBasicInfo]:
        if not self._settings.is_source_enabled("datalake_reference"):
            return []
        return datalake_reference.get_stock_catalog(codes, name, exchange, list_status, include_delisted, ensure_limit(limit), offset)

    def get_archive(self, trade_date: str, code: str, name: str, industry: str, area: str, limit: int, offset: int) -> list[StockArchiveItem]:
        if not self._settings.is_source_enabled("tushare_stocks"):
            return []
        return tushare_stocks.get_stock_archive(trade_date, code, name, industry, area, ensure_limit(limit), offset)

    def get_basic(self, code: str) -> StockBasicInfo | None:
        if not self._settings.is_source_enabled("datalake_reference"):
            return None
        return datalake_reference.get_stock_basic(code)

    def get_profile(self, code: str) -> StockProfileItem | None:
        if not self._settings.is_source_enabled("tushare_stocks"):
            return None
        return tushare_stocks.get_company_profile(code)

    def get_name_history(self, code: str, start_date: str, end_date: str) -> list[NameHistoryItem]:
        if not self._settings.is_source_enabled("datalake_reference"):
            return []
        return datalake_reference.get_stock_name_history(code, start_date, end_date)

    def get_managers(self, code: str) -> list[StockManagerItem]:
        if not self._settings.is_source_enabled("tushare_stocks"):
            return []
        return tushare_stocks.get_managers(code)

    def get_management_rewards(self, code: str, start_date: str, end_date: str) -> list[ManagementRewardItem]:
        if not self._settings.is_source_enabled("tushare_stocks"):
            return []
        return tushare_stocks.get_management_rewards(code, start_date, end_date)

    def get_hl_signal(self, code: str, trade_date: str, start_date: str, end_date: str) -> list[HLSignalItem]:
        if not self._settings.is_source_enabled("datalake_reference"):
            return []
        return datalake_reference.get_hl_signal(code, trade_date, start_date, end_date)

    def get_nine_turn(self, code: str, freq: str, trade_date: str, start_date: str, end_date: str) -> list[NineTurnItem]:
        if not self._settings.is_source_enabled("tushare_stocks"):
            return []
        return tushare_stocks.get_nine_turn(code, freq, trade_date, start_date, end_date)

    def get_adj_factors(self, code: str, start_date: str, end_date: str, base_date: str) -> list[AdjFactorItem]:
        if not self._settings.is_source_enabled("datalake"):
            return []
        return datalake.get_adj_factors(code, start_date, end_date, base_date)

    def get_ah_comparisons(self, code: str, trade_date: str, start_date: str, end_date: str, limit: int, offset: int) -> list[StockAHComparisonItem]:
        if not self._settings.is_source_enabled("tushare_stocks"):
            return []
        return tushare_stocks.get_stock_ah_comparisons(code, trade_date, start_date, end_date, ensure_limit(limit), offset)

    def _resolve_indicator_request(self, code: str, codes: str, trade_date: str, start_date: str, end_date: str) -> tuple[list[str], str, str, str]:
        actual_codes = _indicator_codes_from_params(code, codes)
        if actual_codes:
            if len(actual_codes) > MAX_DAILY_INDICATOR_CODES:
                raise ValueError("鏃ラ鎸囨爣鎺ュ彛涓嶆敮鎸佷竴娆′紶鍏ヨ秴杩?200 鍙偂绁紱鍏ㄥ競鍦哄彇鏁拌鎸?trade_date 鍗曟棩鏌ヨ锛屼笉瑕佷紶 code 鎴?codes")
            actual_start, actual_end = normalize_date_range(trade_date, start_date, end_date)
            if actual_start == actual_end:
                return actual_codes, actual_start, "", ""
            return actual_codes, "", actual_start, actual_end
        actual_trade_date = _single_day_indicator_request(trade_date, start_date, end_date)
        if actual_trade_date == "":
            raise ValueError("鏈紶 code 鎴?codes 鏃讹紝浠呮敮鎸佸崟鏃ュ叏甯傚満鏌ヨ锛岃浣跨敤 trade_date")
        return [], actual_trade_date, "", ""

    def get_daily_basic(self, code: str, codes: str, trade_date: str, start_date: str, end_date: str) -> list[StockDailyBasicItem]:
        if not self._settings.is_source_enabled("tushare_stocks"):
            return []
        actual_codes, actual_trade_date, actual_start, actual_end = self._resolve_indicator_request(code, codes, trade_date, start_date, end_date)
        if actual_codes == []:
            items = tushare_stocks.get_stock_daily_basic("", "", actual_trade_date, "", "")
            return sorted(items, key=lambda item: (item.code, item.trade_date))
        ts_items = tushare_stocks.get_stock_daily_basic(code, codes, actual_trade_date, actual_start, actual_end)
        return sorted(ts_items, key=lambda item: (item.code, item.trade_date))

    def get_daily_valuation(self, code: str, codes: str, trade_date: str, start_date: str, end_date: str) -> list[StockDailyValuationItem]:
        if not self._settings.is_source_enabled("tushare_stocks"):
            return []
        actual_codes, actual_trade_date, actual_start, actual_end = self._resolve_indicator_request(code, codes, trade_date, start_date, end_date)
        if actual_codes == []:
            items = tushare_stocks.get_stock_daily_valuation("", "", actual_trade_date, "", "")
            return sorted(items, key=lambda item: (item.code, item.trade_date))
        ts_items = tushare_stocks.get_stock_daily_valuation(code, codes, actual_trade_date, actual_start, actual_end)
        return sorted(ts_items, key=lambda item: (item.code, item.trade_date))

    def get_daily_market_value(self, code: str, codes: str, trade_date: str, start_date: str, end_date: str) -> list[StockDailyMarketValueItem]:
        if not self._settings.is_source_enabled("tushare_stocks"):
            return []
        actual_codes, actual_trade_date, actual_start, actual_end = self._resolve_indicator_request(code, codes, trade_date, start_date, end_date)
        if actual_codes == []:
            items = tushare_stocks.get_stock_daily_market_value("", "", actual_trade_date, "", "")
            return sorted(items, key=lambda item: (item.code, item.trade_date))
        ts_items = tushare_stocks.get_stock_daily_market_value(code, codes, actual_trade_date, actual_start, actual_end)
        return sorted(ts_items, key=lambda item: (item.code, item.trade_date))

    def get_risk_flags(self, trade_date: str, start_date: str, end_date: str, flag_type: str, status: str, limit: int, offset: int) -> list[StockRiskFlagItem]:
        if not self._settings.is_source_enabled("tushare_stocks"):
            return []
        return tushare_stocks.get_stock_risk_flags(trade_date, start_date, end_date, flag_type, status, ensure_limit(limit), offset)

    def get_premarket(self, code: str, trade_date: str, start_date: str, end_date: str) -> list[StockPremarketItem]:
        if not self._settings.is_source_enabled("tushare_stocks"):
            return []
        return tushare_stocks.get_premarket(code, trade_date, start_date, end_date)

    def get_chip_distribution(self, code: str, trade_date: str, start_date: str, end_date: str) -> list[ChipDistributionItem]:
        if not self._settings.is_source_enabled("tushare_stock_chips"):
            return []
        return tushare_stock_chips.get_chip_distribution(code, trade_date, start_date, end_date)

    def get_chip_performance(self, code: str, trade_date: str, start_date: str, end_date: str) -> list[ChipPerformanceItem]:
        if not self._settings.is_source_enabled("tushare_stock_chips"):
            return []
        return tushare_stock_chips.get_chip_performance(code, trade_date, start_date, end_date)

    def get_dividends(self, code: str, start_date: str, end_date: str) -> list[DividendItem]:
        if not self._settings.is_source_enabled("tushare_stock_finance"):
            return []
        return tushare_stock_finance.get_dividends(code, start_date, end_date)

    def get_repurchases(self, code: str, start_date: str, end_date: str) -> list[RepurchaseItem]:
        if not self._settings.is_source_enabled("tushare_stock_finance"):
            return []
        return tushare_stock_finance.get_repurchases(code, start_date, end_date)

    def get_rights_issues(self, code: str, start_date: str, end_date: str) -> list[RightsIssueItem]:
        if not self._settings.is_source_enabled("tushare_stock_finance"):
            return []
        return tushare_stock_finance.get_rights_issues(code, start_date, end_date)

    def get_share_changes(self, code: str, trade_date: str, start_date: str, end_date: str) -> list[ShareChangeItem]:
        if not self._settings.is_source_enabled("tushare_stock_finance"):
            return []
        return tushare_stock_finance.get_share_changes(code, trade_date, start_date, end_date)

    def get_unlock_schedules(self, code: str, unlock_date: str, start_date: str, end_date: str) -> list[UnlockScheduleItem]:
        if not self._settings.is_source_enabled("tushare_stock_finance"):
            return []
        return tushare_stock_finance.get_unlock_schedules(code, unlock_date, start_date, end_date)

    def get_audits(self, code: str, report_period: str, start_period: str, end_period: str) -> list[AuditItem]:
        if not self._settings.is_source_enabled("tushare_stock_finance"):
            return []
        return tushare_stock_finance.get_audits(code, report_period, start_period, end_period)

    def get_disclosure_dates(self, code: str, report_period: str, start_period: str, end_period: str) -> list[DisclosureDateItem]:
        if not self._settings.is_source_enabled("tushare_stock_finance"):
            return []
        return tushare_stock_finance.get_disclosure_dates(code, report_period, start_period, end_period)

    def get_express(self, code: str, report_period: str, start_period: str, end_period: str) -> list[ExpressItem]:
        if not self._settings.is_source_enabled("tushare_stock_finance"):
            return []
        return tushare_stock_finance.get_express(code, report_period, start_period, end_period)

    def get_forecasts(self, code: str, report_period: str, start_period: str, end_period: str) -> list[ForecastItem]:
        if not self._settings.is_source_enabled("tushare_stock_finance"):
            return []
        return tushare_stock_finance.get_forecasts(code, report_period, start_period, end_period)

    def get_main_business(self, code: str, report_period: str, start_period: str, end_period: str, classification: str) -> list[MainBusinessItem]:
        if not self._settings.is_source_enabled("tushare_stock_finance"):
            return []
        return tushare_stock_finance.get_main_business(code, report_period, start_period, end_period, classification)

    def get_ccass_holdings(self, code: str, trade_date: str, start_date: str, end_date: str) -> list[CcassHoldingItem]:
        if not self._settings.is_source_enabled("tushare_stock_ownership"):
            return []
        return tushare_stock_ownership.get_ccass_holdings(code, trade_date, start_date, end_date)

    def get_ccass_holding_details(self, code: str, trade_date: str, start_date: str, end_date: str) -> list[CcassHoldingDetailItem]:
        if not self._settings.is_source_enabled("tushare_stock_ownership"):
            return []
        return tushare_stock_ownership.get_ccass_holding_details(code, trade_date, start_date, end_date)

    def get_hk_connect_holdings(self, code: str, trade_date: str, start_date: str, end_date: str) -> list[HKConnectHoldingItem]:
        if not self._settings.is_source_enabled("tushare_stock_ownership"):
            return []
        return tushare_stock_ownership.get_hk_connect_holdings(code, trade_date, start_date, end_date)

    def get_pledge_stats(self, code: str, trade_date: str, start_date: str, end_date: str) -> list[PledgeStatItem]:
        if not self._settings.is_source_enabled("tushare_stock_ownership"):
            return []
        return tushare_stock_ownership.get_pledge_stats(code, trade_date, start_date, end_date)

    def get_pledge_details(self, code: str, start_date: str, end_date: str, status: str) -> list[PledgeDetailItem]:
        if not self._settings.is_source_enabled("tushare_stock_ownership"):
            return []
        return tushare_stock_ownership.get_pledge_details(code, start_date, end_date, status)

    def get_shareholder_count(self, code: str, trade_date: str, start_date: str, end_date: str) -> list[ShareholderCountItem]:
        if not self._settings.is_source_enabled("tushare_stock_ownership"):
            return []
        return tushare_stock_ownership.get_shareholder_count(code, trade_date, start_date, end_date)

    def get_shareholder_top10(self, code: str, report_period: str, start_period: str, end_period: str) -> list[ShareholderTop10Item]:
        if not self._settings.is_source_enabled("tushare_stock_ownership"):
            return []
        return tushare_stock_ownership.get_shareholder_top10(code, report_period, start_period, end_period, False)

    def get_shareholder_top10_float(self, code: str, report_period: str, start_period: str, end_period: str) -> list[ShareholderTop10Item]:
        if not self._settings.is_source_enabled("tushare_stock_ownership"):
            return []
        return tushare_stock_ownership.get_shareholder_top10(code, report_period, start_period, end_period, True)

    def get_research_reports(self, code: str, report_date: str, start_date: str, end_date: str):
        if not self._settings.is_source_enabled("tushare_stocks"):
            return []
        return tushare_stocks.get_research_reports(code, report_date, start_date, end_date)

    def get_surveys(self, code: str, survey_date: str, start_date: str, end_date: str) -> list[SurveyItem]:
        if not self._settings.is_source_enabled("tushare_stocks"):
            return []
        return tushare_stocks.get_surveys(code, survey_date, start_date, end_date)

    def get_bse_code_mappings(self, old_code: str, new_code: str, status: str) -> list[BSECodeMappingItem]:
        if not self._settings.is_source_enabled("tushare_stocks"):
            return []
        return tushare_stocks.get_bse_code_mappings(old_code, new_code, status)

    def get_hk_connect_targets(self, direction: str, status: str, effective_date: str) -> list[HKConnectTargetItem]:
        if not self._settings.is_source_enabled("tushare_stocks"):
            return []
        return tushare_stocks.get_hk_connect_targets(direction, status, effective_date)

    def get_auctions(self, code: str, session: str, trade_date: str, start_date: str, end_date: str) -> list[AuctionItem]:
        if not self._settings.is_source_enabled("tushare_stocks"):
            return []
        return tushare_stocks.get_auctions(code, session, trade_date, start_date, end_date)


