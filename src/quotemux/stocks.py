from __future__ import annotations

from datetime import timedelta

import pandas as pd

from platform_models import AdjFactorItem, AuditItem, AuctionItem, BSECodeMappingItem, CcassHoldingDetailItem, CcassHoldingItem, ChipDistributionItem, ChipPerformanceItem, DisclosureDateItem, DividendItem, ExpressItem, ForecastItem, HKConnectHoldingItem, HKConnectTargetItem, HLSignalItem, MainBusinessItem, ManagementRewardItem, NameHistoryItem, NineTurnItem, PledgeDetailItem, PledgeStatItem, RepurchaseItem, ResearchReportItem, RightsIssueItem, ShareChangeItem, ShareholderChangeItem, ShareholderCountItem, ShareholderTop10Item, StockAHComparisonItem, StockArchiveItem, StockBasicInfo, StockDailyBasicItem, StockDailyMarketValueItem, StockDailyValuationItem, StockFinanceIndicatorItem, StockFinancialStatementItem, StockManagerItem, StockMoneyFlowItem, StockPremarketItem, StockProfileItem, StockQuoteItem, StockRiskFlagItem, SurveyItem, TechnicalFactorItem, UnlockScheduleItem
from quotemux.infra.common import add_quote_metrics, aggregate_ohlc, build_time_bounds, format_date_value, format_datetime_value, normalize_stock_code, parse_date_text
from quotemux.runtime_core.executor import ProviderStep, SourceInstanceExecutor, run_fallback_chain_with_report
from quotemux.infra.tushare.helpers import normalize_date_range
from quotemux.common import MARKET_DAILY_SNAPSHOT_LIMIT, build_missing_expected_date_ranges, ensure_limit, has_enough_stock_quote_rows, merge_model_lists, sort_items, trim_items_per_key
from quotemux.reports import ContractReport
from quotemux.requests.stocks import StockDailySnapshotRequest, StockQuotesRequest
from quotemux.store import load_store_result, store_result
from quotemux.runtime_core.registry import SourceProxy
from quotemux.settings import QuoteMuxSettings


MAX_DAILY_INDICATOR_CODES = 200
_akshare_provider = SourceProxy("akshare")
_efinance_provider = SourceProxy("efinance")
_mootdx_provider = SourceProxy("mootdx")
_opentdx_provider = SourceProxy("opentdx")
_tushare_provider = SourceProxy("tushare")


def _today_text() -> str:
    from datetime import datetime

    return datetime.now().strftime("%Y-%m-%d")


def _payloads_with_as_of_date(items: list[object]) -> list[dict[str, object]]:
    return [{**item.model_dump(), "as_of_date": _today_text()} for item in items if hasattr(item, "model_dump")]


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


def _rsi(series: pd.Series, period: int) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(period, min_periods=period).mean()
    avg_loss = loss.rolling(period, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, pd.NA)
    return 100 - 100 / (1 + rs)


def _build_technical_factor_items(quote_items: list[StockQuoteItem], adjust: str) -> list[TechnicalFactorItem]:
    if quote_items == []:
        return []
    frame = pd.DataFrame([item.model_dump() for item in quote_items])
    frame["trade_date"] = frame["trade_time"].astype(str)
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame["high"] = pd.to_numeric(frame["high"], errors="coerce")
    frame["low"] = pd.to_numeric(frame["low"], errors="coerce")
    frame = frame.sort_values("trade_date").reset_index(drop=True)
    frame["ma5"] = frame["close"].rolling(5, min_periods=5).mean()
    frame["ma10"] = frame["close"].rolling(10, min_periods=10).mean()
    frame["ma20"] = frame["close"].rolling(20, min_periods=20).mean()
    frame["ma60"] = frame["close"].rolling(60, min_periods=60).mean()
    frame["ema12"] = frame["close"].ewm(span=12, adjust=False).mean()
    frame["ema26"] = frame["close"].ewm(span=26, adjust=False).mean()
    frame["dif"] = frame["ema12"] - frame["ema26"]
    frame["dea"] = frame["dif"].ewm(span=9, adjust=False).mean()
    frame["macd"] = (frame["dif"] - frame["dea"]) * 2
    frame["rsi6"] = _rsi(frame["close"], 6)
    frame["rsi12"] = _rsi(frame["close"], 12)
    frame["rsi24"] = _rsi(frame["close"], 24)
    low_n = frame["low"].rolling(9, min_periods=9).min()
    high_n = frame["high"].rolling(9, min_periods=9).max()
    rsv = (frame["close"] - low_n) / (high_n - low_n).replace(0, pd.NA) * 100
    frame["kdj_k"] = rsv.ewm(com=2, adjust=False).mean()
    frame["kdj_d"] = frame["kdj_k"].ewm(com=2, adjust=False).mean()
    frame["kdj_j"] = 3 * frame["kdj_k"] - 2 * frame["kdj_d"]
    boll_mid = frame["close"].rolling(20, min_periods=20).mean()
    boll_std = frame["close"].rolling(20, min_periods=20).std()
    frame["boll_upper"] = boll_mid + 2 * boll_std
    frame["boll_mid"] = boll_mid
    frame["boll_lower"] = boll_mid - 2 * boll_std
    return [
        TechnicalFactorItem(
            code=str(row["code"]),
            trade_date=str(row["trade_date"]),
            adjust=adjust,
            ma5=float(row["ma5"]) if pd.notna(row["ma5"]) else None,
            ma10=float(row["ma10"]) if pd.notna(row["ma10"]) else None,
            ma20=float(row["ma20"]) if pd.notna(row["ma20"]) else None,
            ma60=float(row["ma60"]) if pd.notna(row["ma60"]) else None,
            ema12=float(row["ema12"]) if pd.notna(row["ema12"]) else None,
            ema26=float(row["ema26"]) if pd.notna(row["ema26"]) else None,
            dif=float(row["dif"]) if pd.notna(row["dif"]) else None,
            dea=float(row["dea"]) if pd.notna(row["dea"]) else None,
            macd=float(row["macd"]) if pd.notna(row["macd"]) else None,
            rsi6=float(row["rsi6"]) if pd.notna(row["rsi6"]) else None,
            rsi12=float(row["rsi12"]) if pd.notna(row["rsi12"]) else None,
            rsi24=float(row["rsi24"]) if pd.notna(row["rsi24"]) else None,
            kdj_k=float(row["kdj_k"]) if pd.notna(row["kdj_k"]) else None,
            kdj_d=float(row["kdj_d"]) if pd.notna(row["kdj_d"]) else None,
            kdj_j=float(row["kdj_j"]) if pd.notna(row["kdj_j"]) else None,
            boll_upper=float(row["boll_upper"]) if pd.notna(row["boll_upper"]) else None,
            boll_mid=float(row["boll_mid"]) if pd.notna(row["boll_mid"]) else None,
            boll_lower=float(row["boll_lower"]) if pd.notna(row["boll_lower"]) else None,
        )
        for _, row in frame.iterrows()
    ]


def _build_shareholder_change_items(items: list[ShareholderCountItem]) -> list[ShareholderChangeItem]:
    rows: list[ShareholderChangeItem] = []
    previous_count: int | None = None
    for item in sorted(items, key=lambda value: value.trade_date):
        change_count = item.holder_count - previous_count if item.holder_count is not None and previous_count is not None else None
        change_pct = None
        if change_count is not None and previous_count not in {None, 0}:
            change_pct = change_count / previous_count * 100
        rows.append(
            ShareholderChangeItem(
                code=item.code,
                trade_date=item.trade_date,
                holder_count=item.holder_count,
                change_count=change_count,
                change_pct=change_pct,
            )
        )
        previous_count = item.holder_count
    return rows


def _build_hl_signal_items(code: str, quote_items: list[StockQuoteItem]) -> list[HLSignalItem]:
    if quote_items == []:
        return []
    frame = pd.DataFrame([item.model_dump() for item in quote_items])
    frame["trade_time_dt"] = pd.to_datetime(frame["trade_time"], errors="coerce")
    frame["high"] = pd.to_numeric(frame["high"], errors="coerce")
    frame["low"] = pd.to_numeric(frame["low"], errors="coerce")
    frame = frame.dropna(subset=["trade_time_dt", "high", "low"])
    if frame.empty:
        return []
    frame["trade_date"] = frame["trade_time_dt"].dt.strftime("%Y-%m-%d")
    items: list[HLSignalItem] = []
    for trade_date, group in frame.groupby("trade_date", sort=True):
        max_high = group["high"].max()
        min_low = group["low"].min()
        high_rows = group[group["high"] == max_high].sort_values("trade_time_dt")
        low_rows = group[group["low"] == min_low].sort_values("trade_time_dt")
        high_dt = high_rows.iloc[0]["trade_time_dt"] if not high_rows.empty else None
        low_dt = low_rows.iloc[0]["trade_time_dt"] if not low_rows.empty else None
        high_time = high_dt.strftime("%H:%M:%S") if pd.notna(high_dt) else ""
        low_time = low_dt.strftime("%H:%M:%S") if pd.notna(low_dt) else ""
        if high_time and low_time and high_dt < low_dt:
            first_extreme = "high"
            signal = "high_first"
        elif high_time and low_time and low_dt < high_dt:
            first_extreme = "low"
            signal = "low_first"
        else:
            first_extreme = ""
            signal = "same_time"
        items.append(HLSignalItem(code=code, trade_date=str(trade_date), first_extreme=first_extreme, high_time=high_time, low_time=low_time, signal=signal))
    return items


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
    del trade_date
    del items
    return []


def _build_steps(freq: str, request_freq: str, request_count: int | None, actual_adjust: str, settings: QuoteMuxSettings) -> tuple[ProviderStep[StockQuoteItem], ...]:
    handlers = {
        "get_stock_quotes": lambda instance: lambda missing_codes, missing_start, missing_end: {
            "efinance": _efinance_provider,
            "mootdx": _mootdx_provider,
            "akshare": _akshare_provider,
            "opentdx": _opentdx_provider,
            "tushare": _tushare_provider,
        }[instance.package_id].get_stock_quotes(missing_codes, request_freq, "", missing_start, missing_end, "", "", request_count, actual_adjust),
    }
    if freq == "1d":
        fallback_order = ("tushare", "efinance", "mootdx", "akshare", "opentdx")
    else:
        fallback_order = ("opentdx", "efinance", "mootdx", "akshare")
    return SourceInstanceExecutor(settings).build_steps("stocks.quotes.daily" if freq == "1d" else "stocks.quotes.intraday", handlers, fallback_order)


def _build_daily_snapshot_steps(settings: QuoteMuxSettings) -> tuple[ProviderStep[StockQuoteItem], ...]:
    handlers = {
        "get_stock_daily_snapshot_full": lambda instance: lambda missing_codes, request_trade_date: {
            "tushare": _tushare_provider,
            "efinance": _efinance_provider,
            "akshare": _akshare_provider,
        }[instance.package_id].get_stock_daily_snapshot_full(request_trade_date),
        "get_stock_quotes": lambda instance: lambda missing_codes, request_trade_date: {
            "tushare": _tushare_provider,
            "efinance": _efinance_provider,
            "mootdx": _mootdx_provider,
            "akshare": _akshare_provider,
        }[instance.package_id].get_stock_quotes(missing_codes, "1d", request_trade_date, "", "", "", "", None, "none"),
    }
    return SourceInstanceExecutor(settings).build_steps("stocks.quotes.daily_snapshot", handlers, ("tushare", "efinance", "akshare", "mootdx"))


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
        merged_items = merge_model_lists(store_items if store_read.partial_hit else [], fetched_items, unique_fields)
        sorted_items = sort_items(merged_items, sort_fields) if sort_fields else merged_items
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

    def _store_single(self, capability_id: str, store_identity: dict[str, object], model_type: type[object], fetcher, payload_builder=None):
        store_items, store_read = load_store_result(capability_id, store_identity, model_type)
        if store_read.hit:
            return store_items[0] if store_items else None
        item = fetcher()
        payload_items = [item] if item is not None else []
        if payload_builder is not None:
            payload_items = payload_builder(payload_items)
        store_result(capability_id, store_identity, payload_items, ContractReport(contract_name=capability_id))
        return item

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
        store_enabled = actual_freq not in {"1w", "1mo"}
        store_identity = {
            "codes": list(request.codes),
            "freq": actual_freq,
            "trade_date": request.trade_date,
            "start_date": request.start_date,
            "end_date": request.end_date,
            "start_time": request.start_time,
            "end_time": request.end_time,
            "count": request.count,
            "adjust": actual_adjust,
        }
        store_items: list[StockQuoteItem] = []
        store_status = "skip"
        if store_enabled:
            store_items, store_read = load_store_result(contract_name, store_identity, StockQuoteItem)
            store_status = store_read.status
            if store_read.hit:
                if actual_freq in {"1w", "1mo"}:
                    store_items = _aggregate_stock_quotes(store_items, actual_freq, actual_adjust)
                trimmed_items = trim_items_per_key(store_items, "code", "trade_time", request.count)
                sorted_items = sort_items(trimmed_items, ("code", "trade_time"))
                from quotemux.config_runtime.runtime import get_config_runtime

                active_snapshot = get_config_runtime().get_active_snapshot()
                return sorted_items[:actual_limit], ContractReport(
                    contract_name=contract_name,
                    profile_id=active_snapshot.profile_id,
                    profile_version=active_snapshot.version,
                ).with_store_stats(hit=True)
        merged_items, fallback_report = run_fallback_chain_with_report(
            contract_name,
            store_items if store_status == "partial_hit" else [],
            ("code", "trade_time", "freq"),
            lambda items: _build_missing_quote_requests(request.codes, items, request_freq, request.trade_date, request.start_date, request.end_date, request.start_time, request.end_time, request_count),
            _build_steps(request_freq, request_freq, request_count, actual_adjust, self._settings),
            self._settings.get_contract_source_order(contract_name, ("tushare", "efinance", "mootdx", "akshare", "opentdx")),
        )
        if actual_freq in {"1w", "1mo"}:
            merged_items = _aggregate_stock_quotes(merged_items, actual_freq, actual_adjust)
        trimmed_items = trim_items_per_key(merged_items, "code", "trade_time", request.count)
        sorted_items = sort_items(trimmed_items, ("code", "trade_time"))
        report = ContractReport.from_fallback_report(contract_name, fallback_report)
        if store_enabled:
            store_write = store_result(contract_name, store_identity, merged_items, report, report.quarantine_count)
            report = report.with_store_stats(partial_hit=store_status == "partial_hit", miss=store_status in {"miss", "skip"}, stale=store_status == "stale", write=store_write.status == "write")
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
        store_identity = {
            "trade_date": actual_trade_date,
        }
        store_items, store_read = load_store_result("stocks.quotes.daily_snapshot", store_identity, StockQuoteItem)
        if store_read.hit:
            sorted_items = sort_items(store_items, ("code", "trade_time"))
            from quotemux.config_runtime.runtime import get_config_runtime

            active_snapshot = get_config_runtime().get_active_snapshot()
            return (
                sorted_items[request.offset: request.offset + request.limit],
                ContractReport(
                    contract_name="stocks.quotes.daily_snapshot",
                    profile_id=active_snapshot.profile_id,
                    profile_version=active_snapshot.version,
                ).with_store_stats(hit=True),
            )
        merged_items, fallback_report = run_fallback_chain_with_report(
            "stocks.quotes.daily_snapshot",
            store_items if store_read.partial_hit else [],
            ("code", "trade_time", "freq"),
            lambda items: [([], actual_trade_date)] if items == [] else [],
            _build_daily_snapshot_steps(self._settings),
            self._settings.get_contract_source_order("stocks.quotes.daily_snapshot", ("tushare", "efinance", "akshare", "mootdx")),
        )
        sorted_items = sort_items(merged_items, ("code", "trade_time"))
        report = ContractReport.from_fallback_report("stocks.quotes.daily_snapshot", fallback_report)
        store_write = store_result("stocks.quotes.daily_snapshot", store_identity, sorted_items, report, report.quarantine_count)
        report = report.with_store_stats(partial_hit=store_read.partial_hit, miss=store_read.status in {"miss", "skip"}, stale=store_read.status == "stale", write=store_write.status == "write")
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
        existing_dates = {item.trade_date for item in items}
        missing_ranges = build_missing_expected_date_ranges(expected_trade_dates, existing_dates)
        if missing_ranges == [] and expected_trade_dates == []:
            return _build_missing_date_ranges(actual_start_date, actual_end_date, existing_dates)
        return missing_ranges

    def get_money_flow(self, code: str, trade_date: str, start_date: str, end_date: str, view: str) -> list[StockMoneyFlowItem]:
        handlers = {
            "get_stock_money_flow": lambda instance: lambda missing_start, missing_end: {
                "tushare": _tushare_provider,
                "akshare": _akshare_provider,
            }[instance.package_id].get_stock_money_flow(code, "", missing_start, missing_end, view),
        }
        merged_items, _ = run_fallback_chain_with_report(
            "stocks.indicators.money_flow",
            [],
            ("code", "trade_date", "view"),
            lambda items: self._build_missing_money_flow_requests(items, trade_date, start_date, end_date),
            SourceInstanceExecutor(self._settings).build_steps("stocks.indicators.money_flow", handlers, ("tushare", "akshare")),
            self._settings.get_contract_source_order("stocks.indicators.money_flow", ("tushare", "akshare")),
        )
        return sorted(merged_items, key=lambda item: (item.code, item.trade_date))

    def get_financial_statements(self, codes: list[str], report_period: str, start_period: str, end_period: str, report_type: str) -> list[StockFinancialStatementItem]:
        store_identity = {
            "codes": list(codes),
            "report_period": report_period,
            "start_period": start_period,
            "end_period": end_period,
            "report_type": report_type,
        }
        handlers = {
            "get_stock_financial_statements": lambda instance: lambda: {
                "tushare": _tushare_provider,
                "akshare": _akshare_provider,
            }[instance.package_id].get_stock_financial_statements(codes, report_period, start_period, end_period, report_type),
        }
        return self._store_list(
            "stocks.finance.statements",
            store_identity,
            StockFinancialStatementItem,
            ("code", "report_period", "report_type", "announce_date"),
            ("code", "report_period", "announce_date", "report_type"),
            lambda: self._source_list("stocks.finance.statements", handlers, ("tushare", "akshare"), ("code", "report_period", "report_type", "announce_date")),
        )

    def get_finance_indicators(self, code: str, codes: str, report_period: str, start_period: str, end_period: str) -> list[StockFinanceIndicatorItem]:
        store_identity = {
            "code": code,
            "codes": codes,
            "report_period": report_period,
            "start_period": start_period,
            "end_period": end_period,
        }
        handlers = {
            "get_stock_finance_indicators": lambda instance: lambda: {
                "tushare": _tushare_provider,
                "akshare": _akshare_provider,
                "efinance": _efinance_provider,
            }[instance.package_id].get_stock_finance_indicators(code, codes, report_period, start_period, end_period),
        }
        return self._store_list(
            "stocks.finance.indicators",
            store_identity,
            StockFinanceIndicatorItem,
            ("code", "report_period"),
            ("code", "report_period"),
            lambda: self._source_list("stocks.finance.indicators", handlers, ("tushare", "akshare", "efinance"), ("code", "report_period")),
        )

    def get_catalog(self, codes: list[str], name: str, exchange: str, list_status: str, include_delisted: bool, limit: int, offset: int) -> list[StockBasicInfo]:
        store_identity = {"codes": list(codes), "name": name, "exchange": exchange, "list_status": list_status, "include_delisted": include_delisted, "limit": limit, "offset": offset}
        store_items, store_read = load_store_result("stocks.catalog", store_identity, StockBasicInfo)
        if store_read.hit:
            return store_items
        if not self._settings.is_source_enabled("tushare"):
            return []
        items = _tushare_provider.get_stock_catalog(codes, name, exchange, list_status, include_delisted, ensure_limit(limit), offset)
        store_result("stocks.catalog", store_identity, items, ContractReport(contract_name="stocks.catalog"))
        return items

    def get_archive(self, trade_date: str, code: str, name: str, industry: str, area: str, limit: int, offset: int) -> list[StockArchiveItem]:
        if not self._settings.is_source_enabled("tushare"):
            return []
        return _tushare_provider.get_stock_archive(trade_date, code, name, industry, area, ensure_limit(limit), offset)

    def get_basic(self, code: str) -> StockBasicInfo | None:
        store_identity = {"code": code}
        store_items, store_read = load_store_result("stocks.profile.basic", store_identity, StockBasicInfo)
        if store_read.hit:
            return store_items[0] if store_items else None
        if not self._settings.is_source_enabled("tushare"):
            return None
        item = _tushare_provider.get_stock_basic(code)
        store_result("stocks.profile.basic", store_identity, [item] if item is not None else [], ContractReport(contract_name="stocks.profile.basic"))
        return item

    def get_profile(self, code: str) -> StockProfileItem | None:
        store_identity = {"code": code}
        handlers = {
            "get_company_profile": lambda instance: lambda: [
                item
                for item in [
                    {
                        "tushare": _tushare_provider,
                        "akshare": _akshare_provider,
                    }[instance.package_id].get_company_profile(code)
                ]
                if item is not None
            ],
        }
        items = self._store_list(
            "stocks.profile.company",
            store_identity,
            StockProfileItem,
            ("code",),
            ("code",),
            lambda: self._source_list("stocks.profile.company", handlers, ("tushare", "akshare"), ("code",)),
            _payloads_with_as_of_date,
        )
        return items[0] if items else None

    def get_name_history(self, code: str, start_date: str, end_date: str) -> list[NameHistoryItem]:
        store_identity = {"code": code, "start_date": start_date, "end_date": end_date}
        store_items, store_read = load_store_result("stocks.profile.name_history", store_identity, NameHistoryItem)
        if store_read.hit:
            return store_items
        if not self._settings.is_source_enabled("tushare"):
            return []
        items = _tushare_provider.get_stock_name_history(code, start_date, end_date)
        store_result("stocks.profile.name_history", store_identity, items, ContractReport(contract_name="stocks.profile.name_history"))
        return items

    def get_managers(self, code: str) -> list[StockManagerItem]:
        if not self._settings.is_source_enabled("tushare"):
            return []
        return _tushare_provider.get_managers(code)

    def get_management_rewards(self, code: str, start_date: str, end_date: str) -> list[ManagementRewardItem]:
        if not self._settings.is_source_enabled("tushare"):
            return []
        return _tushare_provider.get_management_rewards(code, start_date, end_date)

    def get_hl_signal(self, code: str, trade_date: str, start_date: str, end_date: str) -> list[HLSignalItem]:
        normalized = normalize_stock_code(code)
        if normalized == "":
            return []
        provider_map = {
            "opentdx": _opentdx_provider,
            "efinance": _efinance_provider,
            "mootdx": _mootdx_provider,
            "akshare": _akshare_provider,
        }
        for source_name in ("opentdx", "efinance", "mootdx", "akshare"):
            if not self._settings.is_source_enabled(source_name):
                continue
            quote_items = provider_map[source_name].get_stock_quotes([normalized], "1m", trade_date, start_date, end_date, "", "", None, "none")
            signal_items = _build_hl_signal_items(normalized, quote_items)
            if signal_items:
                return signal_items
        return []

    def get_nine_turn(self, code: str, freq: str, trade_date: str, start_date: str, end_date: str) -> list[NineTurnItem]:
        if not self._settings.is_source_enabled("tushare"):
            return []
        return _tushare_provider.get_nine_turn(code, freq, trade_date, start_date, end_date)

    def get_adj_factors(self, code: str, start_date: str, end_date: str, base_date: str) -> list[AdjFactorItem]:
        store_identity = {"code": code, "start_date": start_date, "end_date": end_date, "base_date": base_date}
        store_items, store_read = load_store_result("stocks.factors.adj", store_identity, AdjFactorItem)
        if store_read.hit:
            return store_items
        if not self._settings.is_source_enabled("tushare"):
            return []
        items = _tushare_provider.get_adj_factors(code, start_date, end_date, base_date)
        store_result("stocks.factors.adj", store_identity, items, ContractReport(contract_name="stocks.factors.adj"))
        return items

    def get_technical_factors(self, code: str, trade_date: str, start_date: str, end_date: str, adjust: str) -> list[TechnicalFactorItem]:
        normalized = normalize_stock_code(code)
        if normalized == "":
            return []
        quote_items = self.get_quotes(
            StockQuotesRequest(
                codes=[normalized],
                freq="1d",
                trade_date=trade_date,
                start_date=start_date,
                end_date=end_date,
                adjust=adjust,
                limit=5000,
            )
        )
        return _build_technical_factor_items(quote_items, adjust)

    def get_ah_comparisons(self, code: str, trade_date: str, start_date: str, end_date: str, limit: int, offset: int) -> list[StockAHComparisonItem]:
        if not self._settings.is_source_enabled("tushare"):
            return []
        return _tushare_provider.get_stock_ah_comparisons(code, trade_date, start_date, end_date, ensure_limit(limit), offset)

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
        if not self._settings.is_source_enabled("tushare"):
            return []
        actual_codes, actual_trade_date, actual_start, actual_end = self._resolve_indicator_request(code, codes, trade_date, start_date, end_date)
        if actual_codes == []:
            items = _tushare_provider.get_stock_daily_basic("", "", actual_trade_date, "", "")
            return sorted(items, key=lambda item: (item.code, item.trade_date))
        ts_items = _tushare_provider.get_stock_daily_basic(code, codes, actual_trade_date, actual_start, actual_end)
        return sorted(ts_items, key=lambda item: (item.code, item.trade_date))

    def get_daily_valuation(self, code: str, codes: str, trade_date: str, start_date: str, end_date: str) -> list[StockDailyValuationItem]:
        if not self._settings.is_source_enabled("tushare"):
            return []
        actual_codes, actual_trade_date, actual_start, actual_end = self._resolve_indicator_request(code, codes, trade_date, start_date, end_date)
        if actual_codes == []:
            items = _tushare_provider.get_stock_daily_valuation("", "", actual_trade_date, "", "")
            return sorted(items, key=lambda item: (item.code, item.trade_date))
        ts_items = _tushare_provider.get_stock_daily_valuation(code, codes, actual_trade_date, actual_start, actual_end)
        return sorted(ts_items, key=lambda item: (item.code, item.trade_date))

    def get_daily_market_value(self, code: str, codes: str, trade_date: str, start_date: str, end_date: str) -> list[StockDailyMarketValueItem]:
        if not self._settings.is_source_enabled("tushare"):
            return []
        actual_codes, actual_trade_date, actual_start, actual_end = self._resolve_indicator_request(code, codes, trade_date, start_date, end_date)
        if actual_codes == []:
            items = _tushare_provider.get_stock_daily_market_value("", "", actual_trade_date, "", "")
            return sorted(items, key=lambda item: (item.code, item.trade_date))
        ts_items = _tushare_provider.get_stock_daily_market_value(code, codes, actual_trade_date, actual_start, actual_end)
        return sorted(ts_items, key=lambda item: (item.code, item.trade_date))

    def get_risk_flags(self, trade_date: str, start_date: str, end_date: str, flag_type: str, status: str, limit: int, offset: int) -> list[StockRiskFlagItem]:
        if not self._settings.is_source_enabled("tushare"):
            return []
        return _tushare_provider.get_stock_risk_flags(trade_date, start_date, end_date, flag_type, status, ensure_limit(limit), offset)

    def get_premarket(self, code: str, trade_date: str, start_date: str, end_date: str) -> list[StockPremarketItem]:
        if not self._settings.is_source_enabled("tushare"):
            return []
        return _tushare_provider.get_premarket(code, trade_date, start_date, end_date)

    def get_chip_distribution(self, code: str, trade_date: str, start_date: str, end_date: str) -> list[ChipDistributionItem]:
        if not self._settings.is_source_enabled("tushare"):
            return []
        return _tushare_provider.get_chip_distribution(code, trade_date, start_date, end_date)

    def get_chip_performance(self, code: str, trade_date: str, start_date: str, end_date: str) -> list[ChipPerformanceItem]:
        if not self._settings.is_source_enabled("tushare"):
            return []
        return _tushare_provider.get_chip_performance(code, trade_date, start_date, end_date)

    def get_dividends(self, code: str, start_date: str, end_date: str) -> list[DividendItem]:
        store_identity = {"code": code, "start_date": start_date, "end_date": end_date}
        handlers = {
            "get_dividends": lambda instance: lambda: {
                "tushare": _tushare_provider,
                "akshare": _akshare_provider,
            }[instance.package_id].get_dividends(code, start_date, end_date),
        }
        return self._store_list(
            "stocks.corporate_actions.dividends",
            store_identity,
            DividendItem,
            ("code", "announce_date", "record_date", "ex_date"),
            ("code", "announce_date", "record_date", "ex_date"),
            lambda: self._source_list("stocks.corporate_actions.dividends", handlers, ("tushare", "akshare"), ("code", "announce_date", "record_date", "ex_date")),
        )

    def get_repurchases(self, code: str, start_date: str, end_date: str) -> list[RepurchaseItem]:
        store_identity = {"code": code, "start_date": start_date, "end_date": end_date}
        handlers = {
            "get_repurchases": lambda instance: lambda: {
                "tushare": _tushare_provider,
                "akshare": _akshare_provider,
            }[instance.package_id].get_repurchases(code, start_date, end_date),
        }
        return self._store_list(
            "stocks.corporate_actions.repurchases",
            store_identity,
            RepurchaseItem,
            ("code", "announce_date", "progress"),
            ("code", "announce_date", "progress"),
            lambda: self._source_list("stocks.corporate_actions.repurchases", handlers, ("tushare", "akshare"), ("code", "announce_date", "progress")),
        )

    def get_rights_issues(self, code: str, start_date: str, end_date: str) -> list[RightsIssueItem]:
        store_identity = {"code": code, "start_date": start_date, "end_date": end_date}
        handlers = {
            "get_rights_issues": lambda instance: lambda: {
                "tushare": _tushare_provider,
                "akshare": _akshare_provider,
            }[instance.package_id].get_rights_issues(code, start_date, end_date),
        }
        return self._store_list(
            "stocks.corporate_actions.rights_issues",
            store_identity,
            RightsIssueItem,
            ("code", "announce_date", "record_date"),
            ("code", "announce_date", "record_date"),
            lambda: self._source_list("stocks.corporate_actions.rights_issues", handlers, ("tushare", "akshare"), ("code", "announce_date", "record_date")),
        )

    def get_share_changes(self, code: str, trade_date: str, start_date: str, end_date: str) -> list[ShareChangeItem]:
        store_identity = {"code": code, "trade_date": trade_date, "start_date": start_date, "end_date": end_date}
        handlers = {
            "get_share_changes": lambda instance: lambda: {
                "tushare": _tushare_provider,
                "akshare": _akshare_provider,
            }[instance.package_id].get_share_changes(code, trade_date, start_date, end_date),
        }
        return self._store_list(
            "stocks.corporate_actions.share_changes",
            store_identity,
            ShareChangeItem,
            ("code", "change_date", "reason"),
            ("code", "change_date", "reason"),
            lambda: self._source_list("stocks.corporate_actions.share_changes", handlers, ("tushare", "akshare"), ("code", "change_date", "reason")),
        )

    def get_unlock_schedules(self, code: str, unlock_date: str, start_date: str, end_date: str) -> list[UnlockScheduleItem]:
        store_identity = {"code": code, "unlock_date": unlock_date, "start_date": start_date, "end_date": end_date}
        handlers = {
            "get_unlock_schedules": lambda instance: lambda: {
                "tushare": _tushare_provider,
                "akshare": _akshare_provider,
            }[instance.package_id].get_unlock_schedules(code, unlock_date, start_date, end_date),
        }
        return self._store_list(
            "stocks.corporate_actions.unlock_schedules",
            store_identity,
            UnlockScheduleItem,
            ("code", "unlock_date", "holder_type", "share_type"),
            ("code", "unlock_date", "holder_type", "share_type"),
            lambda: self._source_list("stocks.corporate_actions.unlock_schedules", handlers, ("tushare", "akshare"), ("code", "unlock_date", "holder_type", "share_type")),
        )

    def get_audits(self, code: str, report_period: str, start_period: str, end_period: str) -> list[AuditItem]:
        store_identity = {"code": code, "report_period": report_period, "start_period": start_period, "end_period": end_period}
        return self._store_list(
            "stocks.finance.audits",
            store_identity,
            AuditItem,
            ("code", "report_period", "announce_date"),
            ("code", "report_period", "announce_date"),
            lambda: [] if not self._settings.is_source_enabled("tushare") else _tushare_provider.get_audits(code, report_period, start_period, end_period),
        )

    def get_disclosure_dates(self, code: str, report_period: str, start_period: str, end_period: str) -> list[DisclosureDateItem]:
        store_identity = {"code": code, "report_period": report_period, "start_period": start_period, "end_period": end_period}
        handlers = {
            "get_disclosure_dates": lambda instance: lambda: {
                "tushare": _tushare_provider,
                "akshare": _akshare_provider,
            }[instance.package_id].get_disclosure_dates(code, report_period, start_period, end_period),
        }
        return self._store_list(
            "stocks.finance.disclosure_dates",
            store_identity,
            DisclosureDateItem,
            ("code", "report_period", "plan_date", "actual_date"),
            ("code", "report_period", "plan_date", "actual_date"),
            lambda: self._source_list("stocks.finance.disclosure_dates", handlers, ("tushare", "akshare"), ("code", "report_period", "plan_date", "actual_date")),
        )

    def get_express(self, code: str, report_period: str, start_period: str, end_period: str) -> list[ExpressItem]:
        store_identity = {"code": code, "report_period": report_period, "start_period": start_period, "end_period": end_period}
        handlers = {
            "get_express": lambda instance: lambda: {
                "tushare": _tushare_provider,
                "akshare": _akshare_provider,
                "efinance": _efinance_provider,
            }[instance.package_id].get_express(code, report_period, start_period, end_period),
        }
        return self._store_list(
            "stocks.finance.express",
            store_identity,
            ExpressItem,
            ("code", "report_period", "announce_date"),
            ("code", "report_period", "announce_date"),
            lambda: self._source_list("stocks.finance.express", handlers, ("tushare", "akshare", "efinance"), ("code", "report_period", "announce_date")),
        )

    def get_forecasts(self, code: str, report_period: str, start_period: str, end_period: str) -> list[ForecastItem]:
        store_identity = {"code": code, "report_period": report_period, "start_period": start_period, "end_period": end_period}
        handlers = {
            "get_forecasts": lambda instance: lambda: {
                "tushare": _tushare_provider,
                "akshare": _akshare_provider,
            }[instance.package_id].get_forecasts(code, report_period, start_period, end_period),
        }
        return self._store_list(
            "stocks.finance.forecasts",
            store_identity,
            ForecastItem,
            ("code", "report_period", "forecast_type"),
            ("code", "report_period", "forecast_type"),
            lambda: self._source_list("stocks.finance.forecasts", handlers, ("tushare", "akshare"), ("code", "report_period", "forecast_type")),
        )

    def get_main_business(self, code: str, report_period: str, start_period: str, end_period: str, classification: str) -> list[MainBusinessItem]:
        store_identity = {
            "code": code,
            "report_period": report_period,
            "start_period": start_period,
            "end_period": end_period,
            "classification": classification,
        }
        return self._store_list(
            "stocks.finance.main_business",
            store_identity,
            MainBusinessItem,
            ("code", "report_period", "classification", "segment_name"),
            ("code", "report_period", "classification", "segment_name"),
            lambda: self._source_list(
                "stocks.finance.main_business",
                {
                    "get_main_business": lambda instance: lambda: {
                        "tushare": _tushare_provider,
                        "akshare": _akshare_provider,
                    }[instance.package_id].get_main_business(code, report_period, start_period, end_period, classification),
                },
                ("tushare", "akshare"),
                ("code", "report_period", "classification", "segment_name"),
            ),
        )

    def get_ccass_holdings(self, code: str, trade_date: str, start_date: str, end_date: str) -> list[CcassHoldingItem]:
        store_identity = {"code": code, "trade_date": trade_date, "start_date": start_date, "end_date": end_date}
        return self._store_list(
            "stocks.ownership.ccass_holdings",
            store_identity,
            CcassHoldingItem,
            ("code", "trade_date"),
            ("code", "trade_date"),
            lambda: [] if not self._settings.is_source_enabled("tushare") else _tushare_provider.get_ccass_holdings(code, trade_date, start_date, end_date),
        )

    def get_ccass_holding_details(self, code: str, trade_date: str, start_date: str, end_date: str) -> list[CcassHoldingDetailItem]:
        store_identity = {"code": code, "trade_date": trade_date, "start_date": start_date, "end_date": end_date}
        return self._store_list(
            "stocks.ownership.ccass_holding_details",
            store_identity,
            CcassHoldingDetailItem,
            ("code", "trade_date", "participant_id"),
            ("code", "trade_date", "participant_id"),
            lambda: [] if not self._settings.is_source_enabled("tushare") else _tushare_provider.get_ccass_holding_details(code, trade_date, start_date, end_date),
        )

    def get_hk_connect_holdings(self, code: str, trade_date: str, start_date: str, end_date: str) -> list[HKConnectHoldingItem]:
        store_identity = {"code": code, "trade_date": trade_date, "start_date": start_date, "end_date": end_date}
        handlers = {
            "get_hk_connect_holdings": lambda instance: lambda: {
                "tushare": _tushare_provider,
                "akshare": _akshare_provider,
            }[instance.package_id].get_hk_connect_holdings(code, trade_date, start_date, end_date),
        }
        return self._store_list(
            "stocks.ownership.hk_connect_holdings",
            store_identity,
            HKConnectHoldingItem,
            ("code", "trade_date"),
            ("code", "trade_date"),
            lambda: self._source_list("stocks.ownership.hk_connect_holdings", handlers, ("tushare", "akshare"), ("code", "trade_date")),
        )

    def get_pledge_stats(self, code: str, trade_date: str, start_date: str, end_date: str) -> list[PledgeStatItem]:
        store_identity = {"code": code, "trade_date": trade_date, "start_date": start_date, "end_date": end_date}
        handlers = {
            "get_pledge_stats": lambda instance: lambda: {
                "tushare": _tushare_provider,
                "akshare": _akshare_provider,
            }[instance.package_id].get_pledge_stats(code, trade_date, start_date, end_date),
        }
        return self._store_list(
            "stocks.ownership.pledges.stats",
            store_identity,
            PledgeStatItem,
            ("code", "trade_date"),
            ("code", "trade_date"),
            lambda: self._source_list("stocks.ownership.pledges.stats", handlers, ("tushare", "akshare"), ("code", "trade_date")),
        )

    def get_pledge_details(self, code: str, start_date: str, end_date: str, status: str) -> list[PledgeDetailItem]:
        store_identity = {"code": code, "start_date": start_date, "end_date": end_date, "status": status}
        handlers = {
            "get_pledge_details": lambda instance: lambda: {
                "tushare": _tushare_provider,
                "akshare": _akshare_provider,
            }[instance.package_id].get_pledge_details(code, start_date, end_date, status),
        }
        return self._store_list(
            "stocks.ownership.pledges.details",
            store_identity,
            PledgeDetailItem,
            ("code", "holder_name", "start_date", "end_date", "status"),
            ("code", "start_date", "end_date", "holder_name"),
            lambda: self._source_list("stocks.ownership.pledges.details", handlers, ("tushare", "akshare"), ("code", "holder_name", "start_date", "end_date", "status")),
        )

    def get_shareholder_count(self, code: str, trade_date: str, start_date: str, end_date: str) -> list[ShareholderCountItem]:
        store_identity = {"code": code, "trade_date": trade_date, "start_date": start_date, "end_date": end_date}
        handlers = {
            "get_shareholder_count": lambda instance: lambda: {
                "tushare": _tushare_provider,
                "akshare": _akshare_provider,
                "efinance": _efinance_provider,
            }[instance.package_id].get_shareholder_count(code, trade_date, start_date, end_date),
        }
        return self._store_list(
            "stocks.ownership.shareholders.count",
            store_identity,
            ShareholderCountItem,
            ("code", "trade_date"),
            ("code", "trade_date"),
            lambda: self._source_list("stocks.ownership.shareholders.count", handlers, ("tushare", "akshare", "efinance"), ("code", "trade_date")),
        )

    def get_shareholder_changes(self, code: str, trade_date: str, start_date: str, end_date: str) -> list[ShareholderChangeItem]:
        count_items = self.get_shareholder_count(code, trade_date, start_date, end_date)
        return _build_shareholder_change_items(count_items)

    def get_shareholder_top10(self, code: str, report_period: str, start_period: str, end_period: str) -> list[ShareholderTop10Item]:
        store_identity = {"code": code, "report_period": report_period, "start_period": start_period, "end_period": end_period}
        handlers = {
            "get_shareholder_top10": lambda instance: lambda: {
                "tushare": _tushare_provider,
                "akshare": _akshare_provider,
            }[instance.package_id].get_shareholder_top10(code, report_period, start_period, end_period, False),
        }
        return self._store_list(
            "stocks.ownership.shareholders.top10",
            store_identity,
            ShareholderTop10Item,
            ("code", "report_period", "rank", "shareholder_name"),
            ("code", "report_period", "rank"),
            lambda: self._source_list("stocks.ownership.shareholders.top10", handlers, ("tushare", "akshare"), ("code", "report_period", "rank", "shareholder_name")),
        )

    def get_shareholder_top10_float(self, code: str, report_period: str, start_period: str, end_period: str) -> list[ShareholderTop10Item]:
        store_identity = {"code": code, "report_period": report_period, "start_period": start_period, "end_period": end_period}
        handlers = {
            "get_shareholder_top10": lambda instance: lambda: {
                "tushare": _tushare_provider,
                "akshare": _akshare_provider,
            }[instance.package_id].get_shareholder_top10(code, report_period, start_period, end_period, True),
        }
        return self._store_list(
            "stocks.ownership.shareholders.top10_float",
            store_identity,
            ShareholderTop10Item,
            ("code", "report_period", "rank", "shareholder_name"),
            ("code", "report_period", "rank"),
            lambda: self._source_list("stocks.ownership.shareholders.top10_float", handlers, ("tushare", "akshare"), ("code", "report_period", "rank", "shareholder_name")),
        )

    def get_research_reports(self, code: str, report_date: str, start_date: str, end_date: str):
        store_identity = {"code": code, "report_date": report_date, "start_date": start_date, "end_date": end_date}
        handlers = {
            "get_research_reports": lambda instance: lambda: {
                "tushare": _tushare_provider,
                "akshare": _akshare_provider,
            }[instance.package_id].get_research_reports(code, report_date, start_date, end_date),
        }
        return self._store_list(
            "stocks.research.reports",
            store_identity,
            ResearchReportItem,
            ("code", "report_date", "institution", "title"),
            ("code", "report_date", "institution", "title"),
            lambda: self._source_list("stocks.research.reports", handlers, ("tushare", "akshare"), ("code", "report_date", "institution", "title")),
        )

    def get_surveys(self, code: str, survey_date: str, start_date: str, end_date: str) -> list[SurveyItem]:
        store_identity = {"code": code, "survey_date": survey_date, "start_date": start_date, "end_date": end_date}
        handlers = {
            "get_surveys": lambda instance: lambda: {
                "tushare": _tushare_provider,
                "akshare": _akshare_provider,
            }[instance.package_id].get_surveys(code, survey_date, start_date, end_date),
        }
        return self._store_list(
            "stocks.research.surveys",
            store_identity,
            SurveyItem,
            ("code", "survey_date", "org_name", "announcement_date"),
            ("code", "survey_date", "announcement_date", "org_name"),
            lambda: self._source_list("stocks.research.surveys", handlers, ("tushare", "akshare"), ("code", "survey_date", "org_name", "announcement_date")),
        )

    def get_bse_code_mappings(self, old_code: str, new_code: str, status: str) -> list[BSECodeMappingItem]:
        store_identity = {"old_code": old_code, "new_code": new_code, "status": status}
        return self._store_list(
            "stocks.reference.bse_code_mappings",
            store_identity,
            BSECodeMappingItem,
            ("old_code", "new_code", "effective_date"),
            ("effective_date", "old_code", "new_code"),
            lambda: [] if not self._settings.is_source_enabled("tushare") else _tushare_provider.get_bse_code_mappings(old_code, new_code, status),
        )

    def get_hk_connect_targets(self, direction: str, status: str, effective_date: str) -> list[HKConnectTargetItem]:
        store_identity = {"direction": direction, "status": status, "effective_date": effective_date}
        return self._store_list(
            "stocks.reference.hk_connect_targets",
            store_identity,
            HKConnectTargetItem,
            ("code", "direction", "effective_date"),
            ("effective_date", "direction", "code"),
            lambda: [] if not self._settings.is_source_enabled("tushare") else _tushare_provider.get_hk_connect_targets(direction, status, effective_date),
        )

    def get_auctions(self, code: str, session: str, trade_date: str, start_date: str, end_date: str) -> list[AuctionItem]:
        if not self._settings.is_source_enabled("tushare"):
            return []
        return _tushare_provider.get_auctions(code, session, trade_date, start_date, end_date)






