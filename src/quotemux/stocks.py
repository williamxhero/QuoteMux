from __future__ import annotations

from datetime import timedelta

from platform_models import AdjFactorItem, AuditItem, AuctionItem, BSECodeMappingItem, CcassHoldingDetailItem, CcassHoldingItem, ChipDistributionItem, ChipPerformanceItem, DisclosureDateItem, DividendItem, ExpressItem, ForecastItem, HKConnectHoldingItem, HKConnectTargetItem, HLSignalItem, MainBusinessItem, ManagementRewardItem, NameHistoryItem, NineTurnItem, PledgeDetailItem, PledgeStatItem, RepurchaseItem, ResearchReportItem, RightsIssueItem, ShareChangeItem, ShareholderChangeItem, ShareholderCountItem, ShareholderTop10Item, StockAHComparisonItem, StockArchiveItem, StockBasicInfo, StockDailyBasicItem, StockDailyMarketValueItem, StockDailyValuationItem, StockFinanceIndicatorItem, StockFinancialStatementItem, StockManagerItem, StockMoneyFlowItem, StockPremarketItem, StockProfileItem, StockQuoteCodeSummary, StockQuoteItem, StockQuotesMeta, StockQuotesQueryResult, StockRiskFlagItem, SurveyItem, TechnicalFactorItem, UnlockScheduleItem
from quotemux.infra.common import build_time_bounds, format_date_value, normalize_stock_code, parse_date_text
from quotemux.infra.db.reference_reads import load_stock_active_codes_frame
from quotemux.runtime_core.executor import ProviderStep, SourceInstanceExecutor, run_fallback_chain_with_report
from quotemux.infra.tushare.helpers import normalize_date_range
from quotemux.common import MARKET_DAILY_SNAPSHOT_LIMIT, build_missing_expected_date_ranges, ensure_limit, expected_intraday_trade_times, has_enough_stock_quote_rows, missing_expected_keys, sort_items, trim_items_per_key
from quotemux.fact_ref_writes import get_fact_ref_writer
from quotemux.local_daily import get_stock_daily_local_window as get_local_stock_daily_local_window, get_stock_daily_snapshot_full as get_local_stock_daily_snapshot_full, get_stock_quotes as get_local_stock_quotes
from quotemux.local_store import get_local_stock_catalog, get_local_stock_hl_signal, get_local_stock_intraday_quotes, get_local_stock_name_history
from quotemux.query_engine import CapabilityQuerySpec, execute_capability_query
from quotemux.reports import ContractReport
from quotemux.requests.stocks import StockDailyLocalWindowRequest, StockDailySnapshotRequest, StockQuotesRequest
from quotemux.source_packages.registry import get_default_source_package_registry
from quotemux.store import load_store_result, store_result
from quotemux.settings import QuoteMuxSettings


MAX_DAILY_INDICATOR_CODES = 200
QUOTE_REQUEST_CODE_BATCH_SIZE = 10
MONEY_FLOW_REQUEST_CODE_BATCH_SIZE = 10


def _today_text() -> str:
    from datetime import datetime

    return datetime.now().strftime("%Y-%m-%d")


def _payloads_with_as_of_date(items: list[object]) -> list[dict[str, object]]:
    return [{**item.model_dump(), "as_of_date": _today_text()} for item in items if hasattr(item, "model_dump")]


def _stock_daily_basic_has_value(item: StockDailyBasicItem) -> bool:
    return any(
        value is not None
        for value in [
            item.turnover_rate,
            item.volume_ratio,
            item.pe,
            item.pb,
            item.total_share,
            item.float_share,
        ]
    )


def _source_package_call(package_id: str, handler_name: str, *args: object) -> object:
    handler = get_default_source_package_registry().get_handler(package_id, handler_name)
    return handler(*args)


def _source_package_singleton(package_id: str, handler_name: str, *args: object) -> list[object]:
    item = _source_package_call(package_id, handler_name, *args)
    return [item] if item is not None else []


def _fallback_quote_freq(freq: str) -> str:
    return freq


def _fallback_quote_count(freq: str, count: int | None) -> int | None:
    return count


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


def _expected_trade_dates(start_date: str, end_date: str, settings: QuoteMuxSettings) -> list[str]:
    from quotemux.markets import QuoteMuxMarkets
    from quotemux.requests.markets import TradingCalendarRequest

    items = QuoteMuxMarkets(settings).get_trading_calendar(
        TradingCalendarRequest(exchange="SSE", start_date=start_date, end_date=end_date, is_open=True)
    )
    return [item.trade_date for item in items]


def _chunk_quote_codes(codes: list[str]) -> list[list[str]]:
    return [codes[index: index + QUOTE_REQUEST_CODE_BATCH_SIZE] for index in range(0, len(codes), QUOTE_REQUEST_CODE_BATCH_SIZE)]


def _chunk_money_flow_codes(codes: list[str]) -> list[list[str]]:
    return [codes[index: index + MONEY_FLOW_REQUEST_CODE_BATCH_SIZE] for index in range(0, len(codes), MONEY_FLOW_REQUEST_CODE_BATCH_SIZE)]


def _build_quote_range_requests(grouped_ranges: dict[tuple[str, str], list[str]]) -> list[tuple[list[str], str, str]]:
    requests: list[tuple[list[str], str, str]] = []
    for (range_start, range_end), range_codes in grouped_ranges.items():
        for code_batch in _chunk_quote_codes(range_codes):
            requests.append((code_batch, range_start, range_end))
    return requests


def _expected_quote_trade_times(freq: str, expected_dates: list[str]) -> list[str]:
    return expected_intraday_trade_times(freq, expected_dates)


def _build_missing_time_date_ranges(expected_dates: list[str], missing_trade_times: list[str]) -> list[tuple[str, str]]:
    missing_dates = {trade_time[:10] for trade_time in missing_trade_times}
    covered_dates = {trade_date for trade_date in expected_dates if trade_date not in missing_dates}
    return build_missing_expected_date_ranges(expected_dates, covered_dates)


def _missing_quote_time_keys(code: str, freq: str, expected_trade_times: list[str], code_items: list[StockQuoteItem]) -> list[tuple[object, ...]]:
    expected_keys = [(code, trade_time, freq) for trade_time in expected_trade_times]
    existing_keys = {(item.code, item.trade_time, item.freq) for item in code_items}
    return missing_expected_keys(expected_keys, existing_keys)


def _quote_expected_dates(
    trade_date: str,
    start_date: str,
    end_date: str,
    start_time: str,
    end_time: str,
    count: int | None,
    intraday: bool,
    settings: QuoteMuxSettings,
) -> list[str]:
    if trade_date == "" and start_date == "" and end_date == "" and start_time == "" and end_time == "" and count:
        return []
    try:
        request_start_dt, request_end_dt = build_time_bounds(trade_date, start_date, end_date, start_time, end_time, count, intraday)
    except ValueError:
        start_text = start_time[:10] or end_time[:10]
        end_text = end_time[:10] or start_time[:10]
        return _expected_trade_dates(start_text, end_text, settings) if start_text != "" and end_text != "" else []
    if request_start_dt is None or request_end_dt is None:
        return []
    actual_start = request_start_dt.strftime("%Y-%m-%d")
    actual_end = request_end_dt.strftime("%Y-%m-%d")
    return _expected_trade_dates(actual_start, actual_end, settings)


def _limit_quote_items(items: list[StockQuoteItem], limit: int | None) -> list[StockQuoteItem]:
    if limit is None:
        return items
    if limit < 1:
        raise ValueError("limit 蹇呴』澶т簬 0")
    return items[:limit]


def _build_quote_code_summaries(
    codes: list[str],
    total_items: list[StockQuoteItem],
    returned_items: list[StockQuoteItem],
    coverage_items: list[StockQuoteItem],
    freq: str,
    expected_dates: list[str],
    count: int | None,
    excluded_st_codes: set[str],
) -> list[StockQuoteCodeSummary]:
    summaries: list[StockQuoteCodeSummary] = []
    expected_trade_times = _expected_quote_trade_times(freq, expected_dates)
    expected_trade_time_set = set(expected_trade_times)
    for code in codes:
        code_total_items = [item for item in total_items if item.code == code]
        code_returned_items = [item for item in returned_items if item.code == code]
        code_coverage_items = [item for item in coverage_items if item.code == code]
        trade_times = [item.trade_time for item in code_returned_items]
        excluded_by_st = code in excluded_st_codes
        if excluded_by_st:
            summaries.append(
                StockQuoteCodeSummary(
                    code=code,
                    row_count=0,
                    expected_bar_count=len(expected_trade_times),
                    actual_bar_count=0,
                    first_trade_time="",
                    last_trade_time="",
                    complete=False,
                    truncated=False,
                    missing_trade_dates=[],
                    missing_trade_times=[],
                )
            )
            continue
        actual_dates = {item.trade_time[:10] for item in code_coverage_items}
        missing_time_keys = _missing_quote_time_keys(code, freq, expected_trade_times, code_coverage_items)
        missing_trade_dates = [trade_date for trade_date in expected_dates if trade_date not in actual_dates]
        missing_trade_times = [str(key[1]) for key in missing_time_keys]
        truncated = len(code_returned_items) < len(code_total_items)
        enough_count = count is None or len(code_total_items) >= count
        complete = missing_trade_dates == [] and missing_trade_times == [] and enough_count and not truncated
        summaries.append(
            StockQuoteCodeSummary(
                code=code,
                row_count=len(code_returned_items),
                expected_bar_count=len(expected_trade_times),
                actual_bar_count=sum(1 for item in code_coverage_items if item.trade_time in expected_trade_time_set) if expected_trade_times else 0,
                first_trade_time=min(trade_times) if trade_times else "",
                last_trade_time=max(trade_times) if trade_times else "",
                complete=complete,
                truncated=truncated,
                missing_trade_dates=missing_trade_dates,
                missing_trade_times=missing_trade_times,
            )
        )
    return summaries


def _build_stock_quotes_query_result(
    codes: list[str],
    total_items: list[StockQuoteItem],
    returned_source_items: list[StockQuoteItem],
    coverage_items: list[StockQuoteItem],
    freq: str,
    limit: int | None,
    expected_dates: list[str],
    count: int | None,
    excluded_st_codes: set[str],
) -> StockQuotesQueryResult:
    returned_items = _limit_quote_items(returned_source_items, limit)
    summaries = _build_quote_code_summaries(codes, total_items, returned_items, coverage_items, freq, expected_dates, count, excluded_st_codes)
    truncated = len(returned_items) < len(returned_source_items)
    complete = all(item.complete for item in summaries) and not truncated
    return StockQuotesQueryResult(
        items=returned_items,
        meta=StockQuotesMeta(
            total_rows=len(total_items),
            returned_rows=len(returned_items),
            complete=complete,
            truncated=truncated,
            codes=summaries,
        ),
    )


def _base_source_report(contract_name: str, base_source_name: str, base_hit: bool) -> ContractReport:
    from quotemux.config_runtime.runtime import get_config_runtime

    active_snapshot = get_config_runtime().get_active_snapshot()
    return ContractReport(
        contract_name=contract_name,
        profile_id=active_snapshot.profile_id,
        profile_version=active_snapshot.version,
        source_hit_counts={base_source_name: int(base_hit)},
        source_request_counts={base_source_name: 1},
    )


def _has_explicit_quote_window(request: StockQuotesRequest) -> bool:
    return request.trade_date != "" or request.start_date != "" or request.end_date != "" or request.start_time != "" or request.end_time != ""


def _missing_ranges_are_current_or_future(missing_requests: list[tuple[list[str], str, str]]) -> bool:
    today_text = _today_text()
    for _, missing_start, missing_end in missing_requests:
        missing_date = missing_end or missing_start
        if missing_date == "" or missing_date < today_text:
            return False
    return True


def _should_return_local_daily(request: StockQuotesRequest, missing_requests: list[tuple[list[str], str, str]]) -> bool:
    if missing_requests == []:
        return True
    if not _has_explicit_quote_window(request):
        return False
    return _missing_ranges_are_current_or_future(missing_requests)


def _filter_suspended_quote_items(items: list[StockQuoteItem], skip_suspended: bool, fill_missing: bool, freq: str) -> list[StockQuoteItem]:
    if freq not in {"1d", "1w", "1mo"}:
        return items
    if not skip_suspended:
        return items
    return [item for item in items if not item.is_suspended]


def _find_st_codes(items: list[StockQuoteItem]) -> set[str]:
    return {item.code for item in items if item.is_st}


def _filter_st_quote_items(items: list[StockQuoteItem], skip_st: bool, freq: str) -> tuple[list[StockQuoteItem], set[str]]:
    if not skip_st or freq not in {"1d", "1w", "1mo"}:
        return items, set()
    excluded_codes = _find_st_codes(items)
    if not excluded_codes:
        return items, set()
    return [item for item in items if item.code not in excluded_codes], excluded_codes


def _apply_quote_filters(items: list[StockQuoteItem], skip_suspended: bool, skip_st: bool, fill_missing: bool, freq: str) -> tuple[list[StockQuoteItem], set[str]]:
    filtered_items, excluded_st_codes = _filter_st_quote_items(items, skip_st, freq)
    return _filter_suspended_quote_items(filtered_items, skip_suspended, fill_missing, freq), excluded_st_codes


def _apply_snapshot_filters(items: list[StockQuoteItem], skip_suspended: bool, skip_st: bool) -> list[StockQuoteItem]:
    filtered_items, _ = _apply_quote_filters(items, skip_suspended, skip_st, True, "1d")
    return filtered_items


def _build_local_daily_query_result(
    contract_name: str,
    request: StockQuotesRequest,
    local_items: list[StockQuoteItem],
    actual_freq: str,
    actual_limit: int | None,
    actual_adjust: str,
    request_freq: str,
    request_count: int | None,
    settings: QuoteMuxSettings,
) -> tuple[StockQuotesQueryResult, ContractReport]:
    st_filtered_items, excluded_st_codes = _filter_st_quote_items(local_items, request.skip_st, actual_freq)
    filtered_items = _filter_suspended_quote_items(st_filtered_items, request.skip_suspended, request.fill_missing, actual_freq)
    trimmed_items = trim_items_per_key(filtered_items, "code", "trade_time", request.count)
    sorted_items = sort_items(trimmed_items, ("code", "trade_time"))
    expected_dates = _quote_expected_dates(request.trade_date, request.start_date, request.end_date, request.start_time, request.end_time, request_count, request_freq != "1d", settings)
    report = _base_source_report(contract_name, "fact.stock_daily_1d", local_items != [])
    return _build_stock_quotes_query_result(request.codes, filtered_items, sorted_items, st_filtered_items, actual_freq, actual_limit, expected_dates, request.count, excluded_st_codes), report


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
    settings: QuoteMuxSettings,
) -> list[tuple[list[str], str, str]]:
    if trade_date == "" and start_date == "" and end_date == "" and start_time == "" and end_time == "" and count:
        if has_enough_stock_quote_rows(current_items, codes, count, "code"):
            return []
        missing_codes = [code for code in codes if sum(1 for item in current_items if item.code == code) < count]
        return [(code_batch, "", "") for code_batch in _chunk_quote_codes(missing_codes)] if missing_codes else []
    if freq in {"1w", "1mo"}:
        return [] if current_items else [(codes, trade_date or start_date, trade_date or end_date)]
    if freq != "1d":
        if has_enough_stock_quote_rows(current_items, codes, count, "code"):
            return []
        request_start_dt, request_end_dt = build_time_bounds(trade_date, start_date, end_date, start_time, end_time, count, True)
        actual_start_date = request_start_dt.strftime("%Y-%m-%d") if request_start_dt is not None else ""
        actual_end_date = request_end_dt.strftime("%Y-%m-%d") if request_end_dt is not None else ""
        if actual_start_date == "" and actual_end_date == "":
            missing_codes = [code for code in codes if sum(1 for item in current_items if item.code == code) < (count or 1)]
            return [(code_batch, "", "") for code_batch in _chunk_quote_codes(missing_codes)]
        if actual_start_date == "":
            actual_start_date = actual_end_date
        if actual_end_date == "":
            actual_end_date = actual_start_date
        grouped_ranges: dict[tuple[str, str], list[str]] = {}
        expected_trade_dates = _expected_trade_dates(actual_start_date, actual_end_date, settings)
        expected_trade_times = _expected_quote_trade_times(freq, expected_trade_dates)
        for code in codes:
            code_items = [item for item in current_items if item.code == code and item.freq == freq]
            existing_dates = {item.trade_time[:10] for item in code_items}
            if expected_trade_times:
                missing_times = [str(key[1]) for key in _missing_quote_time_keys(code, freq, expected_trade_times, code_items)]
                missing_ranges = _build_missing_time_date_ranges(expected_trade_dates, missing_times)
            else:
                missing_ranges = build_missing_expected_date_ranges(expected_trade_dates, existing_dates)
            if missing_ranges == [] and expected_trade_dates == []:
                missing_ranges = _build_missing_date_ranges(actual_start_date, actual_end_date, existing_dates)
            for missing_start, missing_end in missing_ranges:
                grouped_ranges.setdefault((missing_start, missing_end), []).append(code)
        return _build_quote_range_requests(grouped_ranges)
    request_start_dt, request_end_dt = build_time_bounds(trade_date, start_date, end_date, start_time, end_time, count, False)
    actual_start_date = request_start_dt.strftime("%Y-%m-%d") if request_start_dt is not None else ""
    actual_end_date = request_end_dt.strftime("%Y-%m-%d") if request_end_dt is not None else ""
    if actual_start_date == "" and actual_end_date == "":
        if has_enough_stock_quote_rows(current_items, codes, count, "code"):
            return []
        missing_codes = [code for code in codes if sum(1 for item in current_items if item.code == code) < (count or 1)]
        return [(code_batch, "", "") for code_batch in _chunk_quote_codes(missing_codes)] if missing_codes else []
    if actual_start_date == "":
        actual_start_date = actual_end_date
    if actual_end_date == "":
        actual_end_date = actual_start_date
    expected_trade_dates = _expected_trade_dates(actual_start_date, actual_end_date, settings)
    grouped_ranges: dict[tuple[str, str], list[str]] = {}
    for code in codes:
        existing_dates = {item.trade_time for item in current_items if item.code == code and item.freq == "1d"}
        missing_ranges = build_missing_expected_date_ranges(expected_trade_dates, existing_dates)
        if missing_ranges == [] and expected_trade_dates == []:
            missing_ranges = _build_missing_date_ranges(actual_start_date, actual_end_date, existing_dates)
        for missing_start, missing_end in missing_ranges:
            grouped_ranges.setdefault((missing_start, missing_end), []).append(code)
    return _build_quote_range_requests(grouped_ranges)


def _has_complete_stock_snapshot_item(item: StockQuoteItem) -> bool:
    return item.close is not None and item.pre_close is not None and item.pct_chg is not None and item.amount is not None


def _missing_snapshot_codes(trade_date: str, items: list[StockQuoteItem], limit: int = MARKET_DAILY_SNAPSHOT_LIMIT, offset: int = 0) -> list[str]:
    active_frame = load_stock_active_codes_frame(trade_date)
    if active_frame.empty:
        return [item.code for item in items if item.freq == "1d" and format_date_value(item.trade_time) == trade_date and not _has_complete_stock_snapshot_item(item)]
    active_codes = [normalize_stock_code(str(row["code"])).zfill(6) for row in active_frame.to_dict("records")]
    page_codes = active_codes[: offset + limit]
    existing_codes = {normalize_stock_code(item.code).zfill(6) for item in items if item.freq == "1d" and format_date_value(item.trade_time) == trade_date and _has_complete_stock_snapshot_item(item)}
    return [code for code in dict.fromkeys(page_codes) if code != "" and code not in existing_codes]


def _build_snapshot_requests(trade_date: str, items: list[StockQuoteItem], limit: int = MARKET_DAILY_SNAPSHOT_LIMIT, offset: int = 0) -> list[tuple[list[str], str]]:
    missing_codes = _missing_snapshot_codes(trade_date, items, limit, offset)
    if missing_codes != []:
        return [(missing_codes, trade_date)]
    if any(not _has_complete_stock_snapshot_item(item) for item in items):
        return [([], trade_date)]
    return [([], trade_date)] if items == [] else []


def _build_steps(freq: str, request_freq: str, request_count: int | None, actual_adjust: str, settings: QuoteMuxSettings) -> tuple[ProviderStep[StockQuoteItem], ...]:
    handlers = {
        "get_stock_quotes": lambda instance: lambda missing_codes, missing_start, missing_end: _source_package_call(instance.package_id, "get_stock_quotes", missing_codes, request_freq, "", missing_start, missing_end, "", "", request_count, actual_adjust),
    }
    if freq in {"1d", "1w", "1mo"}:
        fallback_order = ("tushare", "efinance", "mootdx", "akshare", "opentdx")
    else:
        fallback_order = ("opentdx", "efinance", "mootdx", "akshare")
    capability_id = "stocks.quotes.daily" if freq in {"1d", "1w", "1mo"} else "stocks.quotes.intraday"
    return SourceInstanceExecutor(settings).build_steps(capability_id, handlers, fallback_order)


def _build_daily_snapshot_steps(settings: QuoteMuxSettings) -> tuple[ProviderStep[StockQuoteItem], ...]:
    def fetch_snapshot(package_id: str, missing_codes: list[str], request_trade_date: str) -> list[StockQuoteItem]:
        if missing_codes != []:
            return _source_package_call(package_id, "get_stock_quotes", missing_codes, "1d", request_trade_date, "", "", "", "", None, "none")
        return _source_package_call(package_id, "get_stock_daily_snapshot_full", request_trade_date)

    handlers = {
        "get_stock_daily_snapshot_full": lambda instance: lambda missing_codes, request_trade_date: fetch_snapshot(instance.package_id, missing_codes, request_trade_date),
        "get_stock_quotes": lambda instance: lambda missing_codes, request_trade_date: _source_package_call(instance.package_id, "get_stock_quotes", missing_codes, "1d", request_trade_date, "", "", "", "", None, "none"),
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

    def _store_single(self, capability_id: str, store_identity: dict[str, object], model_type: type[object], fetcher, payload_builder=None):
        items = self._store_list(
            capability_id,
            store_identity,
            model_type,
            ("code",),
            ("code",),
            lambda: [item for item in [fetcher()] if item is not None],
            payload_builder,
        )
        return items[0] if items else None

    def get_quotes(self, request: StockQuotesRequest) -> list[StockQuoteItem]:
        items, _ = self.get_quotes_with_report(request)
        return items

    def get_quotes_with_report(self, request: StockQuotesRequest) -> tuple[list[StockQuoteItem], ContractReport]:
        result, report = self.get_quotes_query_result_with_report(request)
        return result.items, report

    def get_quotes_query_result(self, request: StockQuotesRequest) -> StockQuotesQueryResult:
        result, _ = self.get_quotes_query_result_with_report(request)
        return result

    def get_quotes_query_result_with_report(self, request: StockQuotesRequest) -> tuple[StockQuotesQueryResult, ContractReport]:
        if request.codes == []:
            return StockQuotesQueryResult(items=[], meta=StockQuotesMeta(total_rows=0, returned_rows=0, complete=True, truncated=False)), ContractReport.empty("stocks.quotes")
        actual_limit = None if request.limit is None else ensure_limit(request.limit)
        actual_freq = request.freq or "1d"
        actual_adjust = request.adjust or "none"
        request_freq = _fallback_quote_freq(actual_freq)
        request_count = _fallback_quote_count(actual_freq, request.count)
        contract_name = "stocks.quotes.intraday" if request_freq in {"1m", "5m", "15m", "30m", "60m", "tick"} else "stocks.quotes.daily"
        store_enabled = actual_freq not in {"1w", "1mo", "30m"}
        fact_ref_writer = get_fact_ref_writer(contract_name) if request_freq in {"1d", "1m", "30m"} else None
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
        local_items: list[StockQuoteItem] = []
        local_missing_requests: list[tuple[list[str], str, str]] = []
        if request_freq == "1d":
            local_items = get_local_stock_quotes(request.codes, request_freq, request.trade_date, request.start_date, request.end_date, request.start_time, request.end_time, request_count, actual_adjust)
            local_missing_requests = _build_missing_quote_requests(request.codes, local_items, request_freq, request.trade_date, request.start_date, request.end_date, request.start_time, request.end_time, request_count, self._settings)
            if _should_return_local_daily(request, local_missing_requests):
                return _build_local_daily_query_result(contract_name, request, local_items, actual_freq, actual_limit, actual_adjust, request_freq, request_count, self._settings)
        else:
            local_items = get_local_stock_intraday_quotes(request.codes, request_freq, request.trade_date, request.start_date, request.end_date, request.start_time, request.end_time, request_count)
        merged_items, report = execute_capability_query(
            CapabilityQuerySpec(
                capability_id=contract_name,
                store_identity=store_identity,
                model_type=StockQuoteItem,
                key_fields=("code", "trade_time", "freq"),
                sort_fields=("code", "trade_time"),
                request_builder=lambda items: _build_missing_quote_requests(request.codes, items, request_freq, request.trade_date, request.start_date, request.end_date, request.start_time, request.end_time, request_count, self._settings),
                provider_steps=lambda: _build_steps(request_freq, request_freq, request_count, actual_adjust, self._settings),
                source_order=self._settings.get_contract_source_order(contract_name, ("tushare", "efinance", "mootdx", "akshare", "opentdx")),
                base_items=local_items,
                base_source_name="fact.stock_daily_1d" if request_freq == "1d" else ("fact.stock_bar_30m" if request_freq == "30m" else "fact.stock_bar_1m"),
                store_enabled=store_enabled,
                fact_ref_writer=fact_ref_writer,
            )
        )
        st_filtered_items, excluded_st_codes = _filter_st_quote_items(merged_items, request.skip_st, actual_freq)
        filtered_items = _filter_suspended_quote_items(st_filtered_items, request.skip_suspended, request.fill_missing, actual_freq)
        trimmed_items = trim_items_per_key(filtered_items, "code", "trade_time", request.count)
        sorted_items = sort_items(trimmed_items, ("code", "trade_time"))
        expected_dates = _quote_expected_dates(request.trade_date, request.start_date, request.end_date, request.start_time, request.end_time, request_count, request_freq != "1d", self._settings)
        return _build_stock_quotes_query_result(request.codes, filtered_items, sorted_items, st_filtered_items, actual_freq, actual_limit, expected_dates, request.count, excluded_st_codes), report

    def get_daily_snapshot(self, request: StockDailySnapshotRequest) -> list[StockQuoteItem]:
        items, _ = self.get_daily_snapshot_with_report(request)
        return items

    def get_daily_snapshot_with_report(self, request: StockDailySnapshotRequest) -> tuple[list[StockQuoteItem], ContractReport]:
        actual_trade_date = format_date_value(request.trade_date)
        if actual_trade_date == "":
            raise ValueError("trade_date 不能为空")
        if request.limit < 1 or request.limit > MARKET_DAILY_SNAPSHOT_LIMIT:
            raise ValueError("limit 超出允许范围")
        if request.offset < 0:
            raise ValueError("offset 不能小于 0")
        local_items = get_local_stock_daily_snapshot_full(actual_trade_date)
        items, report = execute_capability_query(
            CapabilityQuerySpec(
                capability_id="stocks.quotes.daily_snapshot",
                store_identity={"trade_date": actual_trade_date},
                model_type=StockQuoteItem,
                key_fields=("code", "trade_time", "freq"),
                sort_fields=("code", "trade_time"),
                request_builder=lambda current_items: _build_snapshot_requests(actual_trade_date, current_items, request.limit, request.offset),
                provider_steps=lambda: _build_daily_snapshot_steps(self._settings),
                source_order=self._settings.get_contract_source_order("stocks.quotes.daily_snapshot", ("tushare", "efinance", "akshare", "mootdx")),
                base_items=local_items,
                base_source_name="fact.stock_daily_1d",
                fact_ref_writer=get_fact_ref_writer("stocks.quotes.daily_snapshot"),
            )
        )
        filtered_items = _apply_snapshot_filters(items, request.skip_suspended, request.skip_st)
        return filtered_items[request.offset: request.offset + request.limit], report
    def get_daily_local_window(self, request: StockDailyLocalWindowRequest) -> list[StockQuoteItem]:
        actual_start_date = format_date_value(request.start_date)
        actual_end_date = format_date_value(request.end_date)
        if actual_start_date == "" or actual_end_date == "" or actual_start_date > actual_end_date:
            raise ValueError("start_date ? end_date ????????? start_date ???? end_date")
        if request.limit < 1:
            raise ValueError("limit ???? 0")
        if request.offset < 0:
            raise ValueError("offset ???? 0")
        items = get_local_stock_daily_local_window(actual_start_date, actual_end_date, None, 0)
        filtered_items = _apply_snapshot_filters(items, request.skip_suspended, request.skip_st)
        return filtered_items[request.offset: request.offset + request.limit]


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
        expected_trade_dates = _expected_trade_dates(actual_start_date, actual_end_date, self._settings)
        existing_dates = {item.trade_date for item in items}
        missing_ranges = build_missing_expected_date_ranges(expected_trade_dates, existing_dates)
        if missing_ranges == [] and expected_trade_dates == []:
            return _build_missing_date_ranges(actual_start_date, actual_end_date, existing_dates)
        return missing_ranges

    def get_money_flow(self, code: str, trade_date: str, start_date: str, end_date: str, view: str) -> list[StockMoneyFlowItem]:
        store_identity = {"code": code, "trade_date": trade_date, "start_date": start_date, "end_date": end_date, "view": view}
        handlers = {
            "get_stock_money_flow": lambda instance: lambda missing_start, missing_end: _source_package_call(instance.package_id, "get_stock_money_flow", code, trade_date, missing_start, missing_end, view),
        }
        sorted_items, _ = execute_capability_query(
            CapabilityQuerySpec(
                capability_id="stocks.indicators.money_flow",
                store_identity=store_identity,
                model_type=StockMoneyFlowItem,
                key_fields=("code", "trade_date", "view"),
                sort_fields=("code", "trade_date"),
                request_builder=lambda items: self._build_missing_money_flow_requests(items, trade_date, start_date, end_date),
                provider_steps=lambda: SourceInstanceExecutor(self._settings).build_steps("stocks.indicators.money_flow", handlers, ("tushare", "akshare")),
                source_order=self._settings.get_contract_source_order("stocks.indicators.money_flow", ("tushare", "akshare")),
            )
        )
        return sorted_items

    def get_money_flow_batch(self, codes: str, trade_date: str, view: str) -> list[StockMoneyFlowItem]:
        """批量查询多只股票指定日期的资金流数据"""
        code_list = [c.strip() for c in codes.split(",") if c.strip()]
        if not code_list:
            return []
        normalized_codes = [code for code in (normalize_stock_code(code) for code in code_list) if code]
        requested_codes = set(normalized_codes)
        actual_trade_date = format_date_value(trade_date)

        def build_missing_batch_requests(current_items: list[StockMoneyFlowItem]) -> list[tuple[object, ...]]:
            existing_codes = {
                item.code
                for item in current_items
                if item.trade_date == actual_trade_date and item.view == view
            }
            missing_codes = [code for code in normalized_codes if code not in existing_codes]
            return [(code_batch,) for code_batch in _chunk_money_flow_codes(missing_codes)]

        store_identity = {"codes": ",".join(code_list), "trade_date": trade_date, "view": view}
        handlers = {
            "get_stock_money_flow_batch": lambda instance: lambda code_batch: _source_package_call(instance.package_id, "get_stock_money_flow_batch", ",".join(code_batch), trade_date, view),
        }
        sorted_items, _ = execute_capability_query(
            CapabilityQuerySpec(
                capability_id="stocks.indicators.money_flow.batch",
                store_identity=store_identity,
                model_type=StockMoneyFlowItem,
                key_fields=("code", "trade_date", "view"),
                sort_fields=("code", "trade_date"),
                request_builder=build_missing_batch_requests,
                provider_steps=lambda: SourceInstanceExecutor(self._settings).build_steps("stocks.indicators.money_flow.batch", handlers, ("tushare", "akshare")),
                source_order=self._settings.get_contract_source_order("stocks.indicators.money_flow.batch", ("tushare", "akshare")),
            )
        )
        return [item for item in sorted_items if item.code in requested_codes and item.trade_date == actual_trade_date and item.view == view]

    def get_financial_statements(self, codes: list[str], report_period: str, start_period: str, end_period: str, report_type: str) -> list[StockFinancialStatementItem]:
        store_identity = {
            "codes": list(codes),
            "report_period": report_period,
            "start_period": start_period,
            "end_period": end_period,
            "report_type": report_type,
        }
        handlers = {
            "get_stock_financial_statements": lambda instance: lambda: _source_package_call(instance.package_id, "get_stock_financial_statements", codes, report_period, start_period, end_period, report_type),
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
            "get_stock_finance_indicators": lambda instance: lambda: _source_package_call(instance.package_id, "get_stock_finance_indicators", code, codes, report_period, start_period, end_period),
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
        store_identity = {"codes": list(codes), "name": name, "exchange": exchange, "list_status": list_status, "include_delisted": include_delisted}
        handlers = {
            "get_stock_catalog": lambda instance: lambda: _source_package_call(instance.package_id, "get_stock_catalog", codes, name, exchange, list_status, include_delisted, ensure_limit(limit), offset),
        }
        
        def build_request(current_items: list[StockBasicInfo]) -> list[tuple[()]]:
            if codes:
                return [()] if len(current_items) < len(set(codes)) else []
            if not name and not exchange and list_status in {"", "L", "listed"}:
                return [()] if len(current_items) < 4000 else []
            return [()] if current_items == [] else []

        is_full_snapshot = not codes and not name and not exchange and ensure_limit(limit) >= 4000

        items, _ = execute_capability_query(
            CapabilityQuerySpec(
                capability_id="stocks.catalog",
                store_identity=store_identity,
                model_type=StockBasicInfo,
                key_fields=("code",),
                sort_fields=("code",),
                request_builder=build_request,
                provider_steps=lambda: SourceInstanceExecutor(self._settings).build_steps("stocks.catalog", handlers, ("tushare",)),
                source_order=self._settings.get_contract_source_order("stocks.catalog", ("tushare",)),
                base_items=get_local_stock_catalog(codes, name, exchange, list_status, include_delisted),
                base_source_name="ref.stock",
                fact_ref_writer=get_fact_ref_writer("stocks.catalog") if is_full_snapshot else None,
            )
        )
        return items[offset: offset + ensure_limit(limit)]

    def get_archive(self, trade_date: str, code: str, name: str, industry: str, area: str, limit: int, offset: int) -> list[StockArchiveItem]:
        store_identity = {"trade_date": trade_date, "code": code, "name": name, "industry": industry, "area": area, "limit": ensure_limit(limit), "offset": offset}
        handlers = {
            "get_stock_archive": lambda instance: lambda: _source_package_call(instance.package_id, "get_stock_archive", trade_date, code, name, industry, area, ensure_limit(limit), offset),
        }
        return self._store_list(
            "stocks.catalog.archive",
            store_identity,
            StockArchiveItem,
            ("code", "trade_date"),
            ("trade_date", "code"),
            lambda: self._source_list("stocks.catalog.archive", handlers, ("tushare",), ("code", "trade_date")),
        )

    def get_basic(self, code: str) -> StockBasicInfo | None:
        store_identity = {"code": code}
        handlers = {
            "get_stock_basic": lambda instance: lambda: _source_package_singleton(instance.package_id, "get_stock_basic", code),
        }
        items, _ = execute_capability_query(
            CapabilityQuerySpec(
                capability_id="stocks.profile.basic",
                store_identity=store_identity,
                model_type=StockBasicInfo,
                key_fields=("code",),
                sort_fields=("code",),
                request_builder=lambda current_items: [()] if current_items == [] else [],
                provider_steps=lambda: SourceInstanceExecutor(self._settings).build_steps("stocks.profile.basic", handlers, ("tushare",)),
                source_order=self._settings.get_contract_source_order("stocks.profile.basic", ("tushare",)),
                base_items=get_local_stock_catalog([code], "", "", "", True),
                base_source_name="ref.stock",
                fact_ref_writer=get_fact_ref_writer("stocks.profile.basic"),
            )
        )
        return items[0] if items else None

    def get_profile(self, code: str) -> StockProfileItem | None:
        store_identity = {"code": code}
        handlers = {
            "get_company_profile": lambda instance: lambda: _source_package_singleton(instance.package_id, "get_company_profile", code),
        }
        items, _ = execute_capability_query(
            CapabilityQuerySpec(
                capability_id="stocks.profile.company",
                store_identity=store_identity,
                model_type=StockProfileItem,
                key_fields=("code",),
                sort_fields=("code",),
                request_builder=lambda current_items: [()] if current_items == [] else [],
                provider_steps=lambda: SourceInstanceExecutor(self._settings).build_steps("stocks.profile.company", handlers, ("tushare", "akshare")),
                source_order=self._settings.get_contract_source_order("stocks.profile.company", ("tushare", "akshare")),
                payload_builder=_payloads_with_as_of_date,
            )
        )
        return items[0] if items else None

    def get_name_history(self, code: str, start_date: str, end_date: str) -> list[NameHistoryItem]:
        store_identity = {"code": code, "start_date": start_date, "end_date": end_date}
        handlers = {
            "get_stock_name_history": lambda instance: lambda: _source_package_call(instance.package_id, "get_stock_name_history", code, start_date, end_date),
        }
        items, _ = execute_capability_query(
            CapabilityQuerySpec(
                capability_id="stocks.profile.name_history",
                store_identity=store_identity,
                model_type=NameHistoryItem,
                key_fields=("code", "start_date", "end_date", "name"),
                sort_fields=("code", "start_date", "end_date", "name"),
                request_builder=lambda current_items: [()] if current_items == [] else [],
                provider_steps=lambda: SourceInstanceExecutor(self._settings).build_steps("stocks.profile.name_history", handlers, ("tushare",)),
                source_order=self._settings.get_contract_source_order("stocks.profile.name_history", ("tushare",)),
                base_items=get_local_stock_name_history(code, start_date, end_date),
                base_source_name="ref.stock_name_history",
                fact_ref_writer=get_fact_ref_writer("stocks.profile.name_history"),
            )
        )
        return items

    def get_managers(self, code: str) -> list[StockManagerItem]:
        store_identity = {"code": code}
        handlers = {
            "get_managers": lambda instance: lambda: _source_package_call(instance.package_id, "get_managers", code),
        }
        return self._store_list(
            "stocks.profile.managers",
            store_identity,
            StockManagerItem,
            ("code", "name", "title", "begin_date"),
            ("code", "name", "begin_date"),
            lambda: self._source_list("stocks.profile.managers", handlers, ("tushare",), ("code", "name", "title", "begin_date")),
            _payloads_with_as_of_date,
        )

    def get_management_rewards(self, code: str, start_date: str, end_date: str) -> list[ManagementRewardItem]:
        store_identity = {"code": code, "start_date": start_date, "end_date": end_date}
        handlers = {
            "get_management_rewards": lambda instance: lambda: _source_package_call(instance.package_id, "get_management_rewards", code, start_date, end_date),
        }
        return self._store_list(
            "stocks.profile.management_rewards",
            store_identity,
            ManagementRewardItem,
            ("code", "ann_date", "name", "title"),
            ("code", "ann_date", "name"),
            lambda: self._source_list("stocks.profile.management_rewards", handlers, ("tushare",), ("code", "ann_date", "name", "title")),
        )

    def get_hl_signal(self, code: str, trade_date: str, start_date: str, end_date: str) -> list[HLSignalItem]:
        normalized = normalize_stock_code(code)
        if normalized == "":
            return []
        store_identity = {"code": normalized, "trade_date": trade_date, "start_date": start_date, "end_date": end_date}
        handlers = {
            "get_hl_signal": lambda instance: lambda: _source_package_call(instance.package_id, "get_hl_signal", normalized, trade_date, start_date, end_date),
        }
        return self._store_list(
            "stocks.signals.hl",
            store_identity,
            HLSignalItem,
            ("code", "trade_date", "signal", "first_extreme"),
            ("code", "trade_date", "signal"),
            lambda: get_local_stock_hl_signal(normalized, trade_date, start_date, end_date) or self._source_list("stocks.signals.hl", handlers, ("derived_core",), ("code", "trade_date", "signal", "first_extreme")),
        )

    def get_nine_turn(self, code: str, freq: str, trade_date: str, start_date: str, end_date: str) -> list[NineTurnItem]:
        store_identity = {"code": code, "freq": freq, "trade_date": trade_date, "start_date": start_date, "end_date": end_date}
        handlers = {
            "get_nine_turn": lambda instance: lambda: _source_package_call(instance.package_id, "get_nine_turn", code, freq, trade_date, start_date, end_date),
        }
        return self._store_list(
            "stocks.signals.nine_turn",
            store_identity,
            NineTurnItem,
            ("code", "trade_time", "freq"),
            ("code", "trade_time", "freq"),
            lambda: self._source_list("stocks.signals.nine_turn", handlers, ("tushare", "derived_core"), ("code", "trade_time", "freq")),
        )

    def get_adj_factors(self, code: str, start_date: str, end_date: str, base_date: str) -> list[AdjFactorItem]:
        store_identity = {"code": code, "start_date": start_date, "end_date": end_date, "base_date": base_date}
        handlers = {
            "get_adj_factors": lambda instance: lambda: _source_package_call(instance.package_id, "get_adj_factors", code, start_date, end_date, base_date),
        }
        return self._store_list(
            "stocks.factors.adj",
            store_identity,
            AdjFactorItem,
            ("code", "trade_date"),
            ("code", "trade_date"),
            lambda: self._source_list("stocks.factors.adj", handlers, ("tushare",), ("code", "trade_date")),
        )

    def get_technical_factors(self, code: str, trade_date: str, start_date: str, end_date: str, adjust: str) -> list[TechnicalFactorItem]:
        normalized = normalize_stock_code(code)
        if normalized == "":
            return []
        store_identity = {"code": normalized, "trade_date": trade_date, "start_date": start_date, "end_date": end_date, "adjust": adjust}
        handlers = {
            "get_technical_factors": lambda instance: lambda: _source_package_call(instance.package_id, "get_technical_factors", normalized, trade_date, start_date, end_date, adjust),
        }
        return self._store_list(
            "stocks.factors.technical",
            store_identity,
            TechnicalFactorItem,
            ("code", "trade_date", "adjust"),
            ("code", "trade_date", "adjust"),
            lambda: self._source_list("stocks.factors.technical", handlers, ("derived_core",), ("code", "trade_date", "adjust")),
        )

    def get_ah_comparisons(self, code: str, trade_date: str, start_date: str, end_date: str, limit: int, offset: int) -> list[StockAHComparisonItem]:
        store_identity = {"code": code, "trade_date": trade_date, "start_date": start_date, "end_date": end_date, "limit": ensure_limit(limit), "offset": offset}
        handlers = {
            "get_stock_ah_comparisons": lambda instance: lambda: _source_package_call(instance.package_id, "get_stock_ah_comparisons", code, trade_date, start_date, end_date, ensure_limit(limit), offset),
        }
        return self._store_list(
            "stocks.indicators.ah_comparisons",
            store_identity,
            StockAHComparisonItem,
            ("code", "trade_date"),
            ("trade_date", "code"),
            lambda: self._source_list("stocks.indicators.ah_comparisons", handlers, ("tushare",), ("code", "trade_date")),
        )

    def _resolve_indicator_request(self, code: str, codes: str, trade_date: str, start_date: str, end_date: str) -> tuple[list[str], str, str, str]:
        actual_codes = _indicator_codes_from_params(code, codes)
        if actual_codes:
            if len(actual_codes) > MAX_DAILY_INDICATOR_CODES:
                raise ValueError("閺冦儵顣堕幐鍥ㄧ垼閹恒儱褰涙稉宥嗘暜閹镐椒绔村▎鈥茬炊閸忋儴绉存潻?200 閸欘亣鍋傜粊顭掔幢閸忋劌绔堕崷鍝勫絿閺佹媽顕幐?trade_date 閸楁洘妫╅弻銉嚄閿涘奔绗夌憰浣风炊 code 閹?codes")
            actual_start, actual_end = normalize_date_range(trade_date, start_date, end_date)
            if actual_start == actual_end:
                return actual_codes, actual_start, "", ""
            return actual_codes, "", actual_start, actual_end
        actual_trade_date = _single_day_indicator_request(trade_date, start_date, end_date)
        if actual_trade_date == "":
            raise ValueError("閺堫亙绱?code 閹?codes 閺冭绱濇禒鍛暜閹镐礁宕熼弮銉ュ弿鐢倸婧€閺屻儴顕楅敍宀冾嚞娴ｈ法鏁?trade_date")
        return [], actual_trade_date, "", ""

    def get_daily_basic(self, code: str, codes: str, trade_date: str, start_date: str, end_date: str) -> list[StockDailyBasicItem]:
        items = self._get_daily_indicator("stocks.indicators.daily_basic", StockDailyBasicItem, "get_stock_daily_basic", code, codes, trade_date, start_date, end_date)
        return [item for item in items if _stock_daily_basic_has_value(item)]

    def get_daily_valuation(self, code: str, codes: str, trade_date: str, start_date: str, end_date: str) -> list[StockDailyValuationItem]:
        return self._get_daily_indicator("stocks.indicators.daily_valuation", StockDailyValuationItem, "get_stock_daily_valuation", code, codes, trade_date, start_date, end_date)

    def get_daily_market_value(self, code: str, codes: str, trade_date: str, start_date: str, end_date: str) -> list[StockDailyMarketValueItem]:
        return self._get_daily_indicator("stocks.indicators.daily_market_value", StockDailyMarketValueItem, "get_stock_daily_market_value", code, codes, trade_date, start_date, end_date)

    def _get_daily_indicator(self, capability_id: str, model_type: type[object], source_method_name: str, code: str, codes: str, trade_date: str, start_date: str, end_date: str) -> list[object]:
        actual_codes, actual_trade_date, actual_start, actual_end = self._resolve_indicator_request(code, codes, trade_date, start_date, end_date)
        store_identity = {"code": ",".join(actual_codes), "trade_date": actual_trade_date, "start_date": actual_start, "end_date": actual_end}
        handlers = {
            source_method_name: lambda instance: lambda: _source_package_call(
                instance.package_id,
                source_method_name,
                "" if actual_codes == [] else actual_codes[0] if len(actual_codes) == 1 else "",
                "" if actual_codes == [] else ",".join(actual_codes),
                actual_trade_date,
                "" if actual_codes == [] else actual_start,
                "" if actual_codes == [] else actual_end,
            ),
        }
        sorted_items, _ = execute_capability_query(
            CapabilityQuerySpec(
                capability_id=capability_id,
                store_identity=store_identity,
                model_type=model_type,
                key_fields=("code", "trade_date"),
                sort_fields=("code", "trade_date"),
                request_builder=lambda current_items: [()] if current_items == [] else [],
                provider_steps=lambda: SourceInstanceExecutor(self._settings).build_steps(capability_id, handlers, ("tushare",)),
                source_order=self._settings.get_contract_source_order(capability_id, ("tushare",)),
            )
        )
        return sorted_items

    def get_risk_flags(self, trade_date: str, start_date: str, end_date: str, flag_type: str, status: str, limit: int, offset: int) -> list[StockRiskFlagItem]:
        store_identity = {"trade_date": trade_date, "start_date": start_date, "end_date": end_date, "flag_type": flag_type, "status": status, "limit": ensure_limit(limit), "offset": offset}
        handlers = {
            "get_stock_risk_flags": lambda instance: lambda: _source_package_call(instance.package_id, "get_stock_risk_flags", trade_date, start_date, end_date, flag_type, status, ensure_limit(limit), offset),
        }
        return self._store_list(
            "stocks.indicators.risk_flags",
            store_identity,
            StockRiskFlagItem,
            ("code", "flag_type", "start_date", "end_date", "status"),
            ("start_date", "code", "flag_type"),
            lambda: self._source_list("stocks.indicators.risk_flags", handlers, ("tushare",), ("code", "flag_type", "start_date", "end_date", "status")),
        )

    def get_premarket(self, code: str, trade_date: str, start_date: str, end_date: str) -> list[StockPremarketItem]:
        store_identity = {"code": code, "trade_date": trade_date, "start_date": start_date, "end_date": end_date}
        handlers = {
            "get_premarket": lambda instance: lambda: _source_package_call(instance.package_id, "get_premarket", code, trade_date, start_date, end_date),
        }
        return self._store_list(
            "stocks.indicators.premarket",
            store_identity,
            StockPremarketItem,
            ("code", "trade_date"),
            ("code", "trade_date"),
            lambda: self._source_list("stocks.indicators.premarket", handlers, ("tushare",), ("code", "trade_date")),
        )

    def get_chip_distribution(self, code: str, trade_date: str, start_date: str, end_date: str) -> list[ChipDistributionItem]:
        store_identity = {"code": code, "trade_date": trade_date, "start_date": start_date, "end_date": end_date}
        handlers = {
            "get_chip_distribution": lambda instance: lambda: _source_package_call(instance.package_id, "get_chip_distribution", code, trade_date, start_date, end_date),
        }
        return self._store_list(
            "stocks.indicators.chip_distribution",
            store_identity,
            ChipDistributionItem,
            ("code", "trade_date", "price"),
            ("code", "trade_date", "price"),
            lambda: self._source_list("stocks.indicators.chip_distribution", handlers, ("tushare",), ("code", "trade_date", "price")),
        )

    def get_chip_performance(self, code: str, trade_date: str, start_date: str, end_date: str) -> list[ChipPerformanceItem]:
        store_identity = {"code": code, "trade_date": trade_date, "start_date": start_date, "end_date": end_date}
        handlers = {
            "get_chip_performance": lambda instance: lambda: _source_package_call(instance.package_id, "get_chip_performance", code, trade_date, start_date, end_date),
        }
        return self._store_list(
            "stocks.indicators.chip_performance",
            store_identity,
            ChipPerformanceItem,
            ("code", "trade_date"),
            ("code", "trade_date"),
            lambda: self._source_list("stocks.indicators.chip_performance", handlers, ("tushare",), ("code", "trade_date")),
        )

    def get_dividends(self, code: str, start_date: str, end_date: str) -> list[DividendItem]:
        store_identity = {"code": code, "start_date": start_date, "end_date": end_date}
        handlers = {
            "get_dividends": lambda instance: lambda: _source_package_call(instance.package_id, "get_dividends", code, start_date, end_date),
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
            "get_repurchases": lambda instance: lambda: _source_package_call(instance.package_id, "get_repurchases", code, start_date, end_date),
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
            "get_rights_issues": lambda instance: lambda: _source_package_call(instance.package_id, "get_rights_issues", code, start_date, end_date),
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
            "get_share_changes": lambda instance: lambda: _source_package_call(instance.package_id, "get_share_changes", code, trade_date, start_date, end_date),
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
            "get_unlock_schedules": lambda instance: lambda: _source_package_call(instance.package_id, "get_unlock_schedules", code, unlock_date, start_date, end_date),
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
        handlers = {
            "get_audits": lambda instance: lambda: _source_package_call(instance.package_id, "get_audits", code, report_period, start_period, end_period),
        }
        return self._store_list(
            "stocks.finance.audits",
            store_identity,
            AuditItem,
            ("code", "report_period", "announce_date"),
            ("code", "report_period", "announce_date"),
            lambda: self._source_list("stocks.finance.audits", handlers, ("tushare",), ("code", "report_period", "announce_date")),
        )

    def get_disclosure_dates(self, code: str, report_period: str, start_period: str, end_period: str) -> list[DisclosureDateItem]:
        store_identity = {"code": code, "report_period": report_period, "start_period": start_period, "end_period": end_period}
        handlers = {
            "get_disclosure_dates": lambda instance: lambda: _source_package_call(instance.package_id, "get_disclosure_dates", code, report_period, start_period, end_period),
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
            "get_express": lambda instance: lambda: _source_package_call(instance.package_id, "get_express", code, report_period, start_period, end_period),
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
            "get_forecasts": lambda instance: lambda: _source_package_call(instance.package_id, "get_forecasts", code, report_period, start_period, end_period),
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
                    "get_main_business": lambda instance: lambda: _source_package_call(instance.package_id, "get_main_business", code, report_period, start_period, end_period, classification),
                },
                ("tushare", "akshare"),
                ("code", "report_period", "classification", "segment_name"),
            ),
        )

    def get_ccass_holdings(self, code: str, trade_date: str, start_date: str, end_date: str) -> list[CcassHoldingItem]:
        store_identity = {"code": code, "trade_date": trade_date, "start_date": start_date, "end_date": end_date}
        handlers = {
            "get_ccass_holdings": lambda instance: lambda: _source_package_call(instance.package_id, "get_ccass_holdings", code, trade_date, start_date, end_date),
        }
        return self._store_list(
            "stocks.ownership.ccass_holdings",
            store_identity,
            CcassHoldingItem,
            ("code", "trade_date"),
            ("code", "trade_date"),
            lambda: self._source_list("stocks.ownership.ccass_holdings", handlers, ("tushare",), ("code", "trade_date")),
        )

    def get_ccass_holding_details(self, code: str, trade_date: str, start_date: str, end_date: str) -> list[CcassHoldingDetailItem]:
        store_identity = {"code": code, "trade_date": trade_date, "start_date": start_date, "end_date": end_date}
        handlers = {
            "get_ccass_holding_details": lambda instance: lambda: _source_package_call(instance.package_id, "get_ccass_holding_details", code, trade_date, start_date, end_date),
        }
        return self._store_list(
            "stocks.ownership.ccass_holding_details",
            store_identity,
            CcassHoldingDetailItem,
            ("code", "trade_date", "participant_id"),
            ("code", "trade_date", "participant_id"),
            lambda: self._source_list("stocks.ownership.ccass_holding_details", handlers, ("tushare",), ("code", "trade_date", "participant_id")),
        )

    def get_hk_connect_holdings(self, code: str, trade_date: str, start_date: str, end_date: str) -> list[HKConnectHoldingItem]:
        store_identity = {"code": code, "trade_date": trade_date, "start_date": start_date, "end_date": end_date}
        handlers = {
            "get_hk_connect_holdings": lambda instance: lambda: _source_package_call(instance.package_id, "get_hk_connect_holdings", code, trade_date, start_date, end_date),
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
            "get_pledge_stats": lambda instance: lambda: _source_package_call(instance.package_id, "get_pledge_stats", code, trade_date, start_date, end_date),
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
            "get_pledge_details": lambda instance: lambda: _source_package_call(instance.package_id, "get_pledge_details", code, start_date, end_date, status),
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
            "get_shareholder_count": lambda instance: lambda: _source_package_call(instance.package_id, "get_shareholder_count", code, trade_date, start_date, end_date),
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
        store_identity = {"code": code, "trade_date": trade_date, "start_date": start_date, "end_date": end_date}
        handlers = {
            "get_shareholder_changes": lambda instance: lambda: _source_package_call(instance.package_id, "get_shareholder_changes", code, trade_date, start_date, end_date),
        }
        return self._store_list(
            "stocks.ownership.shareholders.changes",
            store_identity,
            ShareholderChangeItem,
            ("code", "trade_date"),
            ("code", "trade_date"),
            lambda: self._source_list("stocks.ownership.shareholders.changes", handlers, ("derived_core",), ("code", "trade_date")),
        )

    def get_shareholder_top10(self, code: str, report_period: str, start_period: str, end_period: str) -> list[ShareholderTop10Item]:
        store_identity = {"code": code, "report_period": report_period, "start_period": start_period, "end_period": end_period}
        handlers = {
            "get_shareholder_top10": lambda instance: lambda: _source_package_call(instance.package_id, "get_shareholder_top10", code, report_period, start_period, end_period, False),
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
            "get_shareholder_top10": lambda instance: lambda: _source_package_call(instance.package_id, "get_shareholder_top10", code, report_period, start_period, end_period, True),
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
            "get_research_reports": lambda instance: lambda: _source_package_call(instance.package_id, "get_research_reports", code, report_date, start_date, end_date),
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
            "get_surveys": lambda instance: lambda: _source_package_call(instance.package_id, "get_surveys", code, survey_date, start_date, end_date),
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
        handlers = {
            "get_bse_code_mappings": lambda instance: lambda: _source_package_call(instance.package_id, "get_bse_code_mappings", old_code, new_code, status),
        }
        return self._store_list(
            "stocks.reference.bse_code_mappings",
            store_identity,
            BSECodeMappingItem,
            ("old_code", "new_code", "effective_date"),
            ("effective_date", "old_code", "new_code"),
            lambda: self._source_list("stocks.reference.bse_code_mappings", handlers, ("tushare",), ("old_code", "new_code", "effective_date")),
        )

    def get_hk_connect_targets(self, direction: str, status: str, effective_date: str) -> list[HKConnectTargetItem]:
        store_identity = {"direction": direction, "status": status, "effective_date": effective_date}
        handlers = {
            "get_hk_connect_targets": lambda instance: lambda: _source_package_call(instance.package_id, "get_hk_connect_targets", direction, status, effective_date),
        }
        return self._store_list(
            "stocks.reference.hk_connect_targets",
            store_identity,
            HKConnectTargetItem,
            ("code", "direction", "effective_date"),
            ("effective_date", "direction", "code"),
            lambda: self._source_list("stocks.reference.hk_connect_targets", handlers, ("tushare",), ("code", "direction", "effective_date")),
        )

    def get_auctions(self, code: str, session: str, trade_date: str, start_date: str, end_date: str) -> list[AuctionItem]:
        store_identity = {"code": code, "session": session, "trade_date": trade_date, "start_date": start_date, "end_date": end_date}
        handlers = {
            "get_auctions": lambda instance: lambda: _source_package_call(instance.package_id, "get_auctions", code, session, trade_date, start_date, end_date),
        }
        return self._store_list(
            "stocks.quotes.auctions",
            store_identity,
            AuctionItem,
            ("code", "trade_date", "auction_time", "session"),
            ("trade_date", "code", "auction_time"),
            lambda: self._source_list("stocks.quotes.auctions", handlers, ("tushare", "akshare"), ("code", "trade_date", "auction_time", "session")),
        )
