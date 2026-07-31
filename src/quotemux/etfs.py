from __future__ import annotations

from datetime import timedelta

from platform_models import EtfCatalogItem, EtfDailyQuoteCodeSummary, EtfDailyQuoteItem, EtfDailyQuotesMeta, EtfDailyQuotesQueryResult
from quotemux.common import build_missing_expected_date_ranges, ensure_limit, merge_model_lists, sort_items
from quotemux.fact_ref_writes import get_fact_ref_writer
from quotemux.infra.common import build_time_bounds, format_date_value, parse_date_text
from quotemux.local_store import get_local_etf_catalog, get_local_etf_daily_quotes
from quotemux.query_engine import CapabilityQuerySpec, execute_capability_query
from quotemux.reports import ContractReport
from quotemux.requests.etfs import EtfDailyQuotesRequest
from quotemux.runtime_core.executor import ProviderStep, SourceInstanceExecutor
from quotemux.settings import QuoteMuxSettings
from quotemux.source_packages.registry import get_default_source_package_registry


def _source_package_call(package_id: str, handler_name: str, *args: object) -> object:
    return get_default_source_package_registry().get_handler(package_id, handler_name)(*args)


def _expected_trade_dates(start_date: str, end_date: str, settings: QuoteMuxSettings) -> list[str]:
    from quotemux.markets import QuoteMuxMarkets
    from quotemux.requests.markets import TradingCalendarRequest

    items = QuoteMuxMarkets(settings).get_trading_calendar(
        TradingCalendarRequest(exchange="SSE", start_date=start_date, end_date=end_date, is_open=True)
    )
    return [item.trade_date for item in items]


def _request_dates(request: EtfDailyQuotesRequest) -> tuple[str, str]:
    start_dt, end_dt = build_time_bounds(request.trade_date, request.start_date, request.end_date, "", "", None, False)
    start_date = start_dt.strftime("%Y-%m-%d") if start_dt is not None else ""
    end_date = end_dt.strftime("%Y-%m-%d") if end_dt is not None else ""
    return start_date, end_date


def _fallback_missing_ranges(start_date: str, end_date: str, existing_dates: set[str]) -> list[tuple[str, str]]:
    start_day = parse_date_text(start_date)
    end_day = parse_date_text(end_date)
    if start_day is None or end_day is None or start_day > end_day:
        return []
    ranges: list[tuple[str, str]] = []
    range_start = None
    current = start_day
    while current <= end_day:
        date_text = current.strftime("%Y-%m-%d")
        if date_text not in existing_dates and range_start is None:
            range_start = current
        if date_text in existing_dates and range_start is not None:
            ranges.append((range_start.strftime("%Y-%m-%d"), (current - timedelta(days=1)).strftime("%Y-%m-%d")))
            range_start = None
        current += timedelta(days=1)
    if range_start is not None:
        ranges.append((range_start.strftime("%Y-%m-%d"), end_day.strftime("%Y-%m-%d")))
    return ranges


def _expected_dates_for_code(expected_dates: list[str], item: EtfCatalogItem | None) -> list[str]:
    if item is None:
        return expected_dates
    return [
        trade_date
        for trade_date in expected_dates
        if (item.list_date == "" or trade_date >= item.list_date)
        and (item.delist_date == "" or trade_date <= item.delist_date)
    ]


def _catalog_needs_refresh(ts_codes: list[str], items: list[EtfCatalogItem]) -> bool:
    if ts_codes == []:
        return items == []
    existing_codes = {item.ts_code for item in items}
    return any(ts_code not in existing_codes for ts_code in ts_codes)


def _missing_requests(
    request: EtfDailyQuotesRequest,
    items: list[EtfDailyQuoteItem],
    settings: QuoteMuxSettings,
    catalog_by_code: dict[str, EtfCatalogItem],
) -> list[tuple[list[str], str, str]]:
    start_date, end_date = _request_dates(request)
    if start_date == "" and end_date == "":
        return [(request.ts_codes, "", "")] if items == [] else []
    if start_date == "":
        start_date = end_date
    if end_date == "":
        end_date = start_date
    expected_dates = _expected_trade_dates(start_date, end_date, settings)
    grouped: dict[tuple[str, str], list[str]] = {}
    for ts_code in request.ts_codes:
        existing_dates = {item.trade_date for item in items if item.ts_code == ts_code and item.close is not None}
        code_expected_dates = _expected_dates_for_code(expected_dates, catalog_by_code.get(ts_code))
        missing_ranges = build_missing_expected_date_ranges(code_expected_dates, existing_dates)
        if missing_ranges == [] and code_expected_dates == []:
            missing_ranges = _fallback_missing_ranges(start_date, end_date, existing_dates)
        for missing_start, missing_end in missing_ranges:
            grouped.setdefault((missing_start, missing_end), []).append(ts_code)
    return [(codes, start_date, end_date) for (start_date, end_date), codes in grouped.items()]


def _build_daily_steps(settings: QuoteMuxSettings) -> tuple[ProviderStep[EtfDailyQuoteItem], ...]:
    handlers = {
        "get_etf_daily_quotes": lambda instance: lambda ts_codes, start_date, end_date: _source_package_call(
            instance.package_id, "get_etf_daily_quotes", ts_codes, "", start_date, end_date
        ),
    }
    return SourceInstanceExecutor(settings).build_steps("funds.etf.quotes.daily", handlers, ("tushare", "akshare", "efinance"))


def _summary(ts_code: str, items: list[EtfDailyQuoteItem], expected_dates: list[str], truncated: bool, meta_detail: str) -> EtfDailyQuoteCodeSummary:
    code_items = [item for item in items if item.ts_code == ts_code]
    actual_dates = {item.trade_date for item in code_items if item.close is not None}
    missing_dates = [date for date in expected_dates if date not in actual_dates]
    return EtfDailyQuoteCodeSummary(
        ts_code=ts_code,
        row_count=len(code_items),
        expected_bar_count=len(expected_dates),
        actual_bar_count=len(actual_dates & set(expected_dates)) if expected_dates != [] else len(actual_dates),
        missing_count=len(missing_dates),
        first_trade_date=min((item.trade_date for item in code_items), default=""),
        last_trade_date=max((item.trade_date for item in code_items), default=""),
        complete=missing_dates == [] and not truncated,
        truncated=truncated,
        missing_trade_dates=missing_dates if meta_detail == "full" else [],
    )


class QuoteMuxEtfs:
    def __init__(self, settings: QuoteMuxSettings) -> None:
        self._settings = settings

    def get_catalog(self, ts_codes: list[str], name: str, include_delisted: bool, limit: int, offset: int) -> list[EtfCatalogItem]:
        local_items = get_local_etf_catalog(ts_codes, name, include_delisted)
        handlers = {"get_etf_catalog": lambda instance: lambda: _source_package_call(instance.package_id, "get_etf_catalog")}
        items, _ = execute_capability_query(
            CapabilityQuerySpec(
                capability_id="funds.etf.catalog",
                store_identity={"ts_codes": ts_codes, "name": name, "include_delisted": include_delisted},
                model_type=EtfCatalogItem,
                key_fields=("ts_code",),
                sort_fields=("ts_code",),
                request_builder=lambda current: [()] if _catalog_needs_refresh(ts_codes, current) else [],
                provider_steps=lambda: SourceInstanceExecutor(self._settings).build_steps("funds.etf.catalog", handlers, ("tushare",)),
                source_order=self._settings.get_contract_source_order("funds.etf.catalog", ("tushare",)),
                base_items=local_items,
                base_source_name="ref.etf",
                fact_ref_writer=get_fact_ref_writer("funds.etf.catalog"),
            )
        )
        filtered_items = [item for item in items if (ts_codes == [] or item.ts_code in ts_codes) and (name == "" or name.lower() in item.name.lower())]
        if not include_delisted:
            filtered_items = [item for item in filtered_items if item.delist_date == ""]
        return sort_items(filtered_items, ("ts_code",))[offset: offset + ensure_limit(limit)]

    def get_daily_quotes(self, request: EtfDailyQuotesRequest) -> EtfDailyQuotesQueryResult:
        if request.ts_codes == []:
            return EtfDailyQuotesQueryResult(items=[], meta=EtfDailyQuotesMeta(total_rows=0, returned_rows=0, complete=True, truncated=False))
        catalog_by_code = {
            item.ts_code: item
            for item in self.get_catalog(request.ts_codes, "", True, len(request.ts_codes), 0)
        }
        local_items = get_local_etf_daily_quotes(request.ts_codes, request.trade_date, request.start_date, request.end_date)
        merged_items, _ = execute_capability_query(
            CapabilityQuerySpec(
                capability_id="funds.etf.quotes.daily",
                store_identity={"ts_codes": request.ts_codes, "trade_date": request.trade_date, "start_date": request.start_date, "end_date": request.end_date},
                model_type=EtfDailyQuoteItem,
                key_fields=("ts_code", "trade_date"),
                sort_fields=("ts_code", "trade_date"),
                request_builder=lambda current: _missing_requests(request, current, self._settings, catalog_by_code),
                provider_steps=lambda: _build_daily_steps(self._settings),
                source_order=self._settings.get_contract_source_order("funds.etf.quotes.daily", ("tushare", "akshare", "efinance")),
                base_items=local_items,
                base_source_name="fact.etf_daily_1d",
                fact_ref_writer=get_fact_ref_writer("funds.etf.quotes.daily"),
            )
        )
        sorted_items = sort_items(merge_model_lists([], merged_items, ("ts_code", "trade_date")), ("ts_code", "trade_date"))
        returned_items = sorted_items[: request.limit] if request.limit is not None else sorted_items
        truncated = len(returned_items) < len(sorted_items)
        start_date, end_date = _request_dates(request)
        expected_dates = _expected_trade_dates(start_date, end_date, self._settings) if start_date != "" and end_date != "" else []
        summaries = [
            _summary(
                ts_code,
                returned_items,
                _expected_dates_for_code(expected_dates, catalog_by_code.get(ts_code)),
                truncated,
                request.meta_detail,
            )
            for ts_code in request.ts_codes
        ]
        return EtfDailyQuotesQueryResult(
            items=returned_items,
            meta=EtfDailyQuotesMeta(
                total_rows=len(sorted_items),
                returned_rows=len(returned_items),
                complete=all(item.complete for item in summaries) and not truncated,
                truncated=truncated,
                codes=summaries,
            ),
        )
