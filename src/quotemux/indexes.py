from __future__ import annotations

from datetime import timedelta

import pandas as pd

from platform_models import IndexCatalogItem, IndexMemberItem, IndexQuoteItem
from quotemux.infra.common import add_quote_metrics, aggregate_ohlc, build_time_bounds, format_datetime_value, parse_date_text
from quotemux.runtime_core.executor import ProviderStep, SourceInstanceExecutor, run_fallback_chain_with_report
from quotemux.common import build_missing_expected_date_ranges, ensure_limit, has_enough_stock_quote_rows, merge_model_lists, sort_items, trim_items_per_key
from quotemux.fact_ref_writes import get_fact_ref_writer
from quotemux.local_store import get_local_index_catalog, get_local_index_profile, get_local_index_quotes
from quotemux.query_engine import CapabilityQuerySpec, execute_capability_query
from quotemux.reports import ContractReport
from quotemux.requests.indexes import IndexMembersRequest, IndexQuotesRequest
from quotemux.source_packages.registry import get_default_source_package_registry
from quotemux.settings import QuoteMuxSettings
from quotemux.store import load_store_result, store_result


def _source_package_call(package_id: str, handler_name: str, *args: object) -> object:
    handler = get_default_source_package_registry().get_handler(package_id, handler_name)
    return handler(*args)


def _expected_trade_dates(start_date: str, end_date: str, settings: QuoteMuxSettings) -> list[str]:
    from quotemux.markets import QuoteMuxMarkets
    from quotemux.requests.markets import TradingCalendarRequest

    items = QuoteMuxMarkets(settings).get_trading_calendar(
        TradingCalendarRequest(exchange="SSE", start_date=start_date, end_date=end_date, is_open=True)
    )
    return [item.trade_date for item in items]


def _normalize_catalog_market(market: str) -> str:
    if not market:
        return ""
    text = market.strip().lower()
    if text == "a_share":
        return ""
    return text


def _filter_catalog_items(items: list[IndexCatalogItem], category: str, market: str, publisher: str, status: str) -> list[IndexCatalogItem]:
    actual_market = _normalize_catalog_market(market)
    result = items
    if category:
        result = [item for item in result if item.category == category]
    if actual_market:
        result = [item for item in result if item.market == actual_market]
    if publisher:
        result = [item for item in result if item.publisher == publisher]
    if status:
        result = [item for item in result if item.status == status]
    return result


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


def _aggregate_index_quotes(items: list[IndexQuoteItem], freq: str) -> list[IndexQuoteItem]:
    if freq not in {"1w", "1mo"} or items == []:
        return items
    frame = pd.DataFrame([item.model_dump() for item in items])
    frame["trade_time"] = pd.to_datetime(frame["trade_time"], errors="coerce")
    for column in ["open", "high", "low", "close", "amount"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["volume"] = pd.NA
    frame = frame.dropna(subset=["trade_time"])
    if frame.empty:
        return []
    result: list[IndexQuoteItem] = []
    for code_value, group in frame.groupby("index_code", sort=False):
        aggregated = add_quote_metrics(aggregate_ohlc(group[["trade_time", "open", "high", "low", "close", "volume", "amount"]], freq))
        for _, row in aggregated.iterrows():
            result.append(
                IndexQuoteItem(
                    index_code=str(code_value),
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
                )
            )
    return result


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


def _build_missing_quote_requests(
    index_codes: list[str],
    items: list[IndexQuoteItem],
    freq: str,
    trade_date: str,
    start_date: str,
    end_date: str,
    count: int | None,
    settings: QuoteMuxSettings,
) -> list[tuple[list[str], str, str]]:
    if trade_date == "" and start_date == "" and end_date == "" and count:
        if has_enough_stock_quote_rows(items, index_codes, count, "index_code"):
            return []
        missing_codes = [index_code for index_code in index_codes if sum(1 for item in items if item.index_code == index_code) < count]
        return [(missing_codes, "", "")] if missing_codes else []
    if freq != "1d":
        return [] if items else [(index_codes, trade_date or start_date, trade_date or end_date)]
    request_start_dt, request_end_dt = build_time_bounds(trade_date, start_date, end_date, "", "", count, False)
    actual_start_date = request_start_dt.strftime("%Y-%m-%d") if request_start_dt is not None else ""
    actual_end_date = request_end_dt.strftime("%Y-%m-%d") if request_end_dt is not None else ""
    if actual_start_date == "" and actual_end_date == "":
        return [] if items else [(index_codes, "", "")]
    if actual_start_date == "":
        actual_start_date = actual_end_date
    if actual_end_date == "":
        actual_end_date = actual_start_date
    expected_trade_dates = _expected_trade_dates(actual_start_date, actual_end_date, settings)
    grouped_ranges: dict[tuple[str, str], list[str]] = {}
    for index_code in index_codes:
        existing_dates = {item.trade_time for item in items if item.index_code == index_code and item.freq == "1d"}
        missing_ranges = build_missing_expected_date_ranges(expected_trade_dates, existing_dates)
        if missing_ranges == [] and expected_trade_dates == []:
            missing_ranges = _build_missing_date_ranges(actual_start_date, actual_end_date, existing_dates)
        for missing_start, missing_end in missing_ranges:
            grouped_ranges.setdefault((missing_start, missing_end), []).append(index_code)
    return [(range_codes, range_start, range_end) for (range_start, range_end), range_codes in grouped_ranges.items()]


def _is_index_quote_row_usable(item: IndexQuoteItem) -> bool:
    if item.close is None:
        return False
    if item.freq != "1d":
        return True
    normalized_code = item.index_code.strip().upper()
    if normalized_code in {"000001", "SHSE.000001"} and item.close < 100.0:
        return False
    if item.pre_close is None:
        return False
    if item.pct_chg is None:
        return False
    if item.pre_close <= 0:
        return False
    if item.pct_chg < -50.0 or item.pct_chg > 50.0:
        return False
    return True


def _merge_index_members(items: list[IndexMemberItem]) -> list[IndexMemberItem]:
    merged: dict[tuple[str, str], IndexMemberItem] = {}
    for item in items:
        key = (item.index_code, item.code)
        current = merged.get(key)
        if current is None:
            merged[key] = item
            continue
        weight = current.weight
        if (weight is None or weight == 0.0) and item.weight not in {None, 0.0}:
            weight = item.weight
        trade_date = current.trade_date if current.trade_date else item.trade_date
        name = current.name if current.name else item.name
        merged[key] = IndexMemberItem(index_code=current.index_code, code=current.code, name=name, weight=weight, trade_date=trade_date)
    return sorted(merged.values(), key=lambda item: (item.code, item.trade_date))


def _filter_usable_local_index_items(items: list[IndexQuoteItem], freq: str) -> list[IndexQuoteItem]:
    if freq != "1d":
        return items
    return [item for item in items if _is_index_quote_row_usable(item)]


def _build_quote_steps(request_freq: str, request_count: int | None, settings: QuoteMuxSettings) -> tuple[ProviderStep[IndexQuoteItem], ...]:
    handlers = {
        "get_index_quotes": lambda instance: lambda index_codes, missing_start, missing_end: _source_package_call(instance.package_id, "get_index_quotes", index_codes, request_freq, "", missing_start, missing_end, request_count),
    }
    return SourceInstanceExecutor(settings).build_steps("indexes.quotes.daily", handlers, ("tushare", "akshare", "mootdx", "opentdx"))


def _build_member_steps(settings: QuoteMuxSettings) -> tuple[ProviderStep[IndexMemberItem], ...]:
    handlers = {
        "get_index_members": lambda instance: lambda request_index_code, request_trade_date: _source_package_call(instance.package_id, "get_index_members", request_index_code, request_trade_date),
    }
    return SourceInstanceExecutor(settings).build_steps("indexes.members", handlers, ("tushare", "efinance", "mootdx", "akshare"))


class QuoteMuxIndexes:
    def __init__(self, settings: QuoteMuxSettings) -> None:
        self._settings = settings

    def get_catalog(self, category: str, market: str, publisher: str, status: str, limit: int, offset: int) -> list[IndexCatalogItem]:
        actual_limit = ensure_limit(limit)
        actual_market = _normalize_catalog_market(market)
        store_identity = {"category": category, "market": actual_market, "publisher": publisher, "status": status}
        handlers = {
            "get_index_catalog": lambda instance: lambda: _source_package_call(instance.package_id, "get_index_catalog", "", category, actual_market, publisher, status),
        }
        items, _ = execute_capability_query(
            CapabilityQuerySpec(
                capability_id="indexes.catalog",
                store_identity=store_identity,
                model_type=IndexCatalogItem,
                key_fields=("index_code",),
                sort_fields=("index_code",),
                request_builder=lambda current_items: [()] if current_items == [] else [],
                provider_steps=lambda: SourceInstanceExecutor(self._settings).build_steps("indexes.catalog", handlers, ("tushare",)),
                source_order=self._settings.get_contract_source_order("indexes.catalog", ("tushare",)),
                base_items=get_local_index_catalog([]),
                base_source_name="ref.index",
                fact_ref_writer=get_fact_ref_writer("indexes.catalog"),
            )
        )
        filtered_items = _filter_catalog_items(items, category, actual_market, publisher, status)
        return sorted(filtered_items, key=lambda item: item.index_code)[offset: offset + actual_limit]

    def get_profile(self, index_code: str) -> IndexCatalogItem | None:
        store_identity = {"index_code": index_code}
        handlers = {
            "get_index_catalog": lambda instance: lambda: _source_package_call(instance.package_id, "get_index_catalog", index_code, "", "", "", ""),
        }
        items, _ = execute_capability_query(
            CapabilityQuerySpec(
                capability_id="indexes.profile",
                store_identity=store_identity,
                model_type=IndexCatalogItem,
                key_fields=("index_code",),
                sort_fields=("index_code",),
                request_builder=lambda current_items: [()] if current_items == [] else [],
                provider_steps=lambda: SourceInstanceExecutor(self._settings).build_steps("indexes.profile", handlers, ("tushare",)),
                source_order=self._settings.get_contract_source_order("indexes.profile", ("tushare",)),
                base_items=get_local_index_profile(index_code),
                base_source_name="ref.index",
                fact_ref_writer=get_fact_ref_writer("indexes.profile"),
            )
        )
        return items[0] if items else None

    def get_quotes(self, request: IndexQuotesRequest) -> list[IndexQuoteItem]:
        items, _ = self.get_quotes_with_report(request)
        return items

    def get_quotes_with_report(self, request: IndexQuotesRequest) -> tuple[list[IndexQuoteItem], ContractReport]:
        if request.index_codes == []:
            return [], ContractReport.empty("indexes.quotes.daily")
        actual_limit = ensure_limit(request.limit)
        actual_freq = request.freq or "1d"
        request_freq = _fallback_quote_freq(actual_freq)
        request_count = _fallback_quote_count(actual_freq, request.count)
        store_enabled = actual_freq not in {"1w", "1mo"}
        store_identity = {
            "index_codes": list(request.index_codes),
            "freq": actual_freq,
            "trade_date": request.trade_date,
            "start_date": request.start_date,
            "end_date": request.end_date,
            "count": request.count,
        }
        local_items = _filter_usable_local_index_items(
            get_local_index_quotes(request.index_codes, request_freq, request.trade_date, request.start_date, request.end_date, request_count),
            request_freq,
        )
        merged_items, report = execute_capability_query(
            CapabilityQuerySpec(
                capability_id="indexes.quotes.daily",
                store_identity=store_identity,
                model_type=IndexQuoteItem,
                key_fields=("index_code", "trade_time", "freq"),
                sort_fields=("index_code", "trade_time"),
                request_builder=lambda items: _build_missing_quote_requests(request.index_codes, items, request_freq, request.trade_date, request.start_date, request.end_date, request_count, self._settings),
                provider_steps=lambda: _build_quote_steps(request_freq, request_count, self._settings),
                source_order=self._settings.get_contract_source_order("indexes.quotes.daily", ("tushare", "akshare", "mootdx", "opentdx")),
                base_items=local_items,
                base_source_name="fact.index_bar_1d",
                store_enabled=store_enabled,
                fact_ref_writer=get_fact_ref_writer("indexes.quotes.daily") if request_freq == "1d" else None,
            )
        )
        if actual_freq in {"1w", "1mo"}:
            merged_items = _aggregate_index_quotes(merged_items, actual_freq)
        trimmed_items = trim_items_per_key(merged_items, "index_code", "trade_time", request.count)
        sorted_items = sort_items(trimmed_items, ("index_code", "trade_time"))
        return sorted_items[:actual_limit], report

    def get_members(self, request: IndexMembersRequest) -> list[IndexMemberItem]:
        items, _ = self.get_members_with_report(request)
        return items

    def get_members_with_report(self, request: IndexMembersRequest) -> tuple[list[IndexMemberItem], ContractReport]:
        store_identity = {"index_code": request.index_code, "trade_date": request.trade_date}
        merged_items, report = execute_capability_query(
            CapabilityQuerySpec(
                capability_id="indexes.members",
                store_identity=store_identity,
                model_type=IndexMemberItem,
                key_fields=("index_code", "code"),
                sort_fields=("code", "trade_date"),
                request_builder=lambda current_items: [(request.index_code, request.trade_date)] if current_items == [] else [],
                provider_steps=lambda: _build_member_steps(self._settings),
                source_order=self._settings.get_contract_source_order("indexes.members", ("tushare", "efinance", "mootdx", "akshare")),
            )
        )
        normalized_items = _merge_index_members(merged_items)
        if normalized_items == []:
            return [], report
        result = [item.model_copy(update={"trade_date": request.trade_date}) if item.trade_date == "" and request.trade_date != "" else item for item in normalized_items]
        return result, report
