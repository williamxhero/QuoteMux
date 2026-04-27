from __future__ import annotations

from datetime import timedelta

import pandas as pd

from platform_models import IndexCatalogItem, IndexMemberItem, IndexQuoteItem
from quotemux.infra.common import add_quote_metrics, aggregate_ohlc, build_time_bounds, format_datetime_value, parse_date_text
from quotemux.runtime_core.executor import ProviderStep, SourceInstanceExecutor, run_fallback_chain_with_report
from quotemux.common import build_missing_expected_date_ranges, ensure_limit, has_enough_stock_quote_rows, sort_items, trim_items_per_key
from quotemux.reports import ContractReport
from quotemux.requests.indexes import IndexMembersRequest, IndexQuotesRequest
from quotemux.runtime_core.registry import SourceProxy
from quotemux.settings import QuoteMuxSettings
from quotemux.store import load_store_result, store_result


_akshare_provider = SourceProxy("akshare")
_efinance_provider = SourceProxy("efinance")
_mootdx_provider = SourceProxy("mootdx")
_tushare_provider = SourceProxy("tushare")


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
                    volume=None,
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
    expected_trade_dates = []
    grouped_ranges: dict[tuple[str, str], list[str]] = {}
    for index_code in index_codes:
        existing_dates = {item.trade_time for item in items if item.index_code == index_code and item.freq == "1d"}
        missing_ranges = build_missing_expected_date_ranges(expected_trade_dates, existing_dates)
        if missing_ranges == [] and expected_trade_dates == []:
            missing_ranges = _build_missing_date_ranges(actual_start_date, actual_end_date, existing_dates)
        for missing_start, missing_end in missing_ranges:
            grouped_ranges.setdefault((missing_start, missing_end), []).append(index_code)
    return [(range_codes, range_start, range_end) for (range_start, range_end), range_codes in grouped_ranges.items()]


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


def _build_quote_steps(request_freq: str, request_count: int | None, settings: QuoteMuxSettings) -> tuple[ProviderStep[IndexQuoteItem], ...]:
    handlers = {
        "get_index_quotes": lambda instance: lambda index_codes, missing_start, missing_end: {
            "tushare": _tushare_provider,
            "efinance": _efinance_provider,
            "mootdx": _mootdx_provider,
            "akshare": _akshare_provider,
        }[instance.package_id].get_index_quotes(index_codes, request_freq, "", missing_start, missing_end, request_count),
    }
    return SourceInstanceExecutor(settings).build_steps("indexes.quotes.daily", handlers, ("tushare", "efinance", "mootdx", "akshare"))


def _build_member_steps(settings: QuoteMuxSettings) -> tuple[ProviderStep[IndexMemberItem], ...]:
    handlers = {
        "get_index_members": lambda instance: lambda request_index_code, request_trade_date: {
            "tushare": _tushare_provider,
            "efinance": _efinance_provider,
            "mootdx": _mootdx_provider,
            "akshare": _akshare_provider,
        }[instance.package_id].get_index_members(request_index_code, request_trade_date),
    }
    return SourceInstanceExecutor(settings).build_steps("indexes.members", handlers, ("tushare", "efinance", "mootdx", "akshare"))


class QuoteMuxIndexes:
    def __init__(self, settings: QuoteMuxSettings) -> None:
        self._settings = settings

    def get_catalog(self, category: str, market: str, publisher: str, status: str, limit: int, offset: int) -> list[IndexCatalogItem]:
        actual_limit = ensure_limit(limit)
        actual_market = _normalize_catalog_market(market)
        store_identity = {"category": category, "market": actual_market, "publisher": publisher, "status": status, "limit": limit, "offset": offset}
        store_items, store_read = load_store_result("indexes.catalog", store_identity, IndexCatalogItem)
        if store_read.hit:
            return store_items
        ts_items = _tushare_provider.get_index_catalog("", category, actual_market, publisher, status) if self._settings.is_source_enabled("tushare") else []
        filtered_items = _filter_catalog_items(ts_items, category, actual_market, publisher, status)
        result = sorted(filtered_items, key=lambda item: item.index_code)[offset: offset + actual_limit]
        store_result("indexes.catalog", store_identity, result, ContractReport(contract_name="indexes.catalog"))
        return result

    def get_profile(self, index_code: str) -> IndexCatalogItem | None:
        store_identity = {"index_code": index_code}
        store_items, store_read = load_store_result("indexes.profile", store_identity, IndexCatalogItem)
        if store_read.hit:
            return store_items[0] if store_items else None
        ts_items = _tushare_provider.get_index_catalog(index_code, "", "", "", "") if self._settings.is_source_enabled("tushare") else []
        item = ts_items[0] if ts_items else None
        store_result("indexes.profile", store_identity, [item] if item is not None else [], ContractReport(contract_name="indexes.profile"))
        return item

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
        store_items: list[IndexQuoteItem] = []
        store_status = "skip"
        if store_enabled:
            store_items, store_read = load_store_result("indexes.quotes.daily", store_identity, IndexQuoteItem)
            store_status = store_read.status
            if store_read.hit:
                trimmed_items = trim_items_per_key(store_items, "index_code", "trade_time", request.count)
                sorted_items = sort_items(trimmed_items, ("index_code", "trade_time"))
                from quotemux.config_runtime.runtime import get_config_runtime

                active_snapshot = get_config_runtime().get_active_snapshot()
                return sorted_items[:actual_limit], ContractReport(
                    contract_name="indexes.quotes.daily",
                    profile_id=active_snapshot.profile_id,
                    profile_version=active_snapshot.version,
                ).with_store_stats(hit=True)
        merged_items, fallback_report = run_fallback_chain_with_report(
            "indexes.quotes.daily",
            store_items if store_status == "partial_hit" else [],
            ("index_code", "trade_time", "freq"),
            lambda items: _build_missing_quote_requests(request.index_codes, items, request_freq, request.trade_date, request.start_date, request.end_date, request_count),
            _build_quote_steps(request_freq, request_count, self._settings),
            self._settings.get_contract_source_order("indexes.quotes.daily", ("tushare", "efinance", "mootdx", "akshare")),
        )
        if actual_freq in {"1w", "1mo"}:
            merged_items = _aggregate_index_quotes(merged_items, actual_freq)
        trimmed_items = trim_items_per_key(merged_items, "index_code", "trade_time", request.count)
        sorted_items = sort_items(trimmed_items, ("index_code", "trade_time"))
        report = ContractReport.from_fallback_report("indexes.quotes.daily", fallback_report)
        if store_enabled:
            store_write = store_result("indexes.quotes.daily", store_identity, merged_items, report, report.quarantine_count)
            report = report.with_store_stats(partial_hit=store_status == "partial_hit", miss=store_status in {"miss", "skip"}, stale=store_status == "stale", write=store_write.status == "write")
        return sorted_items[:actual_limit], report

    def get_members(self, request: IndexMembersRequest) -> list[IndexMemberItem]:
        items, _ = self.get_members_with_report(request)
        return items

    def get_members_with_report(self, request: IndexMembersRequest) -> tuple[list[IndexMemberItem], ContractReport]:
        store_identity = {"index_code": request.index_code, "trade_date": request.trade_date}
        store_items, store_read = load_store_result("indexes.members", store_identity, IndexMemberItem)
        if store_read.hit:
            from quotemux.config_runtime.runtime import get_config_runtime

            active_snapshot = get_config_runtime().get_active_snapshot()
            return store_items, ContractReport(
                contract_name="indexes.members",
                profile_id=active_snapshot.profile_id,
                profile_version=active_snapshot.version,
            ).with_store_stats(hit=True)
        merged_items, fallback_report = run_fallback_chain_with_report(
            "indexes.members",
            store_items if store_read.partial_hit else [],
            ("index_code", "code"),
            lambda current_items: [(request.index_code, request.trade_date)] if current_items == [] else [],
            _build_member_steps(self._settings),
            self._settings.get_contract_source_order("indexes.members", ("tushare", "efinance", "mootdx", "akshare")),
        )
        normalized_items = _merge_index_members(merged_items)
        if normalized_items == []:
            return [], ContractReport.from_fallback_report("indexes.members", fallback_report, degraded=True)
        result = normalized_items
        report = ContractReport.from_fallback_report("indexes.members", fallback_report, degraded=True)
        store_write = store_result("indexes.members", store_identity, result, report, report.quarantine_count)
        report = report.with_store_stats(partial_hit=store_read.partial_hit, miss=store_read.status in {"miss", "skip"}, stale=store_read.status == "stale", write=store_write.status == "write")
        return result, report






