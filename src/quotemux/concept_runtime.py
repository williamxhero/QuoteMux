from __future__ import annotations

from datetime import timedelta

from platform_models import BoardCatalogItem, BoardCategoryItem, BoardMemberHistoryItem, BoardMemberItem, BoardMoneyFlowItem, BoardQuoteItem, ConceptCatalogItem, ConceptCategoryItem, ConceptMemberHistoryItem, ConceptMemberItem, ConceptMoneyFlowItem, ConceptQuoteItem, StockMoneyFlowItem
from quotemux.infra.common import format_date_value, parse_date_text
from quotemux.common import MARKET_DAILY_SNAPSHOT_LIMIT, build_missing_expected_date_ranges, ensure_limit, has_enough_stock_quote_rows, trim_items_per_key
from quotemux.concepts import ConceptBoardAlias, QuoteMuxConcepts, is_concept_id
from quotemux.fact_ref_writes import get_fact_ref_writer
from quotemux.local_store import get_local_concept_daily_snapshot
from quotemux.query_engine import CapabilityQuerySpec, execute_capability_query
from quotemux.runtime_core.executor import SourceInstanceExecutor
from quotemux.source_packages.registry import get_default_source_package_registry
from quotemux.settings import QuoteMuxSettings
from quotemux.reports import ContractReport
from quotemux.store import load_store_result, store_result


CONCEPT_CATALOG_SOURCE_ORDER = ("tushare", "akshare")
CONCEPT_MEMBERS_SOURCE_ORDER = ("derived_core", "tushare", "akshare")
CONCEPT_MEMBER_HISTORY_SOURCE_ORDER = ("tushare", "akshare")
CONCEPT_QUOTES_SOURCE_ORDER = ("tushare", "efinance", "akshare")
CONCEPT_MONEY_FLOW_SOURCE_ORDER = ("akshare", "tushare", "derived_core")
CONCEPT_MONEY_FLOW_SNAPSHOT_SOURCE_ORDER = ("tushare", "akshare")
CONCEPT_CATEGORIES_SOURCE_ORDER = ("tushare", "akshare")


def _source_package_call(package_id: str, handler_name: str, *args: object) -> object:
    handler = get_default_source_package_registry().get_handler(package_id, handler_name)
    return handler(*args)


def _concept_key(value: str) -> str:
    return value.strip().upper()


def _today_text() -> str:
    from datetime import datetime

    return datetime.now().strftime("%Y-%m-%d")


def _payloads_with_as_of_date(items: list[object]) -> list[dict[str, object]]:
    return [{**item.model_dump(), "as_of_date": _today_text()} for item in items if hasattr(item, "model_dump")]


def _concept_member_store_payloads(items: list[ConceptMemberItem], trade_date: str) -> list[dict[str, object]]:
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


def _build_missing_quote_requests(concept_ids: list[str], items: list[ConceptQuoteItem], freq: str, trade_date: str, start_date: str, end_date: str, count: int | None, settings: QuoteMuxSettings) -> list[tuple[list[str], str, str]]:
    if trade_date == "" and start_date == "" and end_date == "" and count:
        if has_enough_stock_quote_rows(items, concept_ids, count, "concept_id"):
            return []
        missing_codes = [concept_id for concept_id in concept_ids if sum(1 for item in items if item.concept_id == concept_id) < count]
        return [(missing_codes, "", "")] if missing_codes else []
    actual_trade_date = format_date_value(trade_date)
    actual_start_date = actual_trade_date or format_date_value(start_date)
    actual_end_date = actual_trade_date or format_date_value(end_date)
    if actual_start_date == "" and actual_end_date == "":
        return [(concept_ids, "", "")] if items == [] else []
    if actual_start_date == "":
        actual_start_date = actual_end_date
    if actual_end_date == "":
        actual_end_date = actual_start_date
    expected_trade_dates = _expected_trade_dates(actual_start_date, actual_end_date, settings) if freq == "1d" else []
    grouped_ranges: dict[tuple[str, str], list[str]] = {}
    for concept_id in concept_ids:
        existing_dates = {item.trade_time for item in items if item.concept_id == concept_id and item.freq == freq and _has_complete_concept_quote_metrics(item)}
        missing_ranges = build_missing_expected_date_ranges(expected_trade_dates, existing_dates)
        if missing_ranges == [] and expected_trade_dates == []:
            missing_ranges = _build_missing_date_ranges(actual_start_date, actual_end_date, existing_dates)
        for missing_start, missing_end in missing_ranges:
            grouped_ranges.setdefault((missing_start, missing_end), []).append(concept_id)
    return [(range_codes, range_start, range_end) for (range_start, range_end), range_codes in grouped_ranges.items()]


def _has_complete_concept_quote_metrics(item: ConceptQuoteItem) -> bool:
    return item.close is not None and item.pre_close is not None and item.pct_chg is not None and item.amount is not None


def _has_complete_concept_daily_snapshot(items: list[ConceptQuoteItem]) -> bool:
    return items != [] and all(_has_complete_concept_quote_metrics(item) for item in items)


def _has_concept_snapshot_metrics(item: ConceptQuoteItem) -> bool:
    return item.pct_chg is not None and item.amount is not None


def _has_concept_snapshot_metrics_for_ids(items: list[ConceptQuoteItem], concept_ids: list[str]) -> bool:
    if concept_ids == []:
        return items != []
    complete_ids = {item.concept_id for item in items if _has_concept_snapshot_metrics(item)}
    return all(concept_id in complete_ids for concept_id in concept_ids)


def _has_complete_concept_daily_snapshot_for_ids(items: list[ConceptQuoteItem], concept_ids: list[str]) -> bool:
    if concept_ids == []:
        return items != []
    complete_ids = {item.concept_id for item in items if _has_complete_concept_quote_metrics(item)}
    return all(concept_id in complete_ids for concept_id in concept_ids)


def _has_concept_daily_snapshot_for_ids(items: list[ConceptQuoteItem], concept_ids: list[str]) -> bool:
    if concept_ids == []:
        return items != []
    complete_ids = {item.concept_id for item in items if _has_concept_snapshot_metrics(item)}
    return all(concept_id in complete_ids for concept_id in concept_ids)


def _merge_concept_snapshot_items(primary_items: list[ConceptQuoteItem], fallback_items: list[ConceptQuoteItem]) -> list[ConceptQuoteItem]:
    merged_by_code = {item.concept_id: item for item in primary_items if _has_concept_snapshot_metrics(item)}
    for item in fallback_items:
        if _has_concept_snapshot_metrics(item) and item.concept_id not in merged_by_code:
            merged_by_code[item.concept_id] = item
    return list(merged_by_code.values())


def _write_concept_daily_snapshot_items(items: list[ConceptQuoteItem], trade_date: str) -> None:
    fact_ref_writer = get_fact_ref_writer("concepts.quotes.daily")
    if fact_ref_writer is None:
        return
    fact_ref_writer([item for item in items if item.trade_time == trade_date and is_concept_id(item.concept_id)])


def _build_concept_member_requests(current_items: list[ConceptMemberItem]) -> list[tuple[object, ...]]:
    if current_items == []:
        return [()]
    if any(item.name == "" for item in current_items):
        return [()]
    return []


def _load_money_flow_snapshot_item(concept_id: str, trade_date: str, scope: str) -> list[ConceptMoneyFlowItem]:
    actual_trade_date = format_date_value(trade_date)
    if concept_id == "" or actual_trade_date == "":
        return []
    items, read_result = load_store_result(
        "concepts.indicators.money_flow.snapshot",
        {"concept_id": "", "trade_date": actual_trade_date, "scope": scope},
        ConceptMoneyFlowItem,
    )
    if not read_result.hit and not read_result.partial_hit:
        return []
    normalized_concept_id = concept_id.upper()
    return [item for item in items if item.concept_id.upper() == normalized_concept_id and item.trade_date == actual_trade_date and item.scope == scope]


def _load_money_flow_snapshot_range_items(concept_id: str, start_date: str, end_date: str, scope: str) -> list[ConceptMoneyFlowItem]:
    actual_start_date = format_date_value(start_date)
    actual_end_date = format_date_value(end_date)
    start_day = parse_date_text(actual_start_date)
    end_day = parse_date_text(actual_end_date)
    if concept_id == "" or start_day is None or end_day is None or start_day > end_day:
        return []
    items: list[ConceptMoneyFlowItem] = []
    current_day = start_day
    while current_day <= end_day:
        items.extend(_load_money_flow_snapshot_item(concept_id, current_day.strftime("%Y-%m-%d"), scope))
        current_day += timedelta(days=1)
    return sorted(items, key=lambda item: (item.concept_id, item.trade_date))


def _money_flow_date_values(trade_date: str, start_date: str, end_date: str) -> list[str]:
    actual_trade_date = format_date_value(trade_date)
    if actual_trade_date != "":
        return [actual_trade_date]
    actual_start_date = format_date_value(start_date)
    actual_end_date = format_date_value(end_date)
    if actual_start_date == "" and actual_end_date == "":
        return []
    if actual_start_date == "":
        actual_start_date = actual_end_date
    if actual_end_date == "":
        actual_end_date = actual_start_date
    start_day = parse_date_text(actual_start_date)
    end_day = parse_date_text(actual_end_date)
    if start_day is None or end_day is None or start_day > end_day:
        return []
    values: list[str] = []
    current_day = start_day
    while current_day <= end_day:
        values.append(current_day.strftime("%Y-%m-%d"))
        current_day += timedelta(days=1)
    return values


def _sum_money_flow_values(values: list[float | None]) -> float | None:
    present_values = [value for value in values if value is not None]
    if present_values == []:
        return None
    return float(sum(present_values))


def _money_flow_yuan_to_yi(value: float | None) -> float | None:
    if value is None:
        return None
    return float(value) / 100000000.0


def _aggregate_concept_money_flow_item(concept_id: str, trade_date: str, scope: str, items: list[StockMoneyFlowItem]) -> ConceptMoneyFlowItem | None:
    inflow = _sum_money_flow_values([item.main_inflow for item in items])
    outflow = _sum_money_flow_values([item.main_outflow for item in items])
    net_inflow = _sum_money_flow_values([item.net_inflow for item in items])
    if inflow is None and outflow is None and net_inflow is None:
        return None
    return ConceptMoneyFlowItem(concept_id=concept_id.upper(), trade_date=trade_date, scope=scope, inflow=_money_flow_yuan_to_yi(inflow), outflow=_money_flow_yuan_to_yi(outflow), net_inflow=_money_flow_yuan_to_yi(net_inflow))


def _concept_scope_for_provider(scope: str, alias: ConceptBoardAlias) -> str:
    if scope == "concept":
        if alias.board_type == "em":
            return "concept"
        if alias.board_type == "ths":
            return "concept"
    return scope


def _rewrite_provider_quote_items(items: list[BoardQuoteItem], alias: ConceptBoardAlias) -> list[ConceptQuoteItem]:
    return [ConceptQuoteItem(concept_id=alias.concept_id, concept_name=alias.canonical_name, trade_time=item.trade_time, freq=item.freq, open=item.open, high=item.high, low=item.low, close=item.close, pre_close=item.pre_close, change=item.change, pct_chg=item.pct_chg, volume=item.volume, amount=item.amount) for item in items]


def _rewrite_provider_member_items(items: list[BoardMemberItem], alias: ConceptBoardAlias) -> list[ConceptMemberItem]:
    return [ConceptMemberItem(concept_id=alias.concept_id, code=item.code, name=item.name, weight=item.weight, join_date=item.join_date) for item in items]


def _rewrite_provider_member_history_items(items: list[BoardMemberHistoryItem], alias: ConceptBoardAlias) -> list[ConceptMemberHistoryItem]:
    return [ConceptMemberHistoryItem(concept_id=alias.concept_id, code=item.code, name=item.name, effective_date=item.effective_date, action=item.action) for item in items]


def _rewrite_provider_money_flow_items(items: list[BoardMoneyFlowItem], alias: ConceptBoardAlias, scope: str) -> list[ConceptMoneyFlowItem]:
    return [ConceptMoneyFlowItem(concept_id=alias.concept_id, trade_date=item.trade_date, scope=scope, inflow=item.inflow, outflow=item.outflow, net_inflow=item.net_inflow) for item in items]


def _dedupe_member_union(items: list[ConceptMemberItem]) -> list[ConceptMemberItem]:
    by_code: dict[str, ConceptMemberItem] = {}
    for item in items:
        code = item.code.zfill(6) if item.code != "" else ""
        if code == "" or code in by_code:
            continue
        by_code[code] = item.model_copy(update={"code": code})
    return sorted(by_code.values(), key=lambda item: item.code)


def _merge_time_series_items(items: list[ConceptQuoteItem]) -> list[ConceptQuoteItem]:
    by_key: dict[tuple[str, str], ConceptQuoteItem] = {}
    for item in items:
        key = (item.trade_time, item.freq)
        if key not in by_key:
            by_key[key] = item
    return sorted(by_key.values(), key=lambda item: (item.concept_id, item.trade_time, item.freq))


def _merge_money_flow_items(items: list[ConceptMoneyFlowItem]) -> list[ConceptMoneyFlowItem]:
    by_key: dict[tuple[str, str], ConceptMoneyFlowItem] = {}
    for item in items:
        key = (item.trade_date, item.scope)
        if key not in by_key:
            by_key[key] = item
    return sorted(by_key.values(), key=lambda item: (item.concept_id, item.trade_date, item.scope))


class QuoteMuxConceptRuntime:
    def __init__(self, settings: QuoteMuxSettings) -> None:
        self._settings = settings
        self._concepts = QuoteMuxConcepts(settings)

    def _source_order(self, capability_id: str, fallback: tuple[str, ...]) -> tuple[str, ...]:
        source_ids = self._settings.get_contract_source_order(capability_id, fallback)
        if source_ids == ():
            source_ids = fallback
        instances = self._settings.get_contract_source_instances(capability_id, fallback)
        instance_packages = {instance.instance_id: instance.package_id for instance in instances}
        ordered_packages: list[str] = []
        for source_id in source_ids:
            package_id = instance_packages.get(source_id, source_id)
            if not self._settings.is_source_enabled(package_id):
                continue
            if package_id not in ordered_packages:
                ordered_packages.append(package_id)
        return tuple(ordered_packages)

    def _concept_aliases(self, concept_id: str, trade_date: str, capability_id: str, fallback: tuple[str, ...]) -> tuple[ConceptBoardAlias, ...]:
        normalized = _concept_key(concept_id)
        if not is_concept_id(normalized):
            return ()
        return self._concepts.list_concept_aliases(normalized, trade_date, self._source_order(capability_id, fallback))

    def _build_money_flow_requests(self, items: list[ConceptMoneyFlowItem], trade_date: str, start_date: str, end_date: str) -> list[tuple[str, str]]:
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
        concept_ids: list[str],
        freq: str,
        trade_date: str,
        start_date: str,
        end_date: str,
        start_time: str,
        end_time: str,
        count: int | None,
        limit: int,
    ) -> list[ConceptQuoteItem]:
        normalized_codes = [_concept_key(item) for item in concept_ids if item.strip() != ""]
        if any(not is_concept_id(item) for item in normalized_codes):
            return []
        concept_ids = list(dict.fromkeys(normalized_codes))
        items: list[ConceptQuoteItem] = []
        for concept_id in concept_ids:
            concept_items: list[ConceptQuoteItem] = []
            aliases = self._concept_aliases(concept_id, trade_date or start_date or end_date, "concepts.quotes.daily", CONCEPT_QUOTES_SOURCE_ORDER)
            for alias in aliases:
                raw_items = _source_package_call(alias.provider, "get_concept_quotes", [alias.board_code], freq, trade_date, start_date, end_date, start_time, end_time, count)
                if isinstance(raw_items, list):
                    concept_items.extend(_rewrite_provider_quote_items([item for item in raw_items if isinstance(item, BoardQuoteItem)], alias))
            items.extend(_merge_time_series_items(concept_items))
        if count:
            items = trim_items_per_key(items, "concept_id", "trade_time", count)
        return sorted(items, key=lambda item: (item.concept_id, item.trade_time))[: ensure_limit(limit)]

    def get_catalog(self, category: str, market: str, status: str, limit: int, offset: int) -> list[ConceptCatalogItem]:
        if category not in {"", "concept"} or market not in {"", "a_share"} or status not in {"", "active"}:
            return []
        groups = self._concepts.list_alias_groups("")
        sorted_items = [
            ConceptCatalogItem(concept_id=group.concept_id, concept_name=group.canonical_name, category="concept", market="a_share", status="active", start_date=group.start_date, end_date=group.end_date)
            for group in groups
            if group.concept_id != ""
        ]
        return sorted_items[offset: offset + ensure_limit(limit)]

    def get_profile(self, concept_id: str) -> ConceptCatalogItem | None:
        concept_id = _concept_key(concept_id)
        if not is_concept_id(concept_id):
            return None
        group = self._concepts.get_alias_group(concept_id, "")
        if group.concept_id == "":
            return None
        return ConceptCatalogItem(concept_id=group.concept_id, concept_name=group.canonical_name, category="concept", market="a_share", status="active", start_date=group.start_date, end_date=group.end_date)

    def get_members(self, concept_id: str, trade_date: str) -> list[ConceptMemberItem]:
        aliases = self._concept_aliases(concept_id, trade_date, "concepts.members", CONCEPT_MEMBERS_SOURCE_ORDER)
        items: list[ConceptMemberItem] = []
        for alias in aliases:
            raw_items = _source_package_call(alias.provider, "get_concept_members", alias.board_code, trade_date)
            if isinstance(raw_items, list):
                items.extend(_rewrite_provider_member_items([item for item in raw_items if isinstance(item, BoardMemberItem)], alias))
        return _dedupe_member_union(items)

    def get_member_history(self, concept_id: str, start_date: str, end_date: str) -> list[ConceptMemberHistoryItem]:
        aliases = self._concept_aliases(concept_id, start_date or end_date, "concepts.members.history", CONCEPT_MEMBER_HISTORY_SOURCE_ORDER)
        for alias in aliases:
            raw_items = _source_package_call(alias.provider, "get_concept_member_history", alias.board_code, start_date, end_date)
            if isinstance(raw_items, list):
                items = _rewrite_provider_member_history_items([item for item in raw_items if isinstance(item, BoardMemberHistoryItem)], alias)
                if items != []:
                    return sorted(items, key=lambda item: (item.effective_date, item.code, item.action))
        return []

    def _get_concept_money_flow_from_stock_flows(self, concept_id: str, trade_date: str, start_date: str, end_date: str, scope: str) -> list[ConceptMoneyFlowItem]:
        if scope != "concept":
            return []
        date_values = _money_flow_date_values(trade_date, start_date, end_date)
        if date_values == []:
            return []
        from quotemux.stocks import QuoteMuxStocks

        stock_client = QuoteMuxStocks(self._settings)
        rows: list[ConceptMoneyFlowItem] = []
        member_items = self.get_members(concept_id, date_values[-1])
        member_codes = [item.code for item in member_items if item.code != ""]
        member_codes = list(dict.fromkeys(member_codes))
        if member_codes == []:
            return []
        for date_value in date_values:
            if member_codes == []:
                continue
            flow_items = stock_client.get_money_flow_batch(",".join(member_codes), date_value, "main")
            filtered_items = [item for item in flow_items if item.code in member_codes and item.trade_date == date_value and item.view == "main"]
            item = _aggregate_concept_money_flow_item(concept_id, date_value, scope, filtered_items)
            if item is not None:
                rows.append(item)
        return sorted(rows, key=lambda item: (item.concept_id, item.trade_date))

    def _get_tushare_concept_members(self, concept_id: str, trade_date: str) -> list[ConceptMemberItem]:
        aliases = self._concept_aliases(concept_id, trade_date, "concepts.members", CONCEPT_MEMBERS_SOURCE_ORDER)
        for alias in aliases:
            if alias.provider != "tushare":
                continue
            items = _source_package_call("tushare", "get_concept_members", alias.board_code, trade_date)
            if isinstance(items, list):
                return _rewrite_provider_member_items([item for item in items if isinstance(item, BoardMemberItem)], alias)
        return []

    def _get_money_flow_from_market_snapshot(self, concept_id: str, trade_date: str, scope: str) -> tuple[list[ConceptMoneyFlowItem], bool]:
        actual_trade_date = format_date_value(trade_date)
        if concept_id == "" or actual_trade_date == "":
            return [], False
        normalized_concept_id = concept_id.upper()
        snapshot_hit = False
        snapshot_scopes = ("concept",) if scope == "concept" else (scope,)
        for snapshot_scope in snapshot_scopes:
            items = self.get_market_money_flow(actual_trade_date, snapshot_scope, MARKET_DAILY_SNAPSHOT_LIMIT, 0)
            snapshot_hit = snapshot_hit or items != []
            matched = [
                item.model_copy(update={"scope": scope})
                for item in items
                if item.concept_id.upper() == normalized_concept_id and item.trade_date == actual_trade_date
            ]
            if matched != []:
                return matched, snapshot_hit
        return [], snapshot_hit

    def get_money_flow(self, concept_id: str, trade_date: str, start_date: str, end_date: str, scope: str) -> list[ConceptMoneyFlowItem]:
        concept_id = _concept_key(concept_id)
        if not is_concept_id(concept_id):
            return []
        snapshot_items = _load_money_flow_snapshot_item(concept_id, trade_date, scope)
        if snapshot_items != []:
            return sorted(snapshot_items, key=lambda item: (item.concept_id, item.trade_date))
        snapshot_range_items = _load_money_flow_snapshot_range_items(concept_id, start_date, end_date, scope)
        if snapshot_range_items != []:
            return snapshot_range_items
        snapshot_items, _ = self._get_money_flow_from_market_snapshot(concept_id, trade_date, scope)
        if snapshot_items != []:
            return sorted(snapshot_items, key=lambda item: (item.concept_id, item.trade_date))
        store_identity = {"concept_id": concept_id, "trade_date": trade_date, "start_date": start_date, "end_date": end_date, "scope": scope}
        store_items, store_read = load_store_result("concepts.indicators.money_flow", store_identity, ConceptMoneyFlowItem)
        if store_read.hit and self._build_money_flow_requests(store_items, trade_date, start_date, end_date) == []:
            return sorted(store_items, key=lambda item: (item.concept_id, item.trade_date))
        aliases = self._concept_aliases(concept_id, trade_date or start_date or end_date, "concepts.indicators.money_flow", CONCEPT_MONEY_FLOW_SOURCE_ORDER)
        provider_items: list[ConceptMoneyFlowItem] = []
        for alias in aliases:
            provider_scope = _concept_scope_for_provider(scope, alias)
            raw_items = _source_package_call(alias.provider, "get_concept_money_flow", alias.board_code, trade_date, start_date, end_date, provider_scope)
            if isinstance(raw_items, list):
                provider_items.extend(_rewrite_provider_money_flow_items([item for item in raw_items if isinstance(item, BoardMoneyFlowItem)], alias, scope))
        merged_items = _merge_money_flow_items(provider_items)
        if merged_items != []:
            store_result(
                "concepts.indicators.money_flow",
                store_identity,
                merged_items,
                ContractReport(contract_name="concepts.indicators.money_flow"),
            )
            return merged_items
        stock_flow_items = self._get_concept_money_flow_from_stock_flows(concept_id, trade_date, start_date, end_date, scope)
        if stock_flow_items != []:
            store_result(
                "concepts.indicators.money_flow",
                store_identity,
                stock_flow_items,
                ContractReport(
                    contract_name="concepts.indicators.money_flow",
                    source_hit_counts={"stocks.indicators.money_flow.batch": 1},
                    source_request_counts={"stocks.indicators.money_flow.batch": 1},
                ),
            )
            return stock_flow_items
        return []

    def get_market_money_flow(self, trade_date: str, scope: str, limit: int, offset: int) -> list[ConceptMoneyFlowItem]:
        actual_trade_date = format_date_value(trade_date)
        if actual_trade_date == "":
            return []
        items: list[ConceptMoneyFlowItem] = []
        source_order = self._source_order("concepts.indicators.money_flow.snapshot", CONCEPT_MONEY_FLOW_SNAPSHOT_SOURCE_ORDER)
        for provider in source_order:
            raw_items = _source_package_call(provider, "get_concept_daily_money_flow_snapshot", actual_trade_date, scope, MARKET_DAILY_SNAPSHOT_LIMIT, 0)
            if not isinstance(raw_items, list):
                continue
            for item in raw_items:
                if not isinstance(item, BoardMoneyFlowItem):
                    continue
                resolved = self._concepts.resolve_alias(provider, "", item.board_code, actual_trade_date)
                if resolved.concept_id == "":
                    continue
                items.append(ConceptMoneyFlowItem(concept_id=resolved.concept_id, trade_date=item.trade_date, scope=scope, inflow=item.inflow, outflow=item.outflow, net_inflow=item.net_inflow))
        return _merge_money_flow_items(items)[offset: offset + ensure_limit(limit)]


    def get_market_daily_snapshot(self, trade_date: str, limit: int, offset: int) -> list[ConceptQuoteItem]:
        actual_trade_date = format_date_value(trade_date)
        if actual_trade_date == "":
            return []
        actual_limit = ensure_limit(limit)
        local_items = [item for item in get_local_concept_daily_snapshot(actual_trade_date, actual_limit + offset, 0) if is_concept_id(item.concept_id)]
        catalog_items = self.get_catalog("", "", "active", actual_limit, offset)
        concept_ids = [item.concept_id for item in catalog_items]
        if concept_ids == []:
            return []
        if _has_concept_daily_snapshot_for_ids(local_items, concept_ids):
            return sorted([item for item in local_items if item.concept_id in concept_ids], key=lambda item: item.concept_id)[:actual_limit]
        request_codes = concept_ids
        quote_items = self.get_quotes(request_codes, "1d", actual_trade_date, "", "", "", "", None, max(actual_limit, len(request_codes)))
        if _has_concept_daily_snapshot_for_ids(quote_items, concept_ids):
            _write_concept_daily_snapshot_items(quote_items, actual_trade_date)
            return sorted([item for item in quote_items if item.concept_id in concept_ids], key=lambda item: item.concept_id)
        merged_items = _merge_concept_snapshot_items(local_items, quote_items)
        _write_concept_daily_snapshot_items(merged_items, actual_trade_date)
        if _has_concept_snapshot_metrics_for_ids(merged_items, concept_ids):
            return sorted([item for item in merged_items if item.concept_id in concept_ids], key=lambda item: item.concept_id)[:actual_limit]
        return sorted([item for item in merged_items if item.concept_id in concept_ids], key=lambda item: item.concept_id)[:actual_limit]

    def get_categories(self, parent_code: str, level: int | None) -> list[ConceptCategoryItem]:
        store_identity = {"parent_code": parent_code, "level": level}
        handlers = {
            "get_concept_categories": lambda instance: lambda: _source_package_call(instance.package_id, "get_concept_categories", parent_code, level),
        }
        items, _ = execute_capability_query(
            CapabilityQuerySpec(
                capability_id="concepts.reference.categories",
                store_identity=store_identity,
                model_type=ConceptCategoryItem,
                key_fields=("category_code",),
                sort_fields=("category_code",),
                request_builder=lambda current_items: [()] if current_items == [] else [],
                provider_steps=lambda: SourceInstanceExecutor(self._settings).build_steps("concepts.reference.categories", handlers, CONCEPT_CATEGORIES_SOURCE_ORDER),
                source_order=self._settings.get_contract_source_order("concepts.reference.categories", CONCEPT_CATEGORIES_SOURCE_ORDER),
                payload_builder=_payloads_with_as_of_date,
            )
        )
        return items
