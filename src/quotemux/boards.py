from __future__ import annotations

from datetime import timedelta

from platform_models import BoardCatalogItem, BoardCategoryItem, BoardMemberHistoryItem, BoardMemberItem, BoardMoneyFlowItem, BoardQuoteItem, StockMoneyFlowItem
from quotemux.infra.common import format_date_value, parse_date_text
from quotemux.common import MARKET_DAILY_SNAPSHOT_LIMIT, build_missing_expected_date_ranges, ensure_limit, has_enough_stock_quote_rows, trim_items_per_key
from quotemux.concepts import ConceptBoardAlias, QuoteMuxConcepts, is_concept_id
from quotemux.fact_ref_writes import get_fact_ref_writer
from quotemux.local_store import get_latest_complete_board_daily_snapshot_codes, get_local_board_catalog, get_local_board_daily_snapshot, get_local_board_members, get_local_board_profile, get_local_board_quotes
from quotemux.query_engine import CapabilityQuerySpec, execute_capability_query
from quotemux.runtime_core.executor import SourceInstanceExecutor
from quotemux.source_packages.registry import get_default_source_package_registry
from quotemux.settings import QuoteMuxSettings
from quotemux.reports import ContractReport
from quotemux.store import load_store_result, store_result


BOARD_CATALOG_SOURCE_ORDER = ("tushare", "akshare")
BOARD_MEMBERS_SOURCE_ORDER = ("derived_core", "tushare", "akshare")
BOARD_MEMBER_HISTORY_SOURCE_ORDER = ("tushare", "akshare")
BOARD_QUOTES_SOURCE_ORDER = ("tushare", "efinance", "akshare")
BOARD_MONEY_FLOW_SOURCE_ORDER = ("akshare", "tushare", "derived_core")
BOARD_MONEY_FLOW_SNAPSHOT_SOURCE_ORDER = ("tushare", "akshare")
BOARD_CATEGORIES_SOURCE_ORDER = ("tushare", "akshare")


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
        existing_dates = {item.trade_time for item in items if item.board_code == board_code and item.freq == freq and _has_complete_board_quote_metrics(item)}
        missing_ranges = build_missing_expected_date_ranges(expected_trade_dates, existing_dates)
        if missing_ranges == [] and expected_trade_dates == []:
            missing_ranges = _build_missing_date_ranges(actual_start_date, actual_end_date, existing_dates)
        for missing_start, missing_end in missing_ranges:
            grouped_ranges.setdefault((missing_start, missing_end), []).append(board_code)
    return [(range_codes, range_start, range_end) for (range_start, range_end), range_codes in grouped_ranges.items()]


def _has_complete_board_quote_metrics(item: BoardQuoteItem) -> bool:
    return item.close is not None and item.pre_close is not None and item.pct_chg is not None and item.amount is not None


def _has_complete_board_daily_snapshot(items: list[BoardQuoteItem]) -> bool:
    return items != [] and all(_has_complete_board_quote_metrics(item) for item in items)


def _has_board_snapshot_metrics(item: BoardQuoteItem) -> bool:
    return item.pct_chg is not None and item.amount is not None


def _has_board_snapshot_metrics_for_codes(items: list[BoardQuoteItem], board_codes: list[str]) -> bool:
    if board_codes == []:
        return items != []
    complete_codes = {item.board_code for item in items if _has_board_snapshot_metrics(item)}
    return all(board_code in complete_codes for board_code in board_codes)


def _has_complete_board_daily_snapshot_for_codes(items: list[BoardQuoteItem], board_codes: list[str]) -> bool:
    if board_codes == []:
        return items != []
    complete_codes = {item.board_code for item in items if _has_complete_board_quote_metrics(item)}
    return all(board_code in complete_codes for board_code in board_codes)


def _has_board_daily_snapshot_for_codes(items: list[BoardQuoteItem], board_codes: list[str]) -> bool:
    if board_codes == []:
        return items != []
    complete_codes = {item.board_code for item in items if _has_board_snapshot_metrics(item)}
    return all(board_code in complete_codes for board_code in board_codes)


def _merge_board_snapshot_items(primary_items: list[BoardQuoteItem], fallback_items: list[BoardQuoteItem]) -> list[BoardQuoteItem]:
    merged_by_code = {item.board_code: item for item in primary_items if _has_board_snapshot_metrics(item)}
    for item in fallback_items:
        if _has_board_snapshot_metrics(item) and item.board_code not in merged_by_code:
            merged_by_code[item.board_code] = item
    return list(merged_by_code.values())


def _write_board_daily_snapshot_items(items: list[BoardQuoteItem], trade_date: str) -> None:
    fact_ref_writer = get_fact_ref_writer("boards.quotes.daily")
    if fact_ref_writer is None:
        return
    fact_ref_writer([item for item in items if item.trade_time == trade_date and is_concept_id(item.board_code)])


def _build_board_member_requests(current_items: list[BoardMemberItem]) -> list[tuple[object, ...]]:
    if current_items == []:
        return [()]
    if any(item.name == "" for item in current_items):
        return [()]
    return []


def _load_money_flow_snapshot_item(board_code: str, trade_date: str, scope: str) -> list[BoardMoneyFlowItem]:
    actual_trade_date = format_date_value(trade_date)
    if board_code == "" or actual_trade_date == "":
        return []
    items, read_result = load_store_result(
        "boards.indicators.money_flow.snapshot",
        {"board_code": "", "trade_date": actual_trade_date, "scope": scope},
        BoardMoneyFlowItem,
    )
    if not read_result.hit and not read_result.partial_hit:
        return []
    normalized_board_code = board_code.upper()
    return [item for item in items if item.board_code.upper() == normalized_board_code and item.trade_date == actual_trade_date and item.scope == scope]


def _load_money_flow_snapshot_range_items(board_code: str, start_date: str, end_date: str, scope: str) -> list[BoardMoneyFlowItem]:
    actual_start_date = format_date_value(start_date)
    actual_end_date = format_date_value(end_date)
    start_day = parse_date_text(actual_start_date)
    end_day = parse_date_text(actual_end_date)
    if board_code == "" or start_day is None or end_day is None or start_day > end_day:
        return []
    items: list[BoardMoneyFlowItem] = []
    current_day = start_day
    while current_day <= end_day:
        items.extend(_load_money_flow_snapshot_item(board_code, current_day.strftime("%Y-%m-%d"), scope))
        current_day += timedelta(days=1)
    return sorted(items, key=lambda item: (item.board_code, item.trade_date))


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


def _aggregate_board_money_flow_item(board_code: str, trade_date: str, scope: str, items: list[StockMoneyFlowItem]) -> BoardMoneyFlowItem | None:
    inflow = _sum_money_flow_values([item.main_inflow for item in items])
    outflow = _sum_money_flow_values([item.main_outflow for item in items])
    net_inflow = _sum_money_flow_values([item.net_inflow for item in items])
    if inflow is None and outflow is None and net_inflow is None:
        return None
    return BoardMoneyFlowItem(board_code=board_code.upper(), trade_date=trade_date, scope=scope, inflow=_money_flow_yuan_to_yi(inflow), outflow=_money_flow_yuan_to_yi(outflow), net_inflow=_money_flow_yuan_to_yi(net_inflow))


def _board_scope_for_provider(scope: str, alias: ConceptBoardAlias) -> str:
    if scope == "board":
        if alias.board_type == "em":
            return "concept"
        if alias.board_type == "ths":
            return "concept"
    return scope


def _rewrite_board_quote_items(items: list[BoardQuoteItem], alias: ConceptBoardAlias) -> list[BoardQuoteItem]:
    return [item.model_copy(update={"board_code": alias.concept_id, "board_name": alias.canonical_name}) for item in items]


def _rewrite_board_member_items(items: list[BoardMemberItem], alias: ConceptBoardAlias) -> list[BoardMemberItem]:
    return [item.model_copy(update={"board_code": alias.concept_id}) for item in items]


def _rewrite_board_member_history_items(items: list[BoardMemberHistoryItem], alias: ConceptBoardAlias) -> list[BoardMemberHistoryItem]:
    return [item.model_copy(update={"board_code": alias.concept_id}) for item in items]


def _rewrite_board_money_flow_items(items: list[BoardMoneyFlowItem], alias: ConceptBoardAlias, scope: str) -> list[BoardMoneyFlowItem]:
    return [item.model_copy(update={"board_code": alias.concept_id, "scope": scope}) for item in items]


def _dedupe_member_union(items: list[BoardMemberItem]) -> list[BoardMemberItem]:
    by_code: dict[str, BoardMemberItem] = {}
    for item in items:
        code = item.code.zfill(6) if item.code != "" else ""
        if code == "" or code in by_code:
            continue
        by_code[code] = item.model_copy(update={"code": code})
    return sorted(by_code.values(), key=lambda item: item.code)


def _merge_time_series_items(items: list[BoardQuoteItem]) -> list[BoardQuoteItem]:
    by_key: dict[tuple[str, str], BoardQuoteItem] = {}
    for item in items:
        key = (item.trade_time, item.freq)
        if key not in by_key:
            by_key[key] = item
    return sorted(by_key.values(), key=lambda item: (item.board_code, item.trade_time, item.freq))


def _merge_money_flow_items(items: list[BoardMoneyFlowItem]) -> list[BoardMoneyFlowItem]:
    by_key: dict[tuple[str, str], BoardMoneyFlowItem] = {}
    for item in items:
        key = (item.trade_date, item.scope)
        if key not in by_key:
            by_key[key] = item
    return sorted(by_key.values(), key=lambda item: (item.board_code, item.trade_date, item.scope))


class QuoteMuxBoards:
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
        return self._concepts.list_board_aliases(normalized, trade_date, self._source_order(capability_id, fallback))

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
        normalized_codes = [_concept_key(item) for item in board_codes if item.strip() != ""]
        if any(not is_concept_id(item) for item in normalized_codes):
            return []
        concept_ids = list(dict.fromkeys(normalized_codes))
        items: list[BoardQuoteItem] = []
        for concept_id in concept_ids:
            concept_items: list[BoardQuoteItem] = []
            aliases = self._concept_aliases(concept_id, trade_date or start_date or end_date, "boards.quotes.daily", BOARD_QUOTES_SOURCE_ORDER)
            for alias in aliases:
                raw_items = _source_package_call(alias.provider, "get_board_quotes", [alias.board_code], freq, trade_date, start_date, end_date, start_time, end_time, count)
                if isinstance(raw_items, list):
                    concept_items.extend(_rewrite_board_quote_items([item for item in raw_items if isinstance(item, BoardQuoteItem)], alias))
            items.extend(_merge_time_series_items(concept_items))
        if count:
            items = trim_items_per_key(items, "board_code", "trade_time", count)
        return sorted(items, key=lambda item: (item.board_code, item.trade_time))[: ensure_limit(limit)]

    def get_catalog(self, category: str, market: str, status: str, limit: int, offset: int) -> list[BoardCatalogItem]:
        if category not in {"", "concept"} or market not in {"", "a_share"} or status not in {"", "active"}:
            return []
        groups = self._concepts.list_alias_groups("")
        sorted_items = [
            BoardCatalogItem(board_code=group.concept_id, board_name=group.canonical_name, category="concept", market="a_share", status="active", start_date=group.start_date, end_date=group.end_date)
            for group in groups
            if group.concept_id != ""
        ]
        return sorted_items[offset: offset + ensure_limit(limit)]

    def get_profile(self, board_code: str) -> BoardCatalogItem | None:
        concept_id = _concept_key(board_code)
        if not is_concept_id(concept_id):
            return None
        group = self._concepts.get_alias_group(concept_id, "")
        if group.concept_id == "":
            return None
        return BoardCatalogItem(board_code=group.concept_id, board_name=group.canonical_name, category="concept", market="a_share", status="active", start_date=group.start_date, end_date=group.end_date)

    def get_members(self, board_code: str, trade_date: str) -> list[BoardMemberItem]:
        aliases = self._concept_aliases(board_code, trade_date, "boards.members", BOARD_MEMBERS_SOURCE_ORDER)
        items: list[BoardMemberItem] = []
        for alias in aliases:
            raw_items = _source_package_call(alias.provider, "get_board_members", alias.board_code, trade_date)
            if isinstance(raw_items, list):
                items.extend(_rewrite_board_member_items([item for item in raw_items if isinstance(item, BoardMemberItem)], alias))
        return _dedupe_member_union(items)

    def get_member_history(self, board_code: str, start_date: str, end_date: str) -> list[BoardMemberHistoryItem]:
        aliases = self._concept_aliases(board_code, start_date or end_date, "boards.members.history", BOARD_MEMBER_HISTORY_SOURCE_ORDER)
        for alias in aliases:
            raw_items = _source_package_call(alias.provider, "get_board_member_history", alias.board_code, start_date, end_date)
            if isinstance(raw_items, list):
                items = _rewrite_board_member_history_items([item for item in raw_items if isinstance(item, BoardMemberHistoryItem)], alias)
                if items != []:
                    return sorted(items, key=lambda item: (item.effective_date, item.code, item.action))
        return []

    def _get_board_money_flow_from_stock_flows(self, board_code: str, trade_date: str, start_date: str, end_date: str, scope: str) -> list[BoardMoneyFlowItem]:
        if scope != "board":
            return []
        date_values = _money_flow_date_values(trade_date, start_date, end_date)
        if date_values == []:
            return []
        from quotemux.stocks import QuoteMuxStocks

        stock_client = QuoteMuxStocks(self._settings)
        rows: list[BoardMoneyFlowItem] = []
        member_items = self.get_members(board_code, date_values[-1])
        member_codes = [item.code for item in member_items if item.code != ""]
        member_codes = list(dict.fromkeys(member_codes))
        if member_codes == []:
            return []
        for date_value in date_values:
            if member_codes == []:
                continue
            flow_items = stock_client.get_money_flow_batch(",".join(member_codes), date_value, "main")
            filtered_items = [item for item in flow_items if item.code in member_codes and item.trade_date == date_value and item.view == "main"]
            item = _aggregate_board_money_flow_item(board_code, date_value, scope, filtered_items)
            if item is not None:
                rows.append(item)
        return sorted(rows, key=lambda item: (item.board_code, item.trade_date))

    def _get_tushare_board_members(self, board_code: str, trade_date: str) -> list[BoardMemberItem]:
        aliases = self._concept_aliases(board_code, trade_date, "boards.members", BOARD_MEMBERS_SOURCE_ORDER)
        for alias in aliases:
            if alias.provider != "tushare":
                continue
            items = _source_package_call("tushare", "get_board_members", alias.board_code, trade_date)
            if isinstance(items, list):
                return _rewrite_board_member_items([item for item in items if isinstance(item, BoardMemberItem)], alias)
        return []

    def _get_money_flow_from_market_snapshot(self, board_code: str, trade_date: str, scope: str) -> tuple[list[BoardMoneyFlowItem], bool]:
        actual_trade_date = format_date_value(trade_date)
        if board_code == "" or actual_trade_date == "":
            return [], False
        normalized_board_code = board_code.upper()
        snapshot_hit = False
        snapshot_scopes = ("concept", "industry") if scope == "board" else (scope,)
        for snapshot_scope in snapshot_scopes:
            items = self.get_market_money_flow(actual_trade_date, snapshot_scope, MARKET_DAILY_SNAPSHOT_LIMIT, 0)
            snapshot_hit = snapshot_hit or items != []
            matched = [
                item.model_copy(update={"scope": scope})
                for item in items
                if item.board_code.upper() == normalized_board_code and item.trade_date == actual_trade_date
            ]
            if matched != []:
                return matched, snapshot_hit
        return [], snapshot_hit

    def get_money_flow(self, board_code: str, trade_date: str, start_date: str, end_date: str, scope: str) -> list[BoardMoneyFlowItem]:
        concept_id = _concept_key(board_code)
        if not is_concept_id(concept_id):
            return []
        snapshot_items = _load_money_flow_snapshot_item(concept_id, trade_date, scope)
        if snapshot_items != []:
            return sorted(snapshot_items, key=lambda item: (item.board_code, item.trade_date))
        snapshot_range_items = _load_money_flow_snapshot_range_items(concept_id, start_date, end_date, scope)
        if snapshot_range_items != []:
            return snapshot_range_items
        snapshot_items, _ = self._get_money_flow_from_market_snapshot(concept_id, trade_date, scope)
        if snapshot_items != []:
            return sorted(snapshot_items, key=lambda item: (item.board_code, item.trade_date))
        store_identity = {"board_code": concept_id, "trade_date": trade_date, "start_date": start_date, "end_date": end_date, "scope": scope}
        store_items, store_read = load_store_result("boards.indicators.money_flow", store_identity, BoardMoneyFlowItem)
        if store_read.hit and self._build_money_flow_requests(store_items, trade_date, start_date, end_date) == []:
            return sorted(store_items, key=lambda item: (item.board_code, item.trade_date))
        aliases = self._concept_aliases(concept_id, trade_date or start_date or end_date, "boards.indicators.money_flow", BOARD_MONEY_FLOW_SOURCE_ORDER)
        provider_items: list[BoardMoneyFlowItem] = []
        for alias in aliases:
            provider_scope = _board_scope_for_provider(scope, alias)
            raw_items = _source_package_call(alias.provider, "get_board_money_flow", alias.board_code, trade_date, start_date, end_date, provider_scope)
            if isinstance(raw_items, list):
                provider_items.extend(_rewrite_board_money_flow_items([item for item in raw_items if isinstance(item, BoardMoneyFlowItem)], alias, scope))
        merged_items = _merge_money_flow_items(provider_items)
        if merged_items != []:
            store_result(
                "boards.indicators.money_flow",
                store_identity,
                merged_items,
                ContractReport(contract_name="boards.indicators.money_flow"),
            )
            return merged_items
        stock_flow_items = self._get_board_money_flow_from_stock_flows(concept_id, trade_date, start_date, end_date, scope)
        if stock_flow_items != []:
            store_result(
                "boards.indicators.money_flow",
                store_identity,
                stock_flow_items,
                ContractReport(
                    contract_name="boards.indicators.money_flow",
                    source_hit_counts={"stocks.indicators.money_flow.batch": 1},
                    source_request_counts={"stocks.indicators.money_flow.batch": 1},
                ),
            )
            return stock_flow_items
        return []

    def get_market_money_flow(self, trade_date: str, scope: str, limit: int, offset: int) -> list[BoardMoneyFlowItem]:
        actual_trade_date = format_date_value(trade_date)
        if actual_trade_date == "":
            return []
        items: list[BoardMoneyFlowItem] = []
        source_order = self._source_order("boards.indicators.money_flow.snapshot", BOARD_MONEY_FLOW_SNAPSHOT_SOURCE_ORDER)
        for provider in source_order:
            raw_items = _source_package_call(provider, "get_board_daily_money_flow_snapshot", actual_trade_date, scope, MARKET_DAILY_SNAPSHOT_LIMIT, 0)
            if not isinstance(raw_items, list):
                continue
            for item in raw_items:
                if not isinstance(item, BoardMoneyFlowItem):
                    continue
                resolved = self._concepts.resolve_alias(provider, "", item.board_code, actual_trade_date)
                if resolved.concept_id == "":
                    continue
                items.append(item.model_copy(update={"board_code": resolved.concept_id, "scope": scope}))
        return _merge_money_flow_items(items)[offset: offset + ensure_limit(limit)]


    def get_market_daily_snapshot(self, trade_date: str, limit: int, offset: int) -> list[BoardQuoteItem]:
        actual_trade_date = format_date_value(trade_date)
        if actual_trade_date == "":
            return []
        actual_limit = ensure_limit(limit)
        local_items = [item for item in get_local_board_daily_snapshot(actual_trade_date, actual_limit + offset, 0) if is_concept_id(item.board_code)]
        catalog_items = self.get_catalog("", "", "active", actual_limit, offset)
        board_codes = [item.board_code for item in catalog_items]
        if board_codes == []:
            return []
        if _has_board_daily_snapshot_for_codes(local_items, board_codes):
            return sorted([item for item in local_items if item.board_code in board_codes], key=lambda item: item.board_code)[:actual_limit]
        request_codes = board_codes
        quote_items = self.get_quotes(request_codes, "1d", actual_trade_date, "", "", "", "", None, max(actual_limit, len(request_codes)))
        if _has_board_daily_snapshot_for_codes(quote_items, board_codes):
            _write_board_daily_snapshot_items(quote_items, actual_trade_date)
            return sorted([item for item in quote_items if item.board_code in board_codes], key=lambda item: item.board_code)
        merged_items = _merge_board_snapshot_items(local_items, quote_items)
        _write_board_daily_snapshot_items(merged_items, actual_trade_date)
        if _has_board_snapshot_metrics_for_codes(merged_items, board_codes):
            return sorted([item for item in merged_items if item.board_code in board_codes], key=lambda item: item.board_code)[:actual_limit]
        return sorted([item for item in merged_items if item.board_code in board_codes], key=lambda item: item.board_code)[:actual_limit]

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
                provider_steps=lambda: SourceInstanceExecutor(self._settings).build_steps("boards.reference.categories", handlers, BOARD_CATEGORIES_SOURCE_ORDER),
                source_order=self._settings.get_contract_source_order("boards.reference.categories", BOARD_CATEGORIES_SOURCE_ORDER),
                payload_builder=_payloads_with_as_of_date,
            )
        )
        return items
