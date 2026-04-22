from __future__ import annotations

from datetime import date, timedelta

from quotemux.infra.db.reference_reads import load_board_catalog_frame, load_board_member_history_frame, load_board_members_frame, load_index_catalog_frame, load_stock_active_codes_frame, load_stock_catalog_frame, load_stock_hl_frame, load_stock_name_history_frame, load_trade_calendar_frame
from platform_models import BoardCatalogItem, BoardCategoryItem, BoardMemberHistoryItem, BoardMemberItem, HLSignalItem, IndexCatalogItem, NameHistoryItem, StockBasicInfo, TradingCalendarItem
from quotemux.infra.common import format_date_value, normalize_index_code, normalize_stock_code, parse_date_text


def map_exchange(market: str) -> str:
    if market == "BJSE":
        return "BSE"
    if market == "SHSE":
        return "SSE"
    return "SZSE"


def map_market(market: str, board_type: str, code: str) -> str:
    if market == "BJSE" or code.startswith(("4", "8")):
        return "beijing"
    if board_type == "KCB" or code.startswith(("688",)):
        return "star_market"
    if board_type == "CYB" or code.startswith(("300", "301")):
        return "chi_next"
    return "main_board"


def map_list_status(delist_date: str) -> str:
    target_day = parse_date_text(delist_date)
    if target_day is None or target_day >= date.today():
        return "listed"
    return "delisted"


def board_category_from_code(board_code: str) -> str:
    text = str(board_code)
    if text.startswith(("881", "877")):
        return "industry"
    if text.startswith(("885", "886", "BK")):
        return "concept"
    return ""


def index_market_from_code(index_code: str) -> str:
    if index_code.startswith("SZSE."):
        return "szse"
    if index_code.startswith("SHSE."):
        return "sse"
    if index_code.startswith("399"):
        return "szse"
    if index_code.startswith("8"):
        return "sw"
    return "sse"


def index_status_from_last_trade_date(last_trade_date: str) -> str:
    trade_day = parse_date_text(last_trade_date)
    if trade_day is None:
        return "active"
    if (date.today() - trade_day).days <= 40:
        return "active"
    return "inactive"


def get_stock_catalog(codes: list[str], name: str, exchange: str, list_status: str, include_delisted: bool, limit: int, offset: int) -> list[StockBasicInfo]:
    market = {"SSE": "SHSE", "SZSE": "SZSE", "BSE": "BJSE"}.get(exchange, "") if exchange else ""
    if exchange and not market:
        return []
    listed_filter = "listed" if list_status == "listed" or not include_delisted else "delisted" if list_status == "delisted" else ""
    df = load_stock_catalog_frame([normalize_stock_code(code) for code in codes], name, market, listed_filter)
    if df.empty:
        return []
    df = df.sort_values("code").iloc[offset: offset + limit]
    items: list[StockBasicInfo] = []
    for _, row in df.iterrows():
        delist_date = format_date_value(row["delisted_date"])
        list_status_text = map_list_status(delist_date)
        items.append(
            StockBasicInfo(
                code=str(row["code"]),
                name=str(row["name"]),
                exchange=map_exchange(str(row["market"])),
                market=map_market(str(row["market"]), str(row["board_type"]), str(row["code"])),
                list_status=list_status_text,
                list_date=format_date_value(row["listed_date"]),
                delist_date="" if list_status_text == "listed" else delist_date,
            )
        )
    return items


def get_stock_basic(code: str) -> StockBasicInfo | None:
    items = get_stock_catalog([normalize_stock_code(code)], "", "", "", True, 1, 0)
    return items[0] if items else None


def get_stock_name_history(code: str, start_date: str, end_date: str) -> list[NameHistoryItem]:
    normalized = normalize_stock_code(code)
    actual_start_date = format_date_value(start_date)
    actual_end_date = format_date_value(end_date)
    df = load_stock_name_history_frame(normalized, actual_start_date, actual_end_date)
    if df.empty:
        return []
    items: list[NameHistoryItem] = []
    for _, row in df.iterrows():
        valid_to_text = format_date_value(row["valid_to"])
        items.append(
            NameHistoryItem(
                code=normalized,
                name=str(row["name"]),
                start_date=format_date_value(row["valid_from"]),
                end_date=valid_to_text,
                ann_date=format_date_value(row["valid_from"]),
            )
        )
    return items


def get_hl_signal(code: str, trade_date: str, start_date: str, end_date: str) -> list[HLSignalItem]:
    normalized = normalize_stock_code(code)
    actual_trade_date = format_date_value(trade_date)
    actual_start_date = format_date_value(start_date)
    actual_end_date = format_date_value(end_date)
    df = load_stock_hl_frame(normalized, actual_trade_date, actual_start_date, actual_end_date)
    if df.empty:
        return []
    items: list[HLSignalItem] = []
    for _, row in df.iterrows():
        h_time = str(row["h_time"] or "")
        l_time = str(row["l_time"] or "")
        if h_time and l_time and h_time < l_time:
            first_extreme = "high"
            signal = "high_first"
        elif h_time and l_time and l_time < h_time:
            first_extreme = "low"
            signal = "low_first"
        else:
            first_extreme = ""
            signal = "same_time"
        items.append(
            HLSignalItem(
                code=normalized,
                trade_date=format_date_value(row["trade_date"]),
                first_extreme=first_extreme,
                high_time=h_time,
                low_time=l_time,
                signal=signal,
            )
        )
    return items


def get_board_catalog(category: str, market: str, status: str, limit: int, offset: int) -> list[BoardCatalogItem]:
    if market and market != "a_share":
        return []
    df = load_board_catalog_frame(status)
    if df.empty:
        return []
    df["category"] = df["board_code"].map(board_category_from_code)
    df["status"] = df["delisted_date"].map(lambda value: "inactive" if map_list_status(format_date_value(value)) == "delisted" else "active")
    if category:
        df = df[df["category"] == category]
    df = df.sort_values("board_code").iloc[offset: offset + limit]
    items: list[BoardCatalogItem] = []
    for _, row in df.iterrows():
        items.append(
            BoardCatalogItem(
                board_code=str(row["board_code"]),
                board_name=str(row["name"]),
                category=str(row["category"]),
                market="a_share",
                status=str(row["status"]),
            )
        )
    return items


def get_index_catalog(category: str, market: str, publisher: str, status: str, limit: int, offset: int) -> list[IndexCatalogItem]:
    del category
    del publisher
    df = load_index_catalog_frame([])
    if df.empty:
        return []
    df["market"] = df["index_code"].map(index_market_from_code)
    df["status"] = df["last_trade_date"].map(lambda value: index_status_from_last_trade_date(format_date_value(value)))
    if market:
        df = df[df["market"] == market]
    if status:
        df = df[df["status"] == status]
    df = df.sort_values("index_code").iloc[offset: offset + limit]
    items: list[IndexCatalogItem] = []
    for _, row in df.iterrows():
        normalized_index_code = normalize_index_code(str(row["index_code"]))
        items.append(
            IndexCatalogItem(
                index_code=normalized_index_code,
                index_name=str(row["index_name"]),
                market=str(row["market"]),
                list_date=format_date_value(row["list_date"]),
                status=str(row["status"]),
            )
        )
    return items


def get_index_profile(index_code: str) -> IndexCatalogItem | None:
    normalized = normalize_index_code(index_code)
    for item in get_index_catalog("", "", "", "", 100000, 0):
        if item.index_code == normalized:
            return item
    return None


def get_board_profile(board_code: str) -> BoardCatalogItem | None:
    for item in get_board_catalog("", "", "", 100000, 0):
        if item.board_code == board_code:
            return item
    return None


def get_board_categories(parent_code: str, level: int | None) -> list[BoardCategoryItem]:
    items = [
        BoardCategoryItem(category_code="concept", category_name="概念板块", parent_code="", level=1, sort_order=1),
        BoardCategoryItem(category_code="industry", category_name="行业板块", parent_code="", level=1, sort_order=2),
    ]
    if parent_code:
        items = [item for item in items if item.parent_code == parent_code]
    if level is not None:
        items = [item for item in items if item.level == level]
    return items


def get_stock_names(codes: list[str]) -> dict[str, str]:
    normalized_codes = [normalize_stock_code(code) for code in codes if normalize_stock_code(code)]
    if not normalized_codes:
        return {}
    df = load_stock_catalog_frame(normalized_codes, "", "", "")
    if df.empty:
        return {}
    result: dict[str, str] = {}
    for _, row in df.iterrows():
        result[str(row["code"])] = str(row["name"])
    return result


def get_stock_active_codes(trade_date: str) -> list[str]:
    actual_trade_date = format_date_value(trade_date)
    if actual_trade_date == "":
        return []
    df = load_stock_active_codes_frame(actual_trade_date)
    if df.empty:
        return []
    items: list[str] = []
    for _, row in df.iterrows():
        code = normalize_stock_code(str(row["code"]))
        if code:
            items.append(code)
    return list(dict.fromkeys(items))


def get_board_members(board_code: str, trade_date: str) -> list[BoardMemberItem]:
    actual_trade_date = format_date_value(trade_date) or date.today().strftime("%Y-%m-%d")
    df = load_board_members_frame(board_code, actual_trade_date)
    if df.empty:
        return []
    items: list[BoardMemberItem] = []
    for _, row in df.iterrows():
        items.append(
            BoardMemberItem(
                board_code=board_code,
                code=str(row["code"]),
                name=str(row["name"]),
                join_date=format_date_value(row["join_date"]),
            )
        )
    return items


def get_trading_calendar(exchange: str, start_date: str, end_date: str, is_open: bool | None) -> list[TradingCalendarItem]:
    actual_exchange = exchange or "SSE"
    actual_start_date = format_date_value(start_date)
    actual_end_date = format_date_value(end_date)
    df = load_trade_calendar_frame(actual_start_date, actual_end_date, is_open)
    if df.empty:
        return []
    items: list[TradingCalendarItem] = []
    for _, row in df.iterrows():
        items.append(
            TradingCalendarItem(
                exchange=actual_exchange,
                trade_date=format_date_value(row["trade_date"]),
                is_open=bool(row["is_open"]),
            )
        )
    return items


def get_board_member_history(board_code: str, start_date: str, end_date: str) -> list[BoardMemberHistoryItem]:
    df = load_board_member_history_frame(board_code)
    if df.empty:
        return []
    start_day = parse_date_text(start_date)
    end_day = parse_date_text(end_date)
    history_items: list[BoardMemberHistoryItem] = []
    for _, row in df.iterrows():
        valid_from = parse_date_text(str(row["valid_from"]))
        if valid_from is not None:
            if (start_day is None or valid_from >= start_day) and (end_day is None or valid_from <= end_day):
                history_items.append(
                    BoardMemberHistoryItem(
                        board_code=board_code,
                        code=str(row["code"]),
                        name=str(row["name"]),
                        effective_date=valid_from.strftime("%Y-%m-%d"),
                        action="add",
                    )
                )
        valid_to_text = format_date_value(row["valid_to"])
        valid_to = parse_date_text(valid_to_text)
        if valid_to is None:
            continue
        effective_date = valid_to + timedelta(days=1)
        if (start_day is None or effective_date >= start_day) and (end_day is None or effective_date <= end_day):
            history_items.append(
                BoardMemberHistoryItem(
                    board_code=board_code,
                    code=str(row["code"]),
                    name=str(row["name"]),
                    effective_date=effective_date.strftime("%Y-%m-%d"),
                    action="remove",
                )
            )
    return sorted(history_items, key=lambda item: (item.effective_date, item.code, item.action))


