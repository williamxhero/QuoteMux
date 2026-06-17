from __future__ import annotations

import pandas as pd

from platform_models import BoardCatalogItem, BoardMemberHistoryItem, BoardMemberItem, BoardQuoteItem, IndexCatalogItem, IndexQuoteItem, NameHistoryItem, StockBasicInfo, StockProfileItem, TradingCalendarItem
from quotemux.infra.common import add_quote_metrics, aggregate_ohlc, build_time_bounds, format_date_value, format_datetime_value, normalize_index_code, normalize_stock_code
from quotemux.infra.db.market_reads import load_board_daily_frame, load_board_daily_snapshot_frame, load_index_daily_frame, load_stock_intraday_frame
from quotemux.infra.db.reference_reads import load_board_catalog_frame, load_board_member_history_frame, load_board_members_frame, load_index_catalog_frame, load_stock_catalog_frame, load_stock_name_history_frame, load_trade_calendar_frame


def _frame_to_stock_quote_items(frame: pd.DataFrame, freq: str):
    from platform_models import StockQuoteItem

    if frame.empty:
        return []
    work = frame.copy()
    work["trade_time"] = pd.to_datetime(work["trade_time"], errors="coerce")
    work = work.dropna(subset=["trade_time"])
    items: list[StockQuoteItem] = []
    for code, code_frame in work.groupby("code", sort=False):
        result_frame = add_quote_metrics(aggregate_ohlc(code_frame.drop(columns=["code"]).sort_values("trade_time"), freq)) if freq != "1m" else add_quote_metrics(code_frame.drop(columns=["code"]).sort_values("trade_time"))
        for _, row in result_frame.iterrows():
            items.append(
                StockQuoteItem(
                    code=str(code).zfill(6),
                    trade_time=format_datetime_value(row["trade_time"], freq),
                    freq=freq,
                    open=float(row["open"]) if pd.notna(row["open"]) else None,
                    high=float(row["high"]) if pd.notna(row["high"]) else None,
                    low=float(row["low"]) if pd.notna(row["low"]) else None,
                    close=float(row["close"]) if pd.notna(row["close"]) else None,
                    pre_close=float(row["pre_close"]) if "pre_close" in row and pd.notna(row["pre_close"]) else None,
                    change=float(row["change"]) if "change" in row and pd.notna(row["change"]) else None,
                    pct_chg=float(row["pct_chg"]) if "pct_chg" in row and pd.notna(row["pct_chg"]) else None,
                    volume=float(row["volume"]) if pd.notna(row["volume"]) else None,
                    amount=float(row["amount"]) if pd.notna(row["amount"]) else None,
                    adjust="none",
                )
            )
    return items


def get_local_stock_intraday_quotes(codes: list[str], freq: str, trade_date: str, start_date: str, end_date: str, start_time: str, end_time: str, count: int | None) -> list[object]:
    if freq not in {"1m", "5m", "15m", "30m", "60m"}:
        return []
    request_start_dt, request_end_dt = build_time_bounds(trade_date, start_date, end_date, start_time, end_time, count, True)
    normalized_codes = [normalize_stock_code(code) for code in codes]
    normalized_codes = [code for code in dict.fromkeys(normalized_codes) if code]
    raw_frame = load_stock_intraday_frame(normalized_codes, request_start_dt, request_end_dt, "30m" if freq == "30m" else "1m")
    items = _frame_to_stock_quote_items(raw_frame, freq)
    if count:
        grouped: dict[str, list[object]] = {}
        for item in items:
            grouped.setdefault(str(item.code), []).append(item)
        trimmed: list[object] = []
        for code_items in grouped.values():
            trimmed.extend(sorted(code_items, key=lambda item: item.trade_time)[-count:])
        return trimmed
    return items


def get_local_index_quotes(index_codes: list[str], freq: str, trade_date: str, start_date: str, end_date: str, count: int | None) -> list[IndexQuoteItem]:
    if freq not in {"1d", "1w", "1mo"}:
        return []
    request_start_dt, request_end_dt = build_time_bounds(trade_date, start_date, end_date, "", "", count, False)
    start_text = request_start_dt.strftime("%Y-%m-%d") if request_start_dt is not None else ""
    end_text = request_end_dt.strftime("%Y-%m-%d") if request_end_dt is not None else ""
    normalized_codes = [normalize_index_code(code) for code in index_codes]
    normalized_codes = [code for code in dict.fromkeys(normalized_codes) if code]
    frame = load_index_daily_frame(normalized_codes, start_text, end_text)
    if frame.empty:
        return []
    work = frame.copy()
    work["trade_time"] = pd.to_datetime(work["trade_time"], errors="coerce")
    work = work.dropna(subset=["trade_time"])
    items: list[IndexQuoteItem] = []
    for index_code, group in work.groupby("index_code", sort=False):
        group_frame = group.drop(columns=["index_code"]).sort_values("trade_time")
        if "volume" not in group_frame:
            group_frame["volume"] = pd.NA
        if freq != "1d":
            result_frame = add_quote_metrics(aggregate_ohlc(group_frame, freq))
        else:
            result_frame = group_frame.copy()
            if "change" not in result_frame.columns:
                result_frame["change"] = pd.NA
            missing_change = result_frame["change"].isna() if "change" in result_frame.columns else pd.Series(dtype=bool)
            if "pre_close" in result_frame.columns:
                computed_change = result_frame["close"] - result_frame["pre_close"]
                result_frame.loc[missing_change, "change"] = computed_change.loc[missing_change]
            else:
                result_frame["pre_close"] = pd.NA
            if "pct_chg" not in result_frame.columns:
                result_frame["pct_chg"] = pd.NA
            missing_pct = result_frame["pct_chg"].isna()
            valid_pre_close = result_frame["pre_close"].notna() & (result_frame["pre_close"] != 0)
            computed_pct = (result_frame["change"] / result_frame["pre_close"]) * 100
            result_frame.loc[missing_pct & valid_pre_close, "pct_chg"] = computed_pct.loc[missing_pct & valid_pre_close]
        for _, row in result_frame.iterrows():
            items.append(
                IndexQuoteItem(
                    index_code=str(index_code),
                    trade_time=format_datetime_value(row["trade_time"], freq),
                    freq=freq,
                    open=float(row["open"]) if pd.notna(row["open"]) else None,
                    high=float(row["high"]) if pd.notna(row["high"]) else None,
                    low=float(row["low"]) if pd.notna(row["low"]) else None,
                    close=float(row["close"]) if pd.notna(row["close"]) else None,
                    pre_close=float(row["pre_close"]) if "pre_close" in row and pd.notna(row["pre_close"]) else None,
                    change=float(row["change"]) if "change" in row and pd.notna(row["change"]) else None,
                    pct_chg=float(row["pct_chg"]) if "pct_chg" in row and pd.notna(row["pct_chg"]) else None,
                    volume=float(row["volume"]) if "volume" in row and pd.notna(row["volume"]) else None,
                    amount=float(row["amount"]) if pd.notna(row["amount"]) else None,
                )
            )
    return items


def get_local_board_quotes(board_codes: list[str], freq: str, trade_date: str, start_date: str, end_date: str, count: int | None) -> list[BoardQuoteItem]:
    if freq not in {"1d", "1w", "1mo"}:
        return []
    request_start_dt, request_end_dt = build_time_bounds(trade_date, start_date, end_date, "", "", count, False)
    start_text = request_start_dt.strftime("%Y-%m-%d") if request_start_dt is not None else ""
    end_text = request_end_dt.strftime("%Y-%m-%d") if request_end_dt is not None else ""
    frame = load_board_daily_frame(board_codes, start_text, end_text)
    if frame.empty:
        return []
    work = frame.copy()
    work["trade_time"] = pd.to_datetime(work["trade_time"], errors="coerce")
    work = work.dropna(subset=["trade_time"])
    items: list[BoardQuoteItem] = []
    for board_code, group in work.groupby("board_code", sort=False):
        result_frame = add_quote_metrics(aggregate_ohlc(group.drop(columns=["board_code"]).sort_values("trade_time"), freq)) if freq != "1d" else add_quote_metrics(group.drop(columns=["board_code"]).sort_values("trade_time"))
        for _, row in result_frame.iterrows():
            items.append(
                BoardQuoteItem(
                    board_code=str(board_code),
                    trade_time=format_datetime_value(row["trade_time"], freq),
                    freq=freq,
                    open=float(row["open"]) if pd.notna(row["open"]) else None,
                    high=float(row["high"]) if pd.notna(row["high"]) else None,
                    low=float(row["low"]) if pd.notna(row["low"]) else None,
                    close=float(row["close"]) if pd.notna(row["close"]) else None,
                    pre_close=float(row["pre_close"]) if "pre_close" in row and pd.notna(row["pre_close"]) else None,
                    change=float(row["change"]) if "change" in row and pd.notna(row["change"]) else None,
                    pct_chg=float(row["pct_chg"]) if "pct_chg" in row and pd.notna(row["pct_chg"]) else None,
                    volume=float(row["volume"]) if "volume" in row and pd.notna(row["volume"]) else None,
                    amount=float(row["amount"]) if "amount" in row and pd.notna(row["amount"]) else None,
                )
            )
    return items


def get_local_board_daily_snapshot(trade_date: str, limit: int, offset: int) -> list[BoardQuoteItem]:
    frame = load_board_daily_snapshot_frame(trade_date, limit, offset)
    if frame.empty:
        return []
    work = frame.copy()
    work["trade_time"] = pd.to_datetime(work["trade_time"], errors="coerce")
    work = work.dropna(subset=["trade_time"])
    if work.empty:
        return []
    if "change" not in work.columns:
        work["change"] = pd.NA
    missing_change = work["change"].isna()
    work.loc[missing_change, "change"] = (work["close"] - work["pre_close"]).loc[missing_change]
    if "pct_chg" not in work.columns:
        work["pct_chg"] = pd.NA
    valid_pre_close = work["pre_close"].notna() & (work["pre_close"] != 0)
    missing_pct = work["pct_chg"].isna()
    computed_pct = (work["change"] / work["pre_close"]) * 100
    work.loc[missing_pct & valid_pre_close, "pct_chg"] = computed_pct.loc[missing_pct & valid_pre_close]
    items: list[BoardQuoteItem] = []
    for _, row in work.sort_values(["board_code", "trade_time"]).iterrows():
        items.append(
            BoardQuoteItem(
                board_code=str(row["board_code"]),
                trade_time=format_datetime_value(row["trade_time"], "1d"),
                freq="1d",
                open=float(row["open"]) if "open" in row and pd.notna(row["open"]) else None,
                high=float(row["high"]) if "high" in row and pd.notna(row["high"]) else None,
                low=float(row["low"]) if "low" in row and pd.notna(row["low"]) else None,
                close=float(row["close"]) if "close" in row and pd.notna(row["close"]) else None,
                pre_close=float(row["pre_close"]) if "pre_close" in row and pd.notna(row["pre_close"]) else None,
                change=float(row["change"]) if "change" in row and pd.notna(row["change"]) else None,
                pct_chg=float(row["pct_chg"]) if "pct_chg" in row and pd.notna(row["pct_chg"]) else None,
                volume=float(row["volume"]) if "volume" in row and pd.notna(row["volume"]) else None,
                amount=float(row["amount"]) if "amount" in row and pd.notna(row["amount"]) else None,
            )
        )
    return items


def get_local_trading_calendar(exchange: str, start_date: str, end_date: str, is_open: bool | None) -> list[TradingCalendarItem]:
    frame = load_trade_calendar_frame(exchange, start_date, end_date, is_open)
    if frame.empty:
        return []
    return [TradingCalendarItem(exchange=exchange or "SSE", trade_date=format_date_value(row["trade_date"]), is_open=bool(row["is_open"])) for _, row in frame.iterrows()]


def get_local_stock_catalog(codes: list[str], name: str, exchange: str, list_status: str, include_delisted: bool) -> list[StockBasicInfo]:
    listed_filter = "" if include_delisted else "listed"
    frame = load_stock_catalog_frame(codes, name, _normalize_ref_market(exchange), listed_filter or _normalize_list_status(list_status))
    if frame.empty:
        return []
    items: list[StockBasicInfo] = []
    for _, row in frame.iterrows():
        delist_date = format_date_value(row["delisted_date"])
        list_status_text = "D" if delist_date else "L"
        items.append(
            StockBasicInfo(
                code=str(row["code"]).zfill(6),
                name=str(row["name"]),
                exchange=str(row["market"]),
                market=str(row["market"]),
                list_status=list_status_text,
                list_date=format_date_value(row["listed_date"]),
                delist_date=delist_date,
                industry=str(row["board_type"]),
                area="",
            )
        )
    return items


def get_local_stock_profile(code: str) -> list[StockProfileItem]:
    catalog_items = get_local_stock_catalog([code], "", "", "", True)
    if catalog_items == []:
        return []
    item = catalog_items[0]
    return [StockProfileItem(code=item.code, company_name=item.name, full_name=item.name, main_business=item.industry)]


def get_local_stock_name_history(code: str, start_date: str, end_date: str) -> list[NameHistoryItem]:
    actual_code = normalize_stock_code(code)
    if actual_code == "":
        return []
    frame = load_stock_name_history_frame(actual_code, format_date_value(start_date), format_date_value(end_date))
    if frame.empty:
        return []
    return [NameHistoryItem(code=str(row["code"]).zfill(6), name=str(row["name"]), start_date=format_date_value(row["valid_from"]), end_date=format_date_value(row["valid_to"]), ann_date="") for _, row in frame.iterrows()]


def _normalize_ref_market(value: str) -> str:
    text = value.upper()
    if text in {"SSE", "SH", "SHSE"}:
        return "SHSE"
    if text in {"SZSE", "SZ"}:
        return "SZSE"
    if text in {"BSE", "BJ", "BJSE"}:
        return "BJSE"
    return value


def _normalize_list_status(value: str) -> str:
    text = value.upper()
    if text in {"L", "LISTED", "ACTIVE"}:
        return "listed"
    if text in {"D", "DELISTED", "INACTIVE"}:
        return "delisted"
    return value


def get_local_board_catalog(status: str) -> list[BoardCatalogItem]:
    frame = load_board_catalog_frame(status)
    if frame.empty:
        return []
    return [BoardCatalogItem(board_code=str(row["board_code"]), board_name=str(row["name"]), category=str(row["board_type"]), status="inactive" if format_date_value(row["delisted_date"]) else "active") for _, row in frame.iterrows()]


def get_local_board_profile(board_code: str) -> list[BoardCatalogItem]:
    return [item for item in get_local_board_catalog("") if item.board_code == board_code]


def get_local_board_members(board_code: str, trade_date: str) -> list[BoardMemberItem]:
    actual_trade_date = format_date_value(trade_date)
    if actual_trade_date == "":
        return []
    frame = load_board_members_frame(board_code, actual_trade_date)
    if frame.empty:
        return []
    return [BoardMemberItem(board_code=str(row["board_code"]), code=str(row["code"]).zfill(6), name=str(row["name"]), join_date=format_date_value(row["join_date"])) for _, row in frame.iterrows()]


def get_local_board_member_history(board_code: str) -> list[BoardMemberHistoryItem]:
    frame = load_board_member_history_frame(board_code)
    if frame.empty:
        return []
    items: list[BoardMemberHistoryItem] = []
    for _, row in frame.iterrows():
        items.append(BoardMemberHistoryItem(board_code=str(row["board_code"]), code=str(row["code"]).zfill(6), name=str(row["name"]), effective_date=format_date_value(row["valid_from"]), action="in"))
        valid_to = format_date_value(row["valid_to"])
        if valid_to:
            items.append(BoardMemberHistoryItem(board_code=str(row["board_code"]), code=str(row["code"]).zfill(6), name=str(row["name"]), effective_date=valid_to, action="out"))
    return items


def get_local_index_catalog(index_codes: list[str]) -> list[IndexCatalogItem]:
    frame = load_index_catalog_frame(index_codes)
    if frame.empty:
        return []
    return [IndexCatalogItem(index_code=str(row["index_code"]), index_name=str(row["index_name"]), category=str(row["category"]), market=str(row["market"]), publisher=str(row["publisher"]), list_date=format_date_value(row["list_date"]), status=str(row["status"])) for _, row in frame.iterrows()]


def get_local_index_profile(index_code: str) -> list[IndexCatalogItem]:
    return get_local_index_catalog([index_code])
