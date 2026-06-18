from __future__ import annotations

import pandas as pd

from platform_models import StockQuoteItem
from quotemux.infra.common import INTRADAY_RULES, build_time_bounds, format_date_value, format_datetime_value, normalize_stock_code
from quotemux.infra.db.market_reads import load_stock_daily_frame, load_stock_daily_local_window_frame, load_stock_daily_snapshot_full_frame


def _quote_item_from_row(code: str, row: pd.Series, freq: str, adjust: str) -> StockQuoteItem:
    pre_close = float(row["pre_close"]) if "pre_close" in row and pd.notna(row["pre_close"]) else None
    close = float(row["close"]) if pd.notna(row["close"]) else None
    return StockQuoteItem(
        code=str(code).zfill(6),
        trade_time=format_datetime_value(row["trade_time"], freq),
        freq=freq,
        open=float(row["open"]) if pd.notna(row["open"]) else None,
        high=float(row["high"]) if pd.notna(row["high"]) else None,
        low=float(row["low"]) if pd.notna(row["low"]) else None,
        close=close,
        pre_close=pre_close,
        change=float(row["change"]) if "change" in row and pd.notna(row["change"]) else None,
        pct_chg=float(row["pct_chg"]) if "pct_chg" in row and pd.notna(row["pct_chg"]) else None,
        volume=float(row["volume"]) if pd.notna(row["volume"]) else None,
        amount=float(row["amount"]) if pd.notna(row["amount"]) else None,
        adjust=adjust,
        is_suspended=bool(row["is_suspended"]) if "is_suspended" in row and pd.notna(row["is_suspended"]) else False,
        is_st=bool(row["is_st"]) if "is_st" in row and pd.notna(row["is_st"]) else False,
    )


def _daily_frame_to_items(frame: pd.DataFrame, adjust: str, freq: str = "1d") -> list[StockQuoteItem]:
    if frame.empty or adjust != "none" or freq != "1d":
        return []
    work = frame.copy()
    work["trade_time"] = pd.to_datetime(work["trade_time"], errors="coerce")
    work = work.dropna(subset=["trade_time"])
    items: list[StockQuoteItem] = []
    for code, code_frame in work.groupby("code", sort=False):
        result_frame = code_frame.drop(columns=["code"]).sort_values("trade_time")
        for _, row in result_frame.iterrows():
            items.append(_quote_item_from_row(str(code), row, freq, adjust))
    return items


def get_stock_quotes(codes: list[str], freq: str, trade_date: str, start_date: str, end_date: str, start_time: str, end_time: str, count: int | None, adjust: str) -> list[StockQuoteItem]:
    if freq in INTRADAY_RULES or freq == "tick":
        return []
    start_dt, end_dt = build_time_bounds(trade_date, start_date, end_date, start_time, end_time, count, False)
    start_text = start_dt.strftime("%Y-%m-%d") if start_dt is not None else ""
    end_text = end_dt.strftime("%Y-%m-%d") if end_dt is not None else ""
    normalized_codes = [normalize_stock_code(code) for code in codes]
    normalized_codes = [code for code in dict.fromkeys(normalized_codes) if code]
    raw_frame = load_stock_daily_frame(normalized_codes, start_text, end_text)
    items = _daily_frame_to_items(raw_frame, adjust, freq)
    if count:
        grouped: dict[str, list[StockQuoteItem]] = {}
        for item in items:
            grouped.setdefault(item.code, []).append(item)
        trimmed: list[StockQuoteItem] = []
        for code_items in grouped.values():
            trimmed.extend(sorted(code_items, key=lambda item: item.trade_time)[-count:])
        return trimmed
    return items


def get_stock_daily_snapshot_full(trade_date: str) -> list[StockQuoteItem]:
    actual_trade_date = format_date_value(trade_date)
    raw_frame = load_stock_daily_snapshot_full_frame(actual_trade_date)
    return _daily_frame_to_items(raw_frame, "none", "1d")


def get_stock_daily_local_window(start_date: str, end_date: str, limit: int | None, offset: int) -> list[StockQuoteItem]:
    actual_start_date = format_date_value(start_date)
    actual_end_date = format_date_value(end_date)
    raw_frame = load_stock_daily_local_window_frame(actual_start_date, actual_end_date, limit, offset)
    return _daily_frame_to_items(raw_frame, "none", "1d")
