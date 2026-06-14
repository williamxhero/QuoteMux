from __future__ import annotations

import pandas as pd

from platform_models import StockQuoteItem
from quotemux.infra.common import INTRADAY_RULES, PRICE_COLUMNS, add_quote_metrics, aggregate_ohlc, build_time_bounds, format_date_value, format_datetime_value, normalize_stock_code
from quotemux.infra.db.market_reads import load_stock_daily_frame, load_stock_daily_previous_frame, load_stock_daily_snapshot_full_frame, load_stock_daily_window_frame


def _repair_adj_factor_frame(frame: pd.DataFrame) -> pd.Series:
    adj_factor = pd.to_numeric(frame["adj_factor"], errors="coerce") if "adj_factor" in frame.columns else pd.Series([pd.NA] * len(frame), index=frame.index)
    close = pd.to_numeric(frame["close"], errors="coerce")
    prev_factor = adj_factor.ffill()
    next_factor = adj_factor.bfill()
    prev_close = close.where(adj_factor.notna()).ffill()
    next_close = close.where(adj_factor.notna()).bfill()
    repaired = adj_factor.copy()
    same_factor_mask = repaired.isna() & prev_factor.notna() & next_factor.notna() & prev_factor.eq(next_factor)
    prev_only_mask = repaired.isna() & prev_factor.notna() & next_factor.isna()
    next_only_mask = repaired.isna() & prev_factor.isna() & next_factor.notna()
    different_factor_mask = repaired.isna() & close.notna() & prev_factor.notna() & prev_close.notna() & next_factor.notna() & next_close.notna() & ~prev_factor.eq(next_factor)
    repaired = repaired.where(~same_factor_mask, prev_factor)
    repaired = repaired.where(~prev_only_mask, prev_factor)
    repaired = repaired.where(~next_only_mask, next_factor)
    prev_candidate = close * prev_factor
    next_candidate = close * next_factor
    prev_anchor = prev_close * prev_factor
    next_anchor = next_close * next_factor
    prev_score = pd.Series((prev_candidate / prev_anchor).apply(_abs_log), index=repaired.index) + pd.Series((next_anchor / prev_candidate).apply(_abs_log), index=repaired.index)
    next_score = pd.Series((next_candidate / prev_anchor).apply(_abs_log), index=repaired.index) + pd.Series((next_anchor / next_candidate).apply(_abs_log), index=repaired.index)
    repaired = repaired.where(~(different_factor_mask & (prev_score <= next_score)), prev_factor)
    repaired = repaired.where(~(different_factor_mask & (next_score < prev_score)), next_factor)
    return repaired


def _abs_log(value: object) -> float:
    if pd.isna(value):
        return float("inf")
    try:
        number = float(value)
    except (TypeError, ValueError):
        return float("inf")
    if number <= 0:
        return float("inf")
    import math

    return abs(math.log(number))


def _apply_stock_adjust(frame: pd.DataFrame, adjust: str) -> pd.DataFrame:
    if frame.empty or adjust == "none" or "adj_factor" not in frame.columns:
        return frame
    work = frame.copy()
    work["adj_factor"] = _repair_adj_factor_frame(work)
    if not work["adj_factor"].notna().any():
        return frame
    latest_factor = work["adj_factor"].dropna().iloc[-1]
    multiplier = work["adj_factor"] / latest_factor if adjust == "qfq" else work["adj_factor"]
    for column in PRICE_COLUMNS:
        work[column] = pd.to_numeric(work[column], errors="coerce") * multiplier
    return work.drop(columns=["adj_factor"])


def _quote_item_from_row(code: str, row: pd.Series, freq: str, adjust: str) -> StockQuoteItem:
    pre_close = float(row["pre_close"]) if "pre_close" in row and pd.notna(row["pre_close"]) else None
    close = float(row["close"]) if pd.notna(row["close"]) else None
    change = None
    pct_chg = None
    if close is not None and pre_close not in {None, 0.0}:
        change = close - pre_close
        pct_chg = change / pre_close * 100
    return StockQuoteItem(
        code=str(code).zfill(6),
        trade_time=format_datetime_value(row["trade_time"], freq),
        freq=freq,
        open=float(row["open"]) if pd.notna(row["open"]) else None,
        high=float(row["high"]) if pd.notna(row["high"]) else None,
        low=float(row["low"]) if pd.notna(row["low"]) else None,
        close=close,
        pre_close=pre_close,
        change=change,
        pct_chg=pct_chg,
        volume=float(row["volume"]) if pd.notna(row["volume"]) else None,
        amount=float(row["amount"]) if pd.notna(row["amount"]) else None,
        adjust=adjust,
        is_suspended=bool(row["is_suspended"]) if "is_suspended" in row and pd.notna(row["is_suspended"]) else False,
        is_st=bool(row["is_st"]) if "is_st" in row and pd.notna(row["is_st"]) else False,
    )


def _daily_frame_to_items(frame: pd.DataFrame, adjust: str, freq: str = "1d") -> list[StockQuoteItem]:
    if frame.empty:
        return []
    work = frame.copy()
    work["trade_time"] = pd.to_datetime(work["trade_time"], errors="coerce")
    work = work.dropna(subset=["trade_time"])
    items: list[StockQuoteItem] = []
    for code, code_frame in work.groupby("code", sort=False):
        adjusted_frame = _apply_stock_adjust(code_frame.drop(columns=["code"]), adjust)
        if freq == "1d" and adjust == "none" and "pre_close" in adjusted_frame.columns:
            result_frame = adjusted_frame.sort_values("trade_time")
        else:
            result_frame = add_quote_metrics(aggregate_ohlc(adjusted_frame, freq))
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


def get_stock_daily_window(start_date: str, end_date: str, limit: int | None, offset: int) -> list[StockQuoteItem]:
    actual_start_date = format_date_value(start_date)
    actual_end_date = format_date_value(end_date)
    raw_frame = load_stock_daily_window_frame(actual_start_date, actual_end_date, limit, offset)
    return _daily_frame_to_items(raw_frame, "none", "1d")


def get_stock_daily_previous(codes: list[str], before_date: str, adjust: str) -> list[StockQuoteItem]:
    actual_before_date = format_date_value(before_date)
    normalized_codes = [normalize_stock_code(code) for code in codes]
    normalized_codes = [code for code in dict.fromkeys(normalized_codes) if code]
    raw_frame = load_stock_daily_previous_frame(normalized_codes, actual_before_date)
    return _daily_frame_to_items(raw_frame, adjust, "1d")
