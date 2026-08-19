from __future__ import annotations

import pandas as pd

from platform_models import AdjFactorItem, StockQuoteItem
from quotemux.infra.common import INTRADAY_RULES, build_time_bounds, format_date_value, format_datetime_value, normalize_stock_code
from quotemux.infra.db.market_reads import load_stock_adj_factor_frame, load_stock_adjustment_base_factor_frame, load_stock_daily_frame, load_stock_daily_local_window_frame, load_stock_daily_snapshot_full_frame


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


def _daily_frame_to_items(frame: pd.DataFrame, adjust: str, freq: str = "1d", adjustment_base_factors: dict[str, float] | None = None) -> list[StockQuoteItem]:
    if frame.empty or freq != "1d" or adjust not in {"none", "qfq", "hfq"}:
        return []
    work = _apply_daily_adjustment(frame, adjust, adjustment_base_factors or {})
    if work.empty:
        return []
    work["trade_time"] = pd.to_datetime(work["trade_time"], errors="coerce")
    work = work.dropna(subset=["trade_time"])
    items: list[StockQuoteItem] = []
    for code, code_frame in work.groupby("code", sort=False):
        result_frame = code_frame.drop(columns=["code"]).sort_values("trade_time")
        for _, row in result_frame.iterrows():
            items.append(_quote_item_from_row(str(code), row, freq, adjust))
    return items


def _apply_daily_adjustment(frame: pd.DataFrame, adjust: str, adjustment_base_factors: dict[str, float]) -> pd.DataFrame:
    if adjust == "none":
        return frame.copy()
    if "adj_factor" not in frame.columns:
        return pd.DataFrame()
    work = frame.copy()
    work["trade_time"] = pd.to_datetime(work["trade_time"], errors="coerce")
    work["adj_factor"] = pd.to_numeric(work["adj_factor"], errors="coerce")
    work = work.dropna(subset=["code", "trade_time"])
    if work.empty:
        return pd.DataFrame()
    adjusted_groups: list[pd.DataFrame] = []
    for _, code_frame in work.groupby("code", sort=False):
        ordered = code_frame.sort_values("trade_time").copy()
        ordered.loc[ordered["adj_factor"] <= 0, "adj_factor"] = pd.NA
        ordered["adj_factor"] = ordered["adj_factor"].ffill()
        ordered = ordered.dropna(subset=["adj_factor"])
        if ordered.empty:
            continue
        if adjust == "qfq":
            base_factor = adjustment_base_factors.get(str(ordered["code"].iloc[0]))
            if base_factor is None or base_factor <= 0:
                continue
            multiplier = ordered["adj_factor"] / base_factor
        else:
            multiplier = ordered["adj_factor"]
        for column in ("open", "high", "low", "close"):
            ordered[column] = pd.to_numeric(ordered[column], errors="coerce") * multiplier
        ordered["pre_close"] = ordered["close"].shift(1)
        ordered["change"] = ordered["close"] - ordered["pre_close"]
        ordered["pct_chg"] = ordered["change"] / ordered["pre_close"] * 100
        adjusted_groups.append(ordered)
    return pd.concat(adjusted_groups, ignore_index=True) if adjusted_groups else pd.DataFrame()


def get_stock_quotes(codes: list[str], freq: str, trade_date: str, start_date: str, end_date: str, start_time: str, end_time: str, count: int | None, adjust: str, adjustment_base_date: str = "") -> list[StockQuoteItem]:
    if freq in INTRADAY_RULES or freq == "tick":
        return []
    start_dt, end_dt = build_time_bounds(trade_date, start_date, end_date, start_time, end_time, count, False)
    start_text = start_dt.strftime("%Y-%m-%d") if start_dt is not None else ""
    end_text = end_dt.strftime("%Y-%m-%d") if end_dt is not None else ""
    normalized_codes = [normalize_stock_code(code) for code in codes]
    normalized_codes = [code for code in dict.fromkeys(normalized_codes) if code]
    raw_frame = load_stock_daily_frame(normalized_codes, start_text, end_text)
    adjustment_base_factors: dict[str, float] = {}
    if adjust == "qfq" and adjustment_base_date != "":
        base_frame = load_stock_adjustment_base_factor_frame(normalized_codes, adjustment_base_date)
        if not base_frame.empty:
            adjustment_base_factors = {
                str(row["code"]): float(row["adjustment_base_factor"])
                for _, row in base_frame.iterrows()
                if pd.notna(row["adjustment_base_factor"]) and float(row["adjustment_base_factor"]) > 0
            }
    elif adjust == "qfq" and "adj_factor" in raw_frame.columns:
        # 仅供未配置冻结基准日的本地旧调用兼容；HTTP 研究链路必须传入冻结日期。
        adjustment_base_factors = {
            str(code): float(group["adj_factor"].dropna().iloc[-1])
            for code, group in raw_frame.groupby("code", sort=False)
            if not group["adj_factor"].dropna().empty
        }
    items = _daily_frame_to_items(raw_frame, adjust, freq, adjustment_base_factors)
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


def get_local_stock_adj_factors(code: str, start_date: str, end_date: str) -> list[AdjFactorItem]:
    normalized_code = normalize_stock_code(code)
    if normalized_code == "":
        return []
    frame = load_stock_adj_factor_frame(normalized_code, format_date_value(start_date), format_date_value(end_date))
    if frame.empty:
        return []
    return [
        AdjFactorItem(
            code=normalized_code,
            trade_date=format_date_value(row["trade_date"]).replace("-", ""),
            adj_factor=float(row["adj_factor"]),
        )
        for _, row in frame.iterrows()
        if pd.notna(row["adj_factor"])
    ]


def get_stock_codes_missing_adj_factors(codes: list[str], start_date: str, end_date: str) -> list[str]:
    """返回窗口内存在日线但缺少有效复权因子的证券。"""
    normalized_codes = [normalize_stock_code(code) for code in codes]
    normalized_codes = [code for code in dict.fromkeys(normalized_codes) if code]
    if not normalized_codes:
        return []
    frame = load_stock_daily_frame(normalized_codes, format_date_value(start_date), format_date_value(end_date))
    if frame.empty or "adj_factor" not in frame.columns:
        return normalized_codes
    work = frame.copy()
    work["adj_factor"] = pd.to_numeric(work["adj_factor"], errors="coerce")
    if "is_suspended" in work.columns:
        suspended = work["is_suspended"].fillna(False).astype(bool)
        work = work.loc[~suspended]
    incomplete_by_code = (
        work.assign(_missing=~(work["adj_factor"] > 0))
        .groupby("code", sort=False)["_missing"]
        .any()
    )
    return [code for code in normalized_codes if bool(incomplete_by_code.get(code, False))]
