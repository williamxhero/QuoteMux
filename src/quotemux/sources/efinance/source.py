from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd

from platform_models import IndexMemberItem, IndexQuoteItem, StockQuoteItem
from quotemux.infra.cache.store import build_cache_path, filter_frame_by_date_range, filter_frame_by_datetime_range, latest_n_rows, merge_cache_frame, read_cache_frame, write_cache_frame
from quotemux.infra.common import build_time_bounds, format_date_value, format_datetime_value, normalize_index_code, normalize_stock_code
from quotemux.runtime_core.quality import calibrate_quote_units
from quotemux.infra.provider_runtime.core import call_provider_api

try:
    import efinance as ef
except Exception:
    ef = None


EFINANCE_FREQ_MAP = {
    "1m": 1,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "60m": 60,
    "1d": 101,
    "1w": 102,
    "1mo": 103,
}
EFINANCE_ADJUST_MAP = {
    "none": 0,
    "qfq": 1,
    "hfq": 2,
}
DEFAULT_LOOKBACK_DAYS = 30


def _is_available() -> bool:
    return ef is not None


def _require_available() -> None:
    if not _is_available():
        raise RuntimeError("efinance 不可用")


def _call_ef(api_name: str, func, *args, **kwargs):
    _require_available()
    return call_provider_api("efinance", api_name, func, *args, **kwargs)


def _resolve_time_window(
    trade_date: str,
    start_date: str,
    end_date: str,
    start_time: str,
    end_time: str,
    count: int | None,
    intraday: bool,
) -> tuple[datetime, datetime]:
    start_dt, end_dt = build_time_bounds(trade_date, start_date, end_date, start_time, end_time, count, intraday)
    if start_dt is None and end_dt is None:
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=DEFAULT_LOOKBACK_DAYS if intraday else 400)
    elif start_dt is None:
        start_dt = end_dt - timedelta(days=DEFAULT_LOOKBACK_DAYS if intraday else 400)
    elif end_dt is None:
        end_dt = datetime.now()
    return start_dt, end_dt


def _fetch_stock_history_frame(code: str, freq: str, start_dt: datetime, end_dt: datetime, adjust: str) -> pd.DataFrame:
    result = _call_ef(
        "stock.get_quote_history",
        ef.stock.get_quote_history,
        normalize_stock_code(code),
        start_dt.strftime("%Y%m%d"),
        end_dt.strftime("%Y%m%d"),
        EFINANCE_FREQ_MAP[freq],
        EFINANCE_ADJUST_MAP.get(adjust, 0),
        suppress_error=True,
    )
    if result is None or result.empty:
        return pd.DataFrame()
    work = result.copy()
    work["code"] = work["鑲＄エ浠ｇ爜"].astype(str).str.zfill(6)
    work["trade_time"] = pd.to_datetime(work["鏃ユ湡"], errors="coerce")
    work["freq"] = freq
    work["open"] = pd.to_numeric(work["开盘"], errors="coerce")
    work["high"] = pd.to_numeric(work["最高"], errors="coerce")
    work["low"] = pd.to_numeric(work["最低"], errors="coerce")
    work["close"] = pd.to_numeric(work["鏀剁洏"], errors="coerce")
    work["volume"] = pd.to_numeric(work["成交量"], errors="coerce")
    work["amount"] = pd.to_numeric(work["成交额"], errors="coerce")
    work = work[["code", "trade_time", "freq", "open", "high", "low", "close", "volume", "amount"]]
    work = work.dropna(subset=["trade_time"])
    work, _ = calibrate_quote_units(work)
    return work.drop_duplicates(subset=["code", "trade_time", "freq"], keep="last").sort_values("trade_time").reset_index(drop=True)


def _fetch_index_history_frame(index_code: str, freq: str, start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    result = _call_ef(
        "stock.get_quote_history",
        ef.stock.get_quote_history,
        normalize_index_code(index_code),
        start_dt.strftime("%Y%m%d"),
        end_dt.strftime("%Y%m%d"),
        EFINANCE_FREQ_MAP[freq],
        0,
        suppress_error=True,
    )
    if result is None or result.empty:
        return pd.DataFrame()
    work = result.copy()
    work["index_code"] = work["鑲＄エ浠ｇ爜"].astype(str).str.zfill(6)
    work["trade_time"] = pd.to_datetime(work["鏃ユ湡"], errors="coerce")
    work["freq"] = freq
    work["open"] = pd.to_numeric(work["开盘"], errors="coerce")
    work["high"] = pd.to_numeric(work["最高"], errors="coerce")
    work["low"] = pd.to_numeric(work["最低"], errors="coerce")
    work["close"] = pd.to_numeric(work["鏀剁洏"], errors="coerce")
    work["volume"] = pd.to_numeric(work["成交量"], errors="coerce")
    work["amount"] = pd.to_numeric(work["成交额"], errors="coerce")
    work = work[["index_code", "trade_time", "freq", "open", "high", "low", "close", "volume", "amount"]]
    work = work.dropna(subset=["trade_time"])
    work, _ = calibrate_quote_units(work)
    return work.drop_duplicates(subset=["index_code", "trade_time", "freq"], keep="last").sort_values("trade_time").reset_index(drop=True)


def _frame_to_stock_quotes(df: pd.DataFrame, freq: str, adjust: str) -> list[StockQuoteItem]:
    items: list[StockQuoteItem] = []
    if df.empty:
        return items
    work = df.sort_values("trade_time").copy()
    work["pre_close"] = work["close"].shift(1)
    work["change"] = work["close"] - work["pre_close"]
    work["pct_chg"] = work["change"] / work["pre_close"] * 100
    for _, row in work.iterrows():
        items.append(
            StockQuoteItem(
                code=str(row["code"]),
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
                adjust=adjust,
            )
        )
    return items


def _frame_to_index_quotes(df: pd.DataFrame, freq: str) -> list[IndexQuoteItem]:
    items: list[IndexQuoteItem] = []
    if df.empty:
        return items
    work = df.sort_values("trade_time").copy()
    work["pre_close"] = work["close"].shift(1)
    work["change"] = work["close"] - work["pre_close"]
    work["pct_chg"] = work["change"] / work["pre_close"] * 100
    for _, row in work.iterrows():
        items.append(
            IndexQuoteItem(
                index_code=str(row["index_code"]),
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
    return items


def get_stock_quotes(
    codes: list[str],
    freq: str,
    trade_date: str,
    start_date: str,
    end_date: str,
    start_time: str,
    end_time: str,
    count: int | None,
    adjust: str,
) -> list[StockQuoteItem]:
    if freq == "tick":
        return []
    start_dt, end_dt = _resolve_time_window(trade_date, start_date, end_date, start_time, end_time, count, freq.endswith("m"))
    items: list[StockQuoteItem] = []
    for code in codes:
        normalized_code = normalize_stock_code(code)
        cache_path = build_cache_path("efinance", ["stocks", "quotes"], {"code": normalized_code, "freq": freq, "adjust": adjust})
        cache_df = read_cache_frame(cache_path)
        filtered_cache = filter_frame_by_datetime_range(cache_df, "trade_time", start_dt, end_dt)
        if filtered_cache.empty or (count and len(filtered_cache) < count):
            fetched_df = _fetch_stock_history_frame(normalized_code, freq, start_dt, end_dt, adjust)
            if not fetched_df.empty:
                cache_df = merge_cache_frame(cache_df, fetched_df, ["code", "trade_time", "freq"], ["trade_time"])
                write_cache_frame(cache_path, cache_df)
        filtered_df = filter_frame_by_datetime_range(cache_df, "trade_time", start_dt, end_dt)
        filtered_df = latest_n_rows(filtered_df, "trade_time", count)
        items.extend(_frame_to_stock_quotes(filtered_df, freq, adjust))
    return items


def get_index_quotes(index_codes: list[str], freq: str, trade_date: str, start_date: str, end_date: str, count: int | None) -> list[IndexQuoteItem]:
    start_dt, end_dt = _resolve_time_window(trade_date, start_date, end_date, "", "", count, False)
    items: list[IndexQuoteItem] = []
    for index_code in index_codes:
        normalized_code = normalize_index_code(index_code)
        cache_path = build_cache_path("efinance", ["indexes", "quotes"], {"index_code": normalized_code, "freq": freq})
        cache_df = read_cache_frame(cache_path)
        filtered_cache = filter_frame_by_datetime_range(cache_df, "trade_time", start_dt, end_dt)
        if filtered_cache.empty or (count and len(filtered_cache) < count):
            fetched_df = _fetch_index_history_frame(normalized_code, freq, start_dt, end_dt)
            if not fetched_df.empty:
                cache_df = merge_cache_frame(cache_df, fetched_df, ["index_code", "trade_time", "freq"], ["trade_time"])
                write_cache_frame(cache_path, cache_df)
        filtered_df = filter_frame_by_datetime_range(cache_df, "trade_time", start_dt, end_dt)
        filtered_df = latest_n_rows(filtered_df, "trade_time", count)
        items.extend(_frame_to_index_quotes(filtered_df, freq))
    return items


def get_index_members(index_code: str, trade_date: str) -> list[IndexMemberItem]:
    del trade_date
    result = _call_ef("stock.get_members", ef.stock.get_members, normalize_index_code(index_code))
    if result is None or result.empty:
        return []
    work = result.copy()
    work["index_code"] = work["鎸囨暟浠ｇ爜"].astype(str).str.zfill(6)
    work["code"] = work["鑲＄エ浠ｇ爜"].astype(str).str.zfill(6)
    work["name"] = work["鑲＄エ鍚嶇О"].fillna("").astype(str)
    work["weight"] = pd.to_numeric(work["鑲＄エ鏉冮噸"], errors="coerce")
    items: list[IndexMemberItem] = []
    for _, row in work.iterrows():
        items.append(
            IndexMemberItem(
                index_code=str(row["index_code"]),
                code=str(row["code"]),
                name=str(row["name"]),
                weight=float(row["weight"]) if pd.notna(row["weight"]) else None,
                trade_date="",
            )
        )
    return items

