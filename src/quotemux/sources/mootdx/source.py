from __future__ import annotations

from datetime import datetime, timedelta
import os

import pandas as pd

from platform_models import IndexMemberItem, IndexQuoteItem, StockQuoteItem
from quotemux.infra.cache.store import build_cache_path, filter_frame_by_datetime_range, latest_n_rows, merge_cache_frame, read_cache_frame, write_cache_frame
from quotemux.infra.common import build_time_bounds, format_datetime_value, normalize_index_code, normalize_stock_code
from quotemux.runtime_core.quality import calibrate_quote_units
from quotemux.infra.provider_runtime.core import call_provider_api

try:
    from mootdx.quotes import Quotes
except Exception:
    Quotes = None


MOOTDX_FREQ_MAP = {
    "1m": 8,
    "5m": 0,
    "15m": 1,
    "30m": 2,
    "60m": 3,
    "1d": 9,
    "1w": 5,
    "1mo": 6,
}
DEFAULT_SERVERS = (
    ("218.6.170.47", 7709),
    ("124.70.199.56", 7709),
    ("180.153.18.172", 80),
)
INDEX_MEMBER_NAME_MAP = {
    "000016": "涓婅瘉50",
    "000300": "娌繁300",
    "000905": "涓瘉500",
    "399300": "娌繁300",
}


def _is_available() -> bool:
    return Quotes is not None


def _require_available() -> None:
    if not _is_available():
        raise RuntimeError("mootdx 不可用")


def _resolve_servers() -> list[tuple[str, int]]:
    text = os.getenv("MHK_MOOTDX_SERVERS", "")
    if text == "":
        return list(DEFAULT_SERVERS)
    servers: list[tuple[str, int]] = []
    for item in text.split(","):
        if ":" not in item:
            continue
        host, port_text = item.strip().split(":", 1)
        try:
            servers.append((host.strip(), int(port_text)))
        except ValueError:
            continue
    return servers or list(DEFAULT_SERVERS)


def _call_mootdx(api_name: str, callback):
    _require_available()
    last_error: Exception | None = None
    for server in _resolve_servers():
        try:
            def _invoke():
                client = Quotes.factory(market="std", server=server, bestip=False, timeout=10)
                return callback(client)
            result = call_provider_api("mootdx", api_name, _invoke)
            if hasattr(result, "empty") and bool(result.empty):
                continue
            return result
        except Exception as exc:
            last_error = exc
    if last_error is not None:
        raise last_error
    return pd.DataFrame()


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
        start_dt = end_dt - timedelta(days=30 if intraday else 400)
    elif start_dt is None:
        start_dt = end_dt - timedelta(days=30 if intraday else 400)
    elif end_dt is None:
        end_dt = datetime.now()
    return start_dt, end_dt


def _estimate_fetch_count(freq: str, start_dt: datetime, end_dt: datetime) -> int:
    span_days = max(1, (end_dt.date() - start_dt.date()).days + 1)
    if freq == "1m":
        return min(60000, max(242, span_days * 242))
    if freq == "5m":
        return min(12000, max(48, span_days * 48))
    if freq == "15m":
        return min(6000, max(16, span_days * 16))
    if freq == "30m":
        return min(3000, max(8, span_days * 8))
    if freq == "60m":
        return min(2000, max(4, span_days * 4))
    return min(4000, max(30, span_days + 10))


def _normalize_history_frame(records: pd.DataFrame, code_column: str, code_value: str, freq: str) -> pd.DataFrame:
    if records is None or records.empty:
        return pd.DataFrame()
    work = records.copy()
    if "datetime" in work.columns:
        work["trade_time"] = pd.to_datetime(work["datetime"], errors="coerce")
    else:
        work["trade_time"] = pd.to_datetime(work.index, errors="coerce")
    work[code_column] = code_value
    work["freq"] = freq
    work["open"] = pd.to_numeric(work["open"], errors="coerce")
    work["high"] = pd.to_numeric(work["high"], errors="coerce")
    work["low"] = pd.to_numeric(work["low"], errors="coerce")
    work["close"] = pd.to_numeric(work["close"], errors="coerce")
    if "volume" in work.columns:
        work["volume"] = pd.to_numeric(work["volume"], errors="coerce")
    elif "vol" in work.columns:
        work["volume"] = pd.to_numeric(work["vol"], errors="coerce")
    else:
        work["volume"] = pd.NA
    work["amount"] = pd.to_numeric(work["amount"], errors="coerce") if "amount" in work.columns else pd.NA
    work = work[[code_column, "trade_time", "freq", "open", "high", "low", "close", "volume", "amount"]]
    work = work.dropna(subset=["trade_time"])
    work, _ = calibrate_quote_units(work)
    return work.drop_duplicates(subset=[code_column, "trade_time", "freq"], keep="last").sort_values("trade_time").reset_index(drop=True)


def _fetch_stock_history_frame(code: str, freq: str, start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    fetch_count = _estimate_fetch_count(freq, start_dt, end_dt)
    result = _call_mootdx(
        "quotes.bars",
        lambda client: client.bars(symbol=normalize_stock_code(code), frequency=MOOTDX_FREQ_MAP[freq], offset=fetch_count),
    )
    frame = _normalize_history_frame(result, "code", normalize_stock_code(code), freq)
    return filter_frame_by_datetime_range(frame, "trade_time", start_dt, end_dt)


def _fetch_index_history_frame(index_code: str, freq: str, start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    fetch_count = _estimate_fetch_count(freq, start_dt, end_dt)
    result = _call_mootdx(
        "quotes.index",
        lambda client: client.index(symbol=normalize_index_code(index_code), frequency=MOOTDX_FREQ_MAP[freq], offset=fetch_count),
    )
    frame = _normalize_history_frame(result, "index_code", normalize_index_code(index_code), freq)
    return filter_frame_by_datetime_range(frame, "trade_time", start_dt, end_dt)


def _frame_to_stock_quotes(df: pd.DataFrame, freq: str) -> list[StockQuoteItem]:
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
                adjust="none",
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
    del adjust
    if freq == "tick":
        return []
    start_dt, end_dt = _resolve_time_window(trade_date, start_date, end_date, start_time, end_time, count, freq.endswith("m"))
    items: list[StockQuoteItem] = []
    for code in codes:
        normalized_code = normalize_stock_code(code)
        cache_path = build_cache_path("mootdx", ["stocks", "quotes"], {"code": normalized_code, "freq": freq})
        cache_df = read_cache_frame(cache_path)
        filtered_cache = filter_frame_by_datetime_range(cache_df, "trade_time", start_dt, end_dt)
        if filtered_cache.empty or (count and len(filtered_cache) < count):
            fetched_df = _fetch_stock_history_frame(normalized_code, freq, start_dt, end_dt)
            if not fetched_df.empty:
                cache_df = merge_cache_frame(cache_df, fetched_df, ["code", "trade_time", "freq"], ["trade_time"])
                write_cache_frame(cache_path, cache_df)
        filtered_df = filter_frame_by_datetime_range(cache_df, "trade_time", start_dt, end_dt)
        filtered_df = latest_n_rows(filtered_df, "trade_time", count)
        items.extend(_frame_to_stock_quotes(filtered_df, freq))
    return items


def get_index_quotes(index_codes: list[str], freq: str, trade_date: str, start_date: str, end_date: str, count: int | None) -> list[IndexQuoteItem]:
    start_dt, end_dt = _resolve_time_window(trade_date, start_date, end_date, "", "", count, False)
    items: list[IndexQuoteItem] = []
    for index_code in index_codes:
        normalized_code = normalize_index_code(index_code)
        cache_path = build_cache_path("mootdx", ["indexes", "quotes"], {"index_code": normalized_code, "freq": freq})
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
    block_name = INDEX_MEMBER_NAME_MAP.get(normalize_index_code(index_code), "")
    if block_name == "":
        return []
    result = _call_mootdx("quotes.block", lambda client: client.block())
    if result is None or result.empty:
        return []
    work = result.copy()
    work["blockname"] = work["blockname"].fillna("").astype(str)
    filtered = work[work["blockname"] == block_name].copy()
    if filtered.empty:
        return []
    items: list[IndexMemberItem] = []
    for _, row in filtered.iterrows():
        items.append(
            IndexMemberItem(
                index_code=normalize_index_code(index_code),
                code=str(row["code"]).zfill(6),
                name="",
                weight=None,
                trade_date="",
            )
        )
    return items

