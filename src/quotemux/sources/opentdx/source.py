from __future__ import annotations

from datetime import datetime, timedelta
from functools import lru_cache

import pandas as pd

from quotemux.infra.cache.store import build_cache_path, filter_frame_by_datetime_range, latest_n_rows, merge_cache_frame, read_cache_frame, write_cache_frame
from quotemux.infra.common import INTRADAY_RULES, add_quote_metrics, aggregate_ohlc, build_time_bounds, format_datetime_value, normalize_stock_code
from quotemux.infra.provider_runtime.core import call_provider_api
from platform_models import StockQuoteItem

try:
    from opentdx import ADJUST, MARKET, PERIOD, TdxClient
except Exception:
    ADJUST = None
    MARKET = None
    PERIOD = None
    TdxClient = None


DEFAULT_LOOKBACK_DAYS = 10
MINUTES_PER_TRADE_DAY = 242


def _is_available() -> bool:
    return TdxClient is not None and MARKET is not None and PERIOD is not None and ADJUST is not None


def _require_available() -> None:
    if not _is_available():
        raise RuntimeError("OpenTDX 不可用")


def _market_from_code(code: str):
    normalized = normalize_stock_code(code)
    if normalized.startswith(("4", "8")):
        return MARKET.BJ
    if normalized.startswith(("5", "6", "9")):
        return MARKET.SH
    return MARKET.SZ


def _adjust_from_text(adjust: str):
    if adjust == "qfq":
        return ADJUST.QFQ
    if adjust == "hfq":
        return ADJUST.HFQ
    return ADJUST.NONE


def _estimate_bar_count(start_dt: datetime, end_dt: datetime) -> int:
    span_days = max(1, (end_dt.date() - start_dt.date()).days + 1)
    return min(60000, max(MINUTES_PER_TRADE_DAY, span_days * MINUTES_PER_TRADE_DAY))


@lru_cache(maxsize=1)
def _client_factory():
    _require_available()
    return TdxClient


def _call_tdx(api_name: str, func, *args, **kwargs):
    _require_available()

    def _invoke():
        with _client_factory()() as client:
            return func(client, *args, **kwargs)

    return call_provider_api("opentdx", api_name, _invoke)


def _fetch_stock_intraday_frame(code: str, start_dt: datetime, end_dt: datetime, adjust: str) -> pd.DataFrame:
    records = _call_tdx(
        "stock_kline",
        lambda client, market, normalized, bar_count, adjust_value: client.stock_kline(
            market,
            normalized,
            PERIOD.MINS,
            start=0,
            count=bar_count,
            times=1,
            adjust=adjust_value,
        ),
        _market_from_code(code),
        normalize_stock_code(code),
        _estimate_bar_count(start_dt, end_dt),
        _adjust_from_text(adjust),
    )
    if not records:
        return pd.DataFrame()

    out = pd.DataFrame(records)
    if out.empty:
        return out
    out["code"] = normalize_stock_code(code)
    time_column = "date_time" if "date_time" in out.columns else "datetime"
    out["trade_time"] = pd.to_datetime(out[time_column], errors="coerce")
    out["freq"] = "1m"
    out["adjust"] = adjust
    out["open"] = pd.to_numeric(out["open"], errors="coerce")
    out["high"] = pd.to_numeric(out["high"], errors="coerce")
    out["low"] = pd.to_numeric(out["low"], errors="coerce")
    out["close"] = pd.to_numeric(out["close"], errors="coerce")
    out["volume"] = pd.to_numeric(out["vol"], errors="coerce")
    out["amount"] = pd.to_numeric(out["amount"], errors="coerce")
    out = out[["code", "trade_time", "freq", "open", "high", "low", "close", "volume", "amount", "adjust"]]
    out = out.dropna(subset=["trade_time"])
    out = filter_frame_by_datetime_range(out, "trade_time", start_dt, end_dt)
    out = out.drop_duplicates(subset=["code", "trade_time", "freq"], keep="last")
    return out.sort_values("trade_time").reset_index(drop=True)


def _frame_to_stock_quotes(df: pd.DataFrame, freq: str) -> list[StockQuoteItem]:
    if df.empty:
        return []
    items: list[StockQuoteItem] = []
    for _, row in df.sort_values("trade_time").iterrows():
        items.append(
            StockQuoteItem(
                code=str(row["code"]),
                trade_time=format_datetime_value(row["trade_time"], freq),
                freq=str(row["freq"]),
                open=float(row["open"]) if pd.notna(row["open"]) else None,
                high=float(row["high"]) if pd.notna(row["high"]) else None,
                low=float(row["low"]) if pd.notna(row["low"]) else None,
                close=float(row["close"]) if pd.notna(row["close"]) else None,
                pre_close=float(row["pre_close"]) if "pre_close" in row and pd.notna(row["pre_close"]) else None,
                change=float(row["change"]) if "change" in row and pd.notna(row["change"]) else None,
                pct_chg=float(row["pct_chg"]) if "pct_chg" in row and pd.notna(row["pct_chg"]) else None,
                volume=float(row["volume"]) if "volume" in row and pd.notna(row["volume"]) else None,
                amount=float(row["amount"]) if pd.notna(row["amount"]) else None,
                adjust=str(row["adjust"]),
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
    if freq not in INTRADAY_RULES:
        return []

    request_start_dt, request_end_dt = build_time_bounds(trade_date, start_date, end_date, start_time, end_time, count, True)
    if request_start_dt is None and request_end_dt is None:
        request_end_dt = datetime.now()
        request_start_dt = request_end_dt - timedelta(days=DEFAULT_LOOKBACK_DAYS)
    elif request_start_dt is None:
        request_start_dt = request_end_dt - timedelta(days=DEFAULT_LOOKBACK_DAYS)
    elif request_end_dt is None:
        request_end_dt = datetime.now()

    items: list[StockQuoteItem] = []
    for code in codes:
        normalized_code = normalize_stock_code(code)
        cache_path = build_cache_path("opentdx", ["stocks", "quotes"], {"code": normalized_code, "adjust": adjust, "source_freq": "1m"})
        cache_df = read_cache_frame(cache_path)
        need_refresh = True
        if not cache_df.empty:
            filtered_cache = filter_frame_by_datetime_range(cache_df, "trade_time", request_start_dt, request_end_dt)
            need_refresh = filtered_cache.empty
        if need_refresh:
            fetched_df = _fetch_stock_intraday_frame(normalized_code, request_start_dt, request_end_dt, adjust)
            if not fetched_df.empty:
                merged_df = merge_cache_frame(cache_df, fetched_df, ["code", "trade_time", "freq"], ["trade_time"])
                write_cache_frame(cache_path, merged_df)
                cache_df = merged_df
        filtered_df = filter_frame_by_datetime_range(cache_df, "trade_time", request_start_dt, request_end_dt)
        if filtered_df.empty:
            continue
        filtered_df["trade_time"] = pd.to_datetime(filtered_df["trade_time"], errors="coerce")
        agg_df = add_quote_metrics(aggregate_ohlc(filtered_df, freq))
        agg_df["code"] = normalized_code
        agg_df["freq"] = freq
        agg_df["adjust"] = adjust
        agg_df = latest_n_rows(agg_df, "trade_time", count)
        items.extend(_frame_to_stock_quotes(agg_df, freq))
    return items

