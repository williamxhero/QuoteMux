from __future__ import annotations

from datetime import datetime, timedelta
from functools import lru_cache

import pandas as pd

from quotemux.infra.cache.store import build_cache_path, filter_frame_by_date_range, filter_frame_by_datetime_range, latest_n_rows, merge_cache_frame, plan_missing_ranges, read_cache_frame, write_cache_frame
from quotemux.infra.config import DATE_FORMAT, TS_TOKEN
from platform_models import BoardMoneyFlowItem, IndexCatalogItem, IndexMemberItem, IndexQuoteItem, MarketCapitalFlowItem, StockFinancialStatementItem, StockMoneyFlowItem, StockQuoteItem, TradingCalendarItem
from quotemux.infra.common import INTRADAY_RULES, aggregate_ohlc, add_quote_metrics, build_time_bounds, format_date_value, format_datetime_value, index_code_to_ts, normalize_index_code, normalize_stock_code, stock_code_to_ts
from quotemux.infra.tushare.rate_limit import call_tushare_api

try:
    import tushare as ts
except Exception:
    ts = None


TS_FREQ_MAP = {
    "1m": "1min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "60m": "60min",
    "1d": "D",
    "1w": "W",
    "1mo": "M",
}
TS_INDEX_MARKETS = ("CSI", "SSE", "SZSE", "SW", "CICC", "OTH")


@lru_cache(maxsize=1)
def get_ts_pro():
    if ts is None or not TS_TOKEN:
        return None
    return ts.pro_api(TS_TOKEN)


def _normalize_index_market(market: str) -> str:
    if not market:
        return ""
    return market.strip().lower()


def _resolve_index_markets(market: str) -> list[str]:
    text = market.strip().upper()
    if text == "":
        return list(TS_INDEX_MARKETS)
    if text == "A_SHARE":
        return ["CSI", "SSE", "SZSE", "SW", "CICC"]
    if text in TS_INDEX_MARKETS:
        return [text]
    return []


def _fetch_index_catalog_frame(market: str) -> pd.DataFrame:
    pro = get_ts_pro()
    if pro is None:
        return pd.DataFrame()
    try:
        df = call_tushare_api("index_basic", pro.index_basic, market=market)
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    work = df.copy()
    for column in ["ts_code", "name", "category", "market", "publisher", "list_date", "exp_date"]:
        if column not in work.columns:
            work[column] = ""
    work["index_code"] = work["ts_code"].map(normalize_index_code)
    work["index_name"] = work["name"].fillna("").astype(str)
    work["category"] = work["category"].fillna("").astype(str)
    work["market2"] = work["market"].fillna("").astype(str).map(_normalize_index_market)
    work["publisher2"] = work["publisher"].fillna("").astype(str)
    work["list_date2"] = work["list_date"].fillna("").astype(str)
    work["status"] = work["exp_date"].fillna("").astype(str).map(lambda value: "inactive" if value else "active")
    return work[["index_code", "index_name", "category", "market2", "publisher2", "list_date2", "status"]]


def get_index_catalog(index_code: str, category: str, market: str, publisher: str, status: str) -> list[IndexCatalogItem]:
    selected_markets = _resolve_index_markets(market)
    if market and not selected_markets:
        return []
    frames: list[pd.DataFrame] = []
    for market_code in selected_markets:
        cache_path = build_cache_path("tushare", ["indexes", "catalog"], {"market": market_code.lower()})
        cache_df = read_cache_frame(cache_path)
        if cache_df.empty:
            fetched_df = _fetch_index_catalog_frame(market_code)
            if not fetched_df.empty:
                write_cache_frame(cache_path, fetched_df)
                cache_df = fetched_df
        if not cache_df.empty:
            frames.append(cache_df)
    if not frames:
        return []
    work = merge_cache_frame(pd.DataFrame(), pd.concat(frames, ignore_index=True), ["index_code"], ["index_code"])
    normalized_code = normalize_index_code(index_code)
    if normalized_code:
        work = work[work["index_code"] == normalized_code]
    if category:
        work = work[work["category"] == category]
    if publisher:
        work = work[work["publisher2"] == publisher]
    if status:
        work = work[work["status"] == status]
    items: list[IndexCatalogItem] = []
    for _, row in work.sort_values("index_code").iterrows():
        items.append(
            IndexCatalogItem(
                index_code=str(row["index_code"]),
                index_name=str(row["index_name"]),
                category=str(row["category"]),
                market=str(row["market2"]),
                publisher=str(row["publisher2"]),
                list_date=format_date_value(row["list_date2"]),
                status=str(row["status"]),
            )
        )
    return items


def _fetch_index_quotes_frame(index_code: str, start_value: str, end_value: str) -> pd.DataFrame:
    pro = get_ts_pro()
    if pro is None:
        return pd.DataFrame()
    try:
        df = call_tushare_api("index_daily", pro.index_daily, ts_code=index_code_to_ts(index_code), start_date=start_value, end_date=end_value)
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    work = df.copy().sort_values("trade_date")
    for column in ["ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "change", "pct_chg", "vol", "amount"]:
        if column not in work.columns:
            work[column] = None
    work["index_code"] = work["ts_code"].map(normalize_index_code)
    work["trade_time"] = pd.to_datetime(work["trade_date"])
    work["volume2"] = pd.to_numeric(work["vol"], errors="coerce") if "vol" in work.columns else None
    return work[["index_code", "trade_time", "open", "high", "low", "close", "pre_close", "change", "pct_chg", "volume2", "amount"]]


def get_index_quotes(
    index_codes: list[str],
    freq: str,
    trade_date: str,
    start_date: str,
    end_date: str,
    count: int | None,
) -> list[IndexQuoteItem]:
    request_start_dt, request_end_dt = build_time_bounds(trade_date, start_date, end_date, "", "", count, False)
    request_start = request_start_dt.strftime(DATE_FORMAT) if request_start_dt is not None else ""
    request_end = request_end_dt.strftime(DATE_FORMAT) if request_end_dt is not None else ""
    if request_start == "" and request_end == "":
        request_end = datetime.now().strftime(DATE_FORMAT)
        request_start = (datetime.now() - timedelta(days=400)).strftime(DATE_FORMAT)
    elif request_start == "":
        request_start = request_end
    elif request_end == "":
        request_end = request_start
    items: list[IndexQuoteItem] = []
    for index_code in index_codes:
        normalized = normalize_index_code(index_code)
        cache_path = build_cache_path("tushare", ["indexes", "quotes"], {"index_code": normalized})
        cache_df = read_cache_frame(cache_path)
        missing_ranges = plan_missing_ranges(cache_df, "trade_time", request_start, request_end, "day")
        fetched_frames: list[pd.DataFrame] = []
        for missing_start, missing_end in missing_ranges:
            fetched_df = _fetch_index_quotes_frame(normalized, missing_start, missing_end)
            if not fetched_df.empty:
                fetched_frames.append(fetched_df)
        if cache_df.empty and not fetched_frames:
            fetched_df = _fetch_index_quotes_frame(normalized, request_start, request_end)
            if not fetched_df.empty:
                fetched_frames.append(fetched_df)
        if fetched_frames:
            merged_cache = merge_cache_frame(cache_df, pd.concat(fetched_frames, ignore_index=True), ["index_code", "trade_time"], ["trade_time"])
            write_cache_frame(cache_path, merged_cache)
            cache_df = merged_cache
        filtered_df = filter_frame_by_date_range(cache_df, "trade_time", request_start, request_end)
        if filtered_df.empty:
            continue
        filtered_df["trade_time"] = pd.to_datetime(filtered_df["trade_time"])
        agg_df = add_quote_metrics(aggregate_ohlc(filtered_df.rename(columns={"volume2": "volume"}), freq))
        if count:
            agg_df = agg_df.tail(count)
        for _, row in agg_df.iterrows():
            items.append(
                IndexQuoteItem(
                    index_code=normalized,
                    trade_time=format_datetime_value(row["trade_time"], freq),
                    freq=freq,
                    open=float(row["open"]) if pd.notna(row["open"]) else None,
                    high=float(row["high"]) if pd.notna(row["high"]) else None,
                    low=float(row["low"]) if pd.notna(row["low"]) else None,
                    close=float(row["close"]) if pd.notna(row["close"]) else None,
                    pre_close=float(row["pre_close"]) if pd.notna(row["pre_close"]) else None,
                    change=float(row["change"]) if pd.notna(row["change"]) else None,
                    pct_chg=float(row["pct_chg"]) if pd.notna(row["pct_chg"]) else None,
                    volume=float(row["volume"]) if "volume" in row and pd.notna(row["volume"]) else None,
                    amount=float(row["amount"]) if pd.notna(row["amount"]) else None,
                )
            )
    return items


def _fetch_index_members_frame(index_code: str, start_value: str, end_value: str) -> pd.DataFrame:
    pro = get_ts_pro()
    if pro is None:
        return pd.DataFrame()
    try:
        df = call_tushare_api("index_weight", pro.index_weight, index_code=index_code_to_ts(index_code), start_date=start_value, end_date=end_value)
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    work = df.copy()
    for column in ["index_code", "con_code", "trade_date", "weight"]:
        if column not in work.columns:
            work[column] = None
    work["index_code2"] = work["index_code"].map(normalize_index_code)
    work["code"] = work["con_code"].map(normalize_stock_code)
    work["trade_date2"] = work["trade_date"].fillna("").astype(str)
    return work[["index_code2", "code", "trade_date2", "weight"]]


def get_index_members(index_code: str, trade_date: str) -> list[IndexMemberItem]:
    normalized = normalize_index_code(index_code)
    actual_trade_date = format_date_value(trade_date)
    if actual_trade_date:
        target_day = datetime.strptime(actual_trade_date, "%Y-%m-%d")
        start_value = target_day.replace(day=1).strftime(DATE_FORMAT)
        end_value = (target_day.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        end_text = end_value.strftime(DATE_FORMAT)
    else:
        end_text = datetime.now().strftime(DATE_FORMAT)
        start_value = (datetime.now() - timedelta(days=370)).strftime(DATE_FORMAT)
    cache_path = build_cache_path("tushare", ["indexes", "members"], {"index_code": normalized})
    cache_df = read_cache_frame(cache_path)
    missing_ranges = plan_missing_ranges(cache_df, "trade_date2", start_value, end_text, "day")
    fetched_frames: list[pd.DataFrame] = []
    for missing_start, missing_end in missing_ranges:
        fetched_df = _fetch_index_members_frame(normalized, missing_start, missing_end)
        if not fetched_df.empty:
            fetched_frames.append(fetched_df)
    if cache_df.empty and not fetched_frames:
        fetched_df = _fetch_index_members_frame(normalized, start_value, end_text)
        if not fetched_df.empty:
            fetched_frames.append(fetched_df)
    if fetched_frames:
        merged_cache = merge_cache_frame(cache_df, pd.concat(fetched_frames, ignore_index=True), ["index_code2", "code", "trade_date2"], ["trade_date2", "code"])
        write_cache_frame(cache_path, merged_cache)
        cache_df = merged_cache
    filtered_df = filter_frame_by_date_range(cache_df, "trade_date2", start_value, end_text)
    if filtered_df.empty:
        return []
    if actual_trade_date:
        exact_trade_date = actual_trade_date.replace("-", "")
        exact_df = filtered_df[filtered_df["trade_date2"] == exact_trade_date]
        if exact_df.empty:
            candidate_df = filtered_df[filtered_df["trade_date2"] <= exact_trade_date]
            if candidate_df.empty:
                filtered_df = pd.DataFrame()
            else:
                latest_trade_date = candidate_df["trade_date2"].max()
                filtered_df = candidate_df[candidate_df["trade_date2"] == latest_trade_date]
        else:
            filtered_df = exact_df
    else:
        latest_trade_date = filtered_df["trade_date2"].max()
        filtered_df = filtered_df[filtered_df["trade_date2"] == latest_trade_date]
    if filtered_df.empty:
        return []
    items: list[IndexMemberItem] = []
    for _, row in filtered_df.sort_values(["trade_date2", "code"]).iterrows():
        items.append(
            IndexMemberItem(
                index_code=str(row["index_code2"]),
                code=str(row["code"]),
                name="",
                weight=float(row["weight"]) if pd.notna(row["weight"]) else None,
                trade_date=format_date_value(str(row["trade_date2"])),
            )
        )
    return items


def _fetch_stock_quotes_frame(code: str, freq: str, start_dt: datetime | None, end_dt: datetime | None, adjust: str) -> pd.DataFrame:
    if ts is None or not TS_TOKEN or freq == "tick":
        return pd.DataFrame()
    ts.set_token(TS_TOKEN)
    try:
        df = call_tushare_api(
            "pro_bar",
            ts.pro_bar,
            ts_code=stock_code_to_ts(code),
            start_date=start_dt.strftime(DATE_FORMAT) if start_dt else "",
            end_date=end_dt.strftime(DATE_FORMAT) if end_dt else "",
            asset="E",
            adj=None if adjust == "none" else adjust,
            freq=TS_FREQ_MAP.get(freq, "D"),
        )
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    time_column = "trade_time" if "trade_time" in df.columns else "trade_date"
    volume_column = "vol" if "vol" in df.columns else "volume"
    work = df.copy().sort_values(time_column)
    work["code"] = normalize_stock_code(code)
    work["trade_time"] = pd.to_datetime(work[time_column])
    work["freq"] = freq
    work["adjust"] = adjust
    work["volume2"] = work[volume_column] if volume_column in work.columns else None
    return work[["code", "trade_time", "freq", "open", "high", "low", "close", "pre_close", "change", "pct_chg", "volume2", "amount", "adjust"]]


def _fetch_stock_daily_snapshot_frame(trade_date: str) -> pd.DataFrame:
    pro = get_ts_pro()
    if pro is None:
        return pd.DataFrame()
    try:
        df = call_tushare_api("daily", pro.daily, trade_date=trade_date.replace("-", ""))
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    work = df.copy()
    work["code"] = work["ts_code"].astype(str).str.split(".").str[0]
    work["trade_time"] = pd.to_datetime(work["trade_date"])
    work["freq"] = "1d"
    work["adjust"] = "none"
    work["volume2"] = pd.to_numeric(work["vol"], errors="coerce") if "vol" in work.columns else None
    for column in ["open", "high", "low", "close", "pre_close", "change", "pct_chg", "amount"]:
        if column not in work.columns:
            work[column] = None
    return work[["code", "trade_time", "freq", "open", "high", "low", "close", "pre_close", "change", "pct_chg", "volume2", "amount", "adjust"]]


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
                pre_close=float(row["pre_close"]) if pd.notna(row["pre_close"]) else None,
                change=float(row["change"]) if pd.notna(row["change"]) else None,
                pct_chg=float(row["pct_chg"]) if pd.notna(row["pct_chg"]) else None,
                volume=float(row["volume2"]) if pd.notna(row["volume2"]) else None,
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
    if freq == "tick":
        return []
    request_start_dt, request_end_dt = build_time_bounds(trade_date, start_date, end_date, start_time, end_time, count, freq in INTRADAY_RULES)
    items: list[StockQuoteItem] = []
    for code in codes:
        cache_path = build_cache_path("tushare", ["stocks", "quotes"], {"code": normalize_stock_code(code), "freq": freq, "adjust": adjust})
        cache_df = read_cache_frame(cache_path)
        fetch_start_dt = request_start_dt
        fetch_end_dt = request_end_dt
        if fetch_start_dt is None and fetch_end_dt is None:
            fetch_end_dt = datetime.now()
            fetch_start_dt = fetch_end_dt - timedelta(days=30)
        range_start = fetch_start_dt.strftime("%Y%m%d") if fetch_start_dt else ""
        range_end = fetch_end_dt.strftime("%Y%m%d") if fetch_end_dt else ""
        missing_ranges = plan_missing_ranges(cache_df, "trade_time", range_start, range_end, "day")
        fetched_frames: list[pd.DataFrame] = []
        for missing_start, missing_end in missing_ranges:
            start_dt = datetime.strptime(missing_start, "%Y%m%d")
            end_dt = datetime.strptime(missing_end, "%Y%m%d") + timedelta(hours=23, minutes=59, seconds=59)
            fetched_df = _fetch_stock_quotes_frame(code, freq, start_dt, end_dt, adjust)
            if not fetched_df.empty:
                fetched_frames.append(fetched_df)
        if cache_df.empty and not fetched_frames:
            fetched_df = _fetch_stock_quotes_frame(code, freq, fetch_start_dt, fetch_end_dt, adjust)
            if not fetched_df.empty:
                fetched_frames.append(fetched_df)
        if fetched_frames:
            merged_cache = merge_cache_frame(cache_df, pd.concat(fetched_frames, ignore_index=True), ["code", "trade_time", "freq"], ["trade_time"])
            write_cache_frame(cache_path, merged_cache)
            cache_df = merged_cache
        filtered_df = filter_frame_by_datetime_range(cache_df, "trade_time", request_start_dt, request_end_dt)
        filtered_df = latest_n_rows(filtered_df, "trade_time", count)
        items.extend(_frame_to_stock_quotes(filtered_df, freq))
    return items


def get_stock_daily_snapshot(trade_date: str) -> list[StockQuoteItem]:
    actual_trade_date = format_date_value(trade_date)
    if actual_trade_date == "":
        return []
    cache_path = build_cache_path("tushare", ["stocks", "quotes", "daily-snapshot"], {"trade_date": actual_trade_date.replace("-", "")})
    cache_df = read_cache_frame(cache_path)
    if cache_df.empty:
        fetched_df = _fetch_stock_daily_snapshot_frame(actual_trade_date)
        if not fetched_df.empty:
            write_cache_frame(cache_path, fetched_df)
            cache_df = fetched_df
    if cache_df.empty:
        return []
    filtered_df = filter_frame_by_date_range(cache_df, "trade_time", actual_trade_date, actual_trade_date)
    return _frame_to_stock_quotes(filtered_df, "1d")


def _fetch_money_flow_frame(code: str, start_value: str, end_value: str, view: str) -> pd.DataFrame:
    pro = get_ts_pro()
    if pro is None:
        return pd.DataFrame()
    try:
        df = call_tushare_api("moneyflow", pro.moneyflow, ts_code=stock_code_to_ts(code), start_date=start_value, end_date=end_value)
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    work = df.copy()
    work["code"] = normalize_stock_code(code)
    work["view"] = view
    work["main_inflow"] = (work["buy_lg_amount"].fillna(0) + work["buy_elg_amount"].fillna(0)).astype(float)
    work["main_outflow"] = (work["sell_lg_amount"].fillna(0) + work["sell_elg_amount"].fillna(0)).astype(float)
    work["net_inflow"] = work["net_mf_amount"]
    return work[["code", "trade_date", "view", "main_inflow", "main_outflow", "net_inflow"]]


def get_stock_money_flow(code: str, trade_date: str, start_date: str, end_date: str, view: str) -> list[StockMoneyFlowItem]:
    actual_start = trade_date or start_date
    actual_end = trade_date or end_date
    if not actual_start and not actual_end:
        actual_end = datetime.now().strftime(DATE_FORMAT)
        actual_start = (datetime.now() - timedelta(days=30)).strftime(DATE_FORMAT)
    elif not actual_start:
        actual_start = actual_end
    elif not actual_end:
        actual_end = actual_start
    cache_path = build_cache_path("tushare", ["stocks", "indicators", "money-flow"], {"code": normalize_stock_code(code), "view": view})
    cache_df = read_cache_frame(cache_path)
    missing_ranges = plan_missing_ranges(cache_df, "trade_date", actual_start, actual_end, "day")
    fetched_frames: list[pd.DataFrame] = []
    for missing_start, missing_end in missing_ranges:
        fetched_df = _fetch_money_flow_frame(code, missing_start, missing_end, view)
        if not fetched_df.empty:
            fetched_frames.append(fetched_df)
    if cache_df.empty and not fetched_frames:
        fetched_df = _fetch_money_flow_frame(code, actual_start, actual_end, view)
        if not fetched_df.empty:
            fetched_frames.append(fetched_df)
    if fetched_frames:
        merged_cache = merge_cache_frame(cache_df, pd.concat(fetched_frames, ignore_index=True), ["code", "trade_date", "view"], ["trade_date"])
        write_cache_frame(cache_path, merged_cache)
        cache_df = merged_cache
    filtered_df = filter_frame_by_date_range(cache_df, "trade_date", actual_start, actual_end)
    items: list[StockMoneyFlowItem] = []
    for _, row in filtered_df.sort_values("trade_date").iterrows():
        items.append(
            StockMoneyFlowItem(
                code=str(row["code"]),
                trade_date=str(row["trade_date"]),
                view=str(row["view"]),
                main_inflow=float(row["main_inflow"]) if pd.notna(row["main_inflow"]) else None,
                main_outflow=float(row["main_outflow"]) if pd.notna(row["main_outflow"]) else None,
                net_inflow=float(row["net_inflow"]) if pd.notna(row["net_inflow"]) else None,
            )
        )
    return items


def board_code_to_ts(board_code: str) -> str:
    text = board_code.strip().upper()
    if not text:
        return ""
    if "." in text:
        return text
    return f"{text}.TI"


def _fetch_board_money_flow_frame(board_code: str, start_value: str, end_value: str, scope: str) -> pd.DataFrame:
    pro = get_ts_pro()
    if pro is None:
        return pd.DataFrame()
    fetch_name = "moneyflow_ind_ths" if scope == "industry" else "moneyflow_cnt_ths"
    fetcher = getattr(pro, fetch_name, None)
    if fetcher is None:
        return pd.DataFrame()
    try:
        df = call_tushare_api(fetch_name, fetcher, ts_code=board_code_to_ts(board_code), start_date=start_value, end_date=end_value)
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    work = df.copy()
    code_column = "ts_code" if "ts_code" in work.columns else "code"
    work["board_code"] = work[code_column].astype(str).str.split(".").str[0]
    work["scope"] = scope
    work["inflow"] = work["net_buy_amount"] if "net_buy_amount" in work.columns else None
    work["outflow"] = work["net_sell_amount"] if "net_sell_amount" in work.columns else None
    work["net_inflow"] = work["net_amount"] if "net_amount" in work.columns else None
    return work[["board_code", "trade_date", "scope", "inflow", "outflow", "net_inflow"]]


def get_board_money_flow(board_code: str, trade_date: str, start_date: str, end_date: str, scope: str) -> list[BoardMoneyFlowItem]:
    actual_start = trade_date or start_date
    actual_end = trade_date or end_date
    if not actual_start and not actual_end:
        actual_end = datetime.now().strftime(DATE_FORMAT)
        actual_start = (datetime.now() - timedelta(days=30)).strftime(DATE_FORMAT)
    elif not actual_start:
        actual_start = actual_end
    elif not actual_end:
        actual_end = actual_start
    cache_path = build_cache_path("tushare", ["boards", "indicators", "money-flow"], {"board_code": board_code, "scope": scope})
    cache_df = read_cache_frame(cache_path)
    missing_ranges = plan_missing_ranges(cache_df, "trade_date", actual_start, actual_end, "day")
    fetched_frames: list[pd.DataFrame] = []
    for missing_start, missing_end in missing_ranges:
        fetched_df = _fetch_board_money_flow_frame(board_code, missing_start, missing_end, scope)
        if not fetched_df.empty:
            fetched_frames.append(fetched_df)
    if cache_df.empty and not fetched_frames:
        fetched_df = _fetch_board_money_flow_frame(board_code, actual_start, actual_end, scope)
        if not fetched_df.empty:
            fetched_frames.append(fetched_df)
    if fetched_frames:
        merged_cache = merge_cache_frame(cache_df, pd.concat(fetched_frames, ignore_index=True), ["board_code", "trade_date", "scope"], ["trade_date"])
        write_cache_frame(cache_path, merged_cache)
        cache_df = merged_cache
    filtered_df = filter_frame_by_date_range(cache_df, "trade_date", actual_start, actual_end)
    items: list[BoardMoneyFlowItem] = []
    for _, row in filtered_df.sort_values("trade_date").iterrows():
        items.append(
            BoardMoneyFlowItem(
                board_code=str(row["board_code"]),
                trade_date=str(row["trade_date"]),
                scope=str(row["scope"]),
                inflow=float(row["inflow"]) if pd.notna(row["inflow"]) else None,
                outflow=float(row["outflow"]) if pd.notna(row["outflow"]) else None,
                net_inflow=float(row["net_inflow"]) if pd.notna(row["net_inflow"]) else None,
            )
        )
    return items


def _fetch_market_capital_flow_frame(start_value: str, end_value: str) -> pd.DataFrame:
    pro = get_ts_pro()
    if pro is None:
        return pd.DataFrame()
    fetcher = getattr(pro, "moneyflow_mkt_dc", None)
    if fetcher is None:
        return pd.DataFrame()
    try:
        df = call_tushare_api("moneyflow_mkt_dc", fetcher, start_date=start_value, end_date=end_value)
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    work = df.copy()
    work["market"] = "all"
    work["main_inflow"] = None
    work["main_outflow"] = None
    if "buy_elg_amount" in work.columns and "buy_lg_amount" in work.columns:
        net_large = work["buy_elg_amount"].fillna(0) + work["buy_lg_amount"].fillna(0)
        work["main_inflow"] = net_large.where(net_large > 0)
        work["main_outflow"] = (-net_large).where(net_large < 0)
    work["net_inflow"] = work["net_amount"] if "net_amount" in work.columns else None
    return work[["trade_date", "market", "main_inflow", "main_outflow", "net_inflow"]]


def get_market_capital_flow(trade_date: str, start_date: str, end_date: str) -> list[MarketCapitalFlowItem]:
    actual_start = trade_date or start_date
    actual_end = trade_date or end_date
    if not actual_start and not actual_end:
        actual_end = datetime.now().strftime(DATE_FORMAT)
        actual_start = (datetime.now() - timedelta(days=30)).strftime(DATE_FORMAT)
    elif not actual_start:
        actual_start = actual_end
    elif not actual_end:
        actual_end = actual_start
    cache_path = build_cache_path("tushare", ["markets", "indicators", "main-capital-flow"], {"market": "all"})
    cache_df = read_cache_frame(cache_path)
    missing_ranges = plan_missing_ranges(cache_df, "trade_date", actual_start, actual_end, "day")
    fetched_frames: list[pd.DataFrame] = []
    for missing_start, missing_end in missing_ranges:
        fetched_df = _fetch_market_capital_flow_frame(missing_start, missing_end)
        if not fetched_df.empty:
            fetched_frames.append(fetched_df)
    if cache_df.empty and not fetched_frames:
        fetched_df = _fetch_market_capital_flow_frame(actual_start, actual_end)
        if not fetched_df.empty:
            fetched_frames.append(fetched_df)
    if fetched_frames:
        merged_cache = merge_cache_frame(cache_df, pd.concat(fetched_frames, ignore_index=True), ["trade_date", "market"], ["trade_date"])
        write_cache_frame(cache_path, merged_cache)
        cache_df = merged_cache
    filtered_df = filter_frame_by_date_range(cache_df, "trade_date", actual_start, actual_end)
    items: list[MarketCapitalFlowItem] = []
    for _, row in filtered_df.sort_values("trade_date").iterrows():
        items.append(
            MarketCapitalFlowItem(
                trade_date=str(row["trade_date"]),
                market=str(row["market"]),
                main_inflow=float(row["main_inflow"]) if pd.notna(row["main_inflow"]) else None,
                main_outflow=float(row["main_outflow"]) if pd.notna(row["main_outflow"]) else None,
                net_inflow=float(row["net_inflow"]) if pd.notna(row["net_inflow"]) else None,
            )
        )
    return items


def trade_calendar_fetch_exchange(exchange: str) -> str:
    if exchange == "BSE":
        return "SSE"
    if exchange == "HKEX":
        return ""
    return exchange


def _fetch_trading_calendar_frame(exchange: str, start_value: str, end_value: str) -> pd.DataFrame:
    pro = get_ts_pro()
    if pro is None:
        return pd.DataFrame()
    fetch_exchange = trade_calendar_fetch_exchange(exchange)
    if not fetch_exchange:
        return pd.DataFrame()
    try:
        df = call_tushare_api("trade_cal", pro.trade_cal, exchange=fetch_exchange, start_date=start_value, end_date=end_value)
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    work = df.copy()
    work["exchange"] = exchange
    work["trade_date"] = work["cal_date"]
    return work[["exchange", "trade_date", "is_open"]]


def get_trading_calendar(exchange: str, start_date: str, end_date: str, is_open: bool | None) -> list[TradingCalendarItem]:
    actual_end = end_date or datetime.now().strftime(DATE_FORMAT)
    actual_start = start_date or (datetime.now() - timedelta(days=365)).strftime(DATE_FORMAT)
    cache_path = build_cache_path("tushare", ["markets", "calendar", "trading"], {"exchange": exchange})
    cache_df = read_cache_frame(cache_path)
    missing_ranges = plan_missing_ranges(cache_df, "trade_date", actual_start, actual_end, "day")
    fetched_frames: list[pd.DataFrame] = []
    for missing_start, missing_end in missing_ranges:
        fetched_df = _fetch_trading_calendar_frame(exchange, missing_start, missing_end)
        if not fetched_df.empty:
            fetched_frames.append(fetched_df)
    if cache_df.empty and not fetched_frames:
        fetched_df = _fetch_trading_calendar_frame(exchange, actual_start, actual_end)
        if not fetched_df.empty:
            fetched_frames.append(fetched_df)
    if fetched_frames:
        merged_cache = merge_cache_frame(cache_df, pd.concat(fetched_frames, ignore_index=True), ["exchange", "trade_date"], ["trade_date"])
        write_cache_frame(cache_path, merged_cache)
        cache_df = merged_cache
    filtered_df = filter_frame_by_date_range(cache_df, "trade_date", actual_start, actual_end)
    if is_open is not None:
        filtered_df = filtered_df[filtered_df["is_open"].astype(str) == ("1" if is_open else "0")]
    items: list[TradingCalendarItem] = []
    for _, row in filtered_df.sort_values("trade_date").iterrows():
        items.append(
            TradingCalendarItem(
                exchange=str(row["exchange"]),
                trade_date=str(row["trade_date"]),
                is_open=str(row["is_open"]) == "1",
            )
        )
    return items


def _fetch_financial_frame(code: str, start_value: str, end_value: str, report_type: str) -> pd.DataFrame:
    pro = get_ts_pro()
    if pro is None:
        return pd.DataFrame()
    if report_type == "income_statement":
        fetch_name = "income"
        fields = "ts_code,ann_date,end_date,total_revenue,operate_profit,total_profit,n_income"
    elif report_type == "balance_sheet":
        fetch_name = "balancesheet"
        fields = "ts_code,ann_date,end_date,total_assets,total_liab,total_hldr_eqy_exc_min_int"
    else:
        fetch_name = "cashflow"
        fields = "ts_code,ann_date,end_date"
    fetcher = getattr(pro, fetch_name, None)
    if fetcher is None:
        return pd.DataFrame()
    try:
        df = call_tushare_api(fetch_name, fetcher, ts_code=stock_code_to_ts(code), start_date=start_value, end_date=end_value, fields=fields)
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    work = df.copy()
    work["code"] = normalize_stock_code(code)
    work["report_period"] = work["end_date"]
    work["report_type"] = report_type
    work["announce_date"] = work["ann_date"]
    work["revenue"] = work["total_revenue"] if "total_revenue" in work.columns else None
    work["operating_profit"] = work["operate_profit"] if "operate_profit" in work.columns else None
    work["total_profit"] = work["total_profit"] if "total_profit" in work.columns else None
    work["net_profit"] = work["n_income"] if "n_income" in work.columns else None
    work["total_assets2"] = work["total_assets"] if "total_assets" in work.columns else None
    work["total_liabilities2"] = work["total_liab"] if "total_liab" in work.columns else None
    work["equity2"] = work["total_hldr_eqy_exc_min_int"] if "total_hldr_eqy_exc_min_int" in work.columns else None
    return work[["code", "report_period", "report_type", "announce_date", "revenue", "operating_profit", "total_profit", "net_profit", "total_assets2", "total_liabilities2", "equity2"]]


def get_stock_financial_statements(
    codes: list[str],
    report_period: str,
    start_period: str,
    end_period: str,
    report_type: str,
) -> list[StockFinancialStatementItem]:
    start_value = start_period or report_period
    end_value = end_period or report_period
    if not start_value and not end_value:
        end_value = datetime.now().strftime("%Y1231")
        start_value = f"{datetime.now().year - 2}0101"
    elif not start_value:
        start_value = end_value
    elif not end_value:
        end_value = start_value
    items: list[StockFinancialStatementItem] = []
    for code in codes:
        cache_path = build_cache_path("tushare", ["stocks", "finance", "statements"], {"code": normalize_stock_code(code), "report_type": report_type})
        cache_df = read_cache_frame(cache_path)
        missing_ranges = plan_missing_ranges(cache_df, "report_period", start_value, end_value, "quarter")
        fetched_frames: list[pd.DataFrame] = []
        for missing_start, missing_end in missing_ranges:
            fetched_df = _fetch_financial_frame(code, missing_start, missing_end, report_type)
            if not fetched_df.empty:
                fetched_frames.append(fetched_df)
        if cache_df.empty and not fetched_frames:
            fetched_df = _fetch_financial_frame(code, start_value, end_value, report_type)
            if not fetched_df.empty:
                fetched_frames.append(fetched_df)
        if fetched_frames:
            merged_cache = merge_cache_frame(cache_df, pd.concat(fetched_frames, ignore_index=True), ["code", "report_period", "report_type", "announce_date"], ["report_period", "announce_date"])
            write_cache_frame(cache_path, merged_cache)
            cache_df = merged_cache
        filtered_df = filter_frame_by_date_range(cache_df, "report_period", start_value, end_value)
        required_columns = {"code", "report_period", "report_type", "announce_date"}
        if filtered_df.empty or not required_columns.issubset(set(filtered_df.columns)):
            continue
        for _, row in filtered_df.sort_values(["report_period", "announce_date"]).iterrows():
            items.append(
                StockFinancialStatementItem(
                    code=str(row["code"]),
                    report_period=str(row["report_period"]),
                    report_type=str(row["report_type"]),
                    announce_date=str(row["announce_date"]),
                    revenue=float(row["revenue"]) if pd.notna(row["revenue"]) else None,
                    operating_profit=float(row["operating_profit"]) if pd.notna(row["operating_profit"]) else None,
                    total_profit=float(row["total_profit"]) if pd.notna(row["total_profit"]) else None,
                    net_profit=float(row["net_profit"]) if pd.notna(row["net_profit"]) else None,
                    total_assets=float(row["total_assets2"]) if pd.notna(row["total_assets2"]) else None,
                    total_liabilities=float(row["total_liabilities2"]) if pd.notna(row["total_liabilities2"]) else None,
                    equity=float(row["equity2"]) if pd.notna(row["equity2"]) else None,
                )
            )
    return items


