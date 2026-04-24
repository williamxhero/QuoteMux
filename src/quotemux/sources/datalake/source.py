from __future__ import annotations

import numpy as np
import pandas as pd

from quotemux.infra.db.market_reads import load_board_daily_frame, load_board_daily_snapshot_frame, load_index_daily_frame, load_stock_daily_frame, load_stock_daily_snapshot_frame, load_stock_daily_snapshot_full_frame, load_stock_intraday_frame
from platform_models import AdjFactorItem, BoardMoneyFlowItem, BoardQuoteItem, IndexQuoteItem, StockMoneyFlowItem, StockQuoteItem
from quotemux.infra.common import PRICE_COLUMNS, INTRADAY_RULES, add_quote_metrics, aggregate_ohlc, build_time_bounds, format_date_value, format_datetime_value, index_code_to_gm, normalize_stock_code
from quotemux.sources.datalake.news import get_news_event_sources, get_news_events
from quotemux.sources.datalake.reference import get_board_catalog, get_board_categories, get_board_member_history, get_board_members, get_board_profile, get_hl_signal, get_index_catalog, get_index_profile, get_stock_active_codes, get_stock_basic, get_stock_catalog, get_stock_name_history, get_stock_names, get_trading_calendar
from quotemux.sources.datalake.topics import get_market_sessions


def repair_adj_factor_frame(frame: pd.DataFrame) -> pd.Series:
    adj_factor = pd.to_numeric(frame["adj_factor"], errors="coerce")
    close = pd.to_numeric(frame["close"], errors="coerce")
    prev_factor = adj_factor.ffill()
    next_factor = adj_factor.bfill()
    prev_close = close.where(adj_factor.notna()).ffill()
    next_close = close.where(adj_factor.notna()).bfill()
    repaired = adj_factor.copy()
    same_factor_mask = repaired.isna() & prev_factor.notna() & next_factor.notna() & prev_factor.eq(next_factor)
    prev_only_mask = repaired.isna() & prev_factor.notna() & next_factor.isna()
    next_only_mask = repaired.isna() & prev_factor.isna() & next_factor.notna()
    different_factor_mask = (
        repaired.isna()
        & close.notna()
        & prev_factor.notna()
        & prev_close.notna()
        & next_factor.notna()
        & next_close.notna()
        & ~prev_factor.eq(next_factor)
    )
    repaired = repaired.where(~same_factor_mask, prev_factor)
    repaired = repaired.where(~prev_only_mask, prev_factor)
    repaired = repaired.where(~next_only_mask, next_factor)
    prev_candidate = close * prev_factor
    next_candidate = close * next_factor
    prev_anchor = prev_close * prev_factor
    next_anchor = next_close * next_factor
    prev_score = (
        pd.Series(np.abs(np.log(prev_candidate / prev_anchor)), index=repaired.index)
        + pd.Series(np.abs(np.log(next_anchor / prev_candidate)), index=repaired.index)
    )
    next_score = (
        pd.Series(np.abs(np.log(next_candidate / prev_anchor)), index=repaired.index)
        + pd.Series(np.abs(np.log(next_anchor / next_candidate)), index=repaired.index)
    )
    choose_prev_mask = different_factor_mask & (prev_score <= next_score)
    choose_next_mask = different_factor_mask & (next_score < prev_score)
    repaired = repaired.where(~choose_prev_mask, prev_factor)
    repaired = repaired.where(~choose_next_mask, next_factor)
    return repaired


def apply_stock_adjust(df: pd.DataFrame, adjust: str) -> pd.DataFrame:
    if df.empty or adjust == "none":
        return df
    if "adj_factor" not in df.columns:
        return df
    work = df.copy()
    work["adj_factor"] = repair_adj_factor_frame(work)
    if not work["adj_factor"].notna().any():
        return df
    latest_factor = work["adj_factor"].dropna().iloc[-1]
    multiplier = work["adj_factor"] / latest_factor if adjust == "qfq" else work["adj_factor"]
    for column in PRICE_COLUMNS:
        work[column] = pd.to_numeric(work[column], errors="coerce") * multiplier
    return work.drop(columns=["adj_factor"])


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
    start_dt, end_dt = build_time_bounds(trade_date, start_date, end_date, start_time, end_time, count, freq in INTRADAY_RULES)
    normalized_codes = [normalize_stock_code(code) for code in codes]
    normalized_codes = [code for code in dict.fromkeys(normalized_codes) if code]
    if not normalized_codes:
        return []
    if freq in INTRADAY_RULES:
        raw_df = load_stock_intraday_frame(normalized_codes, start_dt, end_dt)
    else:
        start_date_text = start_dt.strftime("%Y-%m-%d") if start_dt is not None else ""
        end_date_text = end_dt.strftime("%Y-%m-%d") if end_dt is not None else ""
        raw_df = load_stock_daily_frame(normalized_codes, start_date_text, end_date_text)
    if not raw_df.empty:
        raw_df["trade_time"] = pd.to_datetime(raw_df["trade_time"])
    items: list[StockQuoteItem] = []
    for code in normalized_codes:
        code_df = raw_df[raw_df["code"] == code].copy() if not raw_df.empty else pd.DataFrame()
        if code_df.empty:
            continue
        adjusted_df = apply_stock_adjust(code_df.drop(columns=["code"]), adjust)
        agg_df = add_quote_metrics(aggregate_ohlc(adjusted_df, freq))
        if count:
            agg_df = agg_df.tail(count)
        for _, row in agg_df.iterrows():
            items.append(
                StockQuoteItem(
                    code=code,
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


def get_stock_daily_snapshot(trade_date: str, limit: int, offset: int) -> list[StockQuoteItem]:
    actual_trade_date = format_date_value(trade_date)
    raw_df = load_stock_daily_snapshot_frame(actual_trade_date, limit, offset)
    if raw_df.empty:
        return []
    items: list[StockQuoteItem] = []
    for _, row in raw_df.iterrows():
        pre_close = float(row["pre_close"]) if pd.notna(row["pre_close"]) else None
        close = float(row["close"]) if pd.notna(row["close"]) else None
        change = None
        pct_chg = None
        if close is not None and pre_close not in {None, 0.0}:
            change = close - pre_close
            pct_chg = change / pre_close * 100
        items.append(
            StockQuoteItem(
                code=str(row["code"]),
                trade_time=format_date_value(row["trade_time"]),
                freq="1d",
                open=float(row["open"]) if pd.notna(row["open"]) else None,
                high=float(row["high"]) if pd.notna(row["high"]) else None,
                low=float(row["low"]) if pd.notna(row["low"]) else None,
                close=close,
                pre_close=pre_close,
                change=change,
                pct_chg=pct_chg,
                volume=float(row["volume"]) if pd.notna(row["volume"]) else None,
                amount=float(row["amount"]) if pd.notna(row["amount"]) else None,
                adjust="none",
            )
        )
    return items


def get_stock_daily_snapshot_full(trade_date: str) -> list[StockQuoteItem]:
    actual_trade_date = format_date_value(trade_date)
    raw_df = load_stock_daily_snapshot_full_frame(actual_trade_date)
    if raw_df.empty:
        return []
    items: list[StockQuoteItem] = []
    for _, row in raw_df.iterrows():
        pre_close = float(row["pre_close"]) if pd.notna(row["pre_close"]) else None
        close = float(row["close"]) if pd.notna(row["close"]) else None
        change = None
        pct_chg = None
        if close is not None and pre_close not in {None, 0.0}:
            change = close - pre_close
            pct_chg = change / pre_close * 100
        items.append(
            StockQuoteItem(
                code=str(row["code"]),
                trade_time=format_date_value(row["trade_time"]),
                freq="1d",
                open=float(row["open"]) if pd.notna(row["open"]) else None,
                high=float(row["high"]) if pd.notna(row["high"]) else None,
                low=float(row["low"]) if pd.notna(row["low"]) else None,
                close=close,
                pre_close=pre_close,
                change=change,
                pct_chg=pct_chg,
                volume=float(row["volume"]) if pd.notna(row["volume"]) else None,
                amount=float(row["amount"]) if pd.notna(row["amount"]) else None,
                adjust="none",
            )
        )
    return items


def get_board_quotes(
    board_codes: list[str],
    freq: str,
    trade_date: str,
    start_date: str,
    end_date: str,
    start_time: str,
    end_time: str,
    count: int | None,
) -> list[BoardQuoteItem]:
    del start_time
    del end_time
    if freq in INTRADAY_RULES:
        return []
    start_dt, end_dt = build_time_bounds(trade_date, start_date, end_date, "", "", count, False)
    start_date_text = start_dt.strftime("%Y-%m-%d") if start_dt is not None else ""
    end_date_text = end_dt.strftime("%Y-%m-%d") if end_dt is not None else ""
    raw_df = load_board_daily_frame([board_code for board_code in board_codes if board_code], start_date_text, end_date_text)
    if not raw_df.empty:
        raw_df["trade_time"] = pd.to_datetime(raw_df["trade_time"])
    items: list[BoardQuoteItem] = []
    for board_code in board_codes:
        board_df = raw_df[raw_df["board_code"] == board_code].copy() if not raw_df.empty else pd.DataFrame()
        if board_df.empty:
            continue
        agg_df = add_quote_metrics(aggregate_ohlc(board_df.drop(columns=["board_code"]), freq))
        if count:
            agg_df = agg_df.tail(count)
        for _, row in agg_df.iterrows():
            items.append(
                BoardQuoteItem(
                    board_code=board_code,
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


def get_index_quotes(
    index_codes: list[str],
    freq: str,
    trade_date: str,
    start_date: str,
    end_date: str,
    count: int | None,
) -> list[IndexQuoteItem]:
    start_dt, end_dt = build_time_bounds(trade_date, start_date, end_date, "", "", count, False)
    start_date_text = start_dt.strftime("%Y-%m-%d") if start_dt is not None else ""
    end_date_text = end_dt.strftime("%Y-%m-%d") if end_dt is not None else ""
    raw_df = load_index_daily_frame([index_code_to_gm(index_code) for index_code in index_codes if index_code], start_date_text, end_date_text)
    if not raw_df.empty:
        raw_df["trade_time"] = pd.to_datetime(raw_df["trade_time"])
        raw_df["volume"] = pd.NA
    items: list[IndexQuoteItem] = []
    for index_code in index_codes:
        actual_code = index_code_to_gm(index_code)
        index_df = raw_df[raw_df["index_code"] == actual_code].copy() if not raw_df.empty else pd.DataFrame()
        if index_df.empty:
            continue
        agg_df = add_quote_metrics(aggregate_ohlc(index_df.drop(columns=["index_code"]), freq))
        agg_df["volume"] = None
        if count:
            agg_df = agg_df.tail(count)
        for _, row in agg_df.iterrows():
            items.append(
                IndexQuoteItem(
                    index_code=str(index_code),
                    trade_time=format_datetime_value(row["trade_time"], freq),
                    freq=freq,
                    open=float(row["open"]) if pd.notna(row["open"]) else None,
                    high=float(row["high"]) if pd.notna(row["high"]) else None,
                    low=float(row["low"]) if pd.notna(row["low"]) else None,
                    close=float(row["close"]) if pd.notna(row["close"]) else None,
                    pre_close=float(row["pre_close"]) if pd.notna(row["pre_close"]) else None,
                    change=float(row["change"]) if pd.notna(row["change"]) else None,
                    pct_chg=float(row["pct_chg"]) if pd.notna(row["pct_chg"]) else None,
                    volume=None,
                    amount=float(row["amount"]) if pd.notna(row["amount"]) else None,
                )
            )
    return items


def get_stock_money_flow(code: str, trade_date: str, start_date: str, end_date: str, view: str) -> list[StockMoneyFlowItem]:
    normalized = normalize_stock_code(code)
    actual_trade_date = format_date_value(trade_date)
    actual_start_date = format_date_value(start_date)
    actual_end_date = format_date_value(end_date)
    df = load_stock_daily_frame([normalized], actual_trade_date or actual_start_date, actual_trade_date or actual_end_date)
    if df.empty:
        return []
    if actual_trade_date:
        df = df[df["trade_time"] == actual_trade_date]
    items: list[StockMoneyFlowItem] = []
    for _, row in df.sort_values("trade_time").iterrows():
        main_inflow = float(row["labi_buy"]) if pd.notna(row["labi_buy"]) else None
        main_outflow = float(row["labi_sell"]) if pd.notna(row["labi_sell"]) else None
        net_inflow = None
        if pd.notna(row["labi_buy"]) and pd.notna(row["labi_sell"]) and pd.notna(row["mism_buy"]) and pd.notna(row["mism_sell"]):
            net_inflow = float((row["labi_buy"] + row["mism_buy"]) - (row["labi_sell"] + row["mism_sell"]))
        items.append(
            StockMoneyFlowItem(
                code=normalized,
                trade_date=format_date_value(row["trade_time"]),
                view=view,
                main_inflow=main_inflow,
                main_outflow=main_outflow,
                net_inflow=net_inflow,
            )
        )
    return items


def get_adj_factors(code: str, start_date: str, end_date: str, base_date: str) -> list[AdjFactorItem]:
    normalized = normalize_stock_code(code)
    actual_start_date = format_date_value(start_date)
    actual_end_date = format_date_value(end_date)
    df = load_stock_daily_frame([normalized], actual_start_date, actual_end_date)
    if df.empty:
        return []
    df = df[["trade_time", "close", "adj_factor"]].copy()
    df["adj_factor"] = repair_adj_factor_frame(df)
    df["trade_date"] = pd.to_datetime(df["trade_time"]).dt.strftime("%Y%m%d")
    base_factor = None
    if base_date:
        base_key = format_date_value(base_date).replace("-", "")
        base_rows = df[df["trade_date"] == base_key]
        if not base_rows.empty and pd.notna(base_rows.iloc[-1]["adj_factor"]):
            base_factor = float(base_rows.iloc[-1]["adj_factor"])
    items: list[AdjFactorItem] = []
    for _, row in df.sort_values("trade_date").iterrows():
        factor_value = float(row["adj_factor"]) if pd.notna(row["adj_factor"]) else None
        if factor_value is not None and base_factor not in {None, 0.0}:
            factor_value = factor_value / base_factor
        items.append(AdjFactorItem(code=normalized, trade_date=str(row["trade_date"]), adj_factor=factor_value))
    return items


def get_board_money_flow(board_code: str, trade_date: str, start_date: str, end_date: str, scope: str) -> list[BoardMoneyFlowItem]:
    actual_trade_date = format_date_value(trade_date)
    actual_start_date = format_date_value(start_date)
    actual_end_date = format_date_value(end_date)
    df = load_board_daily_frame([board_code], actual_trade_date or actual_start_date, actual_trade_date or actual_end_date)
    if df.empty:
        return []
    if actual_trade_date:
        df = df[df["trade_time"] == actual_trade_date]
    items: list[BoardMoneyFlowItem] = []
    for _, row in df.sort_values("trade_time").iterrows():
        inflow = None
        outflow = None
        net_inflow = None
        if all(pd.notna(row[column]) for column in ["labi_buy", "mism_buy", "labi_sell", "mism_sell"]):
            inflow = float(row["labi_buy"] + row["mism_buy"])
            outflow = float(row["labi_sell"] + row["mism_sell"])
            net_inflow = inflow - outflow
        items.append(
            BoardMoneyFlowItem(
                board_code=board_code,
                trade_date=format_date_value(row["trade_time"]),
                scope=scope,
                inflow=inflow,
                outflow=outflow,
                net_inflow=net_inflow,
            )
        )
    return items


def get_board_daily_money_flow_snapshot(trade_date: str, scope: str, limit: int, offset: int) -> list[BoardMoneyFlowItem]:
    actual_trade_date = format_date_value(trade_date)
    df = load_board_daily_snapshot_frame(actual_trade_date, limit, offset)
    if df.empty:
        return []
    items: list[BoardMoneyFlowItem] = []
    for _, row in df.iterrows():
        inflow = None
        outflow = None
        net_inflow = None
        if all(pd.notna(row[column]) for column in ["labi_buy", "mism_buy", "labi_sell", "mism_sell"]):
            inflow = float(row["labi_buy"] + row["mism_buy"])
            outflow = float(row["labi_sell"] + row["mism_sell"])
            net_inflow = inflow - outflow
        items.append(
            BoardMoneyFlowItem(
                board_code=str(row["board_code"]),
                trade_date=format_date_value(row["trade_time"]),
                scope=scope,
                inflow=inflow,
                outflow=outflow,
                net_inflow=net_inflow,
            )
        )
    return items


