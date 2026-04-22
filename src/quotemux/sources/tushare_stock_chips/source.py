from __future__ import annotations

import pandas as pd

from quotemux.infra.cache.store import filter_frame_by_date_range
from platform_models import ChipDistributionItem, ChipPerformanceItem
from quotemux.infra.common import normalize_stock_code, stock_code_to_ts
from quotemux.infra.tushare.helpers import normalize_date_range, query_frame, read_cached_ranges


def _fetch_chip_distribution_frame(code: str, start_value: str, end_value: str) -> pd.DataFrame:
    df = query_frame("cyq_chips", ts_code=stock_code_to_ts(code), start_date=start_value, end_date=end_value)
    if df.empty:
        return df
    work = df.copy()
    work["code"] = normalize_stock_code(code)
    work["trade_date"] = work["trade_date"].astype(str)
    work["price"] = work["price"] if "price" in work.columns else work["cost"] if "cost" in work.columns else None
    work["chip_ratio"] = work["percent"] if "percent" in work.columns else work["weight"] if "weight" in work.columns else None
    return work[["code", "trade_date", "price", "chip_ratio"]]


def get_chip_distribution(code: str, trade_date: str, start_date: str, end_date: str) -> list[ChipDistributionItem]:
    actual_start, actual_end = normalize_date_range(trade_date, start_date, end_date, 120)
    cache_df = read_cached_ranges(
        ["stocks", "indicators", "chip-distribution"],
        {"code": normalize_stock_code(code)},
        "trade_date",
        actual_start,
        actual_end,
        "day",
        lambda start_value, end_value: _fetch_chip_distribution_frame(code, start_value, end_value),
    )
    filtered_df = filter_frame_by_date_range(cache_df, "trade_date", actual_start, actual_end)
    return [
        ChipDistributionItem(
            code=str(row["code"]),
            trade_date=str(row["trade_date"]),
            price=float(row["price"]) if pd.notna(row["price"]) else None,
            chip_ratio=float(row["chip_ratio"]) if pd.notna(row["chip_ratio"]) else None,
        )
        for _, row in filtered_df.sort_values(["trade_date", "price"]).iterrows()
    ]


def _fetch_chip_performance_frame(code: str, start_value: str, end_value: str) -> pd.DataFrame:
    df = query_frame("cyq_perf", ts_code=stock_code_to_ts(code), start_date=start_value, end_date=end_value)
    if df.empty:
        return df
    work = df.copy()
    work["code"] = normalize_stock_code(code)
    work["trade_date"] = work["trade_date"].astype(str)
    work["profit_ratio"] = work["profit_ratio"] if "profit_ratio" in work.columns else None
    work["avg_cost"] = work["avg_cost"] if "avg_cost" in work.columns else None
    work["cost_70"] = work["cost_70pct"] if "cost_70pct" in work.columns else None
    work["cost_90"] = work["cost_90pct"] if "cost_90pct" in work.columns else None
    return work[["code", "trade_date", "profit_ratio", "avg_cost", "cost_70", "cost_90"]]


def get_chip_performance(code: str, trade_date: str, start_date: str, end_date: str) -> list[ChipPerformanceItem]:
    actual_start, actual_end = normalize_date_range(trade_date, start_date, end_date, 120)
    cache_df = read_cached_ranges(
        ["stocks", "indicators", "chip-performance"],
        {"code": normalize_stock_code(code)},
        "trade_date",
        actual_start,
        actual_end,
        "day",
        lambda start_value, end_value: _fetch_chip_performance_frame(code, start_value, end_value),
    )
    filtered_df = filter_frame_by_date_range(cache_df, "trade_date", actual_start, actual_end)
    return [
        ChipPerformanceItem(
            code=str(row["code"]),
            trade_date=str(row["trade_date"]),
            profit_ratio=float(row["profit_ratio"]) if pd.notna(row["profit_ratio"]) else None,
            avg_cost=float(row["avg_cost"]) if pd.notna(row["avg_cost"]) else None,
            cost_70=float(row["cost_70"]) if pd.notna(row["cost_70"]) else None,
            cost_90=float(row["cost_90"]) if pd.notna(row["cost_90"]) else None,
        )
        for _, row in filtered_df.sort_values("trade_date").iterrows()
    ]


