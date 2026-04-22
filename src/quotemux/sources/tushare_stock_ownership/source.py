from __future__ import annotations

import pandas as pd

from quotemux.infra.cache.store import filter_frame_by_date_range
from platform_models import CcassHoldingDetailItem, CcassHoldingItem, HKConnectHoldingItem, PledgeDetailItem, PledgeStatItem, ShareholderCountItem, ShareholderTop10Item
from quotemux.infra.common import normalize_stock_code, stock_code_to_ts
from quotemux.infra.tushare.helpers import normalize_date_range, normalize_period_range, query_frame, read_cached_once, read_cached_ranges


def _fetch_ccass_hold_frame(code: str, start_value: str, end_value: str) -> pd.DataFrame:
    df = query_frame("ccass_hold", ts_code=stock_code_to_ts(code), start_date=start_value, end_date=end_value)
    if df.empty:
        return df
    work = df.copy()
    work["code"] = normalize_stock_code(code)
    work["trade_date"] = work["trade_date"].astype(str)
    work["participant_count"] = work["col_participant_count"] if "col_participant_count" in work.columns else None
    work["holding_volume"] = work["total_shareholding"] if "total_shareholding" in work.columns else work["shareholding"] if "shareholding" in work.columns else None
    work["holding_ratio"] = work["shareholding_ratio"] if "shareholding_ratio" in work.columns else None
    return work[["code", "trade_date", "participant_count", "holding_volume", "holding_ratio"]]


def get_ccass_holdings(code: str, trade_date: str, start_date: str, end_date: str) -> list[CcassHoldingItem]:
    actual_start, actual_end = normalize_date_range(trade_date, start_date, end_date, 120)
    cache_df = read_cached_ranges(
        ["stocks", "ownership", "ccass-holdings"],
        {"code": normalize_stock_code(code)},
        "trade_date",
        actual_start,
        actual_end,
        "day",
        lambda start_value, end_value: _fetch_ccass_hold_frame(code, start_value, end_value),
    )
    filtered_df = filter_frame_by_date_range(cache_df, "trade_date", actual_start, actual_end)
    if filtered_df.empty or "trade_date" not in filtered_df.columns:
        return []
    return [
        CcassHoldingItem(
            code=str(row["code"]),
            trade_date=str(row["trade_date"]),
            participant_count=int(row["participant_count"]) if pd.notna(row["participant_count"]) else None,
            holding_volume=float(row["holding_volume"]) if pd.notna(row["holding_volume"]) else None,
            holding_ratio=float(row["holding_ratio"]) if pd.notna(row["holding_ratio"]) else None,
        )
        for _, row in filtered_df.sort_values("trade_date").iterrows()
    ]


def _fetch_ccass_detail_frame(code: str, start_value: str, end_value: str) -> pd.DataFrame:
    df = query_frame("ccass_hold_detail", ts_code=stock_code_to_ts(code), start_date=start_value, end_date=end_value)
    if df.empty:
        return df
    work = df.copy()
    work["code"] = normalize_stock_code(code)
    work["trade_date"] = work["trade_date"].astype(str)
    work["participant_id"] = work["col_participant_id"] if "col_participant_id" in work.columns else ""
    work["participant_name"] = work["col_participant_name"] if "col_participant_name" in work.columns else ""
    work["holding_volume"] = work["col_shareholding"] if "col_shareholding" in work.columns else None
    work["holding_ratio"] = work["shareholding_ratio"] if "shareholding_ratio" in work.columns else None
    return work[["code", "trade_date", "participant_id", "participant_name", "holding_volume", "holding_ratio"]]


def get_ccass_holding_details(code: str, trade_date: str, start_date: str, end_date: str) -> list[CcassHoldingDetailItem]:
    actual_start, actual_end = normalize_date_range(trade_date, start_date, end_date, 120)
    cache_df = read_cached_ranges(
        ["stocks", "ownership", "ccass-holding-details"],
        {"code": normalize_stock_code(code)},
        "trade_date",
        actual_start,
        actual_end,
        "day",
        lambda start_value, end_value: _fetch_ccass_detail_frame(code, start_value, end_value),
    )
    filtered_df = filter_frame_by_date_range(cache_df, "trade_date", actual_start, actual_end)
    if filtered_df.empty or "trade_date" not in filtered_df.columns:
        return []
    return [
        CcassHoldingDetailItem(
            code=str(row["code"]),
            trade_date=str(row["trade_date"]),
            participant_id=str(row["participant_id"]) if pd.notna(row["participant_id"]) else "",
            participant_name=str(row["participant_name"]) if pd.notna(row["participant_name"]) else "",
            holding_volume=float(row["holding_volume"]) if pd.notna(row["holding_volume"]) else None,
            holding_ratio=float(row["holding_ratio"]) if pd.notna(row["holding_ratio"]) else None,
        )
        for _, row in filtered_df.sort_values(["trade_date", "participant_id"]).iterrows()
    ]


def _fetch_hk_hold_frame(code: str, start_value: str, end_value: str) -> pd.DataFrame:
    df = query_frame("hk_hold", ts_code=stock_code_to_ts(code), start_date=start_value, end_date=end_value)
    if df.empty:
        return df
    work = df.copy()
    work["code"] = normalize_stock_code(code)
    work["trade_date"] = work["trade_date"].astype(str)
    work["holding_volume"] = work["vol"] if "vol" in work.columns else None
    work["holding_ratio"] = work["ratio"] if "ratio" in work.columns else None
    work["change_volume"] = None
    return work[["code", "trade_date", "holding_volume", "holding_ratio", "change_volume"]]


def get_hk_connect_holdings(code: str, trade_date: str, start_date: str, end_date: str) -> list[HKConnectHoldingItem]:
    actual_start, actual_end = normalize_date_range(trade_date, start_date, end_date, 120)
    cache_df = read_cached_ranges(
        ["stocks", "ownership", "hk-connect-holdings"],
        {"code": normalize_stock_code(code)},
        "trade_date",
        actual_start,
        actual_end,
        "day",
        lambda start_value, end_value: _fetch_hk_hold_frame(code, start_value, end_value),
    )
    filtered_df = filter_frame_by_date_range(cache_df, "trade_date", actual_start, actual_end)
    return [
        HKConnectHoldingItem(
            code=str(row["code"]),
            trade_date=str(row["trade_date"]),
            holding_volume=float(row["holding_volume"]) if pd.notna(row["holding_volume"]) else None,
            holding_ratio=float(row["holding_ratio"]) if pd.notna(row["holding_ratio"]) else None,
            change_volume=float(row["change_volume"]) if pd.notna(row["change_volume"]) else None,
        )
        for _, row in filtered_df.sort_values("trade_date").iterrows()
    ]


def _fetch_pledge_stat_frame(code: str, start_value: str, end_value: str) -> pd.DataFrame:
    df = query_frame("pledge_stat", ts_code=stock_code_to_ts(code), start_date=start_value, end_date=end_value)
    if df.empty:
        return df
    work = df.copy()
    work["code"] = normalize_stock_code(code)
    work["trade_date"] = work["end_date"].astype(str) if "end_date" in work.columns else work["ann_date"].astype(str)
    work["pledge_volume"] = work["pledge_count"] if "pledge_count" in work.columns else work["pledge_amount"] if "pledge_amount" in work.columns else None
    work["pledge_ratio"] = work["pledge_ratio"] if "pledge_ratio" in work.columns else None
    work["unrestricted_pledge_volume"] = work["unrest_pledge"] if "unrest_pledge" in work.columns else None
    return work[["code", "trade_date", "pledge_volume", "pledge_ratio", "unrestricted_pledge_volume"]]


def get_pledge_stats(code: str, trade_date: str, start_date: str, end_date: str) -> list[PledgeStatItem]:
    actual_start, actual_end = normalize_date_range(trade_date, start_date, end_date, 720)
    cache_df = read_cached_ranges(
        ["stocks", "ownership", "pledges-stats"],
        {"code": normalize_stock_code(code)},
        "trade_date",
        actual_start,
        actual_end,
        "day",
        lambda start_value, end_value: _fetch_pledge_stat_frame(code, start_value, end_value),
    )
    filtered_df = filter_frame_by_date_range(cache_df, "trade_date", actual_start, actual_end)
    if filtered_df.empty or "trade_date" not in filtered_df.columns:
        return []
    return [
        PledgeStatItem(
            code=str(row["code"]),
            trade_date=str(row["trade_date"]),
            pledge_volume=float(row["pledge_volume"]) if pd.notna(row["pledge_volume"]) else None,
            pledge_ratio=float(row["pledge_ratio"]) if pd.notna(row["pledge_ratio"]) else None,
            unrestricted_pledge_volume=float(row["unrestricted_pledge_volume"]) if pd.notna(row["unrestricted_pledge_volume"]) else None,
        )
        for _, row in filtered_df.sort_values("trade_date").iterrows()
    ]


def _fetch_pledge_detail_frame(code: str) -> pd.DataFrame:
    df = query_frame("pledge_detail", ts_code=stock_code_to_ts(code))
    if df.empty:
        return df
    work = df.copy()
    work["code"] = normalize_stock_code(code)
    work["holder_name"] = work["holder_name"] if "holder_name" in work.columns else ""
    work["start_date"] = work["start_date"].astype(str) if "start_date" in work.columns else ""
    work["end_date"] = work["end_date"].astype(str) if "end_date" in work.columns else ""
    work["pledge_volume"] = work["pledge_amount"] if "pledge_amount" in work.columns else None
    work["pledge_ratio"] = work["pledge_ratio"] if "pledge_ratio" in work.columns else None
    work["status"] = work["is_release"].map(lambda value: "released" if str(value) == "1" else "active") if "is_release" in work.columns else ""
    return work[["code", "holder_name", "start_date", "end_date", "pledge_volume", "pledge_ratio", "status"]]


def get_pledge_details(code: str, start_date: str, end_date: str, status: str) -> list[PledgeDetailItem]:
    actual_start, actual_end = normalize_date_range("", start_date, end_date, 720)
    cache_df = read_cached_once(["stocks", "ownership", "pledges-details"], {"code": normalize_stock_code(code)}, lambda: _fetch_pledge_detail_frame(code))
    filtered_df = filter_frame_by_date_range(cache_df, "start_date", actual_start, actual_end)
    if status:
        filtered_df = filtered_df[filtered_df["status"] == status]
    return [
        PledgeDetailItem(
            code=str(row["code"]),
            holder_name=str(row["holder_name"]) if pd.notna(row["holder_name"]) else "",
            start_date=str(row["start_date"]),
            end_date=str(row["end_date"]) if pd.notna(row["end_date"]) else "",
            pledge_volume=float(row["pledge_volume"]) if pd.notna(row["pledge_volume"]) else None,
            pledge_ratio=float(row["pledge_ratio"]) if pd.notna(row["pledge_ratio"]) else None,
            status=str(row["status"]) if pd.notna(row["status"]) else "",
        )
        for _, row in filtered_df.sort_values("start_date").iterrows()
    ]


def _fetch_holder_count_frame(code: str, start_value: str, end_value: str) -> pd.DataFrame:
    df = query_frame("stk_holdernumber", ts_code=stock_code_to_ts(code), start_date=start_value, end_date=end_value)
    if df.empty:
        return df
    work = df.copy()
    work["code"] = normalize_stock_code(code)
    work["trade_date"] = work["end_date"].astype(str) if "end_date" in work.columns else work["ann_date"].astype(str)
    work["holder_count"] = work["holder_num"] if "holder_num" in work.columns else None
    work["avg_holding"] = work["avg_hold_num"] if "avg_hold_num" in work.columns else None
    return work[["code", "trade_date", "holder_count", "avg_holding"]]


def get_shareholder_count(code: str, trade_date: str, start_date: str, end_date: str) -> list[ShareholderCountItem]:
    actual_start, actual_end = normalize_date_range(trade_date, start_date, end_date, 720)
    cache_df = read_cached_ranges(
        ["stocks", "ownership", "shareholders-count"],
        {"code": normalize_stock_code(code)},
        "trade_date",
        actual_start,
        actual_end,
        "day",
        lambda start_value, end_value: _fetch_holder_count_frame(code, start_value, end_value),
    )
    filtered_df = filter_frame_by_date_range(cache_df, "trade_date", actual_start, actual_end)
    return [
        ShareholderCountItem(
            code=str(row["code"]),
            trade_date=str(row["trade_date"]),
            holder_count=int(row["holder_count"]) if pd.notna(row["holder_count"]) else None,
            avg_holding=float(row["avg_holding"]) if pd.notna(row["avg_holding"]) else None,
        )
        for _, row in filtered_df.sort_values("trade_date").iterrows()
    ]


def _fetch_top10_frame(code: str, start_value: str, end_value: str, float_only: bool) -> pd.DataFrame:
    api_name = "top10_floatholders" if float_only else "top10_holders"
    df = query_frame(api_name, ts_code=stock_code_to_ts(code), start_date=start_value, end_date=end_value)
    if df.empty:
        return df
    work = df.copy()
    work["code"] = normalize_stock_code(code)
    work["report_period"] = work["end_date"].astype(str)
    work["rank"] = work["hold_amount"].rank(method="dense", ascending=False).astype(int) if "hold_amount" in work.columns else None
    work["shareholder_name"] = work["holder_name"] if "holder_name" in work.columns else ""
    work["holding_volume"] = work["hold_amount"] if "hold_amount" in work.columns else None
    work["holding_ratio"] = work["hold_ratio"] if "hold_ratio" in work.columns else None
    work["change_volume"] = work["hold_change"] if "hold_change" in work.columns else None
    return work[["code", "report_period", "rank", "shareholder_name", "holding_volume", "holding_ratio", "change_volume"]]


def get_shareholder_top10(code: str, report_period: str, start_period: str, end_period: str, float_only: bool) -> list[ShareholderTop10Item]:
    actual_start, actual_end = normalize_period_range(report_period, start_period, end_period)
    suffix = "top10-float" if float_only else "top10"
    cache_df = read_cached_ranges(
        ["stocks", "ownership", f"shareholders-{suffix}"],
        {"code": normalize_stock_code(code)},
        "report_period",
        actual_start,
        actual_end,
        "quarter",
        lambda start_value, end_value: _fetch_top10_frame(code, start_value, end_value, float_only),
    )
    filtered_df = filter_frame_by_date_range(cache_df, "report_period", actual_start, actual_end)
    return [
        ShareholderTop10Item(
            code=str(row["code"]),
            report_period=str(row["report_period"]),
            rank=int(row["rank"]) if pd.notna(row["rank"]) else None,
            shareholder_name=str(row["shareholder_name"]) if pd.notna(row["shareholder_name"]) else "",
            holding_volume=float(row["holding_volume"]) if pd.notna(row["holding_volume"]) else None,
            holding_ratio=float(row["holding_ratio"]) if pd.notna(row["holding_ratio"]) else None,
            change_volume=float(row["change_volume"]) if pd.notna(row["change_volume"]) else None,
        )
        for _, row in filtered_df.sort_values(["report_period", "rank"]).iterrows()
    ]


