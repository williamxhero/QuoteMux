from __future__ import annotations

from datetime import datetime
import threading

import pandas as pd

from quotemux.infra.cache.store import build_cache_path, filter_frame_by_date_range, merge_cache_frame, read_cache_frame, write_cache_frame
from platform_models import AuctionItem, BSECodeMappingItem, HKConnectTargetItem, ManagementRewardItem, NineTurnItem, RankingBrokerPickItem, RankingResearchReportItem, ResearchReportItem, StockAHComparisonItem, StockArchiveItem, StockDailyBasicItem, StockDailyMarketValueItem, StockDailyValuationItem, StockFinanceIndicatorItem, StockManagerItem, StockPremarketItem, StockProfileItem, StockRiskFlagItem, SurveyItem
from quotemux.infra.common import format_date_value, normalize_stock_code, split_csv, stock_code_to_ts
from quotemux.infra.tushare.helpers import normalize_date_range, normalize_period_range, plan_days, query_frame, read_cached_once, read_cached_ranges


_DAILY_MARKET_LOCKS: dict[str, threading.Lock] = {}
_DAILY_MARKET_LOCKS_LOCK = threading.Lock()


def _get_daily_market_lock(trade_date: str) -> threading.Lock:
    with _DAILY_MARKET_LOCKS_LOCK:
        lock = _DAILY_MARKET_LOCKS.get(trade_date)
        if lock is None:
            lock = threading.Lock()
            _DAILY_MARKET_LOCKS[trade_date] = lock
        return lock


def _codes_from_params(code: str, codes: str) -> list[str]:
    items = []
    if code:
        items.append(normalize_stock_code(code))
    items.extend(normalize_stock_code(item) for item in split_csv(codes))
    return [item for item in dict.fromkeys(items) if item]


def _fetch_stock_archive_frame(start_value: str, end_value: str) -> pd.DataFrame:
    df = query_frame("bak_basic", start_date=start_value, end_date=end_value)
    if df.empty or "trade_date" not in df.columns:
        return pd.DataFrame()
    work = df.copy()
    work["trade_date"] = work["trade_date"].astype(str)
    work["code"] = work["ts_code"].astype(str).str.split(".").str[0]
    work["exchange"] = work["ts_code"].astype(str).str.split(".").str[1].map({"SH": "SSE", "SZ": "SZSE", "BJ": "BSE"}).fillna("")
    work["market"] = work["code"].map(
        lambda value: "beijing" if value.startswith(("4", "8")) else "star_market" if value.startswith("688") else "chi_next" if value.startswith(("300", "301")) else "main_board"
    )
    work["list_status"] = "listed"
    return work[["trade_date", "code", "name", "exchange", "market", "list_status", "industry", "area"]]


def get_stock_archive(trade_date: str, code: str, name: str, industry: str, area: str, limit: int, offset: int) -> list[StockArchiveItem]:
    if not trade_date:
        return []
    cache_df = read_cached_ranges(["stocks", "catalog", "archive"], {"scope": "all"}, "trade_date", trade_date, trade_date, "day", _fetch_stock_archive_frame)
    filtered_df = filter_frame_by_date_range(cache_df, "trade_date", trade_date, trade_date)
    if code:
        filtered_df = filtered_df[filtered_df["code"] == normalize_stock_code(code)]
    if name:
        filtered_df = filtered_df[filtered_df["name"].astype(str).str.contains(name, na=False)]
    if industry:
        filtered_df = filtered_df[filtered_df["industry"].astype(str).str.contains(industry, na=False)]
    if area:
        filtered_df = filtered_df[filtered_df["area"].astype(str).str.contains(area, na=False)]
    return [
        StockArchiveItem(
            trade_date=str(row["trade_date"]),
            code=str(row["code"]),
            name=str(row["name"]),
            exchange=str(row["exchange"]),
            market=str(row["market"]),
            list_status=str(row["list_status"]),
            industry=str(row["industry"]) if pd.notna(row["industry"]) else "",
            area=str(row["area"]) if pd.notna(row["area"]) else "",
        )
        for _, row in filtered_df.sort_values(["trade_date", "code"]).iloc[offset: offset + limit].iterrows()
    ]


def _fetch_finance_indicator_frame(code: str, start_value: str, end_value: str) -> pd.DataFrame:
    df = query_frame("fina_indicator", ts_code=stock_code_to_ts(code), start_date=start_value, end_date=end_value)
    if df.empty:
        return df
    work = df.copy()
    work["code"] = normalize_stock_code(code)
    work["report_period"] = work["end_date"].astype(str)
    return work[
        ["code", "report_period", "roe", "roa", "grossprofit_margin", "netprofit_margin", "assets_turn", "current_ratio", "debt_to_assets"]
    ].rename(
        columns={"grossprofit_margin": "gross_margin", "netprofit_margin": "net_margin", "assets_turn": "asset_turnover", "debt_to_assets": "debt_to_asset"}
    )


def get_stock_finance_indicators(code: str, codes: str, report_period: str, start_period: str, end_period: str) -> list[StockFinanceIndicatorItem]:
    actual_codes = _codes_from_params(code, codes)
    actual_start, actual_end = normalize_period_range(report_period, start_period, end_period)
    items: list[StockFinanceIndicatorItem] = []
    for actual_code in actual_codes:
        cache_df = read_cached_ranges(
            ["stocks", "finance", "indicators"],
            {"code": actual_code},
            "report_period",
            actual_start,
            actual_end,
            "quarter",
            lambda start_value, end_value: _fetch_finance_indicator_frame(actual_code, start_value, end_value),
        )
        filtered_df = filter_frame_by_date_range(cache_df, "report_period", actual_start, actual_end)
        for _, row in filtered_df.sort_values("report_period").iterrows():
            items.append(
                StockFinanceIndicatorItem(
                    code=str(row["code"]),
                    report_period=str(row["report_period"]),
                    roe=float(row["roe"]) if pd.notna(row["roe"]) else None,
                    roa=float(row["roa"]) if pd.notna(row["roa"]) else None,
                    gross_margin=float(row["gross_margin"]) if pd.notna(row["gross_margin"]) else None,
                    net_margin=float(row["net_margin"]) if pd.notna(row["net_margin"]) else None,
                    asset_turnover=float(row["asset_turnover"]) if pd.notna(row["asset_turnover"]) else None,
                    current_ratio=float(row["current_ratio"]) if pd.notna(row["current_ratio"]) else None,
                    debt_to_asset=float(row["debt_to_asset"]) if pd.notna(row["debt_to_asset"]) else None,
                )
            )
    return items


def _fetch_ah_frame(start_value: str, end_value: str) -> pd.DataFrame:
    df = query_frame("stk_ah_comparison", start_date=start_value, end_date=end_value)
    if df.empty:
        return df
    work = df.copy()
    trade_column = "trade_date" if "trade_date" in work.columns else "end_date"
    work["trade_date"] = work[trade_column].astype(str)
    work["code"] = work["ts_code"].astype(str).str.split(".").str[0]
    work["h_code"] = work["h_ts_code"].astype(str).str.split(".").str[0] if "h_ts_code" in work.columns else ""
    work["premium_ratio"] = work["compare"] if "compare" in work.columns else None
    if "name" not in work.columns:
        work["name"] = ""
    if "a_close" not in work.columns:
        work["a_close"] = None
    if "h_close" not in work.columns:
        work["h_close"] = None
    return work[["code", "name", "h_code", "trade_date", "a_close", "h_close", "premium_ratio"]]


def get_stock_ah_comparisons(code: str, trade_date: str, start_date: str, end_date: str, limit: int, offset: int) -> list[StockAHComparisonItem]:
    actual_start, actual_end = normalize_date_range(trade_date, start_date, end_date)
    cache_df = read_cached_ranges(["stocks", "indicators", "ah-comparisons"], {"scope": "all"}, "trade_date", actual_start, actual_end, "day", _fetch_ah_frame)
    filtered_df = filter_frame_by_date_range(cache_df, "trade_date", actual_start, actual_end)
    if filtered_df.empty or "trade_date" not in filtered_df.columns:
        return []
    if code:
        filtered_df = filtered_df[filtered_df["code"] == normalize_stock_code(code)]
    return [
        StockAHComparisonItem(
            code=str(row["code"]),
            name=str(row["name"]) if pd.notna(row["name"]) else "",
            h_code=str(row["h_code"]) if pd.notna(row["h_code"]) else "",
            trade_date=str(row["trade_date"]),
            a_close=float(row["a_close"]) if pd.notna(row["a_close"]) else None,
            h_close=float(row["h_close"]) if pd.notna(row["h_close"]) else None,
            premium_ratio=float(row["premium_ratio"]) if pd.notna(row["premium_ratio"]) else None,
        )
        for _, row in filtered_df.sort_values(["trade_date", "code"]).iloc[offset: offset + limit].iterrows()
    ]


def _fetch_daily_basic_frame(code: str, start_value: str, end_value: str) -> pd.DataFrame:
    df = query_frame("daily_basic", ts_code=stock_code_to_ts(code), start_date=start_value, end_date=end_value)
    if df.empty:
        return df
    work = df.copy()
    work["code"] = normalize_stock_code(code)
    work["trade_date"] = work["trade_date"].map(format_date_value)
    for column in ("turnover_rate", "volume_ratio", "pe", "pb", "ps", "pcf", "dv_ratio", "total_share", "float_share", "total_mv", "circ_mv"):
        if column not in work.columns:
            work[column] = None
    return work[["code", "trade_date", "turnover_rate", "volume_ratio", "pe", "pb", "ps", "pcf", "dv_ratio", "total_share", "float_share", "total_mv", "circ_mv"]]


def _fetch_daily_basic_market_frame(start_value: str, end_value: str) -> pd.DataFrame:
    del end_value
    trade_date = format_date_value(start_value).replace("-", "")
    df = query_frame("daily_basic", trade_date=trade_date)
    if df.empty:
        return df
    work = df.copy()
    work["code"] = work["ts_code"].astype(str).str.split(".").str[0]
    work["trade_date"] = work["trade_date"].map(format_date_value)
    for column in ("turnover_rate", "volume_ratio", "pe", "pb", "ps", "pcf", "dv_ratio", "total_share", "float_share", "total_mv", "circ_mv"):
        if column not in work.columns:
            work[column] = None
    return work[["code", "trade_date", "turnover_rate", "volume_ratio", "pe", "pb", "ps", "pcf", "dv_ratio", "total_share", "float_share", "total_mv", "circ_mv"]]


def _build_daily_frames(code: str, trade_date: str, start_date: str, end_date: str) -> pd.DataFrame:
    actual_start, actual_end = normalize_date_range(trade_date, start_date, end_date)
    return read_cached_ranges(
        ["stocks", "indicators", "daily-basic"],
        {"code": normalize_stock_code(code)},
        "trade_date",
        actual_start,
        actual_end,
        "day",
        lambda start_value, end_value: _fetch_daily_basic_frame(code, start_value, end_value),
    )


def _build_daily_market_frames(trade_date: str) -> pd.DataFrame:
    actual_start, actual_end = normalize_date_range(trade_date, "", "")
    lock = _get_daily_market_lock(actual_start)
    with lock:
        cache_path = build_cache_path("tushare", ["stocks", "indicators", "daily-basic", "market"], {"trade_date": actual_start.replace("-", "")})
        cache_df = read_cache_frame(cache_path)
        filtered_df = filter_frame_by_date_range(cache_df, "trade_date", actual_start, actual_end)
        if not filtered_df.empty:
            return cache_df
        fetched_df = _fetch_daily_basic_market_frame(actual_start, actual_end)
        if fetched_df.empty:
            return cache_df
        merged_df = merge_cache_frame(cache_df, fetched_df, ["code", "trade_date"], ["trade_date", "code"])
        write_cache_frame(cache_path, merged_df)
        return merged_df


def get_stock_daily_basic(code: str, codes: str, trade_date: str, start_date: str, end_date: str) -> list[StockDailyBasicItem]:
    actual_codes = _codes_from_params(code, codes)
    actual_start, actual_end = normalize_date_range(trade_date, start_date, end_date)
    items: list[StockDailyBasicItem] = []
    if actual_codes == []:
        if actual_start != actual_end:
            return []
        filtered_df = filter_frame_by_date_range(_build_daily_market_frames(actual_start), "trade_date", actual_start, actual_end)
        if filtered_df.empty or "trade_date" not in filtered_df.columns or "code" not in filtered_df.columns:
            return []
        filtered_df = filtered_df.copy()
        filtered_df["trade_date"] = filtered_df["trade_date"].map(format_date_value)
        for _, row in filtered_df.sort_values(["trade_date", "code"]).iterrows():
            items.append(
                StockDailyBasicItem(
                    code=str(row["code"]),
                    trade_date=str(row["trade_date"]),
                    turnover_rate=float(row["turnover_rate"]) if pd.notna(row["turnover_rate"]) else None,
                    volume_ratio=float(row["volume_ratio"]) if pd.notna(row["volume_ratio"]) else None,
                    pe=float(row["pe"]) if pd.notna(row["pe"]) else None,
                    pb=float(row["pb"]) if pd.notna(row["pb"]) else None,
                    total_share=float(row["total_share"]) if pd.notna(row["total_share"]) else None,
                    float_share=float(row["float_share"]) if pd.notna(row["float_share"]) else None,
                )
            )
        return items
    for actual_code in actual_codes:
        filtered_df = filter_frame_by_date_range(_build_daily_frames(actual_code, trade_date, start_date, end_date), "trade_date", actual_start, actual_end)
        if filtered_df.empty or "trade_date" not in filtered_df.columns or "code" not in filtered_df.columns:
            continue
        filtered_df = filtered_df.copy()
        filtered_df["trade_date"] = filtered_df["trade_date"].map(format_date_value)
        for _, row in filtered_df.sort_values("trade_date").iterrows():
            items.append(
                StockDailyBasicItem(
                    code=str(row["code"]),
                    trade_date=str(row["trade_date"]),
                    turnover_rate=float(row["turnover_rate"]) if pd.notna(row["turnover_rate"]) else None,
                    volume_ratio=float(row["volume_ratio"]) if pd.notna(row["volume_ratio"]) else None,
                    pe=float(row["pe"]) if pd.notna(row["pe"]) else None,
                    pb=float(row["pb"]) if pd.notna(row["pb"]) else None,
                    total_share=float(row["total_share"]) if pd.notna(row["total_share"]) else None,
                    float_share=float(row["float_share"]) if pd.notna(row["float_share"]) else None,
                )
            )
    return items


def get_stock_daily_valuation(code: str, codes: str, trade_date: str, start_date: str, end_date: str) -> list[StockDailyValuationItem]:
    actual_codes = _codes_from_params(code, codes)
    actual_start, actual_end = normalize_date_range(trade_date, start_date, end_date)
    items: list[StockDailyValuationItem] = []
    if actual_codes == []:
        if actual_start != actual_end:
            return []
        filtered_df = filter_frame_by_date_range(_build_daily_market_frames(actual_start), "trade_date", actual_start, actual_end)
        if filtered_df.empty or "trade_date" not in filtered_df.columns or "code" not in filtered_df.columns:
            return []
        filtered_df = filtered_df.copy()
        filtered_df["trade_date"] = filtered_df["trade_date"].map(format_date_value)
        for _, row in filtered_df.sort_values(["trade_date", "code"]).iterrows():
            items.append(
                StockDailyValuationItem(
                    code=str(row["code"]),
                    trade_date=str(row["trade_date"]),
                    pe=float(row["pe"]) if pd.notna(row["pe"]) else None,
                    pb=float(row["pb"]) if pd.notna(row["pb"]) else None,
                    ps=float(row["ps"]) if pd.notna(row["ps"]) else None,
                    pcf=float(row["pcf"]) if pd.notna(row["pcf"]) else None,
                    dv_ratio=float(row["dv_ratio"]) if pd.notna(row["dv_ratio"]) else None,
                )
            )
        return items
    for actual_code in actual_codes:
        filtered_df = filter_frame_by_date_range(_build_daily_frames(actual_code, trade_date, start_date, end_date), "trade_date", actual_start, actual_end)
        if filtered_df.empty or "trade_date" not in filtered_df.columns or "code" not in filtered_df.columns:
            continue
        filtered_df = filtered_df.copy()
        filtered_df["trade_date"] = filtered_df["trade_date"].map(format_date_value)
        for _, row in filtered_df.sort_values("trade_date").iterrows():
            items.append(
                StockDailyValuationItem(
                    code=str(row["code"]),
                    trade_date=str(row["trade_date"]),
                    pe=float(row["pe"]) if pd.notna(row["pe"]) else None,
                    pb=float(row["pb"]) if pd.notna(row["pb"]) else None,
                    ps=float(row["ps"]) if pd.notna(row["ps"]) else None,
                    pcf=float(row["pcf"]) if pd.notna(row["pcf"]) else None,
                    dv_ratio=float(row["dv_ratio"]) if pd.notna(row["dv_ratio"]) else None,
                )
            )
    return items


def get_stock_daily_market_value(code: str, codes: str, trade_date: str, start_date: str, end_date: str) -> list[StockDailyMarketValueItem]:
    actual_codes = _codes_from_params(code, codes)
    actual_start, actual_end = normalize_date_range(trade_date, start_date, end_date)
    items: list[StockDailyMarketValueItem] = []
    if actual_codes == []:
        if actual_start != actual_end:
            return []
        filtered_df = filter_frame_by_date_range(_build_daily_market_frames(actual_start), "trade_date", actual_start, actual_end)
        if filtered_df.empty or "trade_date" not in filtered_df.columns or "code" not in filtered_df.columns:
            return []
        filtered_df = filtered_df.copy()
        filtered_df["trade_date"] = filtered_df["trade_date"].map(format_date_value)
        for _, row in filtered_df.sort_values(["trade_date", "code"]).iterrows():
            items.append(
                StockDailyMarketValueItem(
                    code=str(row["code"]),
                    trade_date=str(row["trade_date"]),
                    total_mv=float(row["total_mv"]) if pd.notna(row["total_mv"]) else None,
                    float_mv=float(row["circ_mv"]) if pd.notna(row["circ_mv"]) else None,
                    free_mv=None,
                )
            )
        return items
    for actual_code in actual_codes:
        filtered_df = filter_frame_by_date_range(_build_daily_frames(actual_code, trade_date, start_date, end_date), "trade_date", actual_start, actual_end)
        if filtered_df.empty or "trade_date" not in filtered_df.columns or "code" not in filtered_df.columns:
            continue
        filtered_df = filtered_df.copy()
        filtered_df["trade_date"] = filtered_df["trade_date"].map(format_date_value)
        for _, row in filtered_df.sort_values("trade_date").iterrows():
            items.append(
                StockDailyMarketValueItem(
                    code=str(row["code"]),
                    trade_date=str(row["trade_date"]),
                    total_mv=float(row["total_mv"]) if pd.notna(row["total_mv"]) else None,
                    float_mv=float(row["circ_mv"]) if pd.notna(row["circ_mv"]) else None,
                    free_mv=None,
                )
            )
    return items


def _fetch_risk_flag_frame(start_value: str, end_value: str) -> pd.DataFrame:
    df = query_frame("stock_st", start_date=start_value, end_date=end_value)
    if df.empty:
        return df
    work = df.copy()
    work["code"] = work["ts_code"].astype(str).str.split(".").str[0]
    work["name"] = work["name"] if "name" in work.columns else ""
    work["flag_type"] = "st"
    work["start_date"] = work["start_date"].astype(str) if "start_date" in work.columns else ""
    work["end_date"] = work["end_date"].astype(str) if "end_date" in work.columns else ""
    work["status"] = "active"
    return work[["code", "name", "flag_type", "start_date", "end_date", "status"]]


def get_stock_risk_flags(trade_date: str, start_date: str, end_date: str, flag_type: str, status: str, limit: int, offset: int) -> list[StockRiskFlagItem]:
    actual_start, actual_end = normalize_date_range(trade_date, start_date, end_date, 180)
    cache_df = read_cached_ranges(["stocks", "indicators", "risk-flags"], {"scope": "all"}, "start_date", actual_start, actual_end, "day", _fetch_risk_flag_frame)
    filtered_df = filter_frame_by_date_range(cache_df, "start_date", actual_start, actual_end)
    if flag_type:
        filtered_df = filtered_df[filtered_df["flag_type"] == flag_type]
    if status:
        filtered_df = filtered_df[filtered_df["status"] == status]
    return [
        StockRiskFlagItem(
            code=str(row["code"]),
            name=str(row["name"]) if pd.notna(row["name"]) else "",
            flag_type=str(row["flag_type"]),
            start_date=str(row["start_date"]),
            end_date=str(row["end_date"]) if pd.notna(row["end_date"]) else "",
            status=str(row["status"]),
        )
        for _, row in filtered_df.sort_values(["start_date", "code"]).iloc[offset: offset + limit].iterrows()
    ]


def get_bse_code_mappings(old_code: str, new_code: str, status: str) -> list[BSECodeMappingItem]:
    cache_df = read_cached_once(["stocks", "reference", "bse-code-mappings"], {"scope": "all"}, lambda: query_frame("bse_mapping"))
    if cache_df.empty:
        return []
    work = cache_df.copy()
    work["old_code"] = work["o_code"].astype(str).str.split(".").str[0] if "o_code" in work.columns else ""
    work["new_code"] = work["ts_code"].astype(str).str.split(".").str[0] if "ts_code" in work.columns else ""
    work["effective_date"] = work["list_date"].astype(str) if "list_date" in work.columns else ""
    work["status"] = "active"
    if old_code:
        work = work[work["old_code"] == normalize_stock_code(old_code)]
    if new_code:
        work = work[work["new_code"] == normalize_stock_code(new_code)]
    if status:
        work = work[work["status"] == status]
    return [BSECodeMappingItem(old_code=str(row["old_code"]), new_code=str(row["new_code"]), effective_date=str(row["effective_date"]), status=str(row["status"])) for _, row in work.iterrows()]


def get_hk_connect_targets(direction: str, status: str, effective_date: str) -> list[HKConnectTargetItem]:
    actual_date = effective_date or datetime.now().strftime("%Y%m%d")
    type_values = ["HK_SH", "HK_SZ", "SH_HK", "SZ_HK"]
    if direction == "northbound":
        type_values = ["HK_SH", "HK_SZ"]
    elif direction == "southbound":
        type_values = ["SH_HK", "SZ_HK"]
    items: list[HKConnectTargetItem] = []
    for type_value in type_values:
        cache_df = read_cached_ranges(
            ["stocks", "reference", "hk-connect-targets"],
            {"type": type_value},
            "trade_date",
            actual_date,
            actual_date,
            "day",
            lambda start_value, end_value: query_frame("stock_hsgt", trade_date=start_value, type=type_value),
        )
        filtered_df = filter_frame_by_date_range(cache_df, "trade_date", actual_date, actual_date)
        for _, row in filtered_df.iterrows():
            item = HKConnectTargetItem(
                code=str(row["ts_code"]).split(".")[0],
                name=str(row["name"]) if "name" in filtered_df.columns and pd.notna(row["name"]) else "",
                direction="northbound" if type_value in {"HK_SH", "HK_SZ"} else "southbound",
                status="active",
                effective_date=str(row["trade_date"]),
            )
            items.append(item)
    if status:
        items = [item for item in items if item.status == status]
    return items


def get_company_profile(code: str) -> StockProfileItem | None:
    cache_df = read_cached_once(["stocks", "profile"], {"code": normalize_stock_code(code)}, lambda: query_frame("stock_company", ts_code=stock_code_to_ts(code)))
    if cache_df.empty:
        return None
    row = cache_df.iloc[0]
    return StockProfileItem(
        code=normalize_stock_code(code),
        company_name=str(row["com_name"]) if "com_name" in cache_df.columns and pd.notna(row["com_name"]) else "",
        full_name=str(row["fullname"]) if "fullname" in cache_df.columns and pd.notna(row["fullname"]) else "",
        chairman=str(row["chairman"]) if "chairman" in cache_df.columns and pd.notna(row["chairman"]) else "",
        manager=str(row["manager"]) if "manager" in cache_df.columns and pd.notna(row["manager"]) else "",
        website=str(row["website"]) if "website" in cache_df.columns and pd.notna(row["website"]) else "",
        employee_count=int(row["employees"]) if "employees" in cache_df.columns and pd.notna(row["employees"]) else None,
        main_business=str(row["main_business"]) if "main_business" in cache_df.columns and pd.notna(row["main_business"]) else "",
        office=str(row["office"]) if "office" in cache_df.columns and pd.notna(row["office"]) else "",
    )


def get_managers(code: str) -> list[StockManagerItem]:
    cache_df = read_cached_once(["stocks", "profile", "managers"], {"code": normalize_stock_code(code)}, lambda: query_frame("stk_managers", ts_code=stock_code_to_ts(code)))
    if cache_df.empty:
        return []
    return [
        StockManagerItem(
            code=normalize_stock_code(code),
            name=str(row["name"]) if "name" in cache_df.columns and pd.notna(row["name"]) else "",
            title=str(row["title"]) if "title" in cache_df.columns and pd.notna(row["title"]) else "",
            gender=str(row["gender"]) if "gender" in cache_df.columns and pd.notna(row["gender"]) else "",
            education=str(row["edu"]) if "edu" in cache_df.columns and pd.notna(row["edu"]) else "",
            begin_date=str(row["begin_date"]) if "begin_date" in cache_df.columns and pd.notna(row["begin_date"]) else "",
            end_date=str(row["end_date"]) if "end_date" in cache_df.columns and pd.notna(row["end_date"]) else "",
        )
        for _, row in cache_df.iterrows()
    ]


def get_management_rewards(code: str, start_date: str, end_date: str) -> list[ManagementRewardItem]:
    cache_df = read_cached_once(["stocks", "profile", "management-rewards"], {"code": normalize_stock_code(code)}, lambda: query_frame("stk_rewards", ts_code=stock_code_to_ts(code)))
    if cache_df.empty:
        return []
    actual_start, actual_end = normalize_date_range("", start_date, end_date, 720)
    filtered_df = filter_frame_by_date_range(cache_df, "ann_date", actual_start, actual_end)
    return [
        ManagementRewardItem(
            code=normalize_stock_code(code),
            ann_date=str(row["ann_date"]) if "ann_date" in filtered_df.columns and pd.notna(row["ann_date"]) else "",
            name=str(row["name"]) if "name" in filtered_df.columns and pd.notna(row["name"]) else "",
            title=str(row["title"]) if "title" in filtered_df.columns and pd.notna(row["title"]) else "",
            reward_amount=float(row["reward"]) if "reward" in filtered_df.columns and pd.notna(row["reward"]) else None,
            hold_amount=float(row["hold_vol"]) if "hold_vol" in filtered_df.columns and pd.notna(row["hold_vol"]) else None,
        )
        for _, row in filtered_df.sort_values("ann_date").iterrows()
    ]


def _fetch_report_frame(start_value: str, end_value: str, code: str) -> pd.DataFrame:
    kwargs: dict[str, object] = {"start_date": start_value, "end_date": end_value}
    if code:
        kwargs["ts_code"] = stock_code_to_ts(code)
    df = query_frame("report_rc", **kwargs)
    if df.empty:
        return df
    work = df.copy()
    work["code"] = work["ts_code"].astype(str).str.split(".").str[0] if "ts_code" in work.columns else normalize_stock_code(code)
    work["report_date"] = work["report_date"].astype(str) if "report_date" in work.columns else ""
    work["institution"] = work["org_name"] if "org_name" in work.columns else ""
    work["analyst"] = work["author_name"] if "author_name" in work.columns else ""
    if "target_price" not in work.columns:
        work["target_price"] = None
    if "title" not in work.columns:
        work["title"] = ""
    return work[["code", "report_date", "institution", "analyst", "rating", "target_price", "title"]]


def get_research_reports(code: str, report_date: str, start_date: str, end_date: str) -> list[ResearchReportItem]:
    actual_start, actual_end = normalize_date_range(report_date, start_date, end_date, 365)
    cache_df = read_cached_ranges(
        ["stocks", "research", "reports"],
        {"code": normalize_stock_code(code) or "all"},
        "report_date",
        actual_start,
        actual_end,
        "day",
        lambda start_value, end_value: _fetch_report_frame(start_value, end_value, code),
    )
    filtered_df = filter_frame_by_date_range(cache_df, "report_date", actual_start, actual_end)
    if filtered_df.empty or "report_date" not in filtered_df.columns:
        return []
    if code:
        filtered_df = filtered_df[filtered_df["code"] == normalize_stock_code(code)]
    return [
        ResearchReportItem(
            code=str(row["code"]),
            report_date=str(row["report_date"]),
            institution=str(row["institution"]) if pd.notna(row["institution"]) else "",
            analyst=str(row["analyst"]) if pd.notna(row["analyst"]) else "",
            rating=str(row["rating"]) if pd.notna(row["rating"]) else "",
            target_price=float(row["target_price"]) if pd.notna(row["target_price"]) else None,
            title=str(row["title"]) if pd.notna(row["title"]) else "",
        )
        for _, row in filtered_df.sort_values(["report_date", "code"]).iterrows()
    ]


def get_rank_research_reports(trade_date: str, start_date: str, end_date: str, limit: int) -> list[RankingResearchReportItem]:
    return [
        RankingResearchReportItem(
            trade_date=item.report_date,
            code=item.code,
            name="",
            institution=item.institution,
            rating=item.rating,
            target_price=item.target_price,
            title=item.title,
        )
        for item in get_research_reports("", trade_date, start_date, end_date)[:limit]
    ]


def get_rank_broker_monthly_picks(trade_month: str, limit: int) -> list[RankingBrokerPickItem]:
    actual_month = trade_month.replace("-", "") if trade_month else datetime.now().strftime("%Y%m")
    cache_df = read_cached_once(["rankings", "research", "broker-monthly-picks"], {"trade_month": actual_month}, lambda: query_frame("broker_recommend", month=actual_month))
    if cache_df.empty:
        return []
    items: list[RankingBrokerPickItem] = []
    for index, (_, row) in enumerate(cache_df.sort_values("name").head(limit).iterrows(), start=1):
        items.append(
            RankingBrokerPickItem(
                trade_month=actual_month,
                code=str(row["ts_code"]).split(".")[0] if "ts_code" in cache_df.columns and pd.notna(row["ts_code"]) else "",
                name=str(row["name"]) if "name" in cache_df.columns and pd.notna(row["name"]) else "",
                institution=str(row["broker"]) if "broker" in cache_df.columns and pd.notna(row["broker"]) else "",
                rank=index,
                recommend_count=int(row["recommend_count"]) if "recommend_count" in cache_df.columns and pd.notna(row["recommend_count"]) else None,
                rating=str(row["rating"]) if "rating" in cache_df.columns and pd.notna(row["rating"]) else "",
            )
        )
    return items


def _fetch_survey_frame(code: str, start_value: str, end_value: str) -> pd.DataFrame:
    df = query_frame("stk_surv", ts_code=stock_code_to_ts(code), start_date=start_value, end_date=end_value)
    if df.empty:
        return df
    work = df.copy()
    work["code"] = normalize_stock_code(code)
    work["survey_date"] = work["surv_date"].astype(str) if "surv_date" in work.columns else work["trade_date"].astype(str) if "trade_date" in work.columns else ""
    work["org_name"] = work["rece_org"] if "rece_org" in work.columns else ""
    work["survey_method"] = work["rece_mode"] if "rece_mode" in work.columns else ""
    work["topic"] = work["survey_content"] if "survey_content" in work.columns else ""
    work["announcement_date"] = work["ann_date"] if "ann_date" in work.columns else ""
    return work[["code", "survey_date", "org_name", "survey_method", "topic", "announcement_date"]]


def get_surveys(code: str, survey_date: str, start_date: str, end_date: str) -> list[SurveyItem]:
    actual_start, actual_end = normalize_date_range(survey_date, start_date, end_date, 365)
    cache_df = read_cached_ranges(
        ["stocks", "research", "surveys"],
        {"code": normalize_stock_code(code)},
        "survey_date",
        actual_start,
        actual_end,
        "day",
        lambda start_value, end_value: _fetch_survey_frame(code, start_value, end_value),
    )
    filtered_df = filter_frame_by_date_range(cache_df, "survey_date", actual_start, actual_end)
    if filtered_df.empty or "survey_date" not in filtered_df.columns:
        return []
    return [
        SurveyItem(
            code=str(row["code"]),
            survey_date=str(row["survey_date"]),
            org_name=str(row["org_name"]) if pd.notna(row["org_name"]) else "",
            survey_method=str(row["survey_method"]) if pd.notna(row["survey_method"]) else "",
            topic=str(row["topic"]) if pd.notna(row["topic"]) else "",
            announcement_date=str(row["announcement_date"]) if pd.notna(row["announcement_date"]) else "",
        )
        for _, row in filtered_df.sort_values("survey_date").iterrows()
    ]


def _fetch_nine_turn_frame(code: str, freq: str, start_value: str, end_value: str) -> pd.DataFrame:
    df = query_frame("stk_nineturn", ts_code=stock_code_to_ts(code), freq=freq, start_date=start_value, end_date=end_value)
    if df.empty:
        return df
    work = df.copy()
    work["code"] = normalize_stock_code(code)
    time_column = "trade_time" if "trade_time" in work.columns else "trade_date"
    work["trade_time"] = work[time_column].astype(str)
    work["freq"] = freq
    work["setup_index"] = work["up_count"] if "up_count" in work.columns else work["down_count"] if "down_count" in work.columns else None
    work["countdown_index"] = work["nine_up_turn"] if "nine_up_turn" in work.columns else work["nine_down_turn"] if "nine_down_turn" in work.columns else None
    work["signal"] = work.apply(
        lambda row: "nine_up" if str(row.get("nine_up_turn", "")) in {"1", "True", "true"} else "nine_down" if str(row.get("nine_down_turn", "")) in {"1", "True", "true"} else "",
        axis=1,
    )
    return work[["code", "trade_time", "freq", "setup_index", "countdown_index", "signal"]]


def get_nine_turn(code: str, freq: str, trade_date: str, start_date: str, end_date: str) -> list[NineTurnItem]:
    actual_freq = freq or "daily"
    actual_start, actual_end = normalize_date_range(trade_date, start_date, end_date, 120)
    cache_df = read_cached_ranges(
        ["stocks", "signals", "nine-turn"],
        {"code": normalize_stock_code(code), "freq": actual_freq},
        "trade_time",
        actual_start,
        actual_end,
        "day",
        lambda start_value, end_value: _fetch_nine_turn_frame(code, actual_freq, start_value, end_value),
    )
    filtered_df = filter_frame_by_date_range(cache_df, "trade_time", actual_start, actual_end)
    return [
        NineTurnItem(
            code=str(row["code"]),
            trade_time=str(row["trade_time"]),
            freq=str(row["freq"]),
            setup_index=int(row["setup_index"]) if pd.notna(row["setup_index"]) else None,
            countdown_index=int(row["countdown_index"]) if pd.notna(row["countdown_index"]) else None,
            signal=str(row["signal"]) if pd.notna(row["signal"]) else "",
        )
        for _, row in filtered_df.sort_values("trade_time").iterrows()
    ]


def _fetch_premarket_frame(start_value: str, end_value: str) -> pd.DataFrame:
    df = query_frame("stk_premarket", start_date=start_value, end_date=end_value)
    if df.empty:
        return df
    work = df.copy()
    if "ts_code" not in work.columns:
        return pd.DataFrame()
    work["code"] = work["ts_code"].astype(str).str.split(".").str[0]
    work["trade_date"] = work["trade_date"].astype(str)
    work["limit_up"] = work["up_limit"] if "up_limit" in work.columns else None
    work["limit_down"] = work["down_limit"] if "down_limit" in work.columns else None
    return work[["code", "trade_date", "total_share", "float_share", "limit_up", "limit_down"]]


def get_premarket(code: str, trade_date: str, start_date: str, end_date: str) -> list[StockPremarketItem]:
    actual_start, actual_end = normalize_date_range(trade_date, start_date, end_date, 30)
    cache_df = read_cached_ranges(["stocks", "indicators", "premarket"], {"scope": "all"}, "trade_date", actual_start, actual_end, "day", _fetch_premarket_frame)
    filtered_df = filter_frame_by_date_range(cache_df, "trade_date", actual_start, actual_end)
    if filtered_df.empty or "code" not in filtered_df.columns:
        return []
    filtered_df = filtered_df[filtered_df["code"] == normalize_stock_code(code)]
    return [
        StockPremarketItem(
            code=str(row["code"]),
            trade_date=str(row["trade_date"]),
            total_share=float(row["total_share"]) if pd.notna(row["total_share"]) else None,
            float_share=float(row["float_share"]) if pd.notna(row["float_share"]) else None,
            limit_up=float(row["limit_up"]) if pd.notna(row["limit_up"]) else None,
            limit_down=float(row["limit_down"]) if pd.notna(row["limit_down"]) else None,
        )
        for _, row in filtered_df.sort_values("trade_date").iterrows()
    ]


def _fetch_auction_day(code: str, session: str, trade_date: str) -> pd.DataFrame:
    api_name = "stk_auction_o" if session == "open" else "stk_auction_c"
    df = query_frame(api_name, trade_date=trade_date)
    if df.empty:
        return df
    work = df.copy()
    work["code"] = work["ts_code"].astype(str).str.split(".").str[0] if "ts_code" in work.columns else work["code"].astype(str)
    work = work[work["code"] == normalize_stock_code(code)]
    if work.empty:
        return work
    work["trade_date"] = trade_date
    work["auction_time"] = work["trade_time"] if "trade_time" in work.columns else ""
    work["price"] = work["price"] if "price" in work.columns else work["match_price"] if "match_price" in work.columns else None
    work["volume"] = work["vol"] if "vol" in work.columns else work["volume"] if "volume" in work.columns else None
    work["session"] = session
    return work[["code", "trade_date", "auction_time", "price", "volume", "amount", "session"]]


def get_auctions(code: str, session: str, trade_date: str, start_date: str, end_date: str) -> list[AuctionItem]:
    actual_session = session or "open"
    actual_start, actual_end = normalize_date_range(trade_date, start_date, end_date, 30)
    cache_df = read_cached_ranges(
        ["stocks", "quotes", "auctions"],
        {"code": normalize_stock_code(code), "session": actual_session},
        "trade_date",
        actual_start,
        actual_end,
        "day",
        lambda start_value, end_value: pd.concat([_fetch_auction_day(code, actual_session, day) for day in plan_days(start_value, end_value)], ignore_index=True) if plan_days(start_value, end_value) else pd.DataFrame(),
    )
    filtered_df = filter_frame_by_date_range(cache_df, "trade_date", actual_start, actual_end)
    if filtered_df.empty or "trade_date" not in filtered_df.columns:
        return []
    return [
        AuctionItem(
            code=str(row["code"]),
            trade_date=str(row["trade_date"]),
            auction_time=str(row["auction_time"]) if pd.notna(row["auction_time"]) else "",
            price=float(row["price"]) if pd.notna(row["price"]) else None,
            volume=float(row["volume"]) if pd.notna(row["volume"]) else None,
            amount=float(row["amount"]) if pd.notna(row["amount"]) else None,
            session=str(row["session"]) if pd.notna(row["session"]) else "",
        )
        for _, row in filtered_df.sort_values(["trade_date", "auction_time"]).iterrows()
    ]


